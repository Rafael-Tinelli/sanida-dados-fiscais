(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA) return;

  const ZERO = SFA.Decimal.parse('0');
  const CONTEXT = 'thirteenth';
  const TECHNICAL_CONTEXT = 'technical';

  function fail(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function dateParts(value, name) {
    const iso = SFA.normalizeDate(value, name || 'date');
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
    if (!match) fail('thirteenth_date', 'Data inválida.');
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const probe = new Date(Date.UTC(year, month - 1, day));
    if (probe.getUTCFullYear() !== year || probe.getUTCMonth() + 1 !== month || probe.getUTCDate() !== day) {
      fail('thirteenth_date', 'Data civil inválida em ' + (name || 'date') + '.');
    }
    return Object.freeze({ iso, year, month, day });
  }

  function isoDate(year, month, day) {
    return String(year).padStart(4, '0') + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');
  }

  function daysInMonth(year, month) {
    return new Date(Date.UTC(year, month, 0)).getUTCDate();
  }

  function daysInclusive(startIso, endIso) {
    const a = dateParts(startIso, 'period_start');
    const b = dateParts(endIso, 'period_end');
    const delta = Date.UTC(b.year, b.month - 1, b.day) - Date.UTC(a.year, a.month - 1, a.day);
    return Math.floor(delta / 86400000) + 1;
  }

  function ruleAudit(selected) {
    return SFA.ruleAudit(selected);
  }

  function selectedAudit(release, rule, context, targetDate) {
    return Object.freeze({
      release_id: release.release_id,
      rule_id: rule.rule_id,
      rule_version: rule.rule_version,
      context,
      target_date: targetDate,
      effective_from: rule.vigency.effective_from,
      effective_until: rule.vigency.effective_until || null
    });
  }

  function covers(rule, targetDate) {
    return targetDate >= rule.vigency.effective_from && (!rule.vigency.effective_until || targetDate <= rule.vigency.effective_until);
  }

  function selectDependencyRule(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const parent = SFA.selectRule(release, {
      ruleId: req.dependencyOf,
      consumer: req.consumer,
      context: req.context,
      targetDate
    });
    if (!Array.isArray(parent.rule.dependencies) || !parent.rule.dependencies.includes(req.ruleId)) {
      fail('dependency_not_declared', req.ruleId + ' não é dependência declarada de ' + req.dependencyOf + '.');
    }
    const matches = release.rules.filter(function (rule) {
      return rule.rule_id === req.ruleId && rule.contexts.includes(req.context) && covers(rule, targetDate);
    });
    if (matches.length !== 1) {
      fail('dependency_rule_selection', 'Esperada exatamente uma dependência vigente para ' + req.ruleId + '; encontradas ' + matches.length + '.');
    }
    const rule = matches[0];
    if (!rule.quality || rule.quality.status !== 'VALIDATED') fail('dependency_rule_quality', 'Dependência não VALIDATED: ' + req.ruleId);
    return Object.freeze({
      release,
      rule,
      consumer: req.consumer,
      context: req.context,
      target_date: targetDate,
      dependency_of: req.dependencyOf,
      audit: selectedAudit(release, rule, req.context, targetDate)
    });
  }

  function roundingPolicy(selected) {
    const policy = selected && selected.rule && selected.rule.rounding_policy;
    if (!policy || !Number.isInteger(policy.decimal_places) || typeof policy.mode !== 'string' || policy.stage !== 'per_component') {
      fail('thirteenth_rounding', 'Regra ' + (selected && selected.rule ? selected.rule.rule_id : '?') + ' não possui rounding_policy executável para quantização monetária por componente.');
    }
    return policy;
  }

  function selectMoneyRounding(release, consumer, targetDate) {
    const selected = SFA.selectRule(release, {
      ruleId: 'technical.money_decimal_and_rounding',
      consumer,
      context: TECHNICAL_CONTEXT,
      targetDate
    });
    const payload = selected.rule.payload;
    if (!payload || payload.type !== 'policy' || payload.policy_kind !== 'money_decimal_and_rounding') {
      fail('thirteenth_rounding', 'Regra técnica de arredondamento possui payload incompatível.');
    }
    roundingPolicy(selected);
    return selected;
  }

  function pow10BigInt(exp) {
    if (!Number.isInteger(exp) || exp < 0) fail('thirteenth_scale', 'Escala decimal inválida.');
    return 10n ** BigInt(exp);
  }

  function decimalFromScaledInteger(value, places) {
    const negative = value < 0n;
    let digits = (negative ? -value : value).toString();
    if (places === 0) return SFA.Decimal.parse((negative ? '-' : '') + digits);
    if (digits.length <= places) digits = '0'.repeat(places - digits.length + 1) + digits;
    const split = digits.length - places;
    return SFA.Decimal.parse((negative ? '-' : '') + digits.slice(0, split) + '.' + digits.slice(split));
  }

  function roundFraction(value, numerator, denominator, policy) {
    const decimal = SFA.Decimal.parse(value, 'fraction_value');
    if (!Number.isSafeInteger(numerator) || numerator < 0 || !Number.isSafeInteger(denominator) || denominator <= 0) {
      fail('thirteenth_fraction', 'Fração inteira inválida.');
    }
    const places = policy.decimal_places;
    let num = decimal.coefficient * BigInt(numerator);
    let den = BigInt(denominator);
    const exponent = places - decimal.scale;
    if (exponent >= 0) num *= pow10BigInt(exponent);
    else den *= pow10BigInt(-exponent);

    const negative = num < 0n;
    const abs = negative ? -num : num;
    let quotient = abs / den;
    const remainder = abs % den;
    let increment = false;
    if (remainder !== 0n) {
      if (policy.mode === 'ROUND_HALF_UP') increment = remainder * 2n >= den;
      else if (policy.mode === 'ROUND_HALF_EVEN') {
        const doubled = remainder * 2n;
        increment = doubled > den || (doubled === den && quotient % 2n === 1n);
      } else if (policy.mode === 'ROUND_FLOOR') increment = negative;
      else if (policy.mode === 'ROUND_CEILING') increment = !negative;
      else if (policy.mode !== 'ROUND_DOWN') fail('thirteenth_rounding_mode', 'Modo de arredondamento não suportado: ' + policy.mode);
    }
    if (increment) quotient += 1n;
    return decimalFromScaledInteger(negative ? -quotient : quotient, places);
  }

  function selectRule(release, ruleId, consumer, targetDate) {
    return SFA.selectRule(release, { ruleId, consumer, context: CONTEXT, targetDate });
  }

  function calculateAccrual(release, request) {
    const req = request || {};
    const consumer = req.consumer || 'H27';
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const selected = selectRule(release, 'thirteenth.accrual.twelfths', consumer, targetDate);
    const payload = selected.rule.payload;
    if (payload.type !== 'threshold_accrual' || payload.method !== 'one_unit_per_month_if_days_gte_threshold') {
      fail('thirteenth_accrual_payload', 'Payload de avos do 13º incompatível.');
    }

    const referenceYear = Number(req.referenceYear);
    if (!Number.isSafeInteger(referenceYear) || referenceYear < 1900 || referenceYear > 9999) {
      fail('thirteenth_reference_year', 'Ano de referência inválido.');
    }

    if (req.mode === 'manual') {
      const twelfths = Number(req.twelfths);
      if (!Number.isSafeInteger(twelfths) || twelfths < 0 || twelfths > payload.max_units) {
        fail('thirteenth_twelfths', 'Avos manuais fora do intervalo suportado.');
      }
      return Object.freeze({
        reference_year: referenceYear,
        twelfths,
        mode: 'manual',
        months: Object.freeze([]),
        audit: ruleAudit(selected)
      });
    }

    if (req.mode !== 'dates') fail('thirteenth_accrual_mode', 'Modo de apuração de avos não suportado.');
    const start = dateParts(req.employmentStart, 'employmentStart');
    const end = dateParts(req.accrualEnd, 'accrualEnd');
    if (start.iso > end.iso) fail('thirteenth_period', 'Data inicial não pode ser posterior à data final.');

    const yearStart = isoDate(referenceYear, 1, 1);
    const yearEnd = isoDate(referenceYear, 12, 31);
    const clippedStart = start.iso > yearStart ? start.iso : yearStart;
    const clippedEnd = end.iso < yearEnd ? end.iso : yearEnd;
    if (clippedStart > clippedEnd) {
      return Object.freeze({ reference_year: referenceYear, twelfths: 0, mode: 'dates', months: Object.freeze([]), audit: ruleAudit(selected) });
    }

    const first = dateParts(clippedStart);
    const last = dateParts(clippedEnd);
    const months = [];
    let units = 0;
    for (let month = first.month; month <= last.month; month += 1) {
      const monthStart = isoDate(referenceYear, month, 1);
      const monthEnd = isoDate(referenceYear, month, daysInMonth(referenceYear, month));
      const serviceStart = clippedStart > monthStart ? clippedStart : monthStart;
      const serviceEnd = clippedEnd < monthEnd ? clippedEnd : monthEnd;
      const serviceDays = serviceStart <= serviceEnd ? daysInclusive(serviceStart, serviceEnd) : 0;
      const qualifies = serviceDays >= payload.qualifying_days;
      if (qualifies && units < payload.max_units) units += 1;
      months.push(Object.freeze({ month, service_days: serviceDays, qualifies }));
    }
    return Object.freeze({
      reference_year: referenceYear,
      twelfths: units,
      mode: 'dates',
      months: Object.freeze(months),
      audit: ruleAudit(selected)
    });
  }

  function selectReferenceRemuneration(release, request) {
    const req = request || {};
    const consumer = req.consumer || 'H27';
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const reference = selectRule(release, 'thirteenth.reference_remuneration', consumer, targetDate);
    const variableRule = selectRule(release, 'thirteenth.variable_remuneration', consumer, targetDate);
    if (reference.rule.payload.type !== 'remuneration_reference' || reference.rule.payload.annual_reference !== 'december_due_remuneration') {
      fail('thirteenth_reference_payload', 'Referência remuneratória anual incompatível.');
    }
    if (variableRule.rule.payload.type !== 'variable_remuneration' || variableRule.rule.payload.precomputed_average_allowed_only_as_external_input !== true) {
      fail('thirteenth_variable_payload', 'Remuneração variável não pode ser inferida pelo consumidor.');
    }
    const fixed = SFA.Decimal.parse(SFA.normalizeMoneyInput(req.decemberDueRemuneration), 'december_due_remuneration');
    const variable = SFA.Decimal.parse(SFA.normalizeMoneyInput(req.variableComponent || '0'), 'variable_component');
    if (fixed.compare(ZERO) < 0 || variable.compare(ZERO) < 0) fail('thirteenth_negative_reference', 'Remuneração de referência não pode ser negativa.');
    return Object.freeze({
      fixed_reference: fixed.toString(),
      variable_component: variable.toString(),
      variable_component_mode: variable.compare(ZERO) > 0 ? 'external_precomputed' : 'none',
      total_reference: fixed.add(variable).toString(),
      audit: Object.freeze([ruleAudit(reference), ruleAudit(variableRule)])
    });
  }

  function calculateGross(release, request) {
    const req = request || {};
    const consumer = req.consumer || 'H27';
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const selected = selectRule(release, 'thirteenth.accrual.twelfths', consumer, targetDate);
    const rounding = selectMoneyRounding(release, consumer, targetDate);
    const payload = selected.rule.payload;
    const twelfths = Number(req.twelfths);
    if (!Number.isSafeInteger(twelfths) || twelfths < 0 || twelfths > payload.max_units) {
      fail('thirteenth_twelfths', 'Avos fora do intervalo suportado.');
    }
    const policy = roundingPolicy(rounding);
    const totalReference = SFA.Decimal.parse(req.referenceRemuneration, 'reference_remuneration');
    const numerator = twelfths * payload.fraction_numerator;
    const gross = roundFraction(totalReference, numerator, payload.fraction_denominator, policy);
    return Object.freeze({
      reference_remuneration: totalReference.toString(),
      twelfths,
      fraction_numerator: payload.fraction_numerator,
      fraction_denominator: payload.fraction_denominator,
      gross_thirteenth: gross.toFixed(policy.decimal_places, policy.mode),
      audit: ruleAudit(selected),
      rounding_audit: ruleAudit(rounding)
    });
  }

  function calculateAdvance(release, request) {
    const req = request || {};
    const consumer = req.consumer || 'H27';
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const selected = selectRule(release, 'thirteenth.advance', consumer, targetDate);
    const rounding = selectMoneyRounding(release, consumer, targetDate);
    const payload = selected.rule.payload;
    if (payload.type !== 'thirteenth_advance' || payload.fixed_reference !== 'previous_month_salary') {
      fail('thirteenth_advance_payload', 'Regra de adiantamento incompatível.');
    }
    const payment = dateParts(req.paymentDate, 'advancePaymentDate');
    if (payment.month < payload.payment_window_start_month || payment.month > payload.payment_window_end_month) {
      fail('thirteenth_advance_window', 'Data do adiantamento fora da janela suportada pelo contrato.');
    }
    if (req.admissionInYear === true) fail('thirteenth_advance_admission_unsupported', 'Admissão no ano exige tratamento especial do adiantamento.');
    if (req.hasVariableRemuneration === true) fail('thirteenth_advance_variable_unsupported', 'Remuneração variável exige tratamento especial do adiantamento.');
    if (req.previousMonthSalary === null || req.previousMonthSalary === undefined || String(req.previousMonthSalary).trim() === '') {
      fail('thirteenth_advance_reference_required', 'Salário do mês anterior é obrigatório para calcular automaticamente o adiantamento padrão.');
    }
    const previous = SFA.Decimal.parse(SFA.normalizeMoneyInput(req.previousMonthSalary), 'previous_month_salary');
    if (previous.compare(ZERO) < 0) fail('thirteenth_advance_negative', 'Salário do mês anterior não pode ser negativo.');
    const policy = roundingPolicy(rounding);
    const amount = roundFraction(previous, payload.fraction_numerator, payload.fraction_denominator, policy);
    return Object.freeze({
      payment_date: payment.iso,
      reference_remuneration: previous.toString(),
      amount: amount.toFixed(policy.decimal_places, policy.mode),
      source: 'contract_standard',
      audit: ruleAudit(selected),
      rounding_audit: ruleAudit(rounding)
    });
  }

  function assessFiscal(release, request) {
    const req = request || {};
    const consumer = req.consumer || 'H27';
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const gross = SFA.Decimal.parse(SFA.normalizeMoneyInput(req.grossThirteenth), 'gross_thirteenth');
    if (gross.compare(ZERO) < 0) fail('thirteenth_gross_negative', '13º bruto não pode ser negativo.');

    const inssPolicy = selectRule(release, 'thirteenth.inss.separate_assessment', consumer, targetDate);
    if (inssPolicy.rule.payload.type !== 'policy' || inssPolicy.rule.payload.policy_kind !== 'separate_social_security_assessment') {
      fail('thirteenth_inss_policy', 'INSS do 13º não está declarado como apuração separada.');
    }
    const inss = SFA.assessInss(release, {
      consumer,
      context: CONTEXT,
      targetDate,
      base: gross.toString()
    });

    const irrfPolicy = selectRule(release, 'thirteenth.irrf.exclusive_assessment', consumer, targetDate);
    if (irrfPolicy.rule.payload.type !== 'policy' || irrfPolicy.rule.payload.policy_kind !== 'exclusive_irrf_assessment') {
      fail('thirteenth_irrf_policy', 'IRRF do 13º não está declarado como apuração exclusiva.');
    }
    const assessment = SFA.assessmentIdentity('thirteenth', CONTEXT);
    const table = selectDependencyRule(release, {
      ruleId: 'irrf.monthly.progressive_table',
      dependencyOf: 'thirteenth.irrf.exclusive_assessment',
      consumer,
      context: CONTEXT,
      targetDate
    });
    const reduction = selectRule(release, assessment.reduction_rule_id, consumer, targetDate);
    const dependent = selectRule(release, 'irrf.dependent_deduction', consumer, targetDate);
    const simplified = selectRule(release, 'irrf.simplified_monthly_discount', consumer, targetDate);
    if (table.rule.payload.type !== 'progressive_table') fail('thirteenth_irrf_table', 'Tabela de IRRF dependente incompatível.');
    if (reduction.rule.payload.type !== 'affine_reduction' || reduction.rule.payload.input_semantic !== assessment.reduction_input_semantic) {
      fail('thirteenth_irrf_reduction', 'Redutor do IRRF do 13º incompatível.');
    }
    const bundle = Object.freeze({ assessment, table, reduction, dependent, simplified });
    const deductions = SFA.buildIrrfLegalDeductions(bundle, {
      socialSecurity: inss.amount,
      dependentCount: req.dependentCount,
      pension: req.pension
    });
    const legalTotal = SFA.Decimal.parse(deductions.total);
    const simplifiedValue = SFA.scalarValue(simplified, 'BRL');
    const useSimplified = simplifiedValue.compare(legalTotal) > 0;
    const selectedDeduction = useSimplified ? simplifiedValue : legalTotal;
    const taxBase = gross.sub(selectedDeduction).max(ZERO);
    const pre = SFA.executeProgressive(taxBase.toString(), table);
    const red = SFA.executeAffineReduction(gross.toString(), pre.amount, reduction);

    return Object.freeze({
      gross_thirteenth: gross.toString(),
      social_security: inss.amount,
      irrf: Object.freeze({
        assessment,
        deduction_components: deductions,
        legal_deductions: legalTotal.toString(),
        simplified_discount: simplifiedValue.toString(),
        deduction_mode: useSimplified ? 'simplified' : 'legal',
        irrf_tax_base: taxBase.toString(),
        pre_reduction_irrf: pre.amount,
        reduction_input_income: gross.toString(),
        reduction_amount: red.applied_reduction,
        final_irrf: red.final_tax
      }),
      audit: Object.freeze({
        release_id: release.release_id,
        rules: Object.freeze([
          ruleAudit(inssPolicy),
          ...inss.audit.rules,
          ruleAudit(irrfPolicy),
          ruleAudit(table),
          ruleAudit(reduction),
          ruleAudit(dependent),
          ruleAudit(simplified)
        ])
      })
    });
  }

  SFA.THIRTEENTH = Object.freeze({
    selectDependencyRule,
    selectMoneyRounding,
    calculateAccrual,
    selectReferenceRemuneration,
    calculateGross,
    calculateAdvance,
    assessFiscal
  });
})(typeof window !== 'undefined' ? window : globalThis);
