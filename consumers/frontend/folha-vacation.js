(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA) return;

  const ZERO = SFA.Decimal.parse('0');
  const CONSUMER = 'H28';
  const ENJOYED = 'vacation_enjoyed';
  const CASH = 'vacation_cash_allowance';
  const TECHNICAL = 'technical';

  function fail(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function nonNegativeMoney(value, name) {
    const amount = SFA.Decimal.parse(SFA.normalizeMoneyInput(value), name);
    if (amount.compare(ZERO) < 0) fail('vacation_negative_input', name + ' não pode ser negativo.');
    return amount;
  }

  function integer(value, name) {
    const raw = String(value === undefined || value === null ? '' : value).trim();
    if (!/^\d+$/.test(raw)) fail('vacation_integer', name + ' deve ser inteiro não negativo.');
    const result = Number(raw);
    if (!Number.isSafeInteger(result)) fail('vacation_integer', name + ' está fora do intervalo suportado.');
    return result;
  }

  function select(release, ruleId, context, targetDate) {
    return SFA.selectRule(release, {
      ruleId,
      consumer: CONSUMER,
      context,
      targetDate
    });
  }

  function audit(selected) {
    return SFA.ruleAudit(selected);
  }

  function uniqueAudits(items) {
    const seen = new Set();
    const result = [];
    for (const item of items) {
      if (!item) continue;
      const key = item.rule_id + '@' + item.rule_version + '/' + item.context;
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(item);
    }
    return Object.freeze(result);
  }

  function pow10BigInt(exp) {
    if (!Number.isInteger(exp) || exp < 0) fail('vacation_scale', 'Escala decimal inválida.');
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
      fail('vacation_fraction', 'Fração inválida.');
    }
    if (!policy || !Number.isInteger(policy.decimal_places) || typeof policy.mode !== 'string') {
      fail('vacation_rounding', 'Política de arredondamento ausente.');
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
      else if (policy.mode !== 'ROUND_DOWN') fail('vacation_rounding_mode', 'Modo de arredondamento não suportado: ' + policy.mode);
    }
    if (increment) quotient += 1n;
    return decimalFromScaledInteger(negative ? -quotient : quotient, places);
  }

  function entitlement(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const absences = integer(req.unjustifiedAbsences, 'unjustifiedAbsences');
    const selected = select(release, 'vacation.entitlement_days_by_absences', ENJOYED, targetDate);
    const payload = selected.rule.payload;
    if (!payload || payload.type !== 'entitlement_bands' || payload.input_semantic !== 'unjustified_absences_in_acquisition_period') {
      fail('vacation_entitlement_payload', 'Regra de direito a férias incompatível.');
    }
    const band = payload.bands.find(function (item) {
      return absences >= item.min_absences && absences <= item.max_absences;
    });
    if (!band) {
      if (payload.outside_behavior === 'UNSUPPORTED') fail('vacation_absences_unsupported', 'Quantidade de faltas fora do escopo automático do contrato.');
      fail('vacation_entitlement_band', 'Nenhuma faixa de direito foi encontrada.');
    }
    return Object.freeze({
      unjustified_absences: absences,
      entitled_days: band.entitled_days,
      audit: audit(selected)
    });
  }

  function daysSplit(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const entitledDays = integer(req.entitledDays, 'entitledDays');
    if (req.sellOneThird !== true) {
      return Object.freeze({ entitled_days: entitledDays, enjoyed_days: entitledDays, cash_allowance_days: 0, audit: null });
    }
    const selected = select(release, 'vacation.abono_pecuniario', CASH, targetDate);
    const payload = selected.rule.payload;
    if (!payload || payload.type !== 'fraction' || payload.numerator !== 1 || payload.denominator !== 3 ||
        selected.rule.applies_to !== 'vacation_entitled_days_and_corresponding_remuneration_components') {
      fail('vacation_abono_payload', 'Regra do abono pecuniário incompatível.');
    }
    const numerator = entitledDays * payload.numerator;
    if (numerator % payload.denominator !== 0) {
      fail('vacation_abono_non_integral_days', 'O contrato não autoriza arredondar arbitrariamente dias de abono.');
    }
    const sold = numerator / payload.denominator;
    return Object.freeze({
      entitled_days: entitledDays,
      enjoyed_days: entitledDays - sold,
      cash_allowance_days: sold,
      audit: audit(selected)
    });
  }

  function incidenceFlag(selected, component, field, expected) {
    const payload = selected.rule.payload;
    if (!payload || payload.type !== 'incidence_profile' || !Array.isArray(payload.components)) {
      fail('vacation_incidence_payload', 'Perfil de incidência incompatível em ' + selected.rule.rule_id + '.');
    }
    const row = payload.components.find(function (item) { return item.component === component; });
    if (!row || row[field] !== expected) {
      fail('vacation_incidence_mismatch', 'Incidência inesperada para ' + component + '/' + field + '.');
    }
  }

  function calculateComponents(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const fullPrincipal = nonNegativeMoney(req.vacationPayBase, 'vacationPayBase');
    if (fullPrincipal.compare(ZERO) <= 0) fail('vacation_base_required', 'A base integral das férias deve ser maior que zero.');

    const right = entitlement(release, req);
    const split = daysSplit(release, {
      targetDate,
      entitledDays: right.entitled_days,
      sellOneThird: req.sellOneThird === true
    });
    const formula = select(release, 'vacation.remuneration_and_constitutional_third', ENJOYED, targetDate);
    if (!formula.rule.payload || formula.rule.payload.type !== 'component_formula' ||
        formula.rule.applies_to !== 'vacation_remuneration_components_and_constitutional_third') {
      fail('vacation_formula_payload', 'Fórmula remuneratória de férias incompatível.');
    }
    const policy = formula.rule.rounding_policy;
    if (!policy || policy.stage !== 'per_component') fail('vacation_formula_rounding', 'Fórmula de férias sem arredondamento por componente.');

    let cashPrincipal = ZERO;
    let enjoyedPrincipal = fullPrincipal;
    let cashThird = ZERO;
    if (req.sellOneThird === true) {
      const abono = select(release, 'vacation.abono_pecuniario', CASH, targetDate);
      const fraction = abono.rule.payload;
      cashPrincipal = roundFraction(fullPrincipal, fraction.numerator, fraction.denominator, policy);
      enjoyedPrincipal = fullPrincipal.sub(cashPrincipal);
      const cashFormula = select(release, 'vacation.remuneration_and_constitutional_third', CASH, targetDate);
      cashThird = roundFraction(cashPrincipal, 1, 3, cashFormula.rule.rounding_policy);
    }
    const enjoyedThird = roundFraction(enjoyedPrincipal, 1, 3, policy);
    const gross = enjoyedPrincipal.add(enjoyedThird).add(cashPrincipal).add(cashThird);

    return Object.freeze({
      target_date: targetDate,
      entitled_days: right.entitled_days,
      enjoyed_days: split.enjoyed_days,
      cash_allowance_days: split.cash_allowance_days,
      full_vacation_principal_base: fullPrincipal.toFixed(policy.decimal_places, policy.mode),
      enjoyed_principal: enjoyedPrincipal.toFixed(policy.decimal_places, policy.mode),
      enjoyed_constitutional_third: enjoyedThird.toFixed(policy.decimal_places, policy.mode),
      cash_allowance_principal: cashPrincipal.toFixed(policy.decimal_places, policy.mode),
      cash_allowance_constitutional_third: cashThird.toFixed(policy.decimal_places, policy.mode),
      gross_vacation_payment: gross.toFixed(policy.decimal_places, policy.mode),
      audit: Object.freeze([
        right.audit,
        split.audit,
        audit(formula)
      ].filter(Boolean))
    });
  }

  function assessFiscal(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    if (req.pension === undefined || req.pension === null || String(req.pension).trim() === '') {
      fail('vacation_pension_required', 'Pensão deve ser informada explicitamente, inclusive zero.');
    }
    const dependents = integer(req.dependentCount, 'dependentCount');
    const pension = nonNegativeMoney(req.pension, 'pension');
    const components = req.components;
    if (!components || typeof components !== 'object') fail('vacation_components_required', 'Componentes de férias ausentes.');

    const enjoyedPrincipal = nonNegativeMoney(components.enjoyed_principal, 'enjoyed_principal');
    const enjoyedThird = nonNegativeMoney(components.enjoyed_constitutional_third, 'enjoyed_constitutional_third');
    const cashPrincipal = nonNegativeMoney(components.cash_allowance_principal, 'cash_allowance_principal');
    const cashThird = nonNegativeMoney(components.cash_allowance_constitutional_third, 'cash_allowance_constitutional_third');

    const enjoyedIncidence = select(release, 'vacation.inss.enjoyed', ENJOYED, targetDate);
    incidenceFlag(enjoyedIncidence, 'enjoyed_vacation_remuneration', 'social_security', 'yes');
    incidenceFlag(enjoyedIncidence, 'enjoyed_vacation_constitutional_third', 'social_security', 'yes');
    incidenceFlag(enjoyedIncidence, 'enjoyed_vacation_remuneration', 'irrf', 'yes');
    incidenceFlag(enjoyedIncidence, 'enjoyed_vacation_constitutional_third', 'irrf', 'yes');

    const cashExemption = select(release, 'vacation.abono.ir_exemption', CASH, targetDate);
    incidenceFlag(cashExemption, 'cash_allowance_principal', 'social_security', 'no');
    incidenceFlag(cashExemption, 'cash_allowance_principal', 'irrf', 'no');
    const cashThirdIncidence = select(release, 'vacation.abono_constitutional_third.ir_incidence', CASH, targetDate);
    incidenceFlag(cashThirdIncidence, 'constitutional_third_on_cash_allowance', 'social_security', 'no');
    incidenceFlag(cashThirdIncidence, 'constitutional_third_on_cash_allowance', 'irrf', 'yes');

    const socialSecurityBase = enjoyedPrincipal.add(enjoyedThird);
    const inss = SFA.assessInss(release, {
      consumer: CONSUMER,
      context: ENJOYED,
      targetDate,
      base: socialSecurityBase.toString()
    });

    const separate = select(release, 'vacation.irrf.separate_assessment', ENJOYED, targetDate);
    if (!separate.rule.payload || separate.rule.payload.type !== 'policy' ||
        separate.rule.payload.policy_kind !== 'separate_vacation_irrf_assessment') {
      fail('vacation_irrf_separate', 'Regra de apuração separada do IRRF de férias incompatível.');
    }
    const table = select(release, 'irrf.monthly.progressive_table', ENJOYED, targetDate);
    const reduction = select(release, 'vacation.irrf.reduction.2026', ENJOYED, targetDate);
    const dependent = select(release, 'irrf.dependent_deduction', ENJOYED, targetDate);
    const simplified = select(release, 'irrf.simplified_monthly_discount', ENJOYED, targetDate);
    if (!separate.rule.dependencies.includes('irrf.monthly.progressive_table') ||
        !separate.rule.dependencies.includes('irrf.deductions_by_income_type')) {
      fail('vacation_irrf_dependencies', 'Apuração de férias não declara as dependências fiscais esperadas.');
    }
    if (!reduction.rule.dependencies.includes('vacation.irrf.separate_assessment') ||
        !reduction.rule.dependencies.includes('irrf.monthly.progressive_table') ||
        reduction.rule.payload.input_semantic !== 'taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions') {
      fail('vacation_reduction_semantic', 'Redutor dedicado de férias incompatível.');
    }

    const inssAmount = nonNegativeMoney(inss.amount, 'vacation_inss');
    const dependentUnit = nonNegativeMoney(dependent.rule.payload.value, 'dependent_deduction');
    const dependentAmount = dependentUnit.mulInt(dependents);
    const legalDeductions = inssAmount.add(dependentAmount).add(pension);
    const simplifiedAmount = nonNegativeMoney(simplified.rule.payload.value, 'simplified_discount');
    const useSimplified = simplifiedAmount.compare(legalDeductions) > 0;
    const selectedDeduction = useSimplified ? simplifiedAmount : legalDeductions;

    const taxableGross = enjoyedPrincipal.add(enjoyedThird).add(cashThird);
    const taxBase = taxableGross.sub(selectedDeduction).max(ZERO);
    const pre = SFA.executeProgressive(taxBase.toString(), table);
    const red = SFA.executeAffineReduction(taxableGross.toString(), pre.amount, reduction);

    const totalGross = enjoyedPrincipal.add(enjoyedThird).add(cashPrincipal).add(cashThird);
    const net = totalGross.sub(inssAmount).sub(SFA.Decimal.parse(red.final_tax)).sub(pension);
    const audits = uniqueAudits([
      audit(enjoyedIncidence),
      audit(cashExemption),
      audit(cashThirdIncidence),
      audit(separate),
      audit(table),
      audit(reduction),
      audit(dependent),
      audit(simplified),
      ...(inss.audit && Array.isArray(inss.audit.rules) ? inss.audit.rules : [])
    ]);

    return Object.freeze({
      social_security_base: socialSecurityBase.toString(),
      inss: inss.amount,
      taxable_vacation_income: taxableGross.toString(),
      irrf: Object.freeze({
        legal_deductions: legalDeductions.toString(),
        simplified_discount: simplifiedAmount.toString(),
        deduction_mode: useSimplified ? 'simplified' : 'legal',
        tax_base: taxBase.toString(),
        pre_reduction_irrf: pre.amount,
        reduction_input_income: taxableGross.toString(),
        reduction_amount: red.applied_reduction,
        final_irrf: red.final_tax
      }),
      pension: pension.toString(),
      net_vacation_payment: net.toString(),
      audit: Object.freeze({
        release_id: release.release_id,
        rules: audits
      })
    });
  }

  function calculate(release, request) {
    const req = request || {};
    SFA.assertRelease(release, CONSUMER);
    const components = calculateComponents(release, req);
    const fiscal = assessFiscal(release, {
      targetDate: components.target_date,
      dependentCount: req.dependentCount,
      pension: req.pension,
      components
    });
    const rules = uniqueAudits([
      ...components.audit,
      ...fiscal.audit.rules
    ]);
    return Object.freeze({
      consumer: CONSUMER,
      reference_date: components.target_date,
      entitlement: Object.freeze({
        unjustified_absences: integer(req.unjustifiedAbsences, 'unjustifiedAbsences'),
        entitled_days: components.entitled_days,
        enjoyed_days: components.enjoyed_days,
        cash_allowance_days: components.cash_allowance_days
      }),
      components: Object.freeze({
        full_vacation_principal_base: components.full_vacation_principal_base,
        enjoyed_principal: components.enjoyed_principal,
        enjoyed_constitutional_third: components.enjoyed_constitutional_third,
        cash_allowance_principal: components.cash_allowance_principal,
        cash_allowance_constitutional_third: components.cash_allowance_constitutional_third,
        gross_vacation_payment: components.gross_vacation_payment
      }),
      fiscal: Object.freeze({
        social_security_base: fiscal.social_security_base,
        inss: fiscal.inss,
        taxable_vacation_income: fiscal.taxable_vacation_income,
        irrf: fiscal.irrf,
        pension: fiscal.pension,
        net_vacation_payment: fiscal.net_vacation_payment
      }),
      fiscal_metadata: Object.freeze({
        release_id: release.release_id,
        rules
      }),
      scope: Object.freeze({
        vacation_pay_base: 'external_precomputed_for_full_entitlement',
        remaining_month_salary: 'not_included_in_vacation_assessment'
      })
    });
  }

  SFA.VACATION = Object.freeze({
    entitlement,
    daysSplit,
    calculateComponents,
    assessFiscal,
    calculate
  });
})(typeof window !== 'undefined' ? window : globalThis);
