(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA) return;

  const CONSUMER = 'H29';
  const CONTEXT = 'termination';
  const TECHNICAL = 'technical';
  const ZERO = SFA.Decimal.parse('0');
  const EXPECTED_REASON_CODES = Object.freeze(['01', '02', '07', '33']);
  const EXPECTED_INCLUDED_ITEMS = Object.freeze([
    'salary_balance',
    'thirteenth_proportional',
    'vacation_proportional',
    'acquired_or_overdue_vacation'
  ]);

  function fail(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function dateParts(value, name) {
    const iso = SFA.normalizeDate(value, name || 'date');
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
    if (!m) fail('termination_date', 'Data inválida em ' + (name || 'date') + '.');
    const year = Number(m[1]);
    const month = Number(m[2]);
    const day = Number(m[3]);
    const probe = new Date(Date.UTC(year, month - 1, day));
    if (probe.getUTCFullYear() !== year || probe.getUTCMonth() + 1 !== month || probe.getUTCDate() !== day) {
      fail('termination_date', 'Data civil inválida em ' + (name || 'date') + '.');
    }
    return Object.freeze({ iso, year, month, day });
  }

  function isoDate(year, month, day) {
    return String(year).padStart(4, '0') + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');
  }

  function daysInMonth(year, month) {
    return new Date(Date.UTC(year, month, 0)).getUTCDate();
  }

  function dateToEpochDay(iso) {
    const p = dateParts(iso);
    return Math.floor(Date.UTC(p.year, p.month - 1, p.day) / 86400000);
  }

  function addDays(iso, days) {
    if (!Number.isSafeInteger(days)) fail('termination_days', 'Incremento de dias inválido.');
    const p = dateParts(iso);
    const d = new Date(Date.UTC(p.year, p.month - 1, p.day + days));
    return isoDate(d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate());
  }

  function addMonths(anchorIso, months) {
    if (!Number.isSafeInteger(months) || months < 0) fail('termination_months', 'Incremento de meses inválido.');
    const anchor = dateParts(anchorIso);
    const monthIndex = (anchor.month - 1) + months;
    const year = anchor.year + Math.floor(monthIndex / 12);
    const month = (monthIndex % 12) + 1;
    const day = Math.min(anchor.day, daysInMonth(year, month));
    return isoDate(year, month, day);
  }

  function nonNegativeMoney(value, name) {
    const amount = SFA.Decimal.parse(SFA.normalizeMoneyInput(value), name);
    if (amount.compare(ZERO) < 0) fail('termination_negative_input', name + ' não pode ser negativo.');
    return amount;
  }

  function positiveMoney(value, name) {
    const raw = String(value === undefined || value === null ? '' : value).trim();
    if (raw === '') fail('termination_required_input', name + ' deve ser informado e maior que zero.');
    const amount = nonNegativeMoney(raw, name);
    if (amount.compare(ZERO) <= 0) fail('termination_positive_input', name + ' deve ser maior que zero.');
    return amount;
  }

  function integer(value, name) {
    const raw = String(value === undefined || value === null ? '' : value).trim();
    if (!/^\d+$/.test(raw)) fail('termination_integer', name + ' deve ser inteiro não negativo.');
    const result = Number(raw);
    if (!Number.isSafeInteger(result)) fail('termination_integer', name + ' está fora do intervalo suportado.');
    return result;
  }

  function select(release, ruleId, targetDate, context) {
    return SFA.selectRule(release, {
      ruleId,
      consumer: CONSUMER,
      context: context || CONTEXT,
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

  function sameSet(actual, expected) {
    if (!Array.isArray(actual) || actual.length !== expected.length) return false;
    const set = new Set(actual);
    return set.size === expected.length && expected.every(function (item) { return set.has(item); });
  }

  function eligibility(payload, code, name) {
    if (!payload || payload.type !== 'code_eligibility' || payload.code_system !== 'esocial_table_19_termination_reason') {
      fail('termination_eligibility_payload', 'Regra de elegibilidade incompatível em ' + name + '.');
    }
    if (payload.eligible_codes.includes(code)) return true;
    if (payload.ineligible_codes.includes(code)) return false;
    fail('termination_reason_unsupported', 'Motivo ' + code + ' não é suportado por ' + name + '.');
  }

  function predicateMatches(actual, predicate) {
    if (!predicate || typeof predicate !== 'object') fail('termination_applicability', 'Predicado de aplicabilidade inválido.');
    if (predicate.operator === 'eq') return actual === predicate.value;
    if (predicate.operator === 'ne') return actual !== predicate.value;
    if (predicate.operator === 'in') return Array.isArray(predicate.value) && predicate.value.includes(actual);
    if (predicate.operator === 'not_in') return Array.isArray(predicate.value) && !predicate.value.includes(actual);
    fail('termination_applicability', 'Operador de aplicabilidade não suportado: ' + predicate.operator + '.');
  }

  function assertApplicability(reasonRule, employmentRegime, contractTerm) {
    const values = Object.freeze({ employment_regime: employmentRegime, contract_term: contractTerm });
    const predicates = reasonRule.rule.applicability;
    if (!Array.isArray(predicates) || predicates.length !== 2 ||
        !predicates.some(function (item) { return item.field === 'employment_regime'; }) ||
        !predicates.some(function (item) { return item.field === 'contract_term'; })) {
      fail('termination_applicability', 'Escopo H29 deve declarar regime de emprego e prazo contratual.');
    }
    for (const predicate of predicates) {
      if (!Object.prototype.hasOwnProperty.call(values, predicate.field) || !predicateMatches(values[predicate.field], predicate)) {
        fail('termination_scope_unsupported', 'Caso fora do escopo H29 v1: ' + predicate.field + '=' + values[predicate.field] + '.');
      }
    }
  }

  function resolveReason(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const reasonRule = select(release, 'termination.reason_scope', targetDate);
    const scopeRule = select(release, 'termination.partial_output_scope', targetDate);
    const thirteenthRule = select(release, 'termination.thirteenth_proportional', targetDate);
    const vacationRule = select(release, 'termination.vacation_proportional', targetDate);

    const matrix = reasonRule.rule.payload;
    if (!matrix || matrix.type !== 'eligibility_matrix' || matrix.code_system !== 'esocial_table_19_termination_reason' ||
        matrix.unsupported_behavior !== 'UNSUPPORTED' || !Array.isArray(matrix.rows)) {
      fail('termination_reason_payload', 'Matriz de motivos eSocial incompatível.');
    }
    const matrixCodes = matrix.rows.map(function (row) { return row.code; });
    if (!sameSet(matrixCodes, EXPECTED_REASON_CODES)) fail('termination_reason_matrix', 'Matriz H29 não contém exatamente 01/02/07/33.');

    const scope = scopeRule.rule.payload;
    if (!scope || scope.type !== 'scope_declaration' || scope.promise !== 'partial_estimate' ||
        scope.require_user_disclosure !== true || !sameSet(scope.included_items, EXPECTED_INCLUDED_ITEMS)) {
      fail('termination_partial_scope', 'Escopo H29 não corresponde à estimativa parcial fechada.');
    }

    const employmentRegime = String(req.employmentRegime || '');
    const contractTerm = String(req.contractTerm || '');
    assertApplicability(reasonRule, employmentRegime, contractTerm);

    const code = String(req.esocialReason || '');
    if (!/^\d{2}$/.test(code)) fail('termination_reason', 'Motivo eSocial deve ter dois dígitos.');
    const row = matrix.rows.find(function (item) { return item.code === code; });
    if (!row) fail('termination_reason_unsupported', 'Motivo eSocial ' + code + ' é UNSUPPORTED no H29 v1.');

    const thirteenth = eligibility(thirteenthRule.rule.payload, code, 'termination.thirteenth_proportional');
    const vacation = eligibility(vacationRule.rule.payload, code, 'termination.vacation_proportional');
    if (!row.entitlements || thirteenth !== row.entitlements.thirteenth_proportional ||
        vacation !== row.entitlements.vacation_proportional) {
      fail('termination_matrix_divergence', 'Elegibilidades específicas divergem da matriz H29.');
    }

    const matrixCodeSet = new Set(matrixCodes);
    for (const selected of [thirteenthRule, vacationRule]) {
      const p = selected.rule.payload;
      const covered = new Set([...(p.eligible_codes || []), ...(p.ineligible_codes || [])]);
      if (covered.size !== matrixCodeSet.size || !matrixCodes.every(function (item) { return covered.has(item); })) {
        fail('termination_matrix_divergence', selected.rule.rule_id + ' não cobre exatamente a matriz H29.');
      }
    }

    return Object.freeze({
      target_date: targetDate,
      decision: Object.freeze({
        code: row.code,
        label: row.label,
        salary_balance: row.entitlements.salary_balance === true,
        thirteenth_proportional: thirteenth,
        vacation_proportional: vacation,
        acquired_vacation_if_due: row.entitlements.acquired_vacation_if_due === true
      }),
      scope: Object.freeze({
        result_promise: scope.promise,
        user_disclosure_required: scope.require_user_disclosure,
        included_items: Object.freeze(scope.included_items.slice()),
        excluded_items: Object.freeze(scope.excluded_items.slice())
      }),
      audit: Object.freeze([audit(reasonRule), audit(scopeRule), audit(thirteenthRule), audit(vacationRule)])
    });
  }

  function decimalFromScaledInteger(value, places) {
    const negative = value < 0n;
    let digits = (negative ? -value : value).toString();
    if (places === 0) return SFA.Decimal.parse((negative ? '-' : '') + digits);
    if (digits.length <= places) digits = '0'.repeat(places - digits.length + 1) + digits;
    const split = digits.length - places;
    return SFA.Decimal.parse((negative ? '-' : '') + digits.slice(0, split) + '.' + digits.slice(split));
  }

  function pow10BigInt(exp) {
    if (!Number.isInteger(exp) || exp < 0) fail('termination_scale', 'Escala decimal inválida.');
    return 10n ** BigInt(exp);
  }

  function roundRatio(value, numerator, denominator, policy) {
    const decimal = SFA.Decimal.parse(value, 'ratio_value');
    if (!Number.isSafeInteger(numerator) || numerator < 0 || !Number.isSafeInteger(denominator) || denominator <= 0) {
      fail('termination_ratio', 'Razão inteira inválida.');
    }
    if (!policy || !Number.isInteger(policy.decimal_places) || typeof policy.mode !== 'string') {
      fail('termination_rounding', 'Política de arredondamento ausente.');
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
      else if (policy.mode !== 'ROUND_DOWN') fail('termination_rounding', 'Modo de arredondamento não suportado: ' + policy.mode);
    }
    if (increment) quotient += 1n;
    return decimalFromScaledInteger(negative ? -quotient : quotient, places);
  }

  function calculateSalaryBalance(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const selected = select(release, 'termination.salary_balance', targetDate);
    const p = selected.rule.payload;
    if (!p || p.type !== 'proration' || p.formula !== 'base_times_numerator_over_denominator' ||
        p.base_semantic !== 'monthly_base_salary' || p.numerator_semantic !== 'days_counted_through_termination' ||
        p.denominator_semantic !== 'calendar_days_in_month' || p.universal_fixed_denominator !== false) {
      fail('termination_salary_payload', 'Regra de saldo salarial incompatível.');
    }
    const policy = selected.rule.rounding_policy;
    if (!policy || policy.stage !== 'salary_balance_result') fail('termination_salary_rounding', 'Saldo salarial sem arredondamento no estágio correto.');

    const termination = dateParts(req.terminationDate, 'terminationDate');
    const days = integer(req.daysCountedThroughTermination, 'daysCountedThroughTermination');
    const calendarDays = daysInMonth(termination.year, termination.month);
    if (days > termination.day || days > calendarDays) {
      fail('termination_salary_days', 'Dias computados não podem superar o dia do desligamento.');
    }
    const base = positiveMoney(req.monthlyBaseSalary, 'monthlyBaseSalary');
    const amount = roundRatio(base, days, calendarDays, policy);
    if (amount.compare(base) > 0) fail('termination_salary_bounds', 'Saldo salarial ultrapassou a base mensal.');
    return Object.freeze({
      monthly_base_salary: base.toString(),
      termination_date: termination.iso,
      days_counted_through_termination: days,
      calendar_days_in_month: calendarDays,
      amount: amount.toFixed(policy.decimal_places, policy.mode),
      audit: audit(selected)
    });
  }

  function calculateThirteenth(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const start = dateParts(req.employmentStart, 'employmentStart');
    const end = dateParts(req.terminationDate, 'terminationDate');
    if (start.iso > end.iso) fail('termination_period', 'Admissão não pode ser posterior ao desligamento.');

    const accrualRule = select(release, 'thirteenth.accrual.twelfths', targetDate);
    const accrualPayload = accrualRule.rule.payload;
    if (!accrualPayload || accrualPayload.type !== 'threshold_accrual' ||
        accrualPayload.method !== 'one_unit_per_month_if_days_gte_threshold') {
      fail('termination_thirteenth_accrual', 'Regra de avos do 13º incompatível.');
    }
    const referenceRule = select(release, 'thirteenth.reference_remuneration', targetDate);
    const refPayload = referenceRule.rule.payload;
    if (!refPayload || refPayload.type !== 'remuneration_reference' ||
        refPayload.termination_reference !== 'termination_month_remuneration') {
      fail('termination_thirteenth_reference', 'Referência do 13º rescisório incompatível.');
    }
    const variableRule = select(release, 'thirteenth.variable_remuneration', targetDate);
    if (!variableRule.rule.payload || variableRule.rule.payload.type !== 'variable_remuneration' ||
        variableRule.rule.payload.precomputed_average_allowed_only_as_external_input !== true) {
      fail('termination_variable_rule', 'Regra de remuneração variável incompatível.');
    }
    const roundingRule = select(release, 'technical.money_decimal_and_rounding', targetDate, TECHNICAL);
    const rounding = roundingRule.rule.rounding_policy;
    if (!rounding || rounding.stage !== 'per_component') fail('termination_money_rounding', 'Política técnica monetária incompatível.');

    const referenceYear = end.year;
    const clippedStart = start.iso > isoDate(referenceYear, 1, 1) ? start.iso : isoDate(referenceYear, 1, 1);
    const clippedEnd = end.iso < isoDate(referenceYear, 12, 31) ? end.iso : isoDate(referenceYear, 12, 31);
    const months = [];
    let twelfths = 0;
    if (clippedStart <= clippedEnd) {
      const first = dateParts(clippedStart);
      const last = dateParts(clippedEnd);
      for (let month = first.month; month <= last.month; month += 1) {
        const monthStart = isoDate(referenceYear, month, 1);
        const monthEnd = isoDate(referenceYear, month, daysInMonth(referenceYear, month));
        const serviceStart = clippedStart > monthStart ? clippedStart : monthStart;
        const serviceEnd = clippedEnd < monthEnd ? clippedEnd : monthEnd;
        const serviceDays = serviceStart <= serviceEnd ? dateToEpochDay(serviceEnd) - dateToEpochDay(serviceStart) + 1 : 0;
        const qualifies = serviceDays >= accrualPayload.qualifying_days;
        if (qualifies && twelfths < accrualPayload.max_units) twelfths += 1;
        months.push(Object.freeze({ month, service_days: serviceDays, qualifies }));
      }
    }

    const reference = positiveMoney(req.terminationMonthRemuneration, 'terminationMonthRemuneration');
    const numerator = twelfths * accrualPayload.fraction_numerator;
    const gross = roundRatio(reference, numerator, accrualPayload.fraction_denominator, rounding);
    return Object.freeze({
      reference_year: referenceYear,
      twelfths,
      months: Object.freeze(months),
      reference_remuneration: reference.toString(),
      variable_component_mode: 'not_automated_in_h29',
      gross_thirteenth: gross.toFixed(rounding.decimal_places, rounding.mode),
      audit: Object.freeze([
        audit(accrualRule), audit(referenceRule), audit(variableRule), audit(roundingRule)
      ])
    });
  }

  function acquisitionPeriod(employmentStart, targetDate, payload) {
    const start = dateParts(employmentStart, 'employmentStart');
    const target = dateParts(targetDate, 'terminationDate');
    if (target.iso < start.iso) fail('termination_period', 'Desligamento não pode preceder admissão.');
    if (payload.calendar_year_reset !== false || payload.anchor !== 'employment_start_anniversary') {
      fail('termination_vacation_period', 'Período aquisitivo incompatível.');
    }
    const elapsedMonths = (target.year - start.year) * 12 + (target.month - start.month);
    let periodIndex = Math.max(0, Math.floor(elapsedMonths / payload.duration_months));
    let periodStart = addMonths(start.iso, periodIndex * payload.duration_months);
    if (periodStart > target.iso) {
      periodIndex -= 1;
      periodStart = addMonths(start.iso, periodIndex * payload.duration_months);
    }
    const endExclusive = addMonths(start.iso, (periodIndex + 1) * payload.duration_months);
    return Object.freeze({
      employment_start: start.iso,
      period_index: periodIndex,
      start: periodStart,
      end_exclusive: endExclusive,
      end_inclusive: addDays(endExclusive, -1)
    });
  }

  function calculateVacationProportional(release, request) {
    const req = request || {};
    const targetDate = SFA.normalizeDate(req.targetDate, 'targetDate');
    const selected = select(release, 'vacation.acquisition_period', targetDate);
    const payload = selected.rule.payload;
    if (!payload || payload.type !== 'period_rule' ||
        payload.proportional_accrual_method !== 'one_twelfth_per_acquisition_month_or_fraction_gte_days') {
      fail('termination_vacation_payload', 'Regra de período aquisitivo incompatível.');
    }
    const period = acquisitionPeriod(req.employmentStart, req.terminationDate, payload);
    const through = dateParts(req.terminationDate, 'terminationDate');
    const throughExclusive = addDays(through.iso, 1);
    const slices = [];
    let twelfths = 0;

    for (let index = 0; index < payload.duration_months; index += 1) {
      const sliceStart = addMonths(period.start, index);
      if (sliceStart >= period.end_exclusive || sliceStart > through.iso) break;
      let sliceEnd = addMonths(period.start, index + 1);
      if (sliceEnd > period.end_exclusive) sliceEnd = period.end_exclusive;
      const serviceEnd = sliceEnd < throughExclusive ? sliceEnd : throughExclusive;
      const serviceDays = Math.max(0, dateToEpochDay(serviceEnd) - dateToEpochDay(sliceStart));
      const fullMonth = serviceEnd >= sliceEnd;
      const qualifies = fullMonth || serviceDays >= payload.proportional_qualifying_days;
      if (qualifies) twelfths += 1;
      slices.push(Object.freeze({
        twelfth_index: index + 1,
        start: sliceStart,
        end_exclusive: sliceEnd,
        service_days: serviceDays,
        full_acquisition_month: fullMonth,
        qualifies
      }));
    }
    if (twelfths > payload.duration_months) fail('termination_vacation_twelfths', 'Férias proporcionais excederam o período aquisitivo.');
    return Object.freeze({
      period,
      through_date: through.iso,
      twelfths,
      slices: Object.freeze(slices),
      audit: audit(selected)
    });
  }

  function acquiredVacationDisclosure(release, targetDate, isPotentiallyDue) {
    const selected = select(release, 'termination.acquired_and_overdue_vacation', targetDate);
    const p = selected.rule.payload;
    if (!p || p.type !== 'period_state' || p.overdue_double_requires_explicit_rule !== true ||
        p.initial_consumer_may_limit_to_one_period_if_disclosed !== true ||
        !sameSet(p.states, ['acquired_within_concession_period', 'overdue_beyond_concession_period'])) {
      fail('termination_acquired_vacation', 'Regra de férias adquiridas/vencidas incompatível.');
    }
    return Object.freeze({
      potentially_due: isPotentiallyDue === true,
      monetary_calculation: 'not_automated_in_h29_v1',
      overdue_double: 'requires_explicit_rule',
      audit: audit(selected)
    });
  }

  function calculate(release, request) {
    const req = request || {};
    SFA.assertRelease(release, CONSUMER);
    const termination = dateParts(req.terminationDate, 'terminationDate');
    const employment = dateParts(req.employmentStart, 'employmentStart');
    if (employment.iso > termination.iso) fail('termination_period', 'Admissão não pode ser posterior ao desligamento.');

    const resolved = resolveReason(release, {
      targetDate: termination.iso,
      esocialReason: req.esocialReason,
      employmentRegime: req.employmentRegime,
      contractTerm: req.contractTerm
    });

    const salary = resolved.decision.salary_balance ? calculateSalaryBalance(release, {
      targetDate: termination.iso,
      terminationDate: termination.iso,
      monthlyBaseSalary: req.monthlyBaseSalary,
      daysCountedThroughTermination: req.daysCountedThroughTermination
    }) : null;

    const thirteenth = resolved.decision.thirteenth_proportional ? calculateThirteenth(release, {
      targetDate: termination.iso,
      employmentStart: employment.iso,
      terminationDate: termination.iso,
      terminationMonthRemuneration: req.terminationMonthRemuneration
    }) : null;

    const vacation = resolved.decision.vacation_proportional ? calculateVacationProportional(release, {
      targetDate: termination.iso,
      employmentStart: employment.iso,
      terminationDate: termination.iso
    }) : null;

    const acquired = acquiredVacationDisclosure(
      release, termination.iso, resolved.decision.acquired_vacation_if_due
    );
    const audits = uniqueAudits([
      ...resolved.audit,
      salary && salary.audit,
      ...(thirteenth ? thirteenth.audit : []),
      vacation && vacation.audit,
      acquired.audit
    ]);

    return Object.freeze({
      consumer: CONSUMER,
      reference_date: termination.iso,
      result_promise: resolved.scope.result_promise,
      user_disclosure_required: resolved.scope.user_disclosure_required,
      reason: resolved.decision,
      salary_balance: salary,
      thirteenth_proportional: thirteenth,
      vacation_proportional: vacation,
      acquired_or_overdue_vacation: acquired,
      included_items: resolved.scope.included_items,
      excluded_items: resolved.scope.excluded_items,
      fiscal_metadata: Object.freeze({
        release_id: release.release_id,
        rules: audits
      })
    });
  }

  SFA.TERMINATION = Object.freeze({
    resolveReason,
    calculateSalaryBalance,
    calculateThirteenth,
    calculateVacationProportional,
    calculate
  });
})(typeof window !== 'undefined' ? window : globalThis);
