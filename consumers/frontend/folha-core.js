(function (root) {
  'use strict';

  const SFA = (root.SFA_FOLHA = root.SFA_FOLHA || {});

  const CONTRACT_ID = 'br.sanida.fiscal';
  const SCHEMA_VERSION = '1.2.0';
  const CONTRACT_API_VERSION = '1.2.0';
  const RELEASE_ENDPOINT = '/blog/wp-json/sfa/v1/fiscal-release';
  const SUPPORTED_CONSUMERS = new Set(['H26', 'H27', 'H28', 'H29']);
  const SUPPORTED_CONTEXTS = new Set([
    'monthly',
    'thirteenth',
    'vacation_enjoyed',
    'vacation_cash_allowance',
    'vacation_indemnified',
    'termination',
    'technical'
  ]);
  const SUPPORTED_PAYLOAD_TYPES = new Set([
    'progressive_table', 'scalar', 'affine_reduction', 'threshold_accrual',
    'fraction', 'entitlement_bands', 'eligibility_matrix', 'incidence_profile',
    'policy', 'remuneration_reference', 'variable_remuneration',
    'thirteenth_advance', 'period_rule', 'component_formula',
    'scope_declaration', 'proration', 'code_eligibility', 'period_state'
  ]);
  const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
  const SEMVER_RE = /^\d+\.\d+\.\d+$/;
  const RELEASE_ID_RE = /^fiscal-v1-sha256-[0-9a-f]{64}$/;

  class FiscalContractError extends Error {
    constructor(code, message, details) {
      super(message);
      this.name = 'FiscalContractError';
      this.code = code;
      this.details = details || null;
    }
  }

  function fail(code, message, details) {
    throw new FiscalContractError(code, message, details);
  }

  function pow10(n) {
    if (!Number.isInteger(n) || n < 0) fail('decimal_scale', 'Escala decimal inválida.');
    return 10n ** BigInt(n);
  }

  class DecimalValue {
    constructor(coefficient, scale) {
      if (typeof coefficient !== 'bigint' || !Number.isInteger(scale) || scale < 0) {
        fail('decimal_internal', 'Representação decimal inválida.');
      }
      let c = coefficient;
      let s = scale;
      if (c === 0n) s = 0;
      while (s > 0 && c % 10n === 0n) {
        c /= 10n;
        s -= 1;
      }
      this.coefficient = c;
      this.scale = s;
      Object.freeze(this);
    }

    static parse(value, name) {
      const label = name || 'value';
      if (value instanceof DecimalValue) return value;
      if (typeof value === 'bigint') return new DecimalValue(value, 0);
      if (typeof value === 'number') {
        if (!Number.isSafeInteger(value)) {
          fail('binary_float_rejected', label + ' não pode entrar no cálculo fiscal como float binário.');
        }
        return new DecimalValue(BigInt(value), 0);
      }
      if (typeof value !== 'string') fail('decimal_type', label + ' deve ser string decimal ou inteiro seguro.');
      const raw = value.trim();
      const m = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(raw);
      if (!m) fail('decimal_format', 'Decimal inválido em ' + label + '.');
      const sign = m[1] === '-' ? -1n : 1n;
      const frac = m[3] || '';
      const digits = (m[2] + frac).replace(/^0+(?=\d)/, '');
      return new DecimalValue(sign * BigInt(digits || '0'), frac.length);
    }

    align(other) {
      const b = DecimalValue.parse(other);
      const scale = Math.max(this.scale, b.scale);
      return [
        this.coefficient * pow10(scale - this.scale),
        b.coefficient * pow10(scale - b.scale),
        scale
      ];
    }

    compare(other) {
      const [a, b] = this.align(other);
      return a < b ? -1 : (a > b ? 1 : 0);
    }

    add(other) {
      const [a, b, scale] = this.align(other);
      return new DecimalValue(a + b, scale);
    }

    sub(other) {
      const [a, b, scale] = this.align(other);
      return new DecimalValue(a - b, scale);
    }

    mul(other) {
      const b = DecimalValue.parse(other);
      return new DecimalValue(this.coefficient * b.coefficient, this.scale + b.scale);
    }

    mulInt(integer) {
      if (!Number.isSafeInteger(integer)) fail('integer_required', 'Multiplicador deve ser inteiro seguro.');
      return new DecimalValue(this.coefficient * BigInt(integer), this.scale);
    }

    quantize(decimalPlaces, mode) {
      if (!Number.isInteger(decimalPlaces) || decimalPlaces < 0 || decimalPlaces > 12) {
        fail('rounding_places', 'decimal_places inválido.');
      }
      const rounding = mode || 'ROUND_HALF_UP';
      const allowed = new Set(['ROUND_HALF_UP', 'ROUND_HALF_EVEN', 'ROUND_DOWN', 'ROUND_FLOOR', 'ROUND_CEILING']);
      if (!allowed.has(rounding)) fail('rounding_mode', 'Modo de arredondamento não suportado: ' + rounding);
      if (this.scale <= decimalPlaces) {
        return new DecimalValue(this.coefficient * pow10(decimalPlaces - this.scale), decimalPlaces);
      }
      const divisor = pow10(this.scale - decimalPlaces);
      const negative = this.coefficient < 0n;
      const abs = negative ? -this.coefficient : this.coefficient;
      let q = abs / divisor;
      const r = abs % divisor;
      let increment = false;
      if (r !== 0n) {
        if (rounding === 'ROUND_HALF_UP') increment = (r * 2n >= divisor);
        else if (rounding === 'ROUND_HALF_EVEN') {
          const doubled = r * 2n;
          increment = doubled > divisor || (doubled === divisor && q % 2n === 1n);
        } else if (rounding === 'ROUND_FLOOR') increment = negative;
        else if (rounding === 'ROUND_CEILING') increment = !negative;
      }
      if (increment) q += 1n;
      return new DecimalValue(negative ? -q : q, decimalPlaces);
    }

    max(other) {
      const b = DecimalValue.parse(other);
      return this.compare(b) >= 0 ? this : b;
    }

    min(other) {
      const b = DecimalValue.parse(other);
      return this.compare(b) <= 0 ? this : b;
    }

    isNegative() {
      return this.coefficient < 0n;
    }

    toString() {
      const negative = this.coefficient < 0n;
      let digits = (negative ? -this.coefficient : this.coefficient).toString();
      if (this.scale === 0) return (negative ? '-' : '') + digits;
      if (digits.length <= this.scale) digits = '0'.repeat(this.scale - digits.length + 1) + digits;
      const split = digits.length - this.scale;
      return (negative ? '-' : '') + digits.slice(0, split) + '.' + digits.slice(split);
    }

    toFixed(decimalPlaces, mode) {
      const q = this.quantize(decimalPlaces, mode || 'ROUND_HALF_UP');
      if (decimalPlaces === 0) return q.toString().split('.')[0];
      const raw = q.toString();
      const negative = raw.startsWith('-');
      const plain = negative ? raw.slice(1) : raw;
      const parts = plain.split('.');
      const whole = parts[0];
      const fraction = (parts[1] || '').padEnd(decimalPlaces, '0');
      return (negative ? '-' : '') + whole + '.' + fraction;
    }
  }

  const ZERO = DecimalValue.parse('0');

  function nonNegative(value, name) {
    const d = DecimalValue.parse(value, name);
    if (d.isNegative()) fail('negative_amount', (name || 'value') + ' não pode ser negativo.');
    return d;
  }

  function roundingPolicy(rule) {
    const p = rule && rule.rounding_policy;
    if (!p || !Number.isInteger(p.decimal_places) || typeof p.mode !== 'string' || typeof p.stage !== 'string') {
      fail('rounding_policy_missing', 'Regra ' + (rule && rule.rule_id ? rule.rule_id : '?') + ' não possui rounding_policy executável.');
    }
    return p;
  }

  function quantize(value, policy) {
    return DecimalValue.parse(value).quantize(policy.decimal_places, policy.mode);
  }

  function normalizeDate(value, name) {
    const label = name || 'date';
    if (typeof value === 'string' && ISO_DATE_RE.test(value)) return value;
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      const y = value.getUTCFullYear();
      const m = String(value.getUTCMonth() + 1).padStart(2, '0');
      const d = String(value.getUTCDate()).padStart(2, '0');
      return y + '-' + m + '-' + d;
    }
    fail('date_required', label + ' deve ser YYYY-MM-DD ou Date válida.');
  }

  function assertRelease(release, consumer) {
    if (!release || typeof release !== 'object' || Array.isArray(release)) fail('release_shape', 'Release fiscal ausente ou inválida.');
    if (release.schema_version !== SCHEMA_VERSION) fail('schema_mismatch', 'Schema fiscal incompatível.');
    if (release.contract_id !== CONTRACT_ID) fail('contract_mismatch', 'Contrato fiscal incompatível.');
    if (!RELEASE_ID_RE.test(String(release.release_id || ''))) fail('release_id', 'release_id fiscal inválido.');
    if (release.status !== 'PUBLISHED') fail('release_status', 'Somente release PUBLISHED pode ser consumida.');

    const cc = release.consumer_compatibility;
    if (!cc || cc.schema_version !== SCHEMA_VERSION || cc.contract_api_version !== CONTRACT_API_VERSION ||
        cc.schema_match !== 'exact' || cc.contract_api_match !== 'exact' || cc.unknown_fields !== 'reject' ||
        cc.unknown_payload_types !== 'reject' || cc.unsupported_behavior !== 'hard_fail') {
      fail('consumer_compatibility', 'Contrato não declara compatibilidade exata exigida pelo folha-core.');
    }
    if (!Array.isArray(cc.consumers) || cc.consumers.length !== 4 || !cc.consumers.every((id) => SUPPORTED_CONSUMERS.has(id))) {
      fail('consumer_inventory', 'Inventário de consumidores incompatível.');
    }
    if (consumer && !cc.consumers.includes(consumer)) fail('consumer_not_declared', 'Consumidor ' + consumer + ' não declarado pela release.');

    if (!release.lifecycle || !release.lifecycle.validated_at_utc || !release.lifecycle.published_at_utc ||
        !['HUMAN_REVIEWED', 'AUTO_VALIDATED'].includes(release.lifecycle.approval_mode) ||
        !release.lifecycle.approval_reference ||
        (Array.isArray(release.lifecycle.block_reasons) && release.lifecycle.block_reasons.length)) {
      fail('release_lifecycle', 'Lifecycle da release não é consumível.');
    }

    if (!Array.isArray(release.rules) || release.rules.length !== 32) fail('rule_inventory', 'Release fiscal deve conter exatamente 32 regras.');
    const seen = new Set();
    for (const rule of release.rules) {
      if (!rule || typeof rule !== 'object' || Array.isArray(rule)) fail('rule_shape', 'Regra fiscal inválida.');
      if (typeof rule.rule_id !== 'string' || seen.has(rule.rule_id)) fail('rule_identity', 'rule_id ausente ou duplicado.');
      seen.add(rule.rule_id);
      if (!SEMVER_RE.test(String(rule.rule_version || ''))) fail('rule_version', 'rule_version inválida em ' + rule.rule_id);
      if (!Array.isArray(rule.calculators) || !rule.calculators.length) fail('rule_consumers', 'Regra sem calculators: ' + rule.rule_id);
      if (!Array.isArray(rule.contexts) || !rule.contexts.length || !rule.contexts.every((c) => SUPPORTED_CONTEXTS.has(c))) {
        fail('rule_contexts', 'Contextos inválidos em ' + rule.rule_id);
      }
      if (!rule.vigency || !ISO_DATE_RE.test(String(rule.vigency.effective_from || '')) ||
          (rule.vigency.effective_until !== null && rule.vigency.effective_until !== undefined && !ISO_DATE_RE.test(String(rule.vigency.effective_until)))) {
        fail('rule_vigency', 'Vigência inválida em ' + rule.rule_id);
      }
      if (!rule.quality || rule.quality.status !== 'VALIDATED') fail('rule_quality', 'Regra não VALIDATED: ' + rule.rule_id);
      if (!rule.payload || typeof rule.payload.type !== 'string' || !SUPPORTED_PAYLOAD_TYPES.has(rule.payload.type)) {
        fail('payload_type', 'Payload não suportado em ' + rule.rule_id);
      }
    }
    return release;
  }

  function ruleCovers(rule, date) {
    return date >= rule.vigency.effective_from && (!rule.vigency.effective_until || date <= rule.vigency.effective_until);
  }

  function selectRule(release, request) {
    const req = request || {};
    const consumer = String(req.consumer || '');
    const context = String(req.context || '');
    const ruleId = String(req.ruleId || '');
    const targetDate = normalizeDate(req.targetDate, 'targetDate');
    if (!SUPPORTED_CONSUMERS.has(consumer)) fail('consumer_required', 'consumer deve ser H26, H27, H28 ou H29.');
    if (!SUPPORTED_CONTEXTS.has(context)) fail('context_required', 'Assessment context não suportado: ' + context);
    assertRelease(release, consumer);

    const matches = release.rules.filter((rule) =>
      rule.rule_id === ruleId &&
      rule.calculators.includes(consumer) &&
      rule.contexts.includes(context) &&
      ruleCovers(rule, targetDate)
    );
    if (matches.length !== 1) {
      fail('rule_selection', 'Esperada exatamente uma regra para ' + ruleId + '/' + consumer + '/' + context + '/' + targetDate + '; encontradas ' + matches.length + '.');
    }
    const rule = matches[0];
    if (rule.quality.status !== 'VALIDATED') fail('rule_quality', 'Regra selecionada não está VALIDATED.');
    return Object.freeze({
      release,
      rule,
      consumer,
      context,
      target_date: targetDate,
      audit: Object.freeze({
        release_id: release.release_id,
        rule_id: rule.rule_id,
        rule_version: rule.rule_version,
        context,
        target_date: targetDate,
        effective_from: rule.vigency.effective_from,
        effective_until: rule.vigency.effective_until || null
      })
    });
  }

  const IRRF_ASSESSMENTS = Object.freeze({
    monthly: Object.freeze({
      allowed_origins: Object.freeze(['monthly', 'termination']),
      rule_context: 'monthly',
      reduction_rule_id: 'irrf.reduction.2026',
      reduction_input_semantic: 'taxable_income_subject_to_monthly_incidence_before_irrf_deductions'
    }),
    thirteenth: Object.freeze({
      allowed_origins: Object.freeze(['thirteenth', 'termination']),
      rule_context: 'thirteenth',
      reduction_rule_id: 'thirteenth.irrf.reduction.2026',
      reduction_input_semantic: 'thirteenth_taxable_income_before_irrf_deductions'
    }),
    vacation: Object.freeze({
      allowed_origins: Object.freeze(['vacation_enjoyed']),
      rule_context: 'vacation_enjoyed',
      reduction_rule_id: 'irrf.reduction.2026',
      reduction_input_semantic: 'taxable_income_subject_to_monthly_incidence_before_irrf_deductions'
    })
  });

  function assessmentIdentity(incomeType, originContext) {
    const type = String(incomeType || '');
    const origin = String(originContext || '');
    const spec = IRRF_ASSESSMENTS[type];
    if (!spec || !spec.allowed_origins.includes(origin)) {
      fail('assessment_identity', 'IRRF income type incompatível com origin context: ' + type + '/' + origin);
    }
    return Object.freeze({
      income_type: type,
      origin_context: origin,
      rule_context: spec.rule_context,
      reduction_rule_id: spec.reduction_rule_id,
      reduction_input_semantic: spec.reduction_input_semantic
    });
  }

  function competenceBasisFor(selected) {
    const competence = selected && selected.rule && selected.rule.competence;
    if (!competence || typeof competence.basis !== 'string') fail('competence_missing', 'Competence policy ausente.');
    const overrides = competence.context_overrides || {};
    return Object.prototype.hasOwnProperty.call(overrides, selected.context) ? overrides[selected.context] : competence.basis;
  }

  function ruleAudit(selected) {
    if (!selected || !selected.audit) fail('selected_rule_required', 'Regra selecionada obrigatória para audit trail.');
    return selected.audit;
  }

  function executeProgressive(baseInput, selected) {
    const rule = selected && selected.rule;
    if (!rule || !rule.payload || rule.payload.type !== 'progressive_table') fail('progressive_payload', 'Regra selecionada não é progressive_table.');
    const payload = rule.payload;
    const policy = roundingPolicy(rule);
    let inputBase = nonNegative(baseInput, 'base');
    let assessedBase = inputBase;
    if (payload.cap_base !== null && payload.cap_base !== undefined) assessedBase = assessedBase.min(nonNegative(payload.cap_base, 'cap_base'));
    if (!Array.isArray(payload.brackets) || !payload.brackets.length) fail('progressive_brackets', 'Tabela progressiva sem faixas.');

    if (payload.calculation_method === 'rate_times_base_minus_deduction') {
      const bracket = payload.brackets.find((b) => b.upper_bound === null || b.upper_bound === undefined || assessedBase.compare(DecimalValue.parse(b.upper_bound, 'upper_bound')) <= 0);
      if (!bracket) fail('progressive_bracket_missing', 'Nenhuma faixa cobre a base avaliada.');
      const raw = assessedBase.mul(DecimalValue.parse(bracket.rate, 'rate')).sub(DecimalValue.parse(bracket.deduction || '0', 'deduction')).max(ZERO);
      return Object.freeze({
        input_base: inputBase.toString(),
        assessed_base: assessedBase.toString(),
        amount: quantize(raw, policy).toFixed(policy.decimal_places, policy.mode),
        selected_rate: DecimalValue.parse(bracket.rate).toString(),
        selected_deduction: DecimalValue.parse(bracket.deduction || '0').toString(),
        components: Object.freeze([]),
        audit: ruleAudit(selected)
      });
    }

    if (payload.calculation_method !== 'marginal_by_bracket') {
      fail('progressive_method', 'Método progressivo não suportado: ' + payload.calculation_method);
    }

    let lower = ZERO;
    let total = ZERO;
    const components = [];
    for (const bracket of payload.brackets) {
      const upper = bracket.upper_bound === null || bracket.upper_bound === undefined ? null : DecimalValue.parse(bracket.upper_bound, 'upper_bound');
      const segmentUpper = upper === null ? assessedBase : assessedBase.min(upper);
      const taxable = segmentUpper.sub(lower).max(ZERO);
      if (taxable.compare(ZERO) > 0) {
        const rawComponent = taxable.mul(DecimalValue.parse(bracket.rate, 'rate'));
        const componentAmount = policy.stage === 'per_component' ? quantize(rawComponent, policy) : rawComponent;
        components.push(Object.freeze({
          lower_bound: lower.toString(),
          upper_bound: upper ? upper.toString() : null,
          taxable_amount: taxable.toString(),
          rate: DecimalValue.parse(bracket.rate).toString(),
          amount: componentAmount.toFixed(policy.decimal_places, policy.mode)
        }));
        total = total.add(componentAmount);
      }
      if (upper === null || assessedBase.compare(upper) <= 0) break;
      lower = upper;
    }
    return Object.freeze({
      input_base: inputBase.toString(),
      assessed_base: assessedBase.toString(),
      amount: quantize(total, policy).toFixed(policy.decimal_places, policy.mode),
      selected_rate: null,
      selected_deduction: null,
      components: Object.freeze(components),
      audit: ruleAudit(selected)
    });
  }

  function executeAffineReduction(inputIncome, preReductionTax, selected) {
    const rule = selected && selected.rule;
    if (!rule || !rule.payload || rule.payload.type !== 'affine_reduction') fail('reduction_payload', 'Regra selecionada não é affine_reduction.');
    const p = rule.payload;
    const policy = roundingPolicy(rule);
    const income = nonNegative(inputIncome, 'input_income');
    const preTax = nonNegative(preReductionTax, 'pre_reduction_tax');
    const full = nonNegative(p.full_relief_income_limit, 'full_relief_income_limit');
    const phaseout = nonNegative(p.phaseout_income_limit, 'phaseout_income_limit');
    const maxReduction = nonNegative(p.max_reduction, 'max_reduction');
    let statutory;
    if (income.compare(full) <= 0) statutory = maxReduction;
    else if (income.compare(phaseout) <= 0) {
      statutory = DecimalValue.parse(p.intercept, 'intercept').sub(DecimalValue.parse(p.slope, 'slope').mul(income));
      if (p.floor_at_zero === true) statutory = statutory.max(ZERO);
    } else statutory = ZERO;
    statutory = quantize(statutory, policy);
    const applied = quantize(preTax.min(statutory), policy);
    const finalTax = quantize(preTax.sub(applied).max(ZERO), policy);
    return Object.freeze({
      input_income: income.toString(),
      statutory_reduction: statutory.toFixed(policy.decimal_places, policy.mode),
      applied_reduction: applied.toFixed(policy.decimal_places, policy.mode),
      final_tax: finalTax.toFixed(policy.decimal_places, policy.mode),
      audit: ruleAudit(selected)
    });
  }

  function scalarValue(selected, expectedUnit) {
    const rule = selected && selected.rule;
    if (!rule || !rule.payload || rule.payload.type !== 'scalar') fail('scalar_payload', 'Regra selecionada não é scalar.');
    if (expectedUnit && rule.payload.unit !== expectedUnit) fail('scalar_unit', 'Unidade escalar incompatível em ' + rule.rule_id);
    return DecimalValue.parse(rule.payload.value, rule.rule_id + '.value');
  }

  function selectIrrfBundle(release, request) {
    const req = request || {};
    const assessment = assessmentIdentity(req.incomeType, req.originContext);
    const shared = { consumer: req.consumer, targetDate: req.targetDate, context: assessment.rule_context };
    const table = selectRule(release, Object.assign({ ruleId: 'irrf.monthly.progressive_table' }, shared));
    const reduction = selectRule(release, Object.assign({ ruleId: assessment.reduction_rule_id }, shared));
    const dependent = selectRule(release, Object.assign({ ruleId: 'irrf.dependent_deduction' }, shared));
    const simplified = selectRule(release, Object.assign({ ruleId: 'irrf.simplified_monthly_discount' }, shared));
    if (table.rule.payload.type !== 'progressive_table') fail('irrf_table_payload', 'IRRF table não é progressive_table.');
    if (reduction.rule.payload.type !== 'affine_reduction') fail('irrf_reduction_payload', 'IRRF reduction não é affine_reduction.');
    if (reduction.rule.payload.input_semantic !== assessment.reduction_input_semantic) {
      fail('irrf_reduction_semantic', 'input_semantic do redutor não corresponde ao assessment selecionado.');
    }
    scalarValue(dependent, 'BRL_per_dependent');
    scalarValue(simplified, 'BRL');
    return Object.freeze({ assessment, table, reduction, dependent, simplified });
  }

  function buildIrrfLegalDeductions(bundle, request) {
    const req = request || {};
    const dependentCount = req.dependentCount;
    if (!Number.isInteger(dependentCount) || dependentCount < 0) fail('dependent_count', 'dependentCount deve ser inteiro não negativo.');
    if (req.pension === undefined || req.pension === null) fail('pension_required', 'pension deve ser informado explicitamente, inclusive 0.00.');
    const social = nonNegative(req.socialSecurity, 'social_security');
    const pension = nonNegative(req.pension, 'pension');
    const unit = scalarValue(bundle.dependent, 'BRL_per_dependent');
    const dependents = unit.mulInt(dependentCount);
    const total = social.add(dependents).add(pension);
    return Object.freeze({
      social_security: social.toString(),
      dependent_count: dependentCount,
      dependent_unit: unit.toString(),
      dependents: dependents.toString(),
      pension: pension.toString(),
      total: total.toString(),
      audit: Object.freeze([ruleAudit(bundle.dependent)])
    });
  }

  function assessIrrf(release, request) {
    const req = request || {};
    const bundle = selectIrrfBundle(release, req);
    const deductions = buildIrrfLegalDeductions(bundle, {
      socialSecurity: req.socialSecurity,
      dependentCount: req.dependentCount,
      pension: req.pension
    });
    const gross = nonNegative(req.grossTaxableIncome, 'gross_taxable_income');
    const legalTotal = DecimalValue.parse(deductions.total);
    const simplified = scalarValue(bundle.simplified, 'BRL');
    const useSimplified = simplified.compare(legalTotal) > 0;
    const selectedDeduction = useSimplified ? simplified : legalTotal;
    const taxBase = gross.sub(selectedDeduction).max(ZERO);
    const pre = executeProgressive(taxBase.toString(), bundle.table);
    const reduction = executeAffineReduction(gross.toString(), pre.amount, bundle.reduction);
    return Object.freeze({
      assessment: bundle.assessment,
      deduction_components: deductions,
      gross_taxable_income: gross.toString(),
      legal_deductions: legalTotal.toString(),
      simplified_discount: simplified.toString(),
      deduction_mode: useSimplified ? 'simplified' : 'legal',
      irrf_tax_base: taxBase.toString(),
      pre_reduction_irrf: pre.amount,
      reduction_input_income: gross.toString(),
      reduction_amount: reduction.applied_reduction,
      final_irrf: reduction.final_tax,
      audit: Object.freeze({
        release_id: release.release_id,
        rules: Object.freeze([
          ruleAudit(bundle.table),
          ruleAudit(bundle.reduction),
          ruleAudit(bundle.dependent),
          ruleAudit(bundle.simplified)
        ])
      })
    });
  }

  function assessInss(release, request) {
    const req = request || {};
    const selected = selectRule(release, {
      ruleId: 'inss.employee.progressive_table',
      consumer: req.consumer,
      context: req.context,
      targetDate: req.targetDate
    });
    const result = executeProgressive(req.base, selected);
    return Object.freeze(Object.assign({}, result, {
      audit: Object.freeze({ release_id: release.release_id, rules: Object.freeze([ruleAudit(selected)]) })
    }));
  }

  function deepFreeze(value) {
    if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
    Object.freeze(value);
    for (const key of Object.keys(value)) deepFreeze(value[key]);
    return value;
  }

  async function fetchRelease(options) {
    const opts = options || {};
    const consumer = opts.consumer ? String(opts.consumer) : null;
    if (SFA._release && !opts.force) return assertRelease(SFA._release, consumer);
    if (typeof root.fetch !== 'function') fail('fetch_unavailable', 'fetch indisponível neste runtime.');
    const response = await root.fetch(RELEASE_ENDPOINT, {
      credentials: 'same-origin',
      mode: 'same-origin',
      cache: 'no-cache',
      headers: { Accept: 'application/json' }
    });
    if (!response || !response.ok) fail('release_http', 'Falha ao carregar release fiscal: HTTP ' + (response ? response.status : 'unknown'));
    const release = await response.json();
    assertRelease(release, consumer);
    const headerId = response.headers && typeof response.headers.get === 'function'
      ? response.headers.get('X-Sanida-Fiscal-Release')
      : null;
    if (headerId && headerId !== release.release_id) fail('release_header_mismatch', 'Header de release diverge do payload.');
    SFA._release = deepFreeze(release);
    return SFA._release;
  }

  function normalizeMoneyInput(value) {
    if (value === null || value === undefined || value === '') return '0';
    if (typeof value === 'number') {
      if (!Number.isSafeInteger(value)) fail('binary_float_rejected', 'Entrada monetária deve permanecer decimal textual.');
      return String(value);
    }
    let s = String(value).trim().replace(/R\$/gi, '').replace(/\s+/g, '');
    if (s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
    s = s.replace(/[^0-9+\-.]/g, '');
    return DecimalValue.parse(s || '0', 'money_input').toString();
  }

  function formatBrl(value) {
    const d = DecimalValue.parse(value).quantize(2, 'ROUND_HALF_UP');
    const fixed = d.toFixed(2, 'ROUND_HALF_UP');
    const negative = fixed.startsWith('-');
    const plain = negative ? fixed.slice(1) : fixed;
    const parts = plain.split('.');
    const grouped = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return (negative ? '-R$ ' : 'R$ ') + grouped + ',' + parts[1];
  }

  SFA.VERSION = '2.4.0-c62';
  SFA.releaseEndpoint = RELEASE_ENDPOINT;
  SFA.FiscalContractError = FiscalContractError;
  SFA.Decimal = DecimalValue;
  SFA.normalizeDate = normalizeDate;
  SFA.normalizeMoneyInput = normalizeMoneyInput;
  SFA.brl = formatBrl;
  SFA.assertRelease = assertRelease;
  SFA.fetchRelease = fetchRelease;
  SFA.selectRule = selectRule;
  SFA.ruleAudit = ruleAudit;
  SFA.competenceBasisFor = competenceBasisFor;
  SFA.assessmentIdentity = assessmentIdentity;
  SFA.executeProgressive = executeProgressive;
  SFA.executeAffineReduction = executeAffineReduction;
  SFA.scalarValue = scalarValue;
  SFA.selectIrrfBundle = selectIrrfBundle;
  SFA.buildIrrfLegalDeductions = buildIrrfLegalDeductions;
  SFA.assessIrrf = assessIrrf;
  SFA.assessInss = assessInss;

  Object.freeze(IRRF_ASSESSMENTS);
})(typeof window !== 'undefined' ? window : globalThis);
