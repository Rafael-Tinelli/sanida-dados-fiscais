(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA) return;

  const CONSUMER = 'H26';
  const CONTEXT = 'monthly';
  const ZERO = SFA.Decimal.parse('0');
  const INPUT_ERROR_CODES = new Set([
    'h26_negative_input',
    'h26_dependents',
    'h26_salary_required',
    'decimal_format',
    'decimal_type',
    'negative_amount'
  ]);
  const FISCAL_ERROR_CODES = new Set([
    'fetch_unavailable',
    'schema_mismatch',
    'contract_mismatch',
    'consumer_compatibility',
    'consumer_inventory',
    'consumer_not_declared',
    'rule_inventory',
    'rule_selection',
    'rule_quality',
    'payload_type',
    'rounding_policy_missing',
    'irrf_reduction_semantic'
  ]);

  function h26Error(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function nonNegativeMoney(value, name) {
    const decimal = SFA.Decimal.parse(SFA.normalizeMoneyInput(value), name);
    if (decimal.compare(ZERO) < 0) h26Error('h26_negative_input', name + ' não pode ser negativo.');
    return decimal;
  }

  function dependentCount(value) {
    if (value === null || value === undefined || String(value).trim() === '') return 0;
    const raw = String(value).trim();
    if (!/^\d+$/.test(raw)) h26Error('h26_dependents', 'Dependentes deve ser um inteiro não negativo.');
    const count = Number(raw);
    if (!Number.isSafeInteger(count)) h26Error('h26_dependents', 'Quantidade de dependentes fora do intervalo suportado.');
    return count;
  }

  function localTodayIso() {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, '0');
    const d = String(now.getDate()).padStart(2, '0');
    return y + '-' + m + '-' + d;
  }

  function ruleKey(audit) {
    return audit.rule_id + '@' + audit.rule_version;
  }

  function uniqueRuleAudits(audits) {
    const seen = new Set();
    const result = [];
    for (const audit of audits) {
      const key = ruleKey(audit);
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(audit);
    }
    return Object.freeze(result);
  }

  function errorMessageFor(error) {
    const code = error && typeof error.code === 'string' ? error.code : '';
    if (INPUT_ERROR_CODES.has(code)) {
      return 'Revise os dados informados. O salário bruto deve ser maior que zero, dependentes devem ser inteiros não negativos e valores monetários não podem ser negativos.';
    }
    if (code.startsWith('release_') || FISCAL_ERROR_CODES.has(code)) {
      return 'A release fiscal necessária não pôde ser carregada ou validada para esta data. Tente novamente em instantes.';
    }
    return 'Não foi possível concluir o cálculo com os dados informados. Revise os campos e tente novamente.';
  }

  function calculateH26(release, input) {
    const params = input || {};
    const targetDate = SFA.normalizeDate(params.targetDate, 'targetDate');
    const salary = nonNegativeMoney(params.salary, 'salary');
    const variables = nonNegativeMoney(params.variables, 'variables');
    const pension = nonNegativeMoney(params.pension, 'pension');
    const otherDeductions = nonNegativeMoney(params.otherDeductions, 'other_deductions');
    const dependents = dependentCount(params.dependentCount);

    if (salary.compare(ZERO) <= 0) h26Error('h26_salary_required', 'Salário bruto mensal deve ser maior que zero.');

    const gross = salary.add(variables);
    const inss = SFA.assessInss(release, {
      consumer: CONSUMER,
      context: CONTEXT,
      targetDate,
      base: gross.toString()
    });
    const irrf = SFA.assessIrrf(release, {
      consumer: CONSUMER,
      targetDate,
      incomeType: 'monthly',
      originContext: CONTEXT,
      grossTaxableIncome: gross.toString(),
      socialSecurity: inss.amount,
      dependentCount: dependents,
      pension: pension.toString()
    });

    const inssAmount = SFA.Decimal.parse(inss.amount, 'inss_amount');
    const irrfAmount = SFA.Decimal.parse(irrf.final_irrf, 'irrf_amount');
    const totalDeductions = inssAmount.add(irrfAmount).add(pension).add(otherDeductions);
    const net = gross.sub(totalDeductions);

    const inssRule = SFA.selectRule(release, {
      ruleId: 'inss.employee.progressive_table',
      consumer: CONSUMER,
      context: CONTEXT,
      targetDate
    });
    const audits = uniqueRuleAudits([
      ...inss.audit.rules,
      ...irrf.audit.rules
    ]);

    return Object.freeze({
      consumer: CONSUMER,
      reference_date: targetDate,
      gross_remuneration: gross.toString(),
      inss: inss.amount,
      pension: pension.toString(),
      other_deductions: otherDeductions.toString(),
      total_deductions: totalDeductions.toString(),
      net_salary: net.toString(),
      irrf: Object.freeze({
        tax_base: irrf.irrf_tax_base,
        deduction_mode: irrf.deduction_mode,
        legal_deductions: irrf.legal_deductions,
        simplified_discount: irrf.simplified_discount,
        pre_reduction_irrf: irrf.pre_reduction_irrf,
        reduction_input_income: irrf.reduction_input_income,
        reduction_amount: irrf.reduction_amount,
        final_irrf: irrf.final_irrf
      }),
      fiscal_metadata: Object.freeze({
        release_id: release.release_id,
        inss_cap_base: inssRule.rule.payload.cap_base,
        rules: audits
      })
    });
  }

  SFA.H26 = Object.freeze({
    calculate: calculateH26,
    currentReferenceDate: localTodayIso,
    errorMessageFor
  });

  const page = root.document && root.document.getElementById('calc-salario-liquido');
  if (!page) return;

  const form = page.querySelector('form');
  const alertBox = page.querySelector('[data-alert]');
  const result = page.querySelector('[data-result]');

  if (alertBox) {
    alertBox.setAttribute('role', 'alert');
    alertBox.setAttribute('aria-live', 'assertive');
    alertBox.setAttribute('aria-atomic', 'true');
  }
  if (result) {
    result.setAttribute('role', 'status');
    result.setAttribute('aria-live', 'polite');
    result.setAttribute('aria-atomic', 'true');
  }

  function showError(message) {
    if (!alertBox) return;
    alertBox.textContent = message;
    alertBox.style.display = 'block';
  }

  function clearError() {
    if (!alertBox) return;
    alertBox.textContent = '';
    alertBox.style.display = 'none';
  }

  function setText(selector, value) {
    const element = page.querySelector(selector);
    if (element) element.textContent = value;
  }

  function inputValue(name) {
    const field = form && form.querySelector('[name="' + name + '"]');
    return field ? field.value : '';
  }

  function modeLabel(mode) {
    if (mode === 'legal') return 'Deduções legais';
    if (mode === 'simplified') return 'Desconto simplificado';
    return mode || '—';
  }

  function setBusy(busy) {
    if (!form) return;
    const button = form.querySelector('button[type="submit"]');
    if (button) button.disabled = Boolean(busy);
    form.setAttribute('aria-busy', busy ? 'true' : 'false');
  }

  async function run() {
    clearError();
    setBusy(true);
    try {
      const release = await SFA.fetchRelease({ consumer: CONSUMER });
      const calculation = calculateH26(release, {
        targetDate: localTodayIso(),
        salary: inputValue('salario'),
        variables: inputValue('variaveis'),
        dependentCount: inputValue('dependentes'),
        pension: inputValue('pensao'),
        otherDeductions: inputValue('outros')
      });

      setText('[data-kpi="bruto"]', SFA.brl(calculation.gross_remuneration));
      setText('[data-kpi="liquido"]', SFA.brl(calculation.net_salary));
      setText('[data-kpi="descontos"]', SFA.brl(calculation.total_deductions));
      setText('[data-row="inss"]', '- ' + SFA.brl(calculation.inss));
      setText('[data-row="irrf"]', '- ' + SFA.brl(calculation.irrf.final_irrf));
      setText('[data-row="pensao"]', SFA.Decimal.parse(calculation.pension).compare(ZERO) > 0 ? '- ' + SFA.brl(calculation.pension) : '—');
      setText('[data-row="outros"]', SFA.Decimal.parse(calculation.other_deductions).compare(ZERO) > 0 ? '- ' + SFA.brl(calculation.other_deductions) : '—');
      setText('[data-row="base-ir"]', SFA.brl(calculation.irrf.tax_base));
      setText('[data-row="ir-antes-reducao"]', SFA.brl(calculation.irrf.pre_reduction_irrf));
      setText('[data-row="renda-redutor"]', SFA.brl(calculation.irrf.reduction_input_income));
      setText('[data-row="modo"]', modeLabel(calculation.irrf.deduction_mode));
      setText('[data-row="reducao"]', SFA.Decimal.parse(calculation.irrf.reduction_amount).compare(ZERO) > 0 ? SFA.brl(calculation.irrf.reduction_amount) : '—');
      setText('[data-row="aliquotas"]', calculation.fiscal_metadata.inss_cap_base ? 'Base limitada a ' + SFA.brl(calculation.fiscal_metadata.inss_cap_base) : 'Sem teto declarado');
      setText('[data-row="ano"]', calculation.reference_date.slice(0, 4));
      setText('[data-row="referencia"]', calculation.reference_date);
      setText('[data-row="release-id"]', calculation.fiscal_metadata.release_id);

      if (result) {
        result.dataset.releaseId = calculation.fiscal_metadata.release_id;
        result.dataset.referenceDate = calculation.reference_date;
        result.style.display = 'block';
      }
    } catch (error) {
      if (root.console && typeof root.console.error === 'function') root.console.error('[H26]', error);
      showError(errorMessageFor(error));
      if (result) result.style.display = 'none';
    } finally {
      setBusy(false);
    }
  }

  if (form) {
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      run();
    });
  }
})(typeof window !== 'undefined' ? window : globalThis);
