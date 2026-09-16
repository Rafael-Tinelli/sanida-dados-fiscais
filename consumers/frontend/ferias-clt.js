(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA || !SFA.VACATION) return;

  const CONSUMER = 'H28';
  const ZERO = SFA.Decimal.parse('0');

  function h28Error(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function requirePaymentDate(value) {
    const raw = value === null || value === undefined ? '' : String(value).trim();
    if (!raw) {
      h28Error('h28_payment_date_required', 'Informe a data de pagamento das férias para definir a vigência fiscal usada no cálculo.');
    }
    const iso = SFA.normalizeDate(raw, 'data_pagamento');
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
    if (!match) h28Error('h28_payment_date_invalid', 'Informe uma data de pagamento válida.');
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const probe = new Date(Date.UTC(year, month - 1, day));
    if (probe.getUTCFullYear() !== year || probe.getUTCMonth() + 1 !== month || probe.getUTCDate() !== day) {
      h28Error('h28_payment_date_invalid', 'Informe uma data de pagamento válida.');
    }
    return iso;
  }

  function calculateH28(release, input) {
    const params = Object.assign({}, input || {});
    params.targetDate = requirePaymentDate(params.targetDate);
    return SFA.VACATION.calculate(release, params);
  }

  SFA.H28 = Object.freeze({
    calculate: calculateH28,
    requirePaymentDate
  });

  const page = root.document && root.document.getElementById('calc-ferias-clt');
  if (!page) return;
  const form = page.querySelector('form');
  const alertBox = page.querySelector('[data-alert]');
  const result = page.querySelector('[data-result]');

  function inputValue(name) {
    const field = form && form.querySelector('[name="' + name + '"]');
    if (!field) return '';
    if (field.type === 'checkbox') return field.checked;
    return field.value;
  }

  function setText(selector, value) {
    const element = page.querySelector(selector);
    if (element) element.textContent = value;
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

  function setBusy(busy) {
    if (!form) return;
    const button = form.querySelector('button[type="submit"]');
    if (button) button.disabled = Boolean(busy);
    form.setAttribute('aria-busy', busy ? 'true' : 'false');
  }

  function moneyOrDash(value) {
    return SFA.Decimal.parse(value).compare(ZERO) === 0 ? '—' : SFA.brl(value);
  }

  async function run() {
    clearError();
    setBusy(true);
    try {
      const targetDate = requirePaymentDate(inputValue('data_pagamento'));
      const release = await SFA.fetchRelease({ consumer: CONSUMER });
      const calculation = calculateH28(release, {
        targetDate,
        vacationPayBase: inputValue('base_ferias'),
        unjustifiedAbsences: inputValue('faltas_injustificadas'),
        sellOneThird: inputValue('vender_um_terco') === true,
        dependentCount: inputValue('dependentes'),
        pension: inputValue('pensao')
      });

      setText('[data-kpi="bruto"]', SFA.brl(calculation.components.gross_vacation_payment));
      setText('[data-kpi="liquido"]', SFA.brl(calculation.fiscal.net_vacation_payment));
      setText('[data-kpi="dias"]', String(calculation.entitlement.enjoyed_days) + ' dias');
      setText('[data-row="direito"]', String(calculation.entitlement.entitled_days) + ' dias');
      setText('[data-row="gozo"]', String(calculation.entitlement.enjoyed_days) + ' dias');
      setText('[data-row="abono-dias"]', calculation.entitlement.cash_allowance_days ? String(calculation.entitlement.cash_allowance_days) + ' dias' : '—');
      setText('[data-row="principal-gozo"]', SFA.brl(calculation.components.enjoyed_principal));
      setText('[data-row="terco-gozo"]', SFA.brl(calculation.components.enjoyed_constitutional_third));
      setText('[data-row="abono-principal"]', moneyOrDash(calculation.components.cash_allowance_principal));
      setText('[data-row="abono-terco"]', moneyOrDash(calculation.components.cash_allowance_constitutional_third));
      setText('[data-row="base-inss"]', SFA.brl(calculation.fiscal.social_security_base));
      setText('[data-row="inss"]', '- ' + SFA.brl(calculation.fiscal.inss));
      setText('[data-row="renda-ir"]', SFA.brl(calculation.fiscal.taxable_vacation_income));
      setText('[data-row="base-ir"]', SFA.brl(calculation.fiscal.irrf.tax_base));
      setText('[data-row="modo-ir"]', calculation.fiscal.irrf.deduction_mode === 'simplified' ? 'Desconto simplificado' : 'Deduções legais');
      setText('[data-row="ir-antes-reducao"]', SFA.brl(calculation.fiscal.irrf.pre_reduction_irrf));
      setText('[data-row="renda-redutor"]', SFA.brl(calculation.fiscal.irrf.reduction_input_income));
      setText('[data-row="reducao"]', moneyOrDash(calculation.fiscal.irrf.reduction_amount));
      setText('[data-row="irrf"]', '- ' + SFA.brl(calculation.fiscal.irrf.final_irrf));
      setText('[data-row="pensao"]', SFA.Decimal.parse(calculation.fiscal.pension).compare(ZERO) > 0 ? '- ' + SFA.brl(calculation.fiscal.pension) : '—');
      setText('[data-row="referencia"]', calculation.reference_date);
      setText('[data-row="release-id"]', calculation.fiscal_metadata.release_id);

      if (result) {
        result.dataset.releaseId = calculation.fiscal_metadata.release_id;
        result.dataset.referenceDate = calculation.reference_date;
        result.style.display = 'block';
      }
    } catch (error) {
      if (root.console && typeof root.console.error === 'function') root.console.error('[H28]', error);
      if (error && (error.code === 'h28_payment_date_required' || error.code === 'h28_payment_date_invalid')) {
        showError(error.message);
      } else {
        showError('Não foi possível calcular com uma release fiscal válida para os dados informados. Confira a base de férias, as faltas e a data de pagamento.');
      }
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
