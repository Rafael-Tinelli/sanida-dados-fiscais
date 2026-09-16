(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA || !SFA.TERMINATION) return;

  const CONSUMER = 'H29';

  function parseCivilDate(value) {
    const raw = String(value === undefined || value === null ? '' : value).trim();
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
    if (!match) return null;
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const probe = new Date(Date.UTC(year, month - 1, day));
    if (probe.getUTCFullYear() !== year || probe.getUTCMonth() + 1 !== month || probe.getUTCDate() !== day) {
      return null;
    }
    return Object.freeze({ iso: raw, year, month, day });
  }

  function suggestedDaysCounted(employmentStart, terminationDate) {
    const rawStart = String(employmentStart === undefined || employmentStart === null ? '' : employmentStart).trim();
    const start = parseCivilDate(rawStart);
    const end = parseCivilDate(terminationDate);
    if (!end || (rawStart && !start)) return null;
    if (start && start.iso > end.iso) return null;
    if (start && start.year === end.year && start.month === end.month) {
      return end.day - start.day + 1;
    }
    return end.day;
  }

  function calculateH29(release, input) {
    return SFA.TERMINATION.calculate(release, input || {});
  }

  SFA.H29 = Object.freeze({
    calculate: calculateH29,
    suggestedDaysCounted
  });

  const page = root.document && root.document.getElementById('calc-rescisao-clt');
  if (!page) return;

  const form = page.querySelector('form');
  const alertBox = page.querySelector('[data-alert]');
  const result = page.querySelector('[data-result]');
  const reasonField = form && form.querySelector('[name="motivo_esocial"]');
  const employmentStartField = form && form.querySelector('[name="data_admissao"]');
  const terminationDateField = form && form.querySelector('[name="data_desligamento"]');
  const terminationRemunerationField = form && form.querySelector('[name="remuneracao_mes_desligamento"]');

  const SCOPE_LABELS = Object.freeze({
    salary_balance: 'Saldo de salário',
    thirteenth_proportional: '13º proporcional, quando devido pelo motivo',
    vacation_proportional: 'Avos de férias proporcionais, quando devidos pelo motivo',
    acquired_or_overdue_vacation: 'Sinalização de férias adquiridas/vencidas, sem valor automático',
    notice_pay_or_notice_discount: 'Aviso prévio ou desconto de aviso',
    fgts_termination_fine: 'Multa rescisória do FGTS',
    fgts_withdrawal: 'Saque do FGTS',
    unemployment_insurance: 'Seguro-desemprego',
    stability_indemnities: 'Indenizações de estabilidade',
    collective_bargaining_specific_items: 'Verbas específicas de convenção/acordo coletivo',
    fixed_term_contract_termination_rules: 'Regras de contrato por prazo determinado',
    indirect_termination_without_judicially_resolved_context: 'Rescisão indireta sem contexto judicial resolvido',
    variable_termination_items_not_explicitly_modeled: 'Verbas rescisórias variáveis não modeladas'
  });

  function localTodayIso() {
    const now = new Date();
    return String(now.getFullYear()).padStart(4, '0') + '-' +
      String(now.getMonth() + 1).padStart(2, '0') + '-' +
      String(now.getDate()).padStart(2, '0');
  }

  function inputValue(name) {
    const field = form && form.querySelector('[name="' + name + '"]');
    return field ? field.value : '';
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

  function scopeLabel(item) {
    return SCOPE_LABELS[item] || item;
  }

  function renderList(selector, items) {
    const list = page.querySelector(selector);
    if (!list) return;
    list.textContent = '';
    for (const item of items || []) {
      const li = root.document.createElement('li');
      li.textContent = scopeLabel(item);
      list.appendChild(li);
    }
  }

  function moneyOrDash(value) {
    if (value === null || value === undefined) return '—';
    return SFA.brl(value);
  }

  function updateReasonInputs() {
    if (!reasonField || !terminationRemunerationField) return;
    const needsProportional = reasonField.value !== '01';
    terminationRemunerationField.required = needsProportional;
    terminationRemunerationField.closest('[data-conditional="thirteenth"]')?.classList.toggle('is-muted', !needsProportional);
  }

  function syncDaysWithDates() {
    if (!terminationDateField || !form) return;
    const days = form.querySelector('[name="dias_computados"]');
    if (!days || days.dataset.userEdited === 'true') return;
    const suggested = suggestedDaysCounted(
      employmentStartField ? employmentStartField.value : '',
      terminationDateField.value
    );
    days.value = suggested === null ? '' : String(suggested);
  }

  function messageForError(error) {
    if (error && error.code === 'termination_required_input') {
      return 'Informe o salário-base mensal e, quando houver 13º proporcional, a remuneração de referência do mês da extinção.';
    }
    if (error && error.code === 'termination_positive_input') {
      return 'As bases monetárias usadas pelo H29 devem ser maiores que zero. Revise o salário-base e a remuneração de referência do 13º.';
    }
    return 'Não foi possível calcular este caso dentro do escopo H29. Confira motivo eSocial, datas, regime, prazo contratual e bases informadas. Casos fora do modelo suportado são bloqueados em vez de estimados por aproximação.';
  }

  async function run() {
    clearError();
    setBusy(true);
    try {
      const release = await SFA.fetchRelease({ consumer: CONSUMER });
      const calculation = SFA.H29.calculate(release, {
        esocialReason: inputValue('motivo_esocial'),
        employmentRegime: inputValue('regime_emprego'),
        contractTerm: inputValue('prazo_contrato'),
        employmentStart: inputValue('data_admissao'),
        terminationDate: inputValue('data_desligamento'),
        monthlyBaseSalary: inputValue('salario_base_mensal'),
        daysCountedThroughTermination: inputValue('dias_computados'),
        terminationMonthRemuneration: inputValue('remuneracao_mes_desligamento')
      });

      const salary = calculation.salary_balance;
      const thirteenth = calculation.thirteenth_proportional;
      const vacation = calculation.vacation_proportional;
      const acquired = calculation.acquired_or_overdue_vacation;

      setText('[data-kpi="saldo"]', salary ? SFA.brl(salary.amount) : '—');
      setText('[data-kpi="decimo"]', thirteenth ? SFA.brl(thirteenth.gross_thirteenth) : 'Não devido');
      setText('[data-kpi="ferias"]', vacation ? String(vacation.twelfths) + '/12' : 'Não devidas');
      setText('[data-row="motivo"]', calculation.reason.code + ' — ' + calculation.reason.label);
      setText('[data-row="saldo-formula"]', salary ? salary.days_counted_through_termination + '/' + salary.calendar_days_in_month + ' × ' + SFA.brl(salary.monthly_base_salary) : '—');
      setText('[data-row="saldo"]', salary ? SFA.brl(salary.amount) : '—');
      setText('[data-row="13-avos"]', thirteenth ? String(thirteenth.twelfths) + '/12' : 'Não devido pelo motivo');
      setText('[data-row="13-referencia"]', thirteenth ? SFA.brl(thirteenth.reference_remuneration) : '—');
      setText('[data-row="13-bruto"]', thirteenth ? SFA.brl(thirteenth.gross_thirteenth) : '—');
      setText('[data-row="ferias-periodo"]', vacation ? vacation.period.start + ' a ' + vacation.period.end_inclusive : '—');
      setText('[data-row="ferias-avos"]', vacation ? String(vacation.twelfths) + '/12' : 'Não devidas pelo motivo');
      setText('[data-row="ferias-adquiridas"]', acquired.potentially_due ? 'Podem ser devidas; valor não automatizado neste escopo' : 'Não sinalizadas pela matriz');
      setText('[data-row="promessa"]', calculation.result_promise === 'partial_estimate' ? 'Estimativa parcial' : calculation.result_promise);
      setText('[data-row="referencia"]', calculation.reference_date);
      setText('[data-row="release-id"]', calculation.fiscal_metadata.release_id);
      renderList('[data-list="incluidos"]', calculation.included_items);
      renderList('[data-list="excluidos"]', calculation.excluded_items);

      if (result) {
        result.dataset.releaseId = calculation.fiscal_metadata.release_id;
        result.dataset.referenceDate = calculation.reference_date;
        result.style.display = 'block';
      }
    } catch (error) {
      if (root.console && typeof root.console.error === 'function') root.console.error('[H29]', error);
      showError(messageForError(error));
      if (result) result.style.display = 'none';
    } finally {
      setBusy(false);
    }
  }

  if (reasonField) {
    reasonField.addEventListener('change', updateReasonInputs);
    updateReasonInputs();
  }
  if (terminationDateField) {
    terminationDateField.value = terminationDateField.value || localTodayIso();
    terminationDateField.addEventListener('change', syncDaysWithDates);
  }
  if (employmentStartField) {
    employmentStartField.addEventListener('change', syncDaysWithDates);
  }
  syncDaysWithDates();
  if (form) {
    const days = form.querySelector('[name="dias_computados"]');
    if (days) days.addEventListener('input', function () { days.dataset.userEdited = 'true'; });
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      run();
    });
  }
})(typeof window !== 'undefined' ? window : globalThis);
