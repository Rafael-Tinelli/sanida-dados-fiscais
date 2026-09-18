(function (root) {
  'use strict';

  const SFA = root.SFA_FOLHA;
  if (!SFA || !SFA.THIRTEENTH) return;

  const CONSUMER = 'H27';
  const ZERO = SFA.Decimal.parse('0');

  function h27Error(code, message, details) {
    throw new SFA.FiscalContractError(code, message, details || null);
  }

  function money(value, name) {
    const decimal = SFA.Decimal.parse(SFA.normalizeMoneyInput(value), name);
    if (decimal.compare(ZERO) < 0) h27Error('h27_negative_input', name + ' não pode ser negativo.');
    return decimal;
  }

  function count(value, name) {
    const raw = value === null || value === undefined || String(value).trim() === '' ? '0' : String(value).trim();
    if (!/^\d+$/.test(raw)) h27Error('h27_integer', name + ' deve ser inteiro não negativo.');
    const parsed = Number(raw);
    if (!Number.isSafeInteger(parsed)) h27Error('h27_integer', name + ' fora do intervalo suportado.');
    return parsed;
  }

  function uniqueAudits(audits) {
    const seen = new Set();
    const out = [];
    for (const item of audits) {
      if (!item) continue;
      const key = item.rule_id + '@' + item.rule_version + '@' + item.context;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(item);
    }
    return Object.freeze(out);
  }

  function yearOf(iso) {
    return Number(String(iso).slice(0, 4));
  }

  function balanceStatus(balance) {
    const comparison = balance.compare(ZERO);
    if (comparison > 0) return 'PAYABLE';
    if (comparison < 0) return 'INSUFFICIENT';
    return 'ZERO_BALANCE';
  }

  function insufficiencyOf(balance) {
    return balance.compare(ZERO) < 0 ? ZERO.sub(balance) : ZERO;
  }

  function buildSettlement(grossAmount, advance, fiscal, pension) {
    if (!advance || advance.amount === null) {
      return Object.freeze({
        status: 'PENDING_ADVANCE',
        balance_basis: fiscal ? 'net' : 'gross',
        gross_balance_before_deductions: null,
        deductions_total: fiscal ? null : '0',
        net_balance_before_floor: null,
        balance_before_floor: null,
        payable_second_installment_gross: null,
        payable_second_installment_net: null,
        insufficiency_amount: null
      });
    }

    const advanceAmount = SFA.Decimal.parse(advance.amount, 'advance_amount');
    const grossBalance = grossAmount.sub(advanceAmount);
    const payableGross = grossBalance.max(ZERO);
    let deductionsTotal = ZERO;
    let netBalance = null;
    let payableNet = null;
    let balanceBasis = 'gross';
    let effectiveBalance = grossBalance;

    if (fiscal) {
      const inss = SFA.Decimal.parse(fiscal.social_security, 'thirteenth_inss');
      const irrf = SFA.Decimal.parse(fiscal.irrf.final_irrf, 'thirteenth_irrf');
      deductionsTotal = inss.add(irrf).add(pension);
      netBalance = grossBalance.sub(deductionsTotal);
      payableNet = netBalance.max(ZERO);
      balanceBasis = 'net';
      effectiveBalance = netBalance;
    }

    return Object.freeze({
      status: balanceStatus(effectiveBalance),
      balance_basis: balanceBasis,
      gross_balance_before_deductions: grossBalance.toString(),
      deductions_total: deductionsTotal.toString(),
      net_balance_before_floor: netBalance ? netBalance.toString() : null,
      balance_before_floor: effectiveBalance.toString(),
      payable_second_installment_gross: payableGross.toString(),
      payable_second_installment_net: payableNet ? payableNet.toString() : null,
      insufficiency_amount: insufficiencyOf(effectiveBalance).toString()
    });
  }

  function calculateH27(release, input) {
    const params = input || {};
    const targetDate = SFA.normalizeDate(params.targetDate, 'targetDate');
    const referenceYear = Number(params.referenceYear || yearOf(targetDate));
    if (!Number.isSafeInteger(referenceYear) || referenceYear < 1900 || referenceYear > 9999) {
      h27Error('h27_reference_year', 'Ano de referência inválido.');
    }

    const salary = money(params.salary, 'salary');
    const variables = money(params.variables || '0', 'variables');
    const pension = money(params.pension || '0', 'pension');
    const dependents = count(params.dependentCount, 'dependents');
    if (salary.compare(ZERO) <= 0) h27Error('h27_salary_required', 'Salário-base deve ser maior que zero.');

    const accrualMode = params.accrualMode === 'dates' ? 'dates' : 'manual';
    const accrual = accrualMode === 'dates'
      ? SFA.THIRTEENTH.calculateAccrual(release, {
          consumer: CONSUMER,
          targetDate,
          mode: 'dates',
          referenceYear,
          employmentStart: params.employmentStart,
          accrualEnd: params.accrualEnd
        })
      : SFA.THIRTEENTH.calculateAccrual(release, {
          consumer: CONSUMER,
          targetDate,
          mode: 'manual',
          referenceYear,
          twelfths: count(params.twelfths, 'twelfths')
        });

    if (accrual.twelfths <= 0) h27Error('h27_no_twelfths', 'O período informado não gerou avos de 13º.');

    const reference = SFA.THIRTEENTH.selectReferenceRemuneration(release, {
      consumer: CONSUMER,
      targetDate,
      decemberDueRemuneration: salary.toString(),
      variableComponent: variables.toString()
    });
    const gross = SFA.THIRTEENTH.calculateGross(release, {
      consumer: CONSUMER,
      targetDate,
      referenceRemuneration: reference.total_reference,
      twelfths: accrual.twelfths
    });
    const grossAmount = SFA.Decimal.parse(gross.gross_thirteenth, 'gross_thirteenth');

    const reportedAdvanceRaw = params.reportedAdvance === null || params.reportedAdvance === undefined ? '' : String(params.reportedAdvance).trim();
    let advance;
    let advanceAudit = [];
    if (reportedAdvanceRaw !== '') {
      const reported = money(reportedAdvanceRaw, 'reported_advance');
      advance = Object.freeze({ status: 'REPORTED', amount: reported.toString(), source: 'reported_by_user', reason: null });
    } else {
      const admissionInYear = accrualMode === 'dates'
        ? yearOf(SFA.normalizeDate(params.employmentStart, 'employmentStart')) === referenceYear
        : params.admissionInYear === true;
      try {
        const standard = SFA.THIRTEENTH.calculateAdvance(release, {
          consumer: CONSUMER,
          targetDate,
          paymentDate: params.advancePaymentDate,
          previousMonthSalary: params.previousMonthSalary,
          admissionInYear,
          hasVariableRemuneration: variables.compare(ZERO) > 0
        });
        advance = Object.freeze({ status: 'CALCULATED', amount: standard.amount, source: standard.source, reason: null });
        advanceAudit = [standard.audit, standard.rounding_audit];
      } catch (error) {
        const explicitUnsupported = error && [
          'thirteenth_advance_admission_unsupported',
          'thirteenth_advance_variable_unsupported'
        ].includes(error.code);
        if (!explicitUnsupported) throw error;
        advance = Object.freeze({ status: 'UNSUPPORTED', amount: null, source: null, reason: error.code });
      }
    }

    let fiscal = null;
    if (params.estimateNet !== false) {
      fiscal = SFA.THIRTEENTH.assessFiscal(release, {
        consumer: CONSUMER,
        targetDate,
        grossThirteenth: gross.gross_thirteenth,
        dependentCount: dependents,
        pension: pension.toString()
      });
    }

    const settlement = buildSettlement(grossAmount, advance, fiscal, pension);
    const audits = uniqueAudits([
      accrual.audit,
      ...reference.audit,
      gross.audit,
      gross.rounding_audit,
      ...advanceAudit,
      ...(fiscal ? fiscal.audit.rules : [])
    ]);

    return Object.freeze({
      consumer: CONSUMER,
      reference_date: targetDate,
      reference_year: referenceYear,
      accrual,
      reference_remuneration: reference,
      gross_thirteenth: gross.gross_thirteenth,
      advance,
      settlement,
      second_installment_gross: settlement.payable_second_installment_gross,
      second_installment_net: settlement.payable_second_installment_net,
      pension: pension.toString(),
      fiscal: fiscal ? Object.freeze({ inss: fiscal.social_security, irrf: fiscal.irrf }) : null,
      fiscal_metadata: Object.freeze({ release_id: release.release_id, rules: audits })
    });
  }

  SFA.H27 = Object.freeze({ calculate: calculateH27, buildSettlement });
})(typeof window !== 'undefined' ? window : globalThis);
