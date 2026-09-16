'use strict';

const fs = require('fs');
const vm = require('vm');

const [corePath, terminationPath, h29Path, releasePath] = process.argv.slice(2);
if (!corePath || !terminationPath || !h29Path || !releasePath) {
  throw new Error('usage: node phase6_c66_h29_runtime.cjs <core> <termination> <h29> <release>');
}

const sandbox = {
  console,
  globalThis: null,
  window: undefined,
  document: undefined,
  fetch: async function () { throw new Error('network disabled in H29 runtime test'); },
  Date,
  BigInt,
  Intl,
  setTimeout,
  clearTimeout
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
for (const file of [corePath, terminationPath, h29Path]) {
  vm.runInContext(fs.readFileSync(file, 'utf8'), sandbox, { filename: file });
}

const SFA = sandbox.SFA_FOLHA;
if (!SFA || !SFA.TERMINATION) throw new Error('H29 termination runtime was not registered');
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
SFA.assertRelease(release, 'H29');

function calculate(overrides) {
  return SFA.TERMINATION.calculate(release, Object.assign({
    esocialReason: '02',
    employmentRegime: 'monthly',
    contractTerm: 'indefinite',
    employmentStart: '2025-09-01',
    terminationDate: '2026-03-31',
    monthlyBaseSalary: '3100.00',
    daysCountedThroughTermination: 10,
    terminationMonthRemuneration: '3600.00'
  }, overrides || {}));
}

const standard = calculate();
const reasons = ['01', '02', '07', '33'].map(function (code) {
  const value = calculate({ esocialReason: code });
  return {
    code,
    reason: value.reason,
    has_salary_balance: value.salary_balance !== null,
    has_thirteenth: value.thirteenth_proportional !== null,
    has_vacation: value.vacation_proportional !== null
  };
});

const march = calculate({ terminationDate: '2026-03-10', daysCountedThroughTermination: 10 });
const april = calculate({ terminationDate: '2026-04-10', daysCountedThroughTermination: 10 });
const boundary14 = calculate({
  employmentStart: '2026-01-18',
  terminationDate: '2026-01-31',
  daysCountedThroughTermination: 31
});
const boundary15 = calculate({
  employmentStart: '2026-01-17',
  terminationDate: '2026-01-31',
  daysCountedThroughTermination: 31
});

let unsupportedReasonRejected = false;
try { calculate({ esocialReason: '04' }); } catch (error) {
  unsupportedReasonRejected = Boolean(error && error.code === 'termination_reason_unsupported');
}

let hourlyRejected = false;
try { calculate({ employmentRegime: 'hourly' }); } catch (error) {
  hourlyRejected = Boolean(error && error.code === 'termination_scope_unsupported');
}

let fixedTermRejected = false;
try { calculate({ contractTerm: 'fixed' }); } catch (error) {
  fixedTermRejected = Boolean(error && error.code === 'termination_scope_unsupported');
}

let excessDaysRejected = false;
try { calculate({ terminationDate: '2026-03-10', daysCountedThroughTermination: 11 }); } catch (error) {
  excessDaysRejected = Boolean(error && error.code === 'termination_salary_days');
}

let negativeMoneyRejected = false;
try { calculate({ monthlyBaseSalary: '-1.00' }); } catch (error) {
  negativeMoneyRejected = Boolean(error && error.code === 'termination_negative_input');
}

const ids = standard.fiscal_metadata.rules.map(function (item) { return item.rule_id; });
process.stdout.write(JSON.stringify({
  release_id: release.release_id,
  standard,
  reasons,
  march_salary_balance: march.salary_balance,
  april_salary_balance: april.salary_balance,
  boundary14: {
    thirteenth_twelfths: boundary14.thirteenth_proportional.twelfths,
    vacation_twelfths: boundary14.vacation_proportional.twelfths
  },
  boundary15: {
    thirteenth_twelfths: boundary15.thirteenth_proportional.twelfths,
    vacation_twelfths: boundary15.vacation_proportional.twelfths
  },
  unsupported_reason_rejected: unsupportedReasonRejected,
  hourly_rejected: hourlyRejected,
  fixed_term_rejected: fixedTermRejected,
  excess_days_rejected: excessDaysRejected,
  negative_money_rejected: negativeMoneyRejected,
  required_rules_present: [
    'termination.reason_scope',
    'termination.partial_output_scope',
    'termination.salary_balance',
    'termination.thirteenth_proportional',
    'termination.vacation_proportional',
    'termination.acquired_and_overdue_vacation',
    'thirteenth.accrual.twelfths',
    'thirteenth.reference_remuneration',
    'thirteenth.variable_remuneration',
    'vacation.acquisition_period',
    'technical.money_decimal_and_rounding'
  ].every(function (id) { return ids.includes(id); })
}));
