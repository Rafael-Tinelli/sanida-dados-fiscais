'use strict';

const fs = require('fs');
const vm = require('vm');

const [corePath, terminationPath, releasePath] = process.argv.slice(2);
if (!corePath || !terminationPath || !releasePath) {
  throw new Error('usage: node nf03_h29_days_runtime.cjs <core> <termination> <release>');
}

const sandbox = {
  console,
  globalThis: null,
  window: undefined,
  document: undefined,
  fetch: async function () { throw new Error('network disabled in NF03 runtime test'); },
  Date,
  BigInt,
  Intl,
  setTimeout,
  clearTimeout
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

for (const file of [corePath, terminationPath]) {
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
    employmentStart: '2026-01-18',
    terminationDate: '2026-01-31',
    monthlyBaseSalary: '3100.00',
    daysCountedThroughTermination: 14,
    terminationMonthRemuneration: '3100.00'
  }, overrides || {}));
}

function rejectsDays(overrides) {
  try {
    calculate(overrides);
    return false;
  } catch (error) {
    return Boolean(error && error.code === 'termination_salary_days');
  }
}

const sameMonth14 = calculate();
const priorMonth31 = calculate({
  employmentStart: '2025-12-15',
  terminationDate: '2026-01-31',
  daysCountedThroughTermination: 31
});

const monthBoundaries = [
  { employmentStart: '2026-02-15', terminationDate: '2026-02-28', maximum: 14 },
  { employmentStart: '2026-03-18', terminationDate: '2026-03-31', maximum: 14 },
  { employmentStart: '2026-04-17', terminationDate: '2026-04-30', maximum: 14 }
].map(function (item) {
  const valid = calculate({
    employmentStart: item.employmentStart,
    terminationDate: item.terminationDate,
    daysCountedThroughTermination: item.maximum
  });
  return {
    employment_start: item.employmentStart,
    termination_date: item.terminationDate,
    maximum: item.maximum,
    valid_days: valid.salary_balance.days_counted_through_termination,
    excess_rejected: rejectsDays({
      employmentStart: item.employmentStart,
      terminationDate: item.terminationDate,
      daysCountedThroughTermination: item.maximum + 1
    })
  };
});

process.stdout.write(JSON.stringify({
  same_month_14: {
    days: sameMonth14.salary_balance.days_counted_through_termination,
    amount: sameMonth14.salary_balance.amount,
    fifteenth_rejected: rejectsDays({ daysCountedThroughTermination: 15 }),
    thirty_first_rejected: rejectsDays({ daysCountedThroughTermination: 31 })
  },
  prior_month_31: {
    days: priorMonth31.salary_balance.days_counted_through_termination,
    amount: priorMonth31.salary_balance.amount
  },
  month_boundaries: monthBoundaries,
  accrual_boundaries_preserved: {
    fourteen: {
      thirteenth: sameMonth14.thirteenth_proportional.twelfths,
      vacation: sameMonth14.vacation_proportional.twelfths
    },
    fifteen: (function () {
      const result = calculate({
        employmentStart: '2026-01-17',
        terminationDate: '2026-01-31',
        daysCountedThroughTermination: 15
      });
      return {
        thirteenth: result.thirteenth_proportional.twelfths,
        vacation: result.vacation_proportional.twelfths
      };
    }())
  }
}));
