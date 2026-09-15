const fs = require('fs');
const vm = require('vm');

const [corePath, h26Path, releasePath] = process.argv.slice(2);
if (!corePath || !h26Path || !releasePath) {
  console.error('usage: node phase6_c63_h26_runtime.cjs <folha-core.js> <salario-liquido.js> <release.json>');
  process.exit(2);
}

global.window = global;
vm.runInThisContext(fs.readFileSync(corePath, 'utf8'), { filename: corePath });
vm.runInThisContext(fs.readFileSync(h26Path, 'utf8'), { filename: h26Path });

const SFA = global.SFA_FOLHA;
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
const targetDate = '2026-09-13';

const h26 = SFA.H26.calculate(release, {
  targetDate,
  salary: '6000.00',
  variables: '0.00',
  dependentCount: '0',
  pension: '0.00',
  otherDeductions: '0.00'
});

const h26WithOtherDeduction = SFA.H26.calculate(release, {
  targetDate,
  salary: '6000.00',
  variables: '0.00',
  dependentCount: '0',
  pension: '0.00',
  otherDeductions: '100.05'
});

const historicalA01 = SFA.assessIrrf(release, {
  consumer: 'H26',
  targetDate,
  incomeType: 'monthly',
  originContext: 'monthly',
  grossTaxableIncome: '6000.00',
  socialSecurity: '649.60',
  dependentCount: 0,
  pension: '0.00'
});

let expiredReductionRejected = false;
try {
  SFA.H26.calculate(release, {
    targetDate: '2027-01-01',
    salary: '6000.00',
    variables: '0.00',
    dependentCount: '0',
    pension: '0.00',
    otherDeductions: '0.00'
  });
} catch (error) {
  expiredReductionRejected = Boolean(error && error.code === 'rule_selection');
}

let negativeInputRejected = false;
try {
  SFA.H26.calculate(release, {
    targetDate,
    salary: '6000.00',
    variables: '-0.01',
    dependentCount: '0',
    pension: '0.00',
    otherDeductions: '0.00'
  });
} catch (error) {
  negativeInputRejected = Boolean(error && error.code === 'h26_negative_input');
}

process.stdout.write(JSON.stringify({
  release_id: release.release_id,
  h26,
  h26_with_other_deduction: h26WithOtherDeduction,
  historical_a01: historicalA01,
  expired_reduction_rejected: expiredReductionRejected,
  negative_input_rejected: negativeInputRejected
}));
