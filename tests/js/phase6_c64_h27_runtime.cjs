const fs = require('fs');
const vm = require('vm');

const [corePath, thirteenthPath, h27Path, releasePath] = process.argv.slice(2);
if (!corePath || !thirteenthPath || !h27Path || !releasePath) {
  console.error('usage: node phase6_c64_h27_runtime.cjs <folha-core.js> <folha-thirteenth.js> <decimo-terceiro.js> <release.json>');
  process.exit(2);
}

global.window = global;
vm.runInThisContext(fs.readFileSync(corePath, 'utf8'), { filename: corePath });
vm.runInThisContext(fs.readFileSync(thirteenthPath, 'utf8'), { filename: thirteenthPath });
vm.runInThisContext(fs.readFileSync(h27Path, 'utf8'), { filename: h27Path });

const SFA = global.SFA_FOLHA;
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
SFA.assertRelease(release, 'H27');

const targetDate = '2026-12-20';
function standardInput() {
  return {
    targetDate,
    referenceYear: 2026,
    salary: '4000.00',
    variables: '0.00',
    accrualMode: 'manual',
    twelfths: 12,
    dependentCount: 0,
    pension: '0.00',
    reportedAdvance: '',
    previousMonthSalary: '4000.00',
    advancePaymentDate: '2026-11-30',
    admissionInYear: false,
    estimateNet: true
  };
}

function manualCase(twelfths, overrides) {
  return SFA.H27.calculate(release, Object.assign(standardInput(), { twelfths }, overrides || {}));
}

const standard = SFA.H27.calculate(release, standardInput());

const boundary14 = SFA.THIRTEENTH.calculateAccrual(release, {
  consumer: 'H27', targetDate, mode: 'dates', referenceYear: 2026,
  employmentStart: '2026-01-18', accrualEnd: '2026-01-31'
});
const boundary15 = SFA.THIRTEENTH.calculateAccrual(release, {
  consumer: 'H27', targetDate, mode: 'dates', referenceYear: 2026,
  employmentStart: '2026-01-17', accrualEnd: '2026-01-31'
});

const variableUnsupported = SFA.H27.calculate(release, {
  targetDate,
  referenceYear: 2026,
  salary: '4000.00',
  variables: '500.00',
  accrualMode: 'manual',
  twelfths: 12,
  dependentCount: 0,
  pension: '0.00',
  reportedAdvance: '',
  previousMonthSalary: '4000.00',
  advancePaymentDate: '2026-11-30',
  admissionInYear: false,
  estimateNet: true
});

const variableReported = SFA.H27.calculate(release, {
  targetDate,
  referenceYear: 2026,
  salary: '4000.00',
  variables: '500.00',
  accrualMode: 'manual',
  twelfths: 12,
  dependentCount: 0,
  pension: '0.00',
  reportedAdvance: '2100.00',
  previousMonthSalary: '',
  advancePaymentDate: '2026-11-30',
  admissionInYear: false,
  estimateNet: true
});

const admissionUnsupported = SFA.H27.calculate(release, {
  targetDate,
  referenceYear: 2026,
  salary: '4000.00',
  variables: '0.00',
  accrualMode: 'manual',
  twelfths: 8,
  dependentCount: 0,
  pension: '0.00',
  reportedAdvance: '',
  previousMonthSalary: '4000.00',
  advancePaymentDate: '2026-11-30',
  admissionInYear: true,
  estimateNet: true
});

const settlementMatrix = {
  avos1: manualCase(1),
  avos5: manualCase(5),
  avos6: manualCase(6),
  avos7: manualCase(7),
  avos11: manualCase(11),
  avos12: manualCase(12)
};

const reportedAdvanceMatrix = {
  below: manualCase(12, { reportedAdvance: '1000.00', previousMonthSalary: '' }),
  equalGross: manualCase(6, { reportedAdvance: '2000.00', previousMonthSalary: '' }),
  aboveGross: manualCase(5, { reportedAdvance: '2000.00', previousMonthSalary: '' })
};

let missingAdvanceReferenceRejected = false;
try {
  SFA.H27.calculate(release, {
    targetDate, referenceYear: 2026, salary: '4000.00', variables: '0.00',
    accrualMode: 'manual', twelfths: 12, dependentCount: 0, pension: '0.00',
    reportedAdvance: '', previousMonthSalary: '', advancePaymentDate: '2026-11-30',
    admissionInYear: false, estimateNet: true
  });
} catch (error) {
  missingAdvanceReferenceRejected = Boolean(error && error.code === 'thirteenth_advance_reference_required');
}

let negativeInputRejected = false;
try {
  SFA.H27.calculate(release, {
    targetDate, referenceYear: 2026, salary: '-1.00', variables: '0.00',
    accrualMode: 'manual', twelfths: 12, dependentCount: 0, pension: '0.00',
    reportedAdvance: '0.00', estimateNet: true
  });
} catch (error) {
  negativeInputRejected = Boolean(error && error.code === 'h27_negative_input');
}

let technicalRoundingRejected = false;
try {
  const broken = JSON.parse(JSON.stringify(release));
  const technical = broken.rules.find((rule) => rule.rule_id === 'technical.money_decimal_and_rounding');
  if (!technical) throw new Error('technical money rounding rule missing from release fixture');
  delete technical.rounding_policy;
  SFA.H27.calculate(broken, standardInput());
} catch (error) {
  technicalRoundingRejected = Boolean(error && error.code === 'thirteenth_rounding');
}

process.stdout.write(JSON.stringify({
  release_id: release.release_id,
  standard,
  boundary14,
  boundary15,
  variable_unsupported: variableUnsupported,
  variable_reported: variableReported,
  admission_unsupported: admissionUnsupported,
  settlement_matrix: settlementMatrix,
  reported_advance_matrix: reportedAdvanceMatrix,
  missing_advance_reference_rejected: missingAdvanceReferenceRejected,
  negative_input_rejected: negativeInputRejected,
  technical_rounding_rejected: technicalRoundingRejected
}));
