const fs = require('fs');
const vm = require('vm');

const [corePath, releasePath] = process.argv.slice(2);
if (!corePath || !releasePath) {
  console.error('usage: node phase6_c62_runtime.cjs <folha-core.js> <release.json>');
  process.exit(2);
}

global.window = global;
vm.runInThisContext(fs.readFileSync(corePath, 'utf8'), { filename: corePath });
const SFA = global.SFA_FOLHA;
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));

SFA.assertRelease(release, 'H26');

const decimal = {
  sum: SFA.Decimal.parse('0.1').add(SFA.Decimal.parse('0.2')).toString(),
  half_up: SFA.Decimal.parse('2.345').quantize(2, 'ROUND_HALF_UP').toFixed(2, 'ROUND_HALF_UP'),
  half_even: SFA.Decimal.parse('2.345').quantize(2, 'ROUND_HALF_EVEN').toFixed(2, 'ROUND_HALF_EVEN')
};

let floatRejected = false;
try {
  SFA.Decimal.parse(0.1, 'binary_float_probe');
} catch (error) {
  floatRejected = Boolean(error && error.code === 'binary_float_rejected');
}

const normalizedDates = {
  regular: SFA.normalizeDate('2026-09-16', 'regular_date'),
  leap_day: SFA.normalizeDate('2028-02-29', 'leap_date'),
  date_object: SFA.normalizeDate(new Date('2028-02-29T12:00:00.000Z'), 'date_object')
};

const impossibleCivilDates = [
  '2026-02-29',
  '2026-02-31',
  '2026-04-31',
  '2026-00-10',
  '2026-13-01',
  '2026-01-00'
];
const civilDateRejections = {};
for (const value of impossibleCivilDates) {
  try {
    SFA.normalizeDate(value, 'civil_date_probe');
    civilDateRejections[value] = false;
  } catch (error) {
    civilDateRejections[value] = Boolean(error && error.code === 'date_invalid');
  }
}

let malformedDateRejected = false;
try {
  SFA.normalizeDate('2026-2-01', 'malformed_date_probe');
} catch (error) {
  malformedDateRejected = Boolean(error && error.code === 'date_required');
}

let impossibleSelectionDateRejected = false;
try {
  SFA.selectRule(release, {
    ruleId: 'irrf.monthly.progressive_table',
    consumer: 'H26',
    context: 'monthly',
    targetDate: '2026-02-31'
  });
} catch (error) {
  impossibleSelectionDateRejected = Boolean(error && error.code === 'date_invalid');
}

const targetDate = '2026-09-13';
const inss = SFA.assessInss(release, {
  consumer: 'H26',
  context: 'monthly',
  targetDate,
  base: '6000.00'
});

const irrf = SFA.assessIrrf(release, {
  consumer: 'H26',
  targetDate,
  incomeType: 'monthly',
  originContext: 'monthly',
  grossTaxableIncome: '6000.00',
  socialSecurity: inss.amount,
  dependentCount: 0,
  pension: '0.00'
});

const vacationIdentity = SFA.assessmentIdentity('vacation', 'vacation_enjoyed');
const vacationIrrf = SFA.assessIrrf(release, {
  consumer: 'H28',
  targetDate,
  incomeType: 'vacation',
  originContext: 'vacation_enjoyed',
  grossTaxableIncome: '4000.00',
  socialSecurity: '315.27',
  dependentCount: 0,
  pension: '0.00'
});

const selected = SFA.selectRule(release, {
  ruleId: 'irrf.monthly.progressive_table',
  consumer: 'H26',
  context: 'monthly',
  targetDate
});

let missingRuleRejected = false;
try {
  SFA.selectRule(release, {
    ruleId: 'does.not.exist',
    consumer: 'H26',
    context: 'monthly',
    targetDate
  });
} catch (error) {
  missingRuleRejected = Boolean(error && error.code === 'rule_selection');
}

let incompatibleAssessmentRejected = false;
try {
  SFA.assessmentIdentity('vacation', 'termination');
} catch (error) {
  incompatibleAssessmentRejected = Boolean(error && error.code === 'assessment_identity');
}

process.stdout.write(JSON.stringify({
  release_id: release.release_id,
  decimal,
  float_rejected: floatRejected,
  normalized_dates: normalizedDates,
  civil_date_rejections: civilDateRejections,
  malformed_date_rejected: malformedDateRejected,
  impossible_selection_date_rejected: impossibleSelectionDateRejected,
  missing_rule_rejected: missingRuleRejected,
  incompatible_assessment_rejected: incompatibleAssessmentRejected,
  selected_rule: selected.audit,
  competence_basis: SFA.competenceBasisFor(selected),
  inss,
  irrf,
  vacation_identity: vacationIdentity,
  vacation_irrf: vacationIrrf
}));
