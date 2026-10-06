'use strict';
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');

const [corePath, thirteenthPath, runtimePath, releasePath] = process.argv.slice(2);
if (!corePath || !thirteenthPath || !runtimePath || !releasePath) {
  console.error('usage: node h27_pending_advance_runtime.cjs <core> <thirteenth> <runtime> <release>');
  process.exit(2);
}

global.window = global;
for (const path of [corePath, thirteenthPath, runtimePath]) {
  vm.runInThisContext(fs.readFileSync(path, 'utf8'), { filename: path });
}

const SFA = global.SFA_FOLHA;
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
SFA.assertRelease(release, 'H27');

const base = {
  targetDate: '2026-12-20',
  referenceYear: 2026,
  salary: '4000.00',
  variables: '0.00',
  accrualMode: 'manual',
  twelfths: 12,
  dependentCount: 0,
  pension: '0.00',
  reportedAdvance: '',
  previousMonthSalary: '',
  advancePaymentDate: '',
  admissionInYear: false,
  estimateNet: true
};

const pending = SFA.H27.calculate(release, base);
assert.equal(pending.advance.status, 'PENDING');
assert.equal(pending.advance.reason, 'advance_not_informed');
assert.equal(pending.gross_thirteenth, '4000.00');
assert.equal(pending.settlement.status, 'PENDING_ADVANCE');

assert.throws(
  () => SFA.H27.calculate(release, { ...base, advancePaymentDate: '2026-11-30' }),
  (error) => error && error.code === 'h27_advance_salary_required'
);

assert.throws(
  () => SFA.H27.calculate(release, { ...base, previousMonthSalary: '4000.00' }),
  (error) => error && error.code === 'h27_advance_date_required'
);

const zero = SFA.H27.calculate(release, { ...base, reportedAdvance: '0' });
assert.equal(zero.advance.status, 'REPORTED');
assert.equal(zero.advance.amount, '0');
assert.notEqual(zero.settlement.status, 'PENDING_ADVANCE');

process.stdout.write(JSON.stringify({
  result: 'PASS',
  pending_status: pending.advance.status,
  settlement_status: pending.settlement.status,
  partial_input_guards: true,
  reported_zero_supported: true
}));
