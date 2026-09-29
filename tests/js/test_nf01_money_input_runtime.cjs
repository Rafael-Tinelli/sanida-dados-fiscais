'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const ROOT = path.resolve(__dirname, '..', '..');
const releaseManifest = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'releases/fiscal-v1/current.json'), 'utf8')
);
const release = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'releases/fiscal-v1', releaseManifest.artifact), 'utf8')
);

global.window = global;

for (const relative of [
  'consumers/frontend/folha-core.js',
  'consumers/runtime/salario-liquido-runtime.js',
  'consumers/frontend/folha-thirteenth.js',
  'consumers/runtime/decimo-terceiro-runtime.js',
  'consumers/frontend/folha-vacation.js',
  'consumers/frontend/folha-termination.js'
]) {
  vm.runInThisContext(
    fs.readFileSync(path.join(ROOT, relative), 'utf8'),
    { filename: relative }
  );
}

const SFA = global.SFA_FOLHA;
const DATE = '2026-09-29';

function capture(fn) {
  try {
    return { accepted: true, result: fn() };
  } catch (error) {
    return {
      accepted: false,
      code: error && error.code ? error.code : error && error.name,
      message: error && error.message
    };
  }
}

function h26(salary, more) {
  return SFA.H26.calculate(release, Object.assign({
    targetDate: DATE,
    salary,
    variables: '0',
    pension: '0',
    otherDeductions: '0',
    dependentCount: '0'
  }, more || {}));
}

function h27(salary, more) {
  return SFA.H27.calculate(release, Object.assign({
    targetDate: DATE,
    referenceYear: 2026,
    salary,
    variables: '0',
    accrualMode: 'manual',
    twelfths: 12,
    dependentCount: 0,
    pension: '0',
    reportedAdvance: '0',
    estimateNet: true
  }, more || {}));
}

function h28(salary, more) {
  return SFA.VACATION.calculate(release, Object.assign({
    targetDate: DATE,
    vacationPayBase: salary,
    unjustifiedAbsences: 0,
    sellOneThird: true,
    dependentCount: 0,
    pension: '0'
  }, more || {}));
}

function h29(salary, more) {
  return SFA.TERMINATION.calculate(release, Object.assign({
    esocialReason: '02',
    employmentRegime: 'monthly',
    contractTerm: 'indefinite',
    employmentStart: '2025-09-01',
    terminationDate: '2026-03-10',
    monthlyBaseSalary: salary,
    daysCountedThroughTermination: 10,
    terminationMonthRemuneration: salary
  }, more || {}));
}

const validInputs = ['4000', '4.000', '4.000,00', 'R$ 4.000,00', '4000.00'];
const invalidInputs = ['20mil', 'abc', 'NaN', 'Infinity', '1e3', '1,2,3', '1.2.3'];

const normalized = validInputs.map((input) => ({
  input,
  ...capture(() => SFA.normalizeMoneyInput(input))
}));

const rejected = invalidInputs.map((input) => ({
  input,
  ...capture(() => SFA.normalizeMoneyInput(input))
}));

const flows = [];
for (const [id, fn] of [
  ['H26', h26],
  ['H27', h27],
  ['H28', h28],
  ['H29', h29]
]) {
  for (const input of validInputs) {
    flows.push({ id, input, ...capture(() => fn(input)) });
  }
  for (const input of ['20mil', 'abc', '1e3', '1,2,3', '1.2.3']) {
    flows.push({ id, input, ...capture(() => fn(input)) });
  }
}

const optionalInvalid = [
  { id: 'H26', case: 'variables abc', ...capture(() => h26('4000', { variables: 'abc' })) },
  { id: 'H26', case: 'pension abc', ...capture(() => h26('4000', { pension: 'abc' })) },
  { id: 'H27', case: 'reported advance abc', ...capture(() => h27('4000', { reportedAdvance: 'abc' })) },
  { id: 'H28', case: 'pension abc', ...capture(() => h28('4000', { pension: 'abc' })) }
];

process.stdout.write(JSON.stringify({
  release_id: release.release_id,
  normalized,
  rejected,
  flows,
  optional_invalid: optionalInvalid
}));
