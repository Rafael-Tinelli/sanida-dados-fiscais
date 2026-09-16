'use strict';

const fs = require('fs');
const vm = require('vm');

const [corePath, terminationPath, h29Path] = process.argv.slice(2);
if (!corePath || !terminationPath || !h29Path) {
  throw new Error('usage: node h29_f01_f02_runtime.cjs <core> <termination> <h29>');
}

const sandbox = {
  console,
  globalThis: null,
  window: undefined,
  document: undefined,
  fetch: async function () { throw new Error('network disabled in H29 F01/F02 runtime test'); },
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
if (!SFA || !SFA.H29_UI) throw new Error('H29 UI boundary helpers were not registered');

function rejectsRequiredMoney(value) {
  try {
    SFA.H29_UI.requirePositiveMoneyInput(value, 'test', 'Teste');
    return false;
  } catch (error) {
    return Boolean(error && error.code === 'h29_required_money');
  }
}

let invalidOrderRejected = false;
try {
  SFA.H29_UI.defaultDaysCounted('2026-09-21', '2026-09-20');
} catch (error) {
  invalidOrderRejected = Boolean(error && error.code === 'h29_date_order');
}

process.stdout.write(JSON.stringify({
  required_money_rejections: {
    empty: rejectsRequiredMoney(''),
    whitespace: rejectsRequiredMoney('   '),
    zero: rejectsRequiredMoney('0'),
    zero_brl: rejectsRequiredMoney('0,00'),
    invalid_text: rejectsRequiredMoney('abc')
  },
  positive_money: SFA.H29_UI.requirePositiveMoneyInput('3.100,50', 'test', 'Teste'),
  days: {
    same_month: SFA.H29_UI.defaultDaysCounted('2026-09-10', '2026-09-20'),
    same_month_first_day: SFA.H29_UI.defaultDaysCounted('2026-09-01', '2026-09-20'),
    prior_month: SFA.H29_UI.defaultDaysCounted('2026-08-15', '2026-09-20'),
    february_non_leap: SFA.H29_UI.defaultDaysCounted('2026-02-10', '2026-02-28'),
    february_leap: SFA.H29_UI.defaultDaysCounted('2024-02-10', '2024-02-29')
  },
  invalid_order_rejected: invalidOrderRejected
}));
