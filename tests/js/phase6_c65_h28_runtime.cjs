'use strict';

const fs = require('fs');
const vm = require('vm');

const [corePath, vacationPath, h28Path, releasePath] = process.argv.slice(2);
if (!corePath || !vacationPath || !h28Path || !releasePath) {
  throw new Error('usage: node phase6_c65_h28_runtime.cjs <core> <vacation> <h28> <release>');
}

const sandbox = {
  console,
  globalThis: null,
  window: undefined,
  document: undefined,
  fetch: async function () { throw new Error('network disabled in H28 runtime test'); },
  Date,
  BigInt,
  Intl,
  setTimeout,
  clearTimeout
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
for (const file of [corePath, vacationPath, h28Path]) {
  vm.runInContext(fs.readFileSync(file, 'utf8'), sandbox, { filename: file });
}

const SFA = sandbox.SFA_FOLHA;
if (!SFA || !SFA.H28 || !SFA.VACATION) throw new Error('H28 runtime was not registered');
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
SFA.assertRelease(release, 'H28');

function calculate(overrides) {
  return SFA.H28.calculate(release, Object.assign({
    targetDate: '2026-09-15',
    vacationPayBase: '4000.00',
    unjustifiedAbsences: 0,
    sellOneThird: true,
    dependentCount: 0,
    pension: '0.00'
  }, overrides || {}));
}

const standard = calculate();
const noSale = calculate({ sellOneThird: false });
const bands = [
  [5, 30, 10, 20],
  [6, 24, 8, 16],
  [15, 18, 6, 12],
  [24, 12, 4, 8],
  [32, 12, 4, 8]
].map(function (row) {
  const value = calculate({ unjustifiedAbsences: row[0] });
  return {
    absences: row[0],
    expected_entitled: row[1],
    expected_sold: row[2],
    expected_enjoyed: row[3],
    actual: value.entitlement
  };
});

let unsupportedAbsencesRejected = false;
try { calculate({ unjustifiedAbsences: 33 }); } catch (error) {
  unsupportedAbsencesRejected = Boolean(error && error.code === 'vacation_absences_unsupported');
}

let negativeRejected = false;
try { calculate({ vacationPayBase: '-1.00' }); } catch (error) {
  negativeRejected = Boolean(error && error.code === 'vacation_negative_input');
}

let pensionOmissionRejected = false;
try {
  SFA.H28.calculate(release, {
    targetDate: '2026-09-15', vacationPayBase: '4000.00', unjustifiedAbsences: 0,
    sellOneThird: true, dependentCount: 0
  });
} catch (error) {
  pensionOmissionRejected = Boolean(error && error.code === 'vacation_pension_required');
}

const ids = standard.fiscal_metadata.rules.map(function (item) { return item.rule_id; });
const output = {
  release_id: release.release_id,
  standard,
  no_sale: noSale,
  bands,
  unsupported_absences_rejected: unsupportedAbsencesRejected,
  negative_input_rejected: negativeRejected,
  pension_omission_rejected: pensionOmissionRejected,
  dedicated_reduction_present: ids.includes('vacation.irrf.reduction.2026'),
  generic_reduction_absent: !ids.includes('irrf.reduction.2026'),
  cash_principal_profile_present: ids.includes('vacation.abono.ir_exemption'),
  cash_third_profile_present: ids.includes('vacation.abono_constitutional_third.ir_incidence')
};
process.stdout.write(JSON.stringify(output));
