'use strict';
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const { join } = require('path');
const root = process.cwd();
for (const path of [
  'consumers/candidates/af01/folha-core.js',
  'consumers/runtime/salario-liquido-runtime.js',
  'consumers/candidates/af01/folha-thirteenth.js',
  'consumers/candidates/af01/decimo-terceiro-runtime.js',
  'consumers/candidates/af01/folha-vacation.js',
  'consumers/frontend/ferias-clt.js'
]) vm.runInThisContext(fs.readFileSync(join(root, path),'utf8'),{filename:path});
const SFA=globalThis.SFA_FOLHA;
const manifest=JSON.parse(fs.readFileSync('releases/fiscal-v1/current.json'));
const original=JSON.parse(fs.readFileSync(join('releases/fiscal-v1',manifest.artifact)));
const publishedThird=original.rules.find(r=>r.rule_id==='vacation.abono_constitutional_third.ir_incidence');
assert.ok(['no','yes'].includes(publishedThird.payload.components[0].social_security));
const synthetic=JSON.parse(JSON.stringify(original));
synthetic.release_id='fiscal-v1-sha256-'+'f'.repeat(64);
synthetic.rules.find(r=>r.rule_id==='vacation.abono_constitutional_third.ir_incidence').payload.components[0].social_security='yes';
SFA.assertRelease(synthetic,'H28');
const D=x=>SFA.Decimal.parse(String(x));
const h27={
  targetDate:'2026-12-20',salary:'4000.00',variables:'0',accrualMode:'manual',
  twelfths:12,dependentCount:0,pension:'0',reportedAdvance:'',
  previousMonthSalary:'',advancePaymentDate:'',admissionInYear:false,estimateNet:true
};
const pending=SFA.H27.calculate(synthetic,h27);
assert.equal(pending.advance.status,'PENDING');
assert.equal(pending.gross_thirteenth,'4000.00');
assert.equal(pending.settlement.status,'PENDING_ADVANCE');
const zero=SFA.H27.calculate(synthetic,{...h27,reportedAdvance:'0'});
assert.equal(zero.advance.status,'REPORTED');
assert.equal(D(zero.advance.amount).compare(D(0)),0);
assert.notEqual(zero.settlement.status,'PENDING_ADVANCE');
const invalid=[
  [{...h27,previousMonthSalary:'4000',advancePaymentDate:'2025-11-30'},'thirteenth_advance_year'],
  [{...h27,previousMonthSalary:'4000',advancePaymentDate:'2026-12-21'},'thirteenth_advance_after_settlement'],
  [{...h27,previousMonthSalary:'4000',advancePaymentDate:''},'h27_advance_date_required'],
  [{...h27,previousMonthSalary:'',advancePaymentDate:'2026-11-30'},'h27_advance_salary_required'],
];
for(const [input,code] of invalid){
  assert.throws(()=>SFA.H27.calculate(synthetic,input),e=>e&&e.code===code);
}
const h28=SFA.H28.calculate(synthetic,{
  targetDate:'2026-09-15',vacationPayBase:'4000.00',unjustifiedAbsences:0,
  sellOneThird:true,dependentCount:0,pension:'0'
});
assert.equal(D(h28.fiscal.social_security_base).compare(D('4000.00')),0);
const irVac=h28.fiscal.irrf;
assert.equal(D(irVac.calculated_irrf).compare(D(irVac.withholding_waived).add(D(irVac.withheld_irrf))),0);
assert.equal(D(irVac.final_irrf).compare(D(irVac.withheld_irrf)),0);
const mon=SFA.assessIrrf(synthetic,{
  consumer:'H26',targetDate:'2026-09-15',incomeType:'monthly',
  originContext:'monthly',grossTaxableIncome:'6000',socialSecurity:'649.60',
  dependentCount:0,pension:'0'
});
const thirteenth = pending.fiscal.irrf;
assert.equal(D(mon.calculated_irrf).compare(D(mon.withholding_waived).add(D(mon.withheld_irrf))),0);
assert.equal(Object.hasOwn(thirteenth, 'withholding_waived'), false);
assert.equal(thirteenth.final_irrf !== undefined, true);
(async()=>{
  let requests=0;
  globalThis.fetch=async()=>{requests++;return {ok:true,headers:{get:()=>synthetic.release_id},json:async()=>synthetic}};
  SFA._release=synthetic; SFA._releaseValidatedAt=Date.now()-11*60*1000;
  await SFA.fetchRelease({consumer:'H26'});
  assert.equal(requests,1);
  await SFA.fetchRelease({consumer:'H26'});
  assert.equal(requests,1);
  process.stdout.write(JSON.stringify({
    result:'PASS',cases:12,af01_cp_third:true,
    af02_monthly_and_vacation_separate:true,
    af02_13th_unchanged:true,af03_blank_vs_zero:true,
    af04_dates_rejected:true,r02_cache_expiry:true,
    published_cp_third:publishedThird.payload.components[0].social_security,
    fixture:'SYNTHETIC_DO_NOT_PUBLISH'
  }));
})().catch(e=>{console.error(e);process.exit(1)});
