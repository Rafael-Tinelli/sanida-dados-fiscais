from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

readme_path = ROOT / "README.md"
readme = readme_path.read_text(encoding="utf-8")

old_phase3_tail = '''- H29 continua declarando `partial_estimate`, com aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas fora do total;\n- suíte completa: **130 testes verdes** após o checkpoint H29 limitado.\n'''
new_phase3_tail = '''- H29 continua declarando `partial_estimate`, com aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas fora do total;\n- `sanida_fiscal/memory_v1.py` introduz um envelope comum de memória auditável sem fundir semânticas fiscais: IRRF, 13º, férias e H29 continuam preservando identidade/contexto e podem ser compostos como memórias-filhas;\n- valores monetários da memória comum partem de `Decimal` e são serializados como texto canônico, sem reintroduzir `float` no caminho fiscal;\n- property-based tests foram ampliados para fronteiras do redutor de 2026, denominadores civis do saldo salarial, monotonicidade de 13º/férias, matriz H29 fechada e separação tributária do abono;\n- `scripts/validate_phase3_gate.py` tornou o gate de fechamento da Fase 3 executável no `Remake CI`, cobrindo A01, H29, abono, artefatos obrigatórios e higiene de workflows;\n- suíte completa: **150 testes verdes** no checkpoint de memória comum/invariantes, com gate de fechamento da Fase 3 verde.\n'''
if old_phase3_tail not in readme:
    raise SystemExit("README Phase 3 tail marker not found")
readme = readme.replace(old_phase3_tail, new_phase3_tail, 1)

old_decision = '''27. O saldo salarial de H29 v1 não usa divisor 30 universal. O denominador é o número de dias civis do mês de desligamento e o numerador é fornecido explicitamente como dias considerados até o desligamento; regimes fora de mensalista/quinzenalista normalizado ficam fora do escopo.\n'''
new_decision = '''27. O saldo salarial de H29 v1 não usa divisor 30 universal. O denominador é o número de dias civis do mês de desligamento e o numerador é fornecido explicitamente como dias considerados até o desligamento; regimes fora de mensalista/quinzenalista normalizado ficam fora do escopo.\n28. A memória comum de cálculo é um envelope de representação/auditoria, não uma rules engine nem uma fusão semântica. Bases e identidades de mensal, 13º, férias e rescisão permanecem separadas; composições usam memórias-filhas.\n29. O fechamento da Fase 3 exige, no mesmo head, validação do repositório, Contrato Fiscal Canônico, suíte integral, `scripts/validate_phase3_gate.py`, documentação sincronizada e ausência de helpers/workflows temporários.\n'''
if old_decision not in readme:
    raise SystemExit("README decision 27 marker not found")
readme = readme.replace(old_decision, new_decision, 1)

old_next = '''Próximos checkpoints: memória de cálculo comum e expansão dos property-based tests nas fronteiras legais e monetárias. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
new_next = '''Próximo checkpoint: realizar a revisão formal do gate de fechamento da Fase 3 no mesmo head limpo. Se validação do repositório, contrato, suíte integral, gate executável, documentação e higiene do branch permanecerem verdes, a Fase 3 poderá ser promovida para `CONCLUÍDA` em checkpoint próprio e o trabalho seguirá para a Fase 4. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
if old_next not in readme:
    raise SystemExit("README next checkpoint marker not found")
readme = readme.replace(old_next, new_next, 1)

changelog_marker = "### 2026-09-13 — checkpoint H29 limitado na Fase 3"
changelog_entry = '''### 2026-09-13 — checkpoint de memória comum, invariantes e gate da Fase 3\n\n- criado `sanida_fiscal/memory_v1.py` como envelope determinístico de memória auditável, preservando separação entre contextos e bases fiscais;\n- fatos da memória passam a carregar papel, unidade e `rule_ids`, com valores monetários originados em `Decimal` e serializados como texto canônico;\n- H29 passa a expor memória composta por filhos separados para saldo salarial, 13º proporcional e férias proporcionais, sem fabricar filhos inelegíveis no motivo `01`;\n- `tests/test_memory_v1.py` valida reconciliação A01, serialização determinística, unicidade de fatos e separação das memórias;\n- `tests/test_phase3_invariants.py` amplia property-based tests para fronteiras legais e monetárias;\n- criado `scripts/validate_phase3_gate.py` e integrado ao `Remake CI`;\n- criado `docs/phase3-closure-gate.md` com critérios objetivos para a promoção formal da Fase 3;\n- run `34775296512` fecha o checkpoint com validação do repositório, Contrato v1.1, **150 testes** e gate da Fase 3 em `PASS`.\n\n'''
if changelog_marker not in readme:
    raise SystemExit("README changelog marker not found")
readme = readme.replace(changelog_marker, changelog_entry + changelog_marker, 1)
readme_path.write_text(readme, encoding="utf-8")

phase3_path = ROOT / "docs" / "phase3-library-v1.md"
doc = phase3_path.read_text(encoding="utf-8")

for old, new in (
    ("## 12. Próximos checkpoints da Fase 3", "## 13. Próximos checkpoints da Fase 3"),
    ("## 11. Limites preservados", "## 12. Limites preservados"),
    ("## 10. Estado do CI", "## 11. Estado do CI"),
):
    if old not in doc:
        raise SystemExit(f"Phase 3 heading marker not found: {old}")
    doc = doc.replace(old, new, 1)

state_marker = "## 11. Estado do CI"
section = '''## 10. Sexto checkpoint — memória comum, invariantes e gate de fechamento\n\nO checkpoint consolida a **representação auditável** dos cálculos já implementados sem criar uma camada que misture semânticas jurídicas distintas.\n\nArquivos permanentes adicionados:\n\n```text\nsanida_fiscal/memory_v1.py\ntests/test_memory_v1.py\ntests/test_phase3_invariants.py\nscripts/validate_phase3_gate.py\ndocs/phase3-closure-gate.md\n```\n\n### 10.1 Memória comum sem fusão semântica\n\n`CalculationMemory` é um envelope comum para auditoria. Cada nó declara `calculation_type`, `origin_context`, tipo de apuração quando aplicável, fatos ordenados e memórias-filhas.\n\nCada `CalculationFact` carrega:\n\n```text\nkey\nvalue\nunit\nrole\nrule_ids\n```\n\nValores monetários chegam à memória como `Decimal` e são serializados como texto canônico. A camada não converte os cálculos para `float`.\n\nA normalização é apenas de representação: mensal, 13º, férias e rescisão continuam com identidades, bases e regras próprias. No H29, por exemplo, saldo salarial, 13º proporcional e férias proporcionais aparecem como filhos separados; o motivo `01` contém apenas o filho de saldo salarial.\n\n### 10.2 Reconciliação e proveniência mínima\n\nO adaptador de IRRF preserva a memória A01 completa e associa os fatos críticos aos `rule_ids` executados. O caso de R$ 6.000 continua registrando simultaneamente:\n\n```text\ngross_taxable_income     6000.00\nirrf_tax_base            5350.40\nreduction_input_income   6000.00\nfinal_irrf                382.88\n```\n\nO envelope também cobre a apuração fiscal do 13º, saldo salarial, 13º proporcional de H29, férias proporcionais, bases tributárias do abono e composição H29 limitada.\n\n### 10.3 Expansão das invariantes/property-based tests\n\nO novo conjunto cobre adicionalmente:\n\n- fronteiras do redutor de 2026 em `4999.99 / 5000.00 / 5000.01` e `7349.99 / 7350.00 / 7350.01`;\n- saldo salarial usando o número real de dias civis nos doze meses;\n- monotonicidade dos avos de férias dentro de um período aquisitivo;\n- monotonicidade dos avos de 13º dentro do ano de referência;\n- totalidade da matriz H29 exatamente sobre `01/02/07/33`;\n- impossibilidade de o principal do abono vazar para a base de IRRF ou contribuição previdenciária.\n\n### 10.4 Gate executável de fechamento\n\n`scripts/validate_phase3_gate.py` passou a rodar no `Remake CI` depois da suíte completa. Ele valida artefatos obrigatórios, higiene de workflows, A01 e sua memória comum, H29 limitado (`01` e `02`), calendários 3/12 versus 7/12 e separação principal/terço do abono.\n\nO gate não amplia escopo: casos deliberadamente não modelados permanecem fail-closed e estão registrados em `docs/phase3-closure-gate.md`.\n\n'''
if state_marker not in doc:
    raise SystemExit("Phase 3 state marker not found after renumbering")
doc = doc.replace(state_marker, section + state_marker, 1)

old_ci_start = "Após o checkpoint H29 limitado:"
if old_ci_start not in doc:
    raise SystemExit("Phase 3 old CI intro not found")
start = doc.index(old_ci_start)
end_marker = "## 12. Limites preservados"
end = doc.index(end_marker, start)
new_ci = '''Após o checkpoint de memória comum, invariantes e gate de fechamento:\n\n```text\nRepository baseline       PASS\nFiscal Contract v1.1      PASS\nCandidate rules           21\nInventory coverage        32/32\nPayload families          18/18\nFiscal engine             PASS\nProperty-based tests      PASS\nPhase 3 closure gate      PASS\n\n150 passed\n```\n\nRun de validação do checkpoint: `34775296512`.\n\n'''
doc = doc[:start] + new_ci + doc[end:]

old_next = '''1. consolidar memória de cálculo comum entre os núcleos já implementados;\n2. ampliar invariantes/property-based tests nas fronteiras legais e monetárias;\n3. preparar o gate de fechamento da Fase 3 sem antecipar collectors, publicação ou migração de consumidores.\n'''
new_next = '''1. revisar formalmente o gate de fechamento da Fase 3 no mesmo head limpo;\n2. se repositório, contrato, suíte integral, gate, documentação e higiene estiverem verdes, promover a Fase 3 para `CONCLUÍDA` em checkpoint próprio;\n3. somente depois iniciar a Fase 4 — Fontes e sensores, sem antecipar publicação ou migração de consumidores.\n'''
if old_next not in doc:
    raise SystemExit("Phase 3 old next list not found")
doc = doc.replace(old_next, new_next, 1)
phase3_path.write_text(doc, encoding="utf-8")

print("README and Phase 3 memory/gate checkpoint synchronized")
