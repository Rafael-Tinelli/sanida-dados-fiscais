from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

readme_path = ROOT / "README.md"
readme = readme_path.read_text(encoding="utf-8")

old_checkpoint = '''- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;\n- suíte completa: **114 testes verdes** após o checkpoint de férias/A02.\n'''
new_checkpoint = '''- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;\n- H29 ganhou núcleo limitado próprio, com matriz eSocial `01/02/07/33`, sem expansão para motivos não suportados;\n- motivo `01` mantém saldo salarial e bloqueia 13º/férias proporcionais; `02/07/33` habilitam ambos;\n- saldo salarial usa `salário-base mensal normalizado × dias considerados / dias civis do mês`, sem divisor 30 universal;\n- 13º rescisório continua no calendário anual enquanto férias proporcionais continuam no período aquisitivo — no caso `01/09/2025 → 31/03/2026`, isso produz 3/12 de 13º e 7/12 de férias;\n- H29 continua declarando `partial_estimate`, com aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas fora do total;\n- suíte completa: **130 testes verdes** após o checkpoint H29 limitado.\n'''
if old_checkpoint in readme:
    readme = readme.replace(old_checkpoint, new_checkpoint, 1)
elif new_checkpoint not in readme:
    raise SystemExit("README checkpoint marker not found")

old_next = '''Próximos checkpoints: saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
new_next = '''Próximos checkpoints: memória de cálculo comum e expansão dos property-based tests nas fronteiras legais e monetárias. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
if old_next in readme:
    readme = readme.replace(old_next, new_next, 1)
elif new_next not in readme:
    raise SystemExit("README next checkpoint marker not found")

old_decision = '''25. O limiar proporcional de férias não pode existir como constante jurídica escondida no engine: `vacation.acquisition_period` v1.1 declara `proportional_qualifying_days=15` e o método de aquisição proporcional; leitores 1.0.0 não aceitam silenciosamente o contrato 1.1.0.\n'''
new_decision = '''25. O limiar proporcional de férias não pode existir como constante jurídica escondida no engine: `vacation.acquisition_period` v1.1 declara `proportional_qualifying_days=15` e o método de aquisição proporcional; leitores 1.0.0 não aceitam silenciosamente o contrato 1.1.0.\n26. H29 deve executar a matriz eSocial `01/02/07/33` como escopo fechado e cruzar a matriz geral com as regras específicas de 13º e férias proporcionais; divergência ou motivo fora do conjunto suportado é erro, não aproximação.\n27. O saldo salarial de H29 v1 não usa divisor 30 universal. O denominador é o número de dias civis do mês de desligamento e o numerador é fornecido explicitamente como dias considerados até o desligamento; regimes fora de mensalista/quinzenalista normalizado ficam fora do escopo.\n'''
if old_decision in readme:
    readme = readme.replace(old_decision, new_decision, 1)
elif new_decision not in readme:
    raise SystemExit("README decision marker not found")

changelog_marker = "### 2026-09-13 — checkpoint de férias/A02 na Fase 3"
changelog_entry = '''### 2026-09-13 — checkpoint H29 limitado na Fase 3\n\n- criado `sanida_fiscal/termination_v1.py` com escopo fail-closed para mensalista/quinzenalista normalizado, contrato por prazo indeterminado e motivos eSocial `01/02/07/33`;\n- materializada no `CANDIDATE` a regra já inventariada `termination.vacation_proportional`, elevando o exemplo para 21 regras sem criar nova família de payload;\n- matriz geral e regras específicas de 13º/férias proporcionais passam a ser cruzadas em runtime;\n- motivo `01` bloqueia proporcionais; `02/07/33` habilitam 13º e férias proporcionais;\n- saldo salarial usa os dias civis reais do mês e reproduz R$ 3.100 / 31 × 10 = R$ 1.000;\n- no caso A02, H29 preserva simultaneamente 3/12 de 13º no ano civil e 7/12 de férias no período aquisitivo;\n- resultado continua `partial_estimate` e não incorpora aviso, FGTS, seguro-desemprego ou demais verbas excluídas;\n- suíte completa chega a **130 testes verdes**.\n\n'''
if changelog_entry not in readme:
    if changelog_marker not in readme:
        raise SystemExit("README changelog marker not found")
    readme = readme.replace(changelog_marker, changelog_entry + changelog_marker, 1)

readme_path.write_text(readme, encoding="utf-8")

phase3_path = ROOT / "docs" / "phase3-library-v1.md"
doc = phase3_path.read_text(encoding="utf-8")

doc = doc.replace("## 9. Estado do CI", "## 10. Estado do CI", 1)
doc = doc.replace("## 10. Limites preservados", "## 11. Limites preservados", 1)
doc = doc.replace("## 11. Próximos checkpoints da Fase 3", "## 12. Próximos checkpoints da Fase 3", 1)

marker = "## 10. Estado do CI"
section = '''## 9. Quinto checkpoint — saldo salarial e matriz H29 limitada\n\nO núcleo rescisório limitado vive em:\n\n```text\nsanida_fiscal/termination_v1.py\ntests/test_termination_v1.py\n```\n\nO objetivo não é transformar H29 em calculadora universal de rescisão. O engine executa apenas o subconjunto fechado na Fase 1/2 e devolve explicitamente `partial_estimate`.\n\n### 9.1 Matriz de motivos suportados\n\n`select_termination_rule_bundle()` cruza três fontes computacionais do próprio contrato:\n\n```text\ntermination.reason_scope\ntermination.thirteenth_proportional\ntermination.vacation_proportional\n```\n\nA regra de férias proporcionais já existia no inventário/coverage, mas ainda não estava materializada no `CANDIDATE`. Ela foi adicionada como 21ª regra representativa, usando a família já existente `code_eligibility`; nenhuma família de schema nova foi criada.\n\nA matriz executada é:\n\n```text\n01  justa causa pelo empregador\n    saldo salarial             sim\n    13º proporcional           não\n    férias proporcionais       não\n\n02  sem justa causa\n    saldo salarial             sim\n    13º proporcional           sim\n    férias proporcionais       sim\n\n07  pedido de demissão\n    saldo salarial             sim\n    13º proporcional           sim\n    férias proporcionais       sim\n\n33  acordo art. 484-A\n    saldo salarial             sim\n    13º proporcional           sim\n    férias proporcionais       sim\n```\n\nMotivo fora desse conjunto é `UNSUPPORTED`. O bundle também exige que os conjuntos `eligible_codes`/`ineligible_codes` das regras específicas cubram exatamente os mesmos quatro motivos e reproduzam os mesmos booleans da matriz. Uma divergência contratual falha antes do cálculo.\n\n### 9.2 Limite de vínculo suportado\n\nA aplicabilidade de `termination.reason_scope` é executada, não apenas documentada:\n\n```text\nemployment_regime ∈ {monthly, biweekly}\ncontract_term      = indefinite\n```\n\nNo caminho monetário, `monthly_base_salary` é a base mensal normalizada declarada pelo `ProrationPayload`. Horista, diarista, semanalista e contrato a prazo determinado não são convertidos silenciosamente para esse modelo.\n\n### 9.3 Saldo salarial\n\n`calculate_salary_balance()` executa literalmente:\n\n```text\nmonthly_base_salary\n× days_counted_through_termination\n÷ calendar_days_in_month\n```\n\nO denominador vem do calendário do próprio mês de desligamento. Não existe divisor 30 universal. O caso oficial fechado na Fase 1 fica executável:\n\n```text\n3100.00 / 31 × 10 = 1000.00\n```\n\nO mesmo numerador em abril de 2026 produz `1033.33`, demonstrando que a diferença 30/31 não é apagada. O numerador não pode exceder o dia do desligamento e o resultado não pode ultrapassar a base mensal.\n\n### 9.4 13º proporcional e férias proporcionais permanecem calendários distintos\n\nPara motivos `02/07/33`, `calculate_h29_limited_estimate()` reutiliza os núcleos já testados:\n\n- 13º: `calculate_thirteenth_accrual()` + referência remuneratória rescisória + fórmula proporcional do 13º;\n- férias: `calculate_proportional_vacation_accrual()` ancorada no período aquisitivo.\n\nIsso produz uma regressão estrutural útil no mesmo caso:\n\n```text\nadmissão       01/09/2025\ndesligamento   31/03/2026\n\n13º rescisório        3/12  (jan-mar/2026)\nférias proporcionais  7/12  (set/2025-mar/2026)\n```\n\nO engine não reutiliza a contagem de um direito como se fosse a do outro. No motivo `01`, os dois cálculos proporcionais nem são executados.\n\nA fronteira de 15 dias também foi integrada: admissão em 17/03 e desligamento em 30/03 produz zero avo; em 31/03 produz um avo, respeitando cada calendário próprio.\n\n### 9.5 A saída continua parcial\n\n`H29LimitedEstimate` carrega:\n\n```text\nresult_promise = partial_estimate\nuser_disclosure_required = true\nreason\nsalary_balance\nthirteenth_proportional\nvacation_proportional\nacquired_vacation_if_due\nincluded_items\nexcluded_items\n```\n\nNeste checkpoint, `acquired_vacation_if_due` é a elegibilidade da matriz; o cálculo monetário de períodos adquiridos/vencidos não foi expandido a partir do antigo checkbox.\n\nContinuam fora do total, conforme `ScopeDeclarationPayload`:\n\n- aviso prévio ou desconto de aviso;\n- multa e saque do FGTS;\n- seguro-desemprego;\n- indenizações de estabilidade;\n- verbas específicas de CCT;\n- regras de contrato por prazo determinado;\n- rescisão indireta sem contexto judicial resolvido;\n- itens variáveis rescisórios não explicitamente modelados.\n\nA saída, portanto, não pode ser apresentada como “total universal da rescisão”.\n\n'''
if section not in doc:
    if marker not in doc:
        raise SystemExit("phase3 CI marker not found")
    doc = doc.replace(marker, section + marker, 1)

old_ci = '''Após o checkpoint de férias/A02 e a emenda contratual v1.1:\n\n```text\nRepository baseline       PASS\nFiscal Contract v1.1      PASS\nInventory coverage        32/32\nPayload families          18/18\nFiscal engine             PASS\nProperty-based tests      PASS\n\n114 passed\n```\n\nRun de referência: `34767270494`.\n'''
new_ci = '''Após o checkpoint H29 limitado:\n\n```text\nRepository baseline       PASS\nFiscal Contract v1.1      PASS\nCandidate rules           21\nInventory coverage        32/32\nPayload families          18/18\nFiscal engine             PASS\nProperty-based tests      PASS\n\n130 passed\n```\n\nRun de validação do checkpoint: `34770991820`.\n'''
if old_ci in doc:
    doc = doc.replace(old_ci, new_ci, 1)
elif new_ci not in doc:
    raise SystemExit("phase3 old CI block not found")

old_limit = '''- saldo salarial e matriz de elegibilidade completa do H29;\n'''
new_limit = '''- cálculo monetário de períodos integrais adquiridos/vencidos na rescisão além da elegibilidade já exposta;\n- incidências e memória fiscal final do saldo salarial no orquestrador H29, que serão compostas a partir dos primitives já existentes sem ampliar o escopo de verbas;\n'''
if old_limit in doc:
    doc = doc.replace(old_limit, new_limit, 1)
elif new_limit not in doc:
    raise SystemExit("phase3 H29 limit marker not found")

old_next = '''1. implementar saldo salarial e matriz H29 limitada;\n2. consolidar memória de cálculo comum;\n3. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.\n'''
new_next = '''1. consolidar memória de cálculo comum entre os núcleos já implementados;\n2. ampliar invariantes/property-based tests nas fronteiras legais e monetárias;\n3. preparar o gate de fechamento da Fase 3 sem antecipar collectors, publicação ou migração de consumidores.\n'''
if old_next in doc:
    doc = doc.replace(old_next, new_next, 1)
elif new_next not in doc:
    raise SystemExit("phase3 next checkpoint marker not found")

phase3_path.write_text(doc, encoding="utf-8")
print("README and Phase 3 H29 checkpoint synchronized")
