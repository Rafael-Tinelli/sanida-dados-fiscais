from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# README master
readme = ROOT / "README.md"
text = readme.read_text(encoding="utf-8")

old_checkpoint = '''- branches especiais de adiantamento ainda não modeladas falham fechadas em vez de usar `total13 * 0.5`;\n- suíte completa: **89 testes verdes** após o checkpoint de 13º.\n'''
new_checkpoint = '''- branches especiais de adiantamento ainda não modeladas falham fechadas em vez de usar `total13 * 0.5`;\n- o gate da Fase 2 detectou que A02 precisava transportar para a máquina o limiar de 15 dias da fração proporcional de férias; o contrato foi corrigido explicitamente para `schema_version`/`contract_api_version` **1.1.0**, mantendo compatibilidade exata/fail-closed;\n- período aquisitivo e avos proporcionais de férias agora são ancorados no aniversário do vínculo, sem reset em 1º de janeiro;\n- regressão A02 reproduz `01/09/2025 → 31/03/2026 = 7/12`;\n- faixas de direito por faltas e abono de 1/3 do **direito adquirido** estão executáveis;\n- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;\n- suíte completa: **114 testes verdes** após o checkpoint de férias/A02.\n'''
if old_checkpoint in text:
    text = text.replace(old_checkpoint, new_checkpoint, 1)
elif new_checkpoint not in text:
    raise SystemExit("README Phase 3 checkpoint block not found")

old_next = '''Próximos checkpoints: período aquisitivo e férias; saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
new_next = '''Próximos checkpoints: saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.'''
if old_next in text:
    text = text.replace(old_next, new_next, 1)
elif new_next not in text:
    raise SystemExit("README next checkpoint line not found")

old_decision = '''24. Campos narrativos existem para auditoria humana, mas o engine não pode depender deles para decidir operação fiscal.\n'''
new_decision = '''24. Campos narrativos existem para auditoria humana, mas o engine não pode depender deles para decidir operação fiscal.\n25. O limiar proporcional de férias não pode existir como constante jurídica escondida no engine: `vacation.acquisition_period` v1.1 declara `proportional_qualifying_days=15` e o método de aquisição proporcional; leitores 1.0.0 não aceitam silenciosamente o contrato 1.1.0.\n'''
if old_decision in text:
    text = text.replace(old_decision, new_decision, 1)
elif new_decision not in text:
    raise SystemExit("README decision marker not found")

old_open = '''Com a Fase 2 concluída, não restam pendências de design do Contrato Fiscal Canônico v1. As questões abertas pertencem às fases posteriores:\n'''
new_open = '''A Fase 2 permanece formalmente concluída. Se a implementação posterior revelar uma lacuna semântica objetiva, o gate de handoff exige uma emenda explícita e versionada do contrato em vez de hardcode no engine. A primeira ocorrência foi A02, corrigida na Fase 3 como Contrato v1.1. As questões abertas restantes pertencem às fases posteriores:\n'''
if old_open in text:
    text = text.replace(old_open, new_open, 1)
elif new_open not in text:
    raise SystemExit("README open-questions marker not found")

changelog_marker = "### 2026-09-13 — checkpoint de 13º salário na Fase 3"
changelog_entry = '''### 2026-09-13 — checkpoint de férias/A02 na Fase 3\n\n- identificado pelo próprio gate da Fase 2 que o contrato não transportava o limiar de 15 dias das férias proporcionais;\n- Contrato Fiscal Canônico v1 recebeu emenda aditiva e compatibilidade exata em `schema_version`/`contract_api_version` 1.1.0;\n- `vacation.acquisition_period` v1.1 explicita método proporcional e `proportional_qualifying_days=15`;\n- A02 passou a ser executável: `01/09/2025 → 31/03/2026 = 7/12`, sem reset em janeiro;\n- direito por faltas e abono de 1/3 do entitlement passaram a funções puras;\n- principal do abono e terço constitucional permanecem separados nas incidências e bases;\n- suíte completa chega a **114 testes verdes**.\n\n'''
if changelog_entry not in text:
    if changelog_marker not in text:
        raise SystemExit("README changelog marker not found")
    text = text.replace(changelog_marker, changelog_entry + changelog_marker, 1)

readme.write_text(text, encoding="utf-8")

# Phase 3 detailed document
phase3 = ROOT / "docs" / "phase3-library-v1.md"
doc = phase3.read_text(encoding="utf-8")

# Renumber existing tail sections first.
doc = doc.replace("## 8. Estado do CI", "## 9. Estado do CI", 1)
doc = doc.replace("## 9. Limites preservados", "## 10. Limites preservados", 1)
doc = doc.replace("## 10. Próximos checkpoints da Fase 3", "## 11. Próximos checkpoints da Fase 3", 1)

marker = "## 9. Estado do CI"
section = '''## 8. Quarto checkpoint — período aquisitivo, férias proporcionais e abono\n\nO núcleo específico de férias vive em:\n\n```text\nsanida_fiscal/vacation_v1.py\ntests/test_vacation_v1.py\n```\n\n### 8.1 Emenda explícita do Contrato v1 para A02\n\nAo iniciar a implementação, o gate do handoff detectou uma lacuna objetiva: `vacation.acquisition_period` já declarava período de 12 meses, âncora no vínculo e ausência de reset no ano civil, mas não transportava para a máquina o limiar de 15 dias da fração proporcional. Hardcodar `15` no engine violaria a regra de não inferência da Fase 2.\n\nA correção foi feita no próprio contrato, de forma aditiva e versionada:\n\n```text\nschema_version       1.0.0 -> 1.1.0\ncontract_api_version 1.0.0 -> 1.1.0\nrule_inventory       1.0.0 -> 1.1.0\nvacation.acquisition_period.rule_version 1.0.0 -> 1.1.0\n```\n\n`PeriodRulePayload` agora carrega:\n\n```text\nproportional_accrual_method = one_twelfth_per_acquisition_month_or_fraction_gte_days\nproportional_qualifying_days = 15\n```\n\nA compatibilidade continua `exact`: um leitor 1.0.0 não aceita silenciosamente o contrato 1.1.0.\n\n### 8.2 Período aquisitivo e A02\n\n`acquisition_period_for_date()` localiza o período corrente a partir de `employment_start_anniversary`. `calculate_proportional_vacation_accrual()` subdivide esse período em doze fatias aquisitivas sucessivas e conta 1/12 por fatia integral ou fração que alcance o limiar declarado no contrato.\n\nA contagem **não usa meses civis como substituto do período aquisitivo**. Portanto vínculos iniciados no meio do mês continuam ancorados naquele dia.\n\nRegressão A02 congelada e executável:\n\n```text\nadmissão      01/09/2025\ndesligamento  31/03/2026\nperíodo       01/09/2025 .. 31/08/2026\navos           7/12\n```\n\nA fronteira de fração também está testada: 14 dias não geram avo; 15 dias geram 1/12.\n\n### 8.3 Direito em dias por faltas\n\n`vacation_entitlement_from_absences()` executa `EntitlementBandsPayload` sem reconstituir faixas no código:\n\n```text\n0–5 faltas   -> 30 dias\n6–14         -> 24 dias\n15–23        -> 18 dias\n24–32        -> 12 dias\n```\n\nValor fora das faixas suportadas falha fechado; não há extrapolação silenciosa.\n\n### 8.4 Abono pecuniário\n\n`calculate_cash_allowance_days()` aplica o `FractionPayload` de 1/3 sobre **entitled_days**, nunca sobre uma quantidade arbitrária de dias escolhidos para gozo.\n\nCasos suportados ficam exatos:\n\n```text\n30 -> 10 de abono + 20 restantes\n24 ->  8 de abono + 16 restantes\n18 ->  6 de abono + 12 restantes\n12 ->  4 de abono +  8 restantes\n```\n\nSe uma entrada fora do universo contratual produzir fração não inteira, o engine não inventa arredondamento estatutário.\n\n### 8.5 Principal e terço constitucional não são fundidos\n\n`resolve_cash_allowance_tax_treatment()` exige os dois componentes canônicos distintos:\n\n```text\ncash_allowance_principal:                IRRF=no  CP=no\nconstitutional_third_on_cash_allowance:  IRRF=yes CP=no\n```\n\n`cash_allowance_tax_bases()` preserva essa separação até a formação das bases. Exemplo com principal de R$ 1.000 e terço de R$ 333,33:\n\n```text\nbase IRRF = 333.33\nbase CP   = 0.00\n```\n\nPerfis trocados ou componentes fundidos são rejeitados. Isso elimina estruturalmente o defeito de colocar `abono + 1/3` inteiro em um vetor isento de IR.\n\n### 8.6 Limite deliberado\n\nO checkpoint não fabrica uma fórmula monetária específica para transformar dias de abono em principal/terço quando essa fórmula não estiver expressa como payload computacional correspondente. O engine já executa direito em dias e incidências; qualquer semântica monetária adicional precisa entrar explicitamente no contrato antes de ser calculada.\n\n'''
if section not in doc:
    if marker not in doc:
        raise SystemExit("Phase3 CI section marker not found")
    doc = doc.replace(marker, section + marker, 1)

old_ci = '''Após o checkpoint de 13º:\n\n```text\nRepository baseline       PASS\nFiscal Contract v1        PASS\nFiscal engine             PASS\nProperty-based tests      PASS\n\n89 passed\n```\n\nRun de referência: `34766418032`.\n'''
new_ci = '''Após o checkpoint de férias/A02 e a emenda contratual v1.1:\n\n```text\nRepository baseline       PASS\nFiscal Contract v1.1      PASS\nInventory coverage        32/32\nPayload families          18/18\nFiscal engine             PASS\nProperty-based tests      PASS\n\n114 passed\n```\n\nRun de referência: `34767270494`.\n'''
if old_ci in doc:
    doc = doc.replace(old_ci, new_ci, 1)
elif new_ci not in doc:
    raise SystemExit("Phase3 CI block not found")

old_limit = '''- férias/período aquisitivo/abono;\n'''
new_limit = '''- fórmula monetária adicional do abono além das grandezas e incidências já tipadas, se necessária, até que exista payload computacional explícito;\n'''
if old_limit in doc:
    doc = doc.replace(old_limit, new_limit, 1)
elif new_limit not in doc:
    raise SystemExit("Phase3 vacation limit line not found")

old_next_doc = '''1. implementar período aquisitivo, férias proporcionais e abono;\n2. implementar saldo salarial e matriz H29 limitada;\n3. consolidar memória de cálculo comum;\n4. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.\n'''
new_next_doc = '''1. implementar saldo salarial e matriz H29 limitada;\n2. consolidar memória de cálculo comum;\n3. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.\n'''
if old_next_doc in doc:
    doc = doc.replace(old_next_doc, new_next_doc, 1)
elif new_next_doc not in doc:
    raise SystemExit("Phase3 next checkpoints block not found")

phase3.write_text(doc, encoding="utf-8")
print("README and Phase 3 vacation checkpoint synchronized")
