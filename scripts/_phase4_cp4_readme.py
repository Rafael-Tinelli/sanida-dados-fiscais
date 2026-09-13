from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

third = '''Terceiro checkpoint executável:\n\n- `docs/phase4-inss-source-resolution-v1.json` fecha formalmente a divergência `INSS_TABLE_2026` × notícia anual pinned/descoberta;\n- a URL registrada em `INSS_TABLE_2026` passa a ser a única entrada operacional automática do novo pipeline de INSS;\n- notícia anual e `@@search` ficam proibidos como fallback automático: indisponibilidade da URL canônica produz `SOURCE_UNAVAILABLE`;\n- `sanida_fiscal/inss_employee_v1.py` implementa `inss_employee_table_v1@1.0.0` para a tabela oficial de empregado, doméstico e trabalhador avulso;\n- o candidato normalizado preserva quatro faixas progressivas, teto `8475.55`, vigência 2026, referência à Portaria Interministerial MPS/MF nº 13/2026 e separação do 13º;\n- notícia anual, mesmo contendo valores, não satisfaz o contrato estrutural do parser canônico;\n- o runner manual passa a aceitar `--source-id INSS_TABLE_2026` sem chamar discovery legado;\n- o gate da Fase 4 cruza política INSS, source registry, fixture e superfície de coleta;\n- suíte integral chega a **172 testes verdes**.\n'''

fourth = third + '''\nQuarto checkpoint executável:\n\n- `sanida_fiscal/source_catalog_v1.py` centraliza os bindings RFB + INSS usados pelo runner e pelo produtor legado;\n- `scraper.py` deixa de conter fetch/parser próprio de RFB/INSS, pinned URL e discovery `@@search`;\n- `sanida_fiscal/legacy_artifact_v1.py` cria a única fronteira explícita `normalized candidate → dados_fiscais.json 2.2.0`;\n- `dados_fiscais.json` fica formalmente classificado como artefato de compatibilidade, não como release do Contrato Fiscal Canônico;\n- uma nova escrita exige candidatos `PARSED` da execução corrente, mesmo `reference_year` e igualdade com o ano UTC corrente;\n- `SourcePipelineState`/304 isolado não pode gerar nova publicação de compatibilidade;\n- o antigo fallback fiscal estático de `scraper.py` foi removido; sem candidato atual, somente um last-good válido do mesmo ano pode permanecer inalterado;\n- candidato/artefato de ano anterior nunca é relabelado como corrente;\n- proveniência do artefato legado passa a carregar hashes de snapshot/candidato e versão do parser;\n- `requirements.txt` passa a instalar o runtime da Fase 4;\n- suíte integral chega a **180 testes verdes** e o gate registra `legacy artifact bridge prepared`.\n'''
if third not in text:
    raise SystemExit("third checkpoint anchor not found")
text = text.replace(third, fourth, 1)

decision = '33. Para `INSS_TABLE_2026`, o novo pipeline usa exclusivamente a URL canônica registrada; notícia anual pinned e `@@search` não são fallback automático e falha da fonte registrada permanece `SOURCE_UNAVAILABLE`.\n'
addition = decision + '34. `dados_fiscais.json` permanece temporariamente como artefato de compatibilidade 2.2.0: só pode ser reescrito a partir de candidatos RFB/INSS `PARSED` da execução corrente e do mesmo ano UTC; estado operacional isolado, fallback estático ou dado de ano anterior não autorizam nova escrita.\n'
if decision not in text:
    raise SystemExit("decision 33 anchor not found")
text = text.replace(decision, addition, 1)

old_next = '''## 20. Próxima etapa\n\nContinuar a **Fase 4 — Fontes e sensores** com RFB e INSS já materializados como pipelines canônicos independentes do fetch/parser legado.\n\nPróximo checkpoint: preparar a migração de `scraper.py` para consumir os pipelines RFB e INSS sem duplicar fetch/parser, definindo explicitamente a fronteira de transição para `dados_fiscais.json`. A remoção do discovery/pinned do código de produção deve ocorrer apenas nessa migração controlada. Semantic diff, promoção e publicação continuam reservados à Fase 5.\n'''
new_next = '''## 20. Próxima etapa\n\nContinuar a **Fase 4 — Fontes e sensores** com o produtor de compatibilidade já desacoplado dos parsers legados de RFB/INSS.\n\nPróximo checkpoint: resolver a **retenção/persistência de snapshots e estado operacional no caminho que será ativado em produção**, para que os hashes adicionados ao `dados_fiscais.json` correspondam a evidência realmente preservada fora do runner efêmero. Em seguida, migrar o domínio separado `financial_reference` (Selic/SGS e CDI) e eliminar fallback financeiro capaz de fingir atualidade. Semantic diff, promoção e publicação continuam reservados à Fase 5.\n'''
if old_next not in text:
    raise SystemExit("next-step anchor not found")
text = text.replace(old_next, new_next, 1)

anchor = '## 22. Changelog do README\n\n'
entry = '''## 22. Changelog do README\n\n### 2026-09-13 — fronteira `scraper.py` → `dados_fiscais.json` na Fase 4\n\n- criado catálogo único de pipelines para RFB e INSS;\n- removidos de `scraper.py` os parsers de folha, pinned URL e discovery do INSS;\n- criado bridge explícito para manter `dados_fiscais.json` 2.2.0 apenas como artefato de compatibilidade;\n- nova escrita passa a exigir candidatos atuais `PARSED` e competência anual coerente;\n- removido o fallback fiscal estático e bloqueada relabelagem de ano anterior;\n- proveniência legada passa a incluir hashes de snapshot/candidato e parser id/versão;\n- runtime de produção passa a instalar Pydantic/HTTPX/BeautifulSoup necessários ao novo caminho;\n- suíte integral chega a **180 testes verdes**.\n\n'''
if anchor not in text:
    raise SystemExit("changelog anchor not found")
text = text.replace(anchor, entry, 1)

path.write_text(text, encoding="utf-8")
