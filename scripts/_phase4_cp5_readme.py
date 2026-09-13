from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

old = "Na Fase 4, a identidade mínima do snapshot bruto passa a ser `source_id + sha256(raw_bytes)`, com caminho content-addressed e verificação de integridade na leitura. A política final de retenção/backend continua em aberto para não acoplar a semântica de snapshot a um único meio de armazenamento."
new = "Na Fase 4, a identidade mínima do snapshot bruto passa a ser `source_id + sha256(raw_bytes)`, com caminho content-addressed e verificação de integridade na leitura. No quinto checkpoint, o backend de produção foi fechado para o ambiente real do GitHub Actions: `main.yml` usa `evidence/source-runtime-v1`, rastreado no Git, com snapshots e candidatos normalizados imutáveis por SHA-256 e estado operacional materializado por fonte. A política v1 não faz pruning automático; o histórico Git preserva versões anteriores do estado."
assert text.count(old) == 1
text = text.replace(old, new)

anchor = "- suíte integral chega a **180 testes verdes** e o gate registra `legacy artifact bridge prepared`.\n\n### Fase 5 — Diff semântico e gates de publicação"
insert = """- suíte integral chega a **180 testes verdes** e o gate registra `legacy artifact bridge prepared`.

Quinto checkpoint executável:

- `docs/phase4-production-persistence-v1.json` define o backend real de persistência usado pelo workflow de produção;
- `evidence/source-runtime-v1` passa a ser o runtime rastreado em Git para snapshots, candidatos normalizados e estado operacional;
- `CandidateStore` persiste o payload normalizado canônico sob o mesmo `candidate_sha256` exposto na proveniência;
- `SourcePipelineState` v1.1 preserva timestamp, caminho/hash do snapshot last-good, identidade do parser bem-sucedido e caminho/hash do candidato;
- falha corrente não apaga os ponteiros last-good necessários para comprovar um artefato preservado;
- `scripts/validate_production_evidence_v1.py` verifica que hashes do artefato resolvem para bytes realmente persistidos e confinados ao runtime root;
- `main.yml` usa `SFA_SOURCE_RUNTIME_ROOT=evidence/source-runtime-v1`, nunca commitando um `dados_fiscais.json` que falhe no gate de evidência;
- snapshots/estado de uma tentativa falha podem ser persistidos sem publicar o artefato, e o workflow encerra com erro depois dessa persistência;
- retenção v1 mantém todos os conteúdos únicos, sem pruning automático; estado corrente fica materializado e estados anteriores permanecem no histórico Git;
- suíte integral chega a **187 testes verdes** e o gate registra `production evidence persistence anchored`.

### Fase 5 — Diff semântico e gates de publicação"""
assert text.count(anchor) == 1
text = text.replace(anchor, insert)

old = "34. `dados_fiscais.json` permanece temporariamente como artefato de compatibilidade 2.2.0: só pode ser reescrito a partir de candidatos RFB/INSS `PARSED` da execução corrente e do mesmo ano UTC; estado operacional isolado, fallback estático ou dado de ano anterior não autorizam nova escrita."
new = old + "\n35. O runtime de fontes do workflow `main.yml` não pode depender do filesystem efêmero do runner: snapshots, candidatos normalizados e estado operacional de RFB/INSS são persistidos em `evidence/source-runtime-v1`; um artefato não verificado nunca é commitado, mas evidência operacional de uma execução falha pode ser preservada antes de o workflow sinalizar erro."
assert text.count(old) == 1
text = text.replace(old, new)

old = "- política exata de retenção de snapshots e resiliência das fontes — Fase 4;\n"
assert text.count(old) == 1
text = text.replace(old, "")

old = "Próximo checkpoint: resolver a **retenção/persistência de snapshots e estado operacional no caminho que será ativado em produção**, para que os hashes adicionados ao `dados_fiscais.json` correspondam a evidência realmente preservada fora do runner efêmero. Em seguida, migrar o domínio separado `financial_reference` (Selic/SGS e CDI) e eliminar fallback financeiro capaz de fingir atualidade. Semantic diff, promoção e publicação continuam reservados à Fase 5."
new = "Próximo checkpoint: migrar o domínio separado `financial_reference`, começando por Selic/SGS e depois CDI, e eliminar qualquer fallback financeiro capaz de fingir atualidade. A retenção/persistência de RFB + INSS no runner de produção já está resolvida no branch; semantic diff, promoção e publicação continuam reservados à Fase 5."
assert text.count(old) == 1
text = text.replace(old, new)

old = "A ativação em `main`/produção continua congelada: nesta branch `scraper.py` já foi migrado para a fronteira de compatibilidade, mas `dados_fiscais.json`, `update_taxas.py`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 publicados ainda não foram ativados/migrados pelo merge."
new = "A ativação em `main`/produção continua congelada: nesta branch `scraper.py` e `main.yml` já estão preparados para a fronteira de compatibilidade e persistência de evidências, mas o conteúdo publicado de `dados_fiscais.json`, `update_taxas.py`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram ativados/migrados pelo merge."
assert text.count(old) == 1
text = text.replace(old, new)

anchor = "## 22. Changelog do README\n\n### 2026-09-13 — fronteira `scraper.py` → `dados_fiscais.json` na Fase 4"
insert = """## 22. Changelog do README

### 2026-09-13 — persistência real de evidências no workflow de produção

- definido `evidence/source-runtime-v1` como backend Git-tracked para o runner efêmero do GitHub Actions;
- snapshots brutos e candidatos normalizados passam a ter arquivos content-addressed preservados por SHA-256;
- `SourcePipelineState` v1.1 separa a tentativa corrente do conjunto last-good auditável;
- criado gate que resolve a proveniência de `dados_fiscais.json` contra snapshot, candidato e estado persistidos;
- `main.yml` só stageia o artefato quando esse gate passa;
- falha operacional pode persistir evidência/estado sem publicar dados e ainda termina o workflow em erro;
- política v1 retém todos os conteúdos únicos, sem pruning automático;
- suíte integral chega a **187 testes verdes**.

### 2026-09-13 — fronteira `scraper.py` → `dados_fiscais.json` na Fase 4"""
assert text.count(anchor) == 1
text = text.replace(anchor, insert)

path.write_text(text, encoding="utf-8")
