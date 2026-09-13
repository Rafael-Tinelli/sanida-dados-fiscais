from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

old_bcb = """A Selic já é obtida via SGS/BCB no código atual.\n\nA estratégia para CDI será revisada para verificar se o consumo pode ser centralizado em recurso oficial estruturado, reduzindo dependências de FTP/formato legado."""
new_bcb = """Na Fase 4, o domínio `financial_reference` passou a ter registro operacional próprio em `docs/financial-source-registry-v1.json`, separado do registro jurídico-fiscal de folha.\n\nA Selic usa a série oficial **SGS 432** do BCB. O CDI deixou de depender do FTP Cetip/B3 no caminho automático: a entrada operacional canônica passou a ser a série **SGS 12** do BCB, preservada como taxa diária decimal; a anualização em 252 dias úteis ocorre somente na fronteira de compatibilidade de `taxas_bacen.json`. A B3 permanece referência de metodologia/corroboração do benchmark DI, não fallback automático."""
if old_bcb not in text:
    raise SystemExit("README sync: BCB strategy block not found")
text = text.replace(old_bcb, new_bcb, 1)

if "Sexto checkpoint executável:" not in text:
    marker = "\n### Fase 5 — Diff semântico e gates de publicação\n"
    if marker not in text:
        raise SystemExit("README sync: Phase 5 marker not found")
    cp6 = """
Sexto checkpoint executável:

- criado `docs/financial-source-registry-v1.json`, mantendo `financial_reference` separado do registro jurídico-fiscal de folha;
- Selic migra para `BCB_SELIC_META_SGS_432`, via JSON bruto do SGS 432 + parser decimal `bcb_selic_meta_sgs432_v1@1.0.0`;
- CDI migra para `BCB_CDI_DAILY_SGS_12`, via JSON bruto do SGS 12 + parser decimal `bcb_cdi_daily_sgs12_v1@1.0.0`;
- o parser do CDI preserva a unidade de origem `% ao dia útil`; a taxa anual legada é derivada somente no bridge por `((1 + taxa_dia/100)^252 - 1) × 100`, com `Decimal` e `ROUND_HALF_UP` a duas casas;
- a fixture `0.051660% a.d.` reproduz `13.90% a.a.`, coerente com o valor legado esperado;
- `update_taxas.py` deixa de conter FTP Cetip/B3, discovery por arquivo e `FALLBACK_SELIC`/`FALLBACK_CDI`;
- falha de qualquer fonte não reescreve `taxas_bacen.json`, não atualiza `generated_at_utc` e não relabela last-good como observação corrente;
- `sanida_fiscal/financial_artifact_v1.py` cria a fronteira explícita `normalized financial candidates → taxas_bacen.json 1.4.0`;
- `sanida_fiscal/financial_evidence_v1.py` e `scripts/validate_financial_evidence_v1.py` comprovam snapshot, candidato, parser, timestamp e estado antes do commit;
- `taxas.yml` passa a usar `evidence/source-runtime-v1` e a mesma transação fail-closed de artefato + evidência usada pelo workflow principal;
- B3 permanece referência de metodologia/corroboração do DI, mas o FTP legado deixa de ser input/fallback automático;
- suíte integral chega a **200 testes verdes** e o gate registra `Selic SGS 432 + CDI SGS 12 migrated; static financial fallback removed`.

"""
    text = text.replace(marker, "\n" + cp6 + marker.lstrip("\n"), 1)

old_decision = "35. O runtime de fontes do workflow `main.yml` não pode depender do filesystem efêmero do runner: snapshots, candidatos normalizados e estado operacional de RFB/INSS são persistidos em `evidence/source-runtime-v1`; um artefato não verificado nunca é commitado, mas evidência operacional de uma execução falha pode ser preservada antes de o workflow sinalizar erro."
new_decision = old_decision + """
36. `financial_reference` possui registro, parser e política de falha próprios; não herda vigência jurídica de `payroll_fiscal`.
37. Selic de compatibilidade usa BCB SGS 432 como entrada operacional estruturada; CDI usa BCB SGS 12 na unidade diária de origem e só é anualizado no bridge legado em base de 252 dias úteis.
38. FTP B3/Cetip não é fallback automático nem input de produção após a migração; B3 permanece autoridade/metodologia do benchmark DI e fonte de corroboração humana.
39. Falha financeira nunca autoriza fallback estático nem refresh de timestamp: `taxas_bacen.json` só é reescrito com dois candidatos atuais `PARSED` e evidência persistida validada."""
if old_decision not in text:
    raise SystemExit("README sync: decision 35 not found")
text = text.replace(old_decision, new_decision, 1)

old_questions = """- critérios de confirmação multi-fonte para mudanças paramétricas — Fase 4/5;\n- mecanismo de semantic diff e promoção — Fase 5;\n- distribuição para WordPress/SFA e migração dos consumidores — Fase 6;\n- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência."""
new_questions = """- critérios de confirmação multi-fonte para mudanças paramétricas — Fase 4/5;\n- revisão final da fronteira entre `taxas.yml`, `main.yml`, `taxas_bacen.json` e `dados_fiscais.json` — fechamento da Fase 4;\n- mecanismo de semantic diff e promoção — Fase 5;\n- distribuição para WordPress/SFA e migração dos consumidores — Fase 6."""
if old_questions not in text:
    raise SystemExit("README sync: open questions block not found")
text = text.replace(old_questions, new_questions, 1)

old_next = """Continuar a **Fase 4 — Fontes e sensores** com o produtor de compatibilidade já desacoplado dos parsers legados de RFB/INSS.\n\nPróximo checkpoint: migrar o domínio separado `financial_reference`, começando por Selic/SGS e depois CDI, e eliminar qualquer fallback financeiro capaz de fingir atualidade. A retenção/persistência de RFB + INSS no runner de produção já está resolvida no branch; semantic diff, promoção e publicação continuam reservados à Fase 5.\n\nQualquer necessidade de reinterpretar regra jurídica ou alterar a biblioteca da Fase 3 deve voltar explicitamente ao contrato/inventário com evidência concreta, não ser resolvida silenciosamente dentro de collector ou parser.\n\nA ativação em `main`/produção continua congelada: nesta branch `scraper.py` e `main.yml` já estão preparados para a fronteira de compatibilidade e persistência de evidências, mas o conteúdo publicado de `dados_fiscais.json`, `update_taxas.py`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram ativados/migrados pelo merge."""
new_next = """Continuar a **Fase 4 — Fontes e sensores** com os quatro coletores externos já migrados na branch: RFB, INSS, Selic/SGS 432 e CDI/SGS 12.\n\nPróximo checkpoint: revisar a **fronteira final de produção** entre `taxas.yml`, `main.yml`, `taxas_bacen.json` e `dados_fiscais.json`, confirmar que nenhuma rota legada de coleta/fallback permanece alcançável e executar o gate formal de fechamento da Fase 4. Semantic diff, promoção e publicação continuam reservados à Fase 5.\n\nQualquer necessidade de reinterpretar regra jurídica ou alterar a biblioteca da Fase 3 deve voltar explicitamente ao contrato/inventário com evidência concreta, não ser resolvida silenciosamente dentro de collector ou parser.\n\nA ativação em `main`/produção continua congelada: nesta branch `scraper.py`, `update_taxas.py`, `main.yml` e `taxas.yml` já estão preparados para os novos pipelines e persistência de evidências, mas os conteúdos publicados atuais de `dados_fiscais.json` e `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram ativados/migrados pelo merge."""
if old_next not in text:
    raise SystemExit("README sync: next-stage block not found")
text = text.replace(old_next, new_next, 1)

changelog_marker = "### 2026-09-13 — persistência real de evidências no workflow de produção\n"
if "### 2026-09-13 — migração de Selic e CDI na Fase 4\n" not in text:
    if changelog_marker not in text:
        raise SystemExit("README sync: changelog marker not found")
    changelog = """### 2026-09-13 — migração de Selic e CDI na Fase 4

- criado registro próprio de fontes do domínio `financial_reference`;
- Selic passa a usar BCB SGS 432 em pipeline `collector → snapshot → parser decimal → candidato`;
- CDI passa a usar BCB SGS 12 como taxa diária, com annualização explícita somente no bridge legado em base 252;
- removidos FTP Cetip/B3 e fallbacks estáticos do caminho automático de `update_taxas.py`;
- criado `taxas_bacen.json` 1.4.0 como artefato de compatibilidade com proveniência completa;
- `taxas.yml` passa a persistir snapshots/candidatos/estado no mesmo backend Git-tracked e a exigir gate de evidência antes do commit;
- falha financeira preserva o artefato anterior byte a byte e persiste apenas evidência operacional quando aplicável;
- B3 permanece referência metodológica do benchmark DI, sem funcionar como fallback automático;
- suíte integral chega a **200 testes verdes**.

"""
    text = text.replace(changelog_marker, changelog + changelog_marker, 1)

path.write_text(text, encoding="utf-8")
print("README phase4 financial sync: PASS")
