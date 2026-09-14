# Fase 4 — Fontes e sensores

**Status:** CONCLUÍDA  
**Início:** 13/09/2026  
**Encerramento:** 13/09/2026  
**Base:** `main` após o fechamento da Fase 3

## 1. Objetivo

Separar obtenção de bytes, interpretação da fonte, persistência de evidência e formação dos artefatos de compatibilidade.

```text
source registry / source policy
        ↓
collector
        ↓
raw snapshot imutável
        ↓
parser versionado
        ↓
normalized source candidate
        ↓
compatibility bridge
        ↓
legacy artifact
```

A Fase 4 não classifica mudança paramétrica versus estrutural e não publica releases do Contrato Fiscal Canônico. Essas responsabilidades continuam reservadas à Fase 5.

## 2. Superfície legada

`docs/phase4-collection-surface-v1.json` inventaria cinco caminhos:

1. IRRF/RFB em `scraper.py`;
2. INSS em `scraper.py`;
3. Selic/SGS 432 em `update_taxas.py`;
4. CDI via FTP B3/Cetip;
5. reutilização de `taxas_bacen.json` por `scraper.py`.

Na branch de fechamento, os quatro coletores externos foram migrados. `dados_fiscais.json` e `taxas_bacen.json` permanecem artefatos de compatibilidade para consumidores legados até as fases de publicação/migração.

## 3. Fundação comum

`sanida_fiscal/sources_v1.py` implementa `SourceSpec`, `HttpCollectorV1`, `RawSnapshot`, `SnapshotStore`, parser versionado e `NormalizedSourceCandidate`.

Estados de coleta:

```text
COLLECTED
NOT_MODIFIED
SOURCE_UNAVAILABLE
```

Falhas operacionais:

```text
TIMEOUT
NETWORK_ERROR
HTTP_CLIENT_ERROR
HTTP_SERVER_ERROR
```

`PARSER_INCOMPATIBLE` permanece distinto de `SOURCE_UNAVAILABLE`. Snapshots nascem antes do parser e usam identidade `sha256(raw_bytes)`.

`sanida_fiscal/source_runtime_v1.py` acrescenta estado operacional persistente, `CandidateStore`, ETag/Last-Modified e ponteiros last-good para snapshot, candidato, parser e timestamp.

## 4. Domínio `payroll_fiscal`

### 4.1 RFB / IRRF

`RFB_IRRF_TABLE_2026` usa a URL do `docs/source-registry-v1.json`, `HttpCollectorV1` e `rfb_irrf_table_v1@1.0.0`.

### 4.2 INSS

`INSS_TABLE_2026` é a única entrada automática. Notícia anual pinned e `@@search` não são fallback. `inss_employee_table_v1@1.0.0` normaliza as quatro faixas de 2026 e preserva a separação do 13º.

### 4.3 Artefato de compatibilidade

`sanida_fiscal/legacy_artifact_v1.py` é a única fronteira `normalized candidate → dados_fiscais.json 2.2.0`.

Nova escrita exige candidatos RFB + INSS `PARSED` da execução corrente, mesmo `reference_year` e igualdade com o ano UTC corrente. Fallback fiscal estático e relabelagem de ano anterior são proibidos.

## 5. Persistência real em produção

O runner de GitHub Actions é efêmero. A evidência de produção é persistida em:

```text
evidence/source-runtime-v1/
├── snapshots/<source_id>/<prefix>/<sha256>.<ext>
├── candidates/<source_id>/<prefix>/<sha256>.json
└── state/<source_id>.json
```

A política está em `docs/phase4-production-persistence-v1.json`.

`main.yml` e `taxas.yml` usam o mesmo `SFA_SOURCE_RUNTIME_ROOT=evidence/source-runtime-v1` e o mesmo concurrency group de escrita. Um artefato novo só é staged junto com a evidência depois de um gate que prova que seus hashes resolvem para bytes/candidatos persistidos e coerentes com o estado last-good.

Em falha, o artefato é restaurado e a evidência operacional da tentativa pode ser commitada sozinha; o workflow termina em erro depois da persistência do diagnóstico.

## 6. Domínio `financial_reference`

A política do domínio financeiro está em:

- `docs/financial-source-registry-v1.json`;
- `docs/phase4-financial-reference-policy-v1.json`.

Esse registro é separado do registro jurídico-fiscal de folha. Selic/CDI não herdam automaticamente semântica de vigência jurídica de INSS/IRRF.

### 6.1 Selic — BCB SGS 432

Fonte operacional canônica:

```text
BCB_SELIC_META_SGS_432
https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json
```

Pipeline:

```text
financial-source-registry-v1.json
        ↓
HttpCollectorV1
        ↓
raw JSON snapshot
        ↓
bcb_selic_meta_sgs432_v1@1.0.0
        ↓
annual_rate_pct como string decimal
```

Nenhum `float` é introduzido no parser. A conversão para número JSON ocorre apenas na fronteira do artefato legado.

### 6.2 CDI — BCB SGS 12

O caminho automático antigo via `ftp.cetip.com.br` foi removido de `update_taxas.py`.

Fonte operacional canônica:

```text
BCB_CDI_DAILY_SGS_12
https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados/ultimos/1?formato=json
```

O parser `bcb_cdi_daily_sgs12_v1@1.0.0` preserva a unidade efetivamente fornecida pela série: **percentual ao dia útil**. A compatibilidade com o consumidor legado exige taxa anual, derivada somente em `sanida_fiscal/financial_artifact_v1.py`:

```text
annual_pct = ((1 + daily_rate_pct / 100) ^ 252 - 1) * 100
```

com `Decimal` e `ROUND_HALF_UP` a duas casas. A fixture `0.051660% a.d.` reproduz `13.90% a.a.`.

A B3 continua referência de metodologia/corroboração do benchmark DI. O FTP legado deixou de ser input de produção e não é fallback automático.

### 6.3 `taxas_bacen.json` v1.4

`sanida_fiscal/financial_artifact_v1.py` é a fronteira `normalized financial candidates → taxas_bacen.json`.

Nova escrita exige candidatos atuais `PARSED` para SGS 432 e SGS 12. O artefato registra source id/URL, status HTTP, hashes de snapshot/candidato, parser id/versão, timestamp da coleta, data da observação, código SGS, valor e unidade de origem. Para CDI também registra base de 252 dias úteis e valor anualizado.

`cdi_basis` é:

```text
bcb_sgs_12_daily_compounded_252
```

### 6.4 Falha sem falsa atualidade

Foram removidos de `update_taxas.py`:

- `FALLBACK_SELIC`;
- `FALLBACK_CDI`;
- FTP Cetip/B3;
- discovery de arquivos por data;
- `minimal_fallback_written`;
- `static_reference_values`.

Se qualquer fonte falhar, `update_taxas.py` termina em erro **sem reescrever `taxas_bacen.json`**. O workflow persiste estado/snapshot da tentativa quando houver, restaura artefato não comprovado e falha no gate final.

### 6.5 A05 — `PARSER_INCOMPATIBLE` e last-good financeiro

A política final é:

```text
preserve_auditable_block_new_consumption
```

Quando uma coleta corrente obtém bytes mas o parser registrado fica incompatível:

1. o raw snapshot corrente e o estado de falha são persistidos;
2. os ponteiros do último snapshot/candidato `PARSED` permanecem intactos para auditoria;
3. o `taxas_bacen.json` previamente validado permanece byte a byte inalterado;
4. o last-good preservado continua verificável por um caminho **audit-only**;
5. o verificador normal para novo consumo falha fechada enquanto o estado corrente for `PARSER_INCOMPATIBLE`;
6. o candidato antigo não pode formar novo `taxas_bacen.json`;
7. o last-good financeiro em quarentena não pode ser incorporado a um novo `dados_fiscais.json`;
8. `generated_at_utc` não é renovado e o valor anterior não é relabelado como corrente;
9. o consumo normal só volta quando uma coleta corrente produzir novamente `PARSED` com evidência persistida válida.

A implementação está em `sanida_fiscal/financial_evidence_v1.py`. A prova de regressão está em `test_A05_parser_incompatible_preserves_last_good_but_blocks_new_consumption`.

### 6.6 Gate financeiro de evidência

`sanida_fiscal/financial_evidence_v1.py` e `scripts/validate_financial_evidence_v1.py` verificam source id/URL, hashes e existência real de snapshot/candidato, parser last-good, timestamp, coerência com `SourcePipelineState` e confinamento ao runtime root.

`taxas.yml` só commita `taxas_bacen.json` junto da árvore de evidência se produtor + gate passarem.

## 7. Invariantes executáveis

A Fase 4 fecha, entre outras, as seguintes invariantes:

1. snapshot content-addressed e íntegro;
2. candidato normalizado content-addressed e íntegro;
3. `SOURCE_UNAVAILABLE` distinto de `PARSER_INCOMPATIBLE`;
4. RFB e INSS vinculados às URLs registradas;
5. pinned/search INSS fora do caminho automático;
6. fallback fiscal estático proibido;
7. `dados_fiscais.json` só nasce de candidatos atuais e do ano correto;
8. falha corrente não apaga ponteiros last-good;
9. hashes publicados precisam resolver para evidência persistida;
10. Selic usa BCB SGS 432 via JSON bruto + parser decimal;
11. CDI usa BCB SGS 12 via JSON bruto + parser decimal;
12. taxa CDI diária não é confundida com taxa anual;
13. annualização CDI legada é explícita, determinística e testada em 252 dias úteis;
14. FTP B3/Cetip não é input/fallback automático;
15. `update_taxas.py` não contém fallback estático;
16. falha financeira não atualiza `generated_at_utc` nem sobrescreve last-good;
17. `taxas.yml` persiste artefato + evidência em transação verificada;
18. last-good sob `PARSER_INCOMPATIBLE` permanece auditável;
19. esse mesmo last-good fica bloqueado para qualquer novo consumo/publicação;
20. normalidade só é restaurada por candidato corrente `PARSED` com evidência válida.

Os gates permanentes no `Remake CI` são:

- `scripts/validate_phase4_foundation.py`;
- `scripts/validate_phase4_preclosure_gate.py` — fronteira de produção;
- `scripts/validate_phase4_closure_gate.py` — fechamento formal.

## 8. Auditoria final e fechamento

A auditoria machine-readable `docs/phase4-final-boundary-audit-v1.json` encerra **A01–A05 como `CORRECTED`** e autoriza formalmente o fechamento.

O documento `docs/phase4-closure-gate.md` registra os critérios de promoção e a separação entre preservação auditável e autorização de consumo.

No head de fechamento `718378b25346b1db2ffa26e4d7b2607350f10094`, o **Remake CI run 34797878486** concluiu com sucesso:

- repository baseline: PASS;
- Fiscal Contract v1.1: PASS;
- inventário: 32/32;
- famílias de payload: 18/18;
- suíte integral: **210 passed**;
- Phase 3 closure gate: PASS;
- Phase 4 foundation: PASS;
- Phase 4 production boundary: PASS (`A05 CORRECTED`);
- Phase 4 formal closure gate: PASS (`A01-A05 corrected; formal closure authorized`).

## 9. Estado de ativação

O fechamento da fase e a ativação dos workflows são conceitos distintos. Antes do merge, a infraestrutura está materializada na branch e os artefatos commitados na base continuam legados.

Após merge em `main`, a ordem segura de ativação é:

1. `taxas.yml` produzir o primeiro `taxas_bacen.json` 1.4.0 com evidência durável;
2. somente depois `main.yml` pode produzir novo `dados_fiscais.json` com RFB + INSS + Selic + CDI comprovados;
3. semantic diff/promoção continuam reservados à Fase 5;
4. WordPress/plugin, `folha-core` e H26–H29 continuam congelados até a Fase 6.

Nenhuma fixture é semeada como evidência de produção.

## 10. Limites deliberados e handoff

Ficam fora da Fase 4:

- semantic diff e classificação de mudança — **Fase 5**;
- política de promoção/publicação canônica — **Fase 5**;
- substituição dos artefatos de compatibilidade por releases finais — **Fase 5/6**;
- migração de WordPress, `folha-core` e H26–H29 — **Fase 6**.

A próxima etapa é **Fase 5 — Diff semântico e gates de publicação**. A Fase 4 não deve ser reaberta por redesign oportunista; somente por defeito objetivo na camada de fontes/sensores ou por requisito comprovadamente necessário das fases seguintes.
