# Fase 4 — Fontes e sensores

**Status:** EM ANDAMENTO  
**Início:** 13/09/2026  
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

Nesta branch, os quatro coletores externos já foram migrados. `dados_fiscais.json` e `taxas_bacen.json` permanecem artefatos de compatibilidade para consumidores legados.

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

## 4. Domínio payroll_fiscal

### 4.1 RFB / IRRF

`RFB_IRRF_TABLE_2026` usa a URL do `docs/source-registry-v1.json`, `HttpCollectorV1` e `rfb_irrf_table_v1@1.0.0`.

### 4.2 INSS

`INSS_TABLE_2026` é a única entrada automática. Notícia anual pinned e `@@search` não são fallback. `inss_employee_table_v1@1.0.0` normaliza as quatro faixas de 2026 e preserva a separação do 13º.

### 4.3 Artefato de compatibilidade

`sanida_fiscal/legacy_artifact_v1.py` é a única fronteira `normalized candidate → dados_fiscais.json 2.2.0`.

Nova escrita exige candidatos RFB + INSS `PARSED` da execução corrente, mesmo `reference_year` e igualdade com o ano UTC corrente. Fallback fiscal estático e relabelagem de ano anterior são proibidos.

## 5. Persistência real em produção

O runner de GitHub Actions é efêmero. A evidência de produção é, portanto, persistida em:

```text
evidence/source-runtime-v1/
├── snapshots/<source_id>/<prefix>/<sha256>.<ext>
├── candidates/<source_id>/<prefix>/<sha256>.json
└── state/<source_id>.json
```

A política está em `docs/phase4-production-persistence-v1.json`.

`main.yml` e `taxas.yml` usam o mesmo `SFA_SOURCE_RUNTIME_ROOT=evidence/source-runtime-v1` e o mesmo concurrency group de escrita. Um artefato novo só é staged junto com a evidência depois de um gate que prova que seus hashes resolvem para bytes/candidatos persistidos e coerentes com o estado last-good.

Em falha, o artefato é restaurado e a evidência operacional da tentativa pode ser commitada sozinha; o workflow termina em erro depois da persistência do diagnóstico.

## 6. Sexto checkpoint — migração de `financial_reference`

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

O parser `bcb_cdi_daily_sgs12_v1@1.0.0` preserva a unidade efetivamente fornecida pela série: **percentual ao dia útil**. Ele não finge que o valor diário já é taxa anual.

A compatibilidade com o consumidor legado exige uma taxa anual. Essa transformação ocorre somente em `sanida_fiscal/financial_artifact_v1.py`:

```text
annual_pct = ((1 + daily_rate_pct / 100) ^ 252 - 1) * 100
```

com `Decimal` e `ROUND_HALF_UP` a duas casas. A fixture `0.051660% a.d.` reproduz `13.90% a.a.`.

A B3 continua sendo a autoridade/metodologia do benchmark DI e pode servir de corroboração humana. O FTP legado deixa de ser input de produção e não é fallback automático.

### 6.3 `taxas_bacen.json` v1.4

`sanida_fiscal/financial_artifact_v1.py` passa a ser a única fronteira `normalized financial candidates → taxas_bacen.json`.

Nova escrita exige candidatos atuais `PARSED` para SGS 432 e SGS 12. O artefato carrega para cada fonte:

- `source_id` e URL;
- status/HTTP status;
- snapshot SHA-256;
- candidate SHA-256;
- parser id/versão;
- timestamp UTC da coleta;
- data da observação na série;
- código SGS;
- valor/unidade de origem.

Para CDI também registra a base de 252 dias úteis e o valor anualizado.

`cdi_basis` passa a ser:

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

Se qualquer fonte falhar, `update_taxas.py` termina em erro **sem reescrever `taxas_bacen.json`**. O workflow persiste estado/snapshot da tentativa quando houver, restaura qualquer artefato não comprovado e falha no gate final.

Isso permite manter o último artefato exatamente como foi observado, sem atualizar `generated_at_utc` nem relabelar valor antigo como atual.

### 6.5 Gate financeiro de evidência

`sanida_fiscal/financial_evidence_v1.py` e `scripts/validate_financial_evidence_v1.py` verificam antes do commit:

1. source id e URL;
2. hash e existência real do raw snapshot;
3. hash e existência real do candidato;
4. parser id/versão last-good;
5. timestamp da observação;
6. igualdade com `SourcePipelineState`;
7. confinamento de caminhos ao runtime root.

`taxas.yml` só commita `taxas_bacen.json` junto da árvore de evidência se produtor + gate passarem.

## 7. Invariantes executáveis

A Fase 4 agora ancora, entre outras:

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
14. FTP B3/Cetip não é mais input/fallback automático;
15. `update_taxas.py` não contém fallback estático;
16. falha financeira não atualiza `generated_at_utc` nem sobrescreve o last-good;
17. `taxas.yml` persiste artefato + evidência em uma única transação verificada.

`scripts/validate_phase4_foundation.py` ancora esses pontos no `Remake CI`.

## 8. Estado atual de ativação

A migração está materializada na branch, mas não ativa em produção enquanto o PR da Fase 4 não for mergeado.

Por isso:

- `taxas_bacen.json` commitado na base ainda é o artefato 1.3.0 produzido pelo caminho legado;
- nenhuma fixture foi copiada para `evidence/source-runtime-v1`;
- os primeiros snapshots/candidatos financeiros reais só surgirão quando `taxas.yml` migrado rodar em `main`.

## 9. Limites deliberados

Ainda pertencem a fases posteriores:

- semantic diff e classificação de mudança — Fase 5;
- promoção/publicação canônica — Fase 5;
- substituição dos artefatos de compatibilidade pelos contratos/releases finais — Fase 5/6;
- migração de WordPress, `folha-core` e H26–H29 — Fase 6.

## 10. Próximo checkpoint

1. revisar a fronteira final entre `taxas.yml`, `main.yml`, `taxas_bacen.json` e `dados_fiscais.json`;
2. confirmar que nenhuma rota legada de coleta/fallback permanece alcançável;
3. executar gate formal de fechamento da Fase 4;
4. só então considerar merge/ativação da Fase 4.

Semantic diff continua fora do escopo até a Fase 5.
