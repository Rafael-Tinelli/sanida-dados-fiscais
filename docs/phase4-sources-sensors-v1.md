# Fase 4 — Fontes e sensores

**Status:** EM ANDAMENTO  
**Início:** 13/09/2026  
**Base:** `main` após o fechamento da Fase 3

## 1. Objetivo

Separar de forma verificável a obtenção de bytes oficiais da interpretação jurídico-fiscal desses bytes.

O pipeline-alvo desta fase começa em:

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
```

A Fase 4 não publica releases e não classifica mudança paramétrica versus estrutural. Essas responsabilidades pertencem à Fase 5.

## 2. Diagnóstico da superfície legada

O inventário machine-readable está em `docs/phase4-collection-surface-v1.json`.

Foram identificados cinco caminhos operacionais relevantes:

1. IRRF/RFB em `scraper.py`;
2. INSS em `scraper.py`;
3. Selic/SGS 432 em `update_taxas.py`;
4. CDI via FTP B3/Cetip;
5. reutilização de `taxas_bacen.json` pelo scraper principal.

Os dois primeiros caminhos já foram migrados, nesta branch, para os pipelines canônicos da Fase 4. O domínio `financial_reference` continua separado e pendente.

## 3. Fundação implementada

`sanida_fiscal/sources_v1.py` separa:

- `SourceSpec`;
- `HttpCollectorV1`;
- `RawSnapshot` / `SnapshotStore`;
- parser versionado;
- `NormalizedSourceCandidate`.

O collector conhece transporte HTTP, timeout e retry. Ele não interpreta regra fiscal.

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

O snapshot nasce antes do parser, usa identidade `sha256(raw_bytes)` e é verificado na leitura. `PARSER_INCOMPATIBLE` permanece distinto de `SOURCE_UNAVAILABLE`.

## 4. Pipelines canônicos materializados

### 4.1 RFB / IRRF

`RFB_IRRF_TABLE_2026` percorre:

```text
docs/source-registry-v1.json
        ↓
HttpCollectorV1
        ↓
SnapshotStore
        ↓
rfb_irrf_table_v1@1.0.0
        ↓
NormalizedSourceCandidate
        ↓
SourcePipelineState
```

`sanida_fiscal/rfb_irrf_v1.py` normaliza tabela mensal, dependente, desconto simplificado e redutor 2026 sem publicar contrato.

### 4.2 INSS

`docs/phase4-inss-source-resolution-v1.json` resolve a divergência entre a fonte canônica e a notícia anual legada.

Política congelada:

- `INSS_TABLE_2026` é a única entrada operacional automática;
- a URL vem do source registry;
- notícia anual pinned e `@@search` não são fallback automático;
- indisponibilidade da URL canônica permanece `SOURCE_UNAVAILABLE`;
- troca de URL canônica exige mudança explícita do registro;
- notícia anual pode servir apenas como corroboração humana.

`sanida_fiscal/inss_employee_v1.py` implementa `inss_employee_table_v1@1.0.0` e normaliza quatro faixas progressivas, teto de 2026, referência à Portaria Interministerial MPS/MF nº 13/2026 e apuração separada do 13º.

### 4.3 Catálogo único de pipelines

`sanida_fiscal/source_catalog_v1.py` passa a ser o catálogo operacional único de bindings `source_id → parser id/version/reference year/parser`.

Tanto o runner manual quanto o produtor de compatibilidade chamam `run_registered_source_pipeline()`. Isso evita que `scraper.py` reimplemente URL, collector ou parser.

`run_source_pipeline()` ganhou apenas uma chave operacional adicional: `use_http_validators`. O runner manual pode manter revalidação condicional; o produtor do artefato legado exige uma observação `PARSED` da execução corrente e, por isso, usa fetch completo.

## 5. Quarto checkpoint — fronteira de transição para `dados_fiscais.json`

A transição é formalizada em `docs/phase4-legacy-artifact-boundary-v1.json`.

### 5.1 Natureza do artefato legado

`dados_fiscais.json` continua temporariamente com `schema_version: 2.2.0` para não quebrar consumidores ainda não migrados.

Ele passa a ser explicitamente classificado como:

```text
compatibility_artifact_for_existing_consumers
```

Ele **não** é Contrato Fiscal Canônico, release validada ou autorização de promoção.

### 5.2 Entradas permitidas

Para a parte de folha, uma nova escrita de `dados_fiscais.json` só pode nascer de candidatos `PARSED` da execução corrente de:

```text
RFB_IRRF_TABLE_2026
INSS_TABLE_2026
```

A adaptação para o shape 2.2.0 acontece em `sanida_fiscal/legacy_artifact_v1.py`.

É nessa fronteira, e somente nela, que strings decimais canônicas podem ser convertidas para `float` porque os consumidores legados ainda esperam números JSON nesse formato.

A camada adiciona à proveniência do artefato:

- `source_id`;
- URL;
- HTTP status;
- status da coleta;
- SHA-256 do snapshot;
- SHA-256 do candidato;
- parser id/versão;
- horário UTC observado.

### 5.3 Regras de segurança da transição

O bridge exige:

1. RFB e INSS com o mesmo `reference_year`;
2. `reference_year` igual ao ano UTC corrente;
3. candidato `PARSED` atual para as duas fontes;
4. nenhum uso de `SourcePipelineState` sozinho para fabricar nova publicação;
5. nenhum fallback fiscal estático;
6. nenhuma relabelagem de 2026 como 2027;
7. se a coleta falhar, o produtor pode apenas manter inalterado um `dados_fiscais.json` já válido **do mesmo ano**;
8. sem candidato atual e sem last-good do mesmo ano, a execução falha sem escrever arquivo.

Com isso, o antigo bloco `minimal_fallback` foi removido de `scraper.py`.

### 5.4 Remoção efetiva do discovery/parser legado

`scraper.py` não contém mais:

- `parse_irrf_receita`;
- `PINNED_INSS_URLS`;
- `find_inss_article_url`;
- `parse_inss_gov`;
- busca INSS por `@@search`;
- parser HTML próprio de RFB/INSS;
- fallback fiscal estático mínimo.

O produtor passa a chamar apenas o catálogo de pipelines da Fase 4 e o bridge de compatibilidade.

### 5.5 Domínio financeiro ainda separado

`taxas_bacen.json` continua sendo validado e consumido como entrada legada de `financial_reference`. Este checkpoint não declara Selic/CDI como migrados nem aplica a eles a semântica jurídica das fontes de folha.

A migração de `update_taxas.py`, a política de CDI e a retenção definitiva dos snapshots são checkpoints posteriores da Fase 4.

## 6. Estado operacional entre execuções

`sanida_fiscal/source_runtime_v1.py` persiste:

- última observação UTC;
- status/HTTP status da coleta;
- `ETag` e `Last-Modified`;
- falhas correntes;
- último snapshot coletado;
- parser id/versão;
- último snapshot parseado;
- fingerprint do último candidato.

Esse last-good é operacional e não equivale a validade jurídica.

O runner manual pode reutilizar HTTP validators. O produtor do artefato legado, por desenho, não usa 304 para gerar uma nova versão: ele exige candidato atual nesta execução.

## 7. Invariantes executáveis

A Fase 4 agora garante, entre outras:

1. snapshot content-addressed e íntegro;
2. `SOURCE_UNAVAILABLE` distinto de `PARSER_INCOMPATIBLE`;
3. RFB e INSS vinculados às URLs do registro;
4. notícia anual INSS rejeitada como input canônico;
5. discovery/pinned removidos do produtor de folha;
6. catálogo único de parsers para runner e produtor;
7. bridge 2.2.0 reproduz os valores canônicos de 2026;
8. candidato 2026 não pode ser publicado como 2027;
9. runtime state/304 sem candidato corrente não pode gerar nova escrita legada;
10. artefato legado de ano anterior não pode ser mantido como se fosse corrente;
11. fallback fiscal estático não pode reaparecer em `scraper.py`;
12. `requirements.txt` instala as dependências runtime da Fase 4.

`scripts/validate_phase4_foundation.py` ancora essas invariantes no `Remake CI`.

## 8. Limites deliberados

Ainda não foram fechados:

- backend definitivo e retenção de snapshots de produção;
- persistência operacional entre runners efêmeros do GitHub Actions;
- migração de Selic/SGS para collector/parser decimal;
- política operacional e autoridade final de CDI;
- remoção de fallback estático eventualmente existente em `update_taxas.py`;
- semantic diff;
- promoção/publicação canônica;
- migração dos consumidores.

## 9. Próximos checkpoints

1. resolver retenção/persistência de snapshots e estado operacional no caminho que será ativado em produção;
2. migrar o domínio `financial_reference`, começando por Selic/SGS e depois CDI;
3. eliminar qualquer fallback financeiro capaz de fingir atualidade;
4. revisar a fronteira final do workflow `main.yml` antes do merge da Fase 4;
5. fechar a Fase 4 com sensores completos e sem antecipar semantic diff da Fase 5.
