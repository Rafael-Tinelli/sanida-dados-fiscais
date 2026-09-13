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

1. IRRF/RFB em `scraper.py`, hoje com fetch e parsing acoplados;
2. INSS em `scraper.py`, hoje com discovery/pinned URL e parsing acoplados;
3. Selic/SGS 432 em `update_taxas.py`, já em API oficial estruturada, mas com fetch + parse + conversão acoplados;
4. CDI via FTP B3/Cetip, com descoberta de arquivo, fetch e parse acoplados;
5. reutilização de `taxas_bacen.json` pelo scraper principal como artefato interno derivado.

A primeira aresta objetiva encontrada é o INSS: o registro canônico da Fase 1 aponta `INSS_TABLE_2026`, enquanto o legado pode coletar notícia anual descoberta/pinned. A Fase 4 deve resolver essa diferença como política de fonte; não deve escondê-la dentro do parser.

## 3. Fundação implementada no primeiro checkpoint

O módulo `sanida_fiscal/sources_v1.py` introduziu a separação entre:

- `SourceSpec`;
- `HttpCollectorV1`;
- `RawSnapshot` / `SnapshotStore`;
- parser versionado;
- `NormalizedSourceCandidate`.

O collector conhece apenas transporte HTTP, timeout e retry. Ele **não conhece tabela de IR, INSS, férias, 13º ou rescisão**.

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

O snapshot nasce antes do parser e usa identidade content-addressed `sha256(raw_bytes)`. A leitura recalcula o hash e rejeita corrupção. `PARSER_INCOMPATIBLE` continua separado de `SOURCE_UNAVAILABLE`.

## 4. Segundo checkpoint — primeiro pipeline real RFB/IRRF

O primeiro pipeline específico foi materializado para a fonte canônica:

```text
RFB_IRRF_TABLE_2026
```

Fluxo executável:

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

### 4.1 Parser RFB isolado

`sanida_fiscal/rfb_irrf_v1.py` contém o parser específico da página oficial de tributação de 2026.

Ele normaliza, sem publicar contrato:

- as cinco faixas mensais de IRRF;
- dedução mensal por dependente;
- limite mensal do desconto simplificado;
- tabela de redução mensal de 2026;
- a semântica observada de entrada do redutor: rendimentos tributáveis sujeitos à incidência mensal.

Valores monetários e coeficientes são preservados como strings decimais canônicas. O parser não introduz `float` e falha como `PARSER_INCOMPATIBLE` quando os marcadores ou a estrutura esperada deixam de existir.

A fixture mínima em `tests/fixtures/sources/rfb_irrf_2026_fragment.html` reproduz apenas o trecho oficial necessário ao contrato do parser. O CI não depende de rede externa.

### 4.2 Estado operacional entre execuções

`sanida_fiscal/source_runtime_v1.py` introduz `SourcePipelineState` e `SourceStateStore`.

O estado persistido inclui, separadamente:

- última observação UTC;
- status e HTTP status da coleta;
- `ETag` e `Last-Modified`;
- contador e erro atual de fonte;
- último snapshot coletado;
- parser e versão;
- status e contador de incompatibilidade do parser;
- último snapshot parseado com sucesso;
- SHA-256 canônico do último candidato parseado com sucesso.

Uma falha atual de fonte ou parser **não apaga** a identidade do último candidato parseado com sucesso. Isso é estado operacional, não autorização de uso jurídico do last-good — a validade jurídica continua pertencendo ao contrato e aos gates posteriores.

### 4.3 Revalidação HTTP

`HttpCollectorV1` passou a transportar `ETag` e `Last-Modified` e aceita `If-None-Match` / `If-Modified-Since` em execuções posteriores.

Um `304 Not Modified` não fabrica novo snapshot e preserva a trilha anterior.

Os validadores condicionais só são reutilizados quando o último parse foi bem-sucedido **com o mesmo `parser_id` e `parser_version`**. Se o parser muda, a próxima consulta é completa para que a nova versão processe bytes reais, em vez de aceitar 304 e assumir compatibilidade.

### 4.4 Fingerprints sem antecipar a Fase 5

A runtime informa apenas duas igualdades operacionais:

- `raw_snapshot_unchanged` — igualdade de SHA-256 dos bytes brutos;
- `candidate_fingerprint_unchanged` — igualdade do JSON normalizado canônico.

Isso **não** classifica `PARAMETER_CHANGE`, `STRUCTURAL_CHANGE` ou qualquer outra classe do Contrato v1. A interpretação semântica da diferença continua reservada à Fase 5.

### 4.5 Runner real sem publicação

`scripts/run_source_pipeline_v1.py` executa a fonte RFB real por CLI e grava runtime local, por padrão, em:

```text
.source-runtime/snapshots/
.source-runtime/state/
```

`.source-runtime/` é ignorado pelo Git. O runner não modifica `dados_fiscais.json`, não promove release e não publica nada.

## 5. Invariantes já executáveis

O conjunto de testes da Fase 4 garante agora:

1. snapshot é content-addressed e idempotente;
2. corrupção posterior do snapshot é detectada;
3. coleta 2xx persiste bytes antes do parser;
4. 404 não é retryado e nunca fabrica snapshot;
5. 5xx é retryado e, se persistente, vira `SOURCE_UNAVAILABLE` sem snapshot;
6. timeout é distinguível de falha HTTP;
7. `PARSER_INCOMPATIBLE` é distinguível de `SOURCE_UNAVAILABLE`;
8. parser não aceita resultado de coleta malsucedida;
9. candidato normalizado carrega identidade exata do snapshot;
10. o registro oficial da Fase 1 pode ser carregado como superfície de coleta sem reinterpretação;
11. o parser RFB reproduz os parâmetros oficiais congelados de 2026;
12. uma segunda execução usa `ETag` e trata 304 como não modificação operacional;
13. resposta 200 byte a byte igual produz o mesmo snapshot e o mesmo fingerprint de candidato;
14. falha atual da fonte preserva o último candidato parseado, mas registra explicitamente a falha corrente;
15. mudança de versão do parser força refetch completo antes de aceitar o novo parser.

`scripts/validate_phase4_foundation.py` ancora o parser RFB no gate permanente do `Remake CI`.

## 6. Limites deliberados

Ainda não foram definidos ou executados:

- backend definitivo de retenção de snapshots;
- duração de retenção e política de compactação;
- parser novo do INSS;
- migração de `scraper.py` para consumir o novo pipeline RFB;
- registro operacional próprio do domínio financeiro de referência;
- estratégia final de CDI;
- semantic diff;
- promoção automática;
- publicação;
- migração dos consumidores.

O código legado de produção permanece intacto.

## 7. Próximos checkpoints

1. desacoplar **INSS** do fetch/discovery/parsing legado e resolver explicitamente `INSS_TABLE_2026` versus notícia anual pinned;
2. preparar a migração do caminho RFB em `scraper.py` para consumir candidato validado sem duplicar fetch/parser;
3. revisar retenção de snapshots e last-good operacional sem confundi-lo com vigência jurídica;
4. revisar a autoridade e o contrato operacional do domínio `financial_reference`, incluindo CDI;
5. fechar a Fase 4 com sensores capazes de detectar mudança sem publicar semanticamente nada por conta própria.
