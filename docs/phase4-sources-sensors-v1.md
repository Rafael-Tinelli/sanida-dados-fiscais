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

## 3. Fundação implementada neste primeiro checkpoint

Novo módulo:

```text
sanida_fiscal/sources_v1.py
```

Ele introduz cinco conceitos separados.

### 3.1 `SourceSpec`

Representa apenas a identidade operacional mínima necessária para coleta:

- `source_id`;
- URL;
- papel da fonte;
- legibilidade de máquina.

O loader de `docs/source-registry-v1.json` extrai esses campos sem relaxar o schema fechado do registro jurídico-fiscal.

### 3.2 `HttpCollectorV1`

O collector conhece transporte HTTP, timeout e retry. Ele **não conhece tabela de IR, INSS, férias, 13º ou rescisão**.

Estados do primeiro corte:

```text
COLLECTED
NOT_MODIFIED
SOURCE_UNAVAILABLE
```

Falhas operacionais são tipadas separadamente:

```text
TIMEOUT
NETWORK_ERROR
HTTP_CLIENT_ERROR
HTTP_SERVER_ERROR
```

HTTP 4xx definitivo falha sem retry. `429` e `5xx`, timeout e erro de rede são retryable até o limite configurado.

### 3.3 `RawSnapshot` + `SnapshotStore`

O snapshot nasce **antes** do parser.

Sua identidade é content-addressed:

```text
sha256(raw_bytes)
```

O caminho é determinístico por `source_id` e hash. O store usa criação exclusiva (`xb`) e, se o mesmo caminho já existir, exige igualdade byte a byte. Leitura posterior recalcula SHA-256 e falha se houver corrupção.

O primeiro checkpoint não decide ainda a política final de retenção/repositório externo; ele congela a semântica de imutabilidade e integridade que qualquer backend de snapshots deverá preservar.

### 3.4 Parser separado

`parse_snapshot()` recebe exclusivamente um `CollectionResult` já `COLLECTED` e lê o snapshot preservado.

O parser é identificado por:

```text
parser_id
parser_version
```

Uma quebra de formato não é convertida em indisponibilidade da fonte. Ela produz:

```text
PARSER_INCOMPATIBLE
```

mantendo no registro operacional a identidade do snapshot bruto que provocou a incompatibilidade.

### 3.5 `NormalizedSourceCandidate`

É a saída normalizada do parser na Fase 4. Carrega:

- `source_id` e URL;
- timestamp UTC da observação;
- SHA-256 e caminho do snapshot;
- parser e versão;
- status de parse;
- payload normalizado ou erro de incompatibilidade.

Esse objeto **ainda não é um `FiscalContractV1` nem uma release**. A transformação, comparação semântica e promoção ficam para a Fase 5.

## 4. Invariantes já executáveis

O primeiro conjunto de testes garante:

1. snapshot é content-addressed e idempotente;
2. corrupção posterior do snapshot é detectada;
3. coleta 2xx persiste bytes antes do parser;
4. 404 não é retryado e nunca fabrica snapshot;
5. 5xx é retryado e, se persistente, vira `SOURCE_UNAVAILABLE` sem snapshot;
6. timeout é distinguível de falha HTTP;
7. `PARSER_INCOMPATIBLE` é distinguível de `SOURCE_UNAVAILABLE`;
8. parser não aceita resultado de coleta malsucedida;
9. candidato normalizado carrega identidade exata do snapshot;
10. o registro oficial da Fase 1 pode ser carregado como superfície de coleta sem reinterpretação.

## 5. Decisões deliberadamente não tomadas neste checkpoint

Ainda não foram definidos:

- backend definitivo de retenção de snapshots;
- duração de retenção e política de compactação;
- ETag/Last-Modified persistentes entre execuções;
- parser específico de RFB/INSS/BCB no novo pipeline;
- registro operacional próprio do domínio financeiro de referência;
- estratégia final de CDI;
- semantic diff;
- promoção automática;
- publicação;
- migração de `scraper.py`, `update_taxas.py` ou dos consumidores.

O código legado permanece intacto durante este checkpoint.

## 6. Próximos checkpoints

1. materializar o primeiro pipeline real `collector → snapshot → parser` para uma fonte oficial estruturada ou suficientemente estável;
2. separar RFB e INSS do fetch acoplado atual, mantendo os parsers como componentes independentes;
3. definir persistência de estado de coleta (`last observation`, ETag/Last-Modified, falha atual e last-good operacional);
4. revisar a autoridade e o contrato operacional do domínio `financial_reference`, incluindo CDI;
5. fechar a Fase 4 com sensores capazes de detectar mudança sem publicar semanticamente nada por conta própria.
