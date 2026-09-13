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

`sanida_fiscal/source_catalog_v1.py` é o catálogo operacional único de bindings `source_id → parser id/version/reference year/parser`.

Tanto o runner manual quanto o produtor de compatibilidade chamam `run_registered_source_pipeline()`. Isso evita que `scraper.py` reimplemente URL, collector ou parser.

O runner manual pode manter revalidação condicional. O produtor do artefato legado exige uma observação `PARSED` da execução corrente e, por isso, usa fetch completo.

## 5. Quarto checkpoint — fronteira de transição para `dados_fiscais.json`

A transição é formalizada em `docs/phase4-legacy-artifact-boundary-v1.json`.

### 5.1 Natureza do artefato legado

`dados_fiscais.json` continua temporariamente com `schema_version: 2.2.0` para não quebrar consumidores ainda não migrados.

Ele é explicitamente classificado como:

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

O produtor chama apenas o catálogo de pipelines da Fase 4 e o bridge de compatibilidade.

### 5.5 Domínio financeiro ainda separado

`taxas_bacen.json` continua sendo validado e consumido como entrada legada de `financial_reference`. Este checkpoint não declara Selic/CDI como migrados nem aplica a eles a semântica jurídica das fontes de folha.

## 6. Quinto checkpoint — persistência real no GitHub Actions de produção

A política machine-readable está em `docs/phase4-production-persistence-v1.json`.

O ambiente que executa `main.yml` é um runner efêmero do GitHub Actions. Portanto, `.source-runtime/` local não poderia ser considerado retenção de produção: todo o diretório desapareceria ao fim do job.

A solução v1 usa um backend simples e auditável já disponível no próprio ambiente de produção:

```text
main branch
└── evidence/source-runtime-v1/
    ├── snapshots/
    │   └── <source_id>/<prefixo>/<sha256>.<ext>
    ├── candidates/
    │   └── <source_id>/<prefixo>/<sha256>.json
    └── state/
        └── <source_id>.json
```

O workflow define:

```text
SFA_SOURCE_RUNTIME_ROOT=evidence/source-runtime-v1
```

Assim, cada execução começa com o estado/evidência materializado pela execução anterior porque o diretório vem no checkout de `main`.

### 6.1 Snapshots brutos

Snapshots continuam content-addressed por SHA-256. Bytes idênticos não criam duplicatas. Arquivos únicos observados em produção são retidos sem pruning automático na política v1.

Isso garante que `snapshot_sha256` em `dados_fiscais.json` possa ser resolvido para bytes efetivamente preservados fora do runner efêmero.

### 6.2 Candidatos normalizados

`sanida_fiscal/source_runtime_v1.py` passou a incluir `CandidateStore`.

O payload normalizado `PARSED` é serializado canonicamente com chaves ordenadas e separadores estáveis. O SHA-256 desses bytes é exatamente o `candidate_sha256` já usado na proveniência.

O candidato fica armazenado em:

```text
candidates/<source_id>/<prefixo>/<candidate_sha256>.json
```

Logo, tanto o hash do snapshot quanto o hash do candidato agora apontam para evidência persistida.

### 6.3 Estado operacional v1.1

`SourcePipelineState` foi ampliado para preservar o par last-good completo, separado do estado da tentativa corrente:

- `last_parsed_at_utc`;
- `last_parsed_snapshot_sha256`;
- `last_parsed_snapshot_path`;
- `last_successful_parser_id`;
- `last_successful_parser_version`;
- `last_candidate_sha256`;
- `last_candidate_path`.

Uma falha corrente de fonte ou parser não pode destruir esses ponteiros last-good. Isso é necessário porque o artefato de compatibilidade pode permanecer inalterado quando a observação nova falha.

### 6.4 Gate de proveniência antes de commit

`sanida_fiscal/production_evidence_v1.py` e `scripts/validate_production_evidence_v1.py` verificam, antes de qualquer commit de um novo `dados_fiscais.json`:

1. `source_id` da proveniência;
2. URL persistida;
3. hash do snapshot;
4. existência e SHA-256 real do arquivo de snapshot;
5. hash do candidato;
6. existência e SHA-256 real do arquivo de candidato;
7. parser id/versão do last-good;
8. timestamp da observação que gerou o last-good;
9. confinamento dos caminhos dentro do runtime root.

Um artefato não verificado é restaurado para a versão de `HEAD` e nunca é staged.

### 6.5 Persistência mesmo quando a execução falha

Falha de coleta/parser precisa sobreviver ao runner para que contadores e diagnóstico não recomecem do zero na execução seguinte.

Por isso, `main.yml` captura o resultado do scraper em vez de abortar imediatamente. Se o artefato não passar pelo gate:

- `dados_fiscais.json` não é commitado;
- snapshots/estado operacional observados na tentativa ainda podem ser commitados;
- ao final, o workflow falha para manter sinal operacional de erro.

Quando o artefato passa pelo gate, `dados_fiscais.json` e `evidence/source-runtime-v1` são staged juntos. Se apenas estado/evidência mudou, é permitido commit de evidência sem alteração do artefato.

### 6.6 Retenção

Política v1:

- raw snapshots: reter todos os conteúdos únicos;
- normalized candidates: reter todos os conteúdos únicos;
- operational state: materializar o estado mais recente por fonte e preservar versões anteriores no histórico Git;
- automatic pruning: desabilitado.

Não são seedados fixtures nem evidências sintéticas na árvore de produção. Os arquivos reais surgirão apenas depois que `main.yml` atualizado for ativado em `main`.

## 7. Estado operacional entre execuções

`sanida_fiscal/source_runtime_v1.py` persiste:

- última observação UTC;
- status/HTTP status da coleta;
- `ETag` e `Last-Modified`;
- falhas correntes;
- último snapshot coletado;
- parser id/versão da tentativa corrente;
- last-good parseado com timestamp, snapshot, parser e candidato;
- fingerprint do último candidato.

Esse last-good é operacional e não equivale a validade jurídica.

O runner manual pode reutilizar HTTP validators. O produtor do artefato legado, por desenho, não usa 304 para gerar uma nova versão: ele exige candidato atual nesta execução.

## 8. Invariantes executáveis

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
12. `requirements.txt` instala as dependências runtime da Fase 4;
13. snapshot e candidato da proveniência precisam existir e reproduzir seus hashes;
14. falha corrente não apaga ponteiros last-good;
15. `main.yml` usa runtime persistente rastreado em Git, não `.source-runtime/` efêmero;
16. artefato não verificado não pode ser commitado;
17. evidência operacional de execução falha pode ser persistida sem publicar dados.

`scripts/validate_phase4_foundation.py` ancora essas invariantes no `Remake CI`.

## 9. Limites deliberados

Ainda não foram fechados:

- migração de Selic/SGS para collector/parser decimal;
- política operacional e autoridade final de CDI;
- remoção de fallback estático eventualmente existente em `update_taxas.py`;
- semantic diff;
- promoção/publicação canônica;
- migração dos consumidores.

A retenção/persistência de RFB + INSS no workflow de produção está resolvida arquitetural e executavelmente nesta branch, mas só será ativada quando a Fase 4 for mergeada em `main`.

## 10. Próximos checkpoints

1. migrar o domínio `financial_reference`, começando por Selic/SGS e depois CDI;
2. eliminar qualquer fallback financeiro capaz de fingir atualidade;
3. revisar a fronteira final dos workflows `main.yml` + `taxas.yml` antes do merge da Fase 4;
4. fechar a Fase 4 com sensores completos e sem antecipar semantic diff da Fase 5.
