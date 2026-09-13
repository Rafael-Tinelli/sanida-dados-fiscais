# Gate de fechamento — Fase 3

Status: **APROVADO — FASE 3 CONCLUÍDA**

Data: 2026-09-13

## Objetivo

Transformar o fechamento da Fase 3 — Biblioteca fiscal e testes em uma decisão verificável, e não apenas documental.

O gate não amplia o escopo jurídico de H26–H29. Ele verifica se o núcleo já deliberadamente suportado continua coerente, auditável e fail-closed antes de qualquer transição para fontes/sensores ou migração de consumidores.

## O que o gate executa

O script permanente é:

```text
scripts/validate_phase3_gate.py
```

Ele roda no `Remake CI` depois das validações do repositório, do Contrato Fiscal Canônico e da suíte completa de testes.

O gate verifica:

1. presença dos módulos, testes e documentos permanentes que compõem a Fase 3;
2. ausência de workflows temporários deixados no repositório;
3. regressão A01 da Receita Federal, incluindo separação entre `reduction_input_income` e `irrf_tax_base`;
4. reconciliação da memória comum de IRRF com o resultado fiscal executado;
5. H29 limitado e divulgado como `partial_estimate`;
6. motivo eSocial `02` preservando saldo salarial, 3/12 de 13º no calendário anual e 7/12 de férias no período aquisitivo do caso de regressão;
7. motivo eSocial `01` sem fabricação de 13º/férias proporcionais;
8. separação entre principal do abono e terço constitucional nas bases de IRRF/CP;
9. coexistência apenas dos workflows permanentes `main.yml`, `taxas.yml` e `remake-ci.yml`.

## Memória comum de cálculo

O checkpoint introduziu:

```text
sanida_fiscal/memory_v1.py
```

A memória comum normaliza **representação e auditoria**, não semântica jurídica.

Cada nó possui:

- `calculation_type`;
- `origin_context`;
- `assessment_income_type`, quando aplicável;
- fatos ordenados com `key`, `value`, `unit`, `role` e `rule_ids`;
- memórias-filhas para composições, sem fundir bases ou contextos.

Valores monetários são serializados a partir de `Decimal` como texto canônico. O envelope não introduz `float` no caminho fiscal.

Adaptadores permanentes cobrem:

- IRRF mensal/13º/férias quando já existe `IrrfAssessmentMemory`;
- apuração fiscal do 13º;
- saldo salarial;
- 13º proporcional de H29;
- férias proporcionais de H29;
- bases do abono pecuniário;
- composição da estimativa H29 limitada.

## Invariantes adicionais

`tests/test_phase3_invariants.py` amplia o property-based testing para:

- fronteiras do redutor de 2026 em `4999.99 / 5000.00 / 5000.01` e `7349.99 / 7350.00 / 7350.01`;
- saldo salarial com denominador civil real ao longo dos 12 meses;
- monotonicidade dos avos de férias dentro de um período aquisitivo;
- monotonicidade dos avos de 13º dentro do ano de referência;
- totalidade da matriz H29 sobre o conjunto fechado `01/02/07/33`;
- impossibilidade de o principal do abono vazar para a base de IRRF ou contribuição previdenciária.

`tests/test_memory_v1.py` verifica ainda:

- reconciliação A01 na memória comum;
- serialização determinística e JSON-safe;
- unicidade de chaves dentro de cada memória;
- preservação dos filhos separados no H29;
- ausência de filhos proporcionais no motivo `01`;
- separação do principal/terço do abono.

## Limites que o gate não converte em escopo implícito

Permanecem fail-closed, salvo decisão posterior de escopo:

- branches especiais do adiantamento do 13º para admissão no ano/remuneração variável;
- cálculo interno completo da remuneração variável do 13º;
- modalidades rescisórias fora de `01/02/07/33`;
- contratos por prazo determinado e regimes fora do escopo H29 v1;
- cálculo monetário completo de múltiplos períodos de férias adquiridas/vencidas na rescisão;
- itens rescisórios explicitamente excluídos da estimativa parcial;
- collectors, sensores, semantic diff, publicação e migração de consumidores.

Esses limites não impedem o fechamento enquanto o engine os rejeitar ou os mantiver explicitamente fora da promessa do produto.

## Evidência executável

O estado de aceite é:

```text
Repository baseline       PASS
Fiscal Contract v1.1      PASS
Candidate rules           21
Inventory coverage        32/32
Payload families          18/18
Fiscal engine             PASS
Property-based tests      PASS
Phase 3 closure gate      PASS

150 passed
```

A evidência operacional é o `Remake CI` associado ao head corrente do PR. O histórico de GitHub Actions preserva IDs e logs; este documento registra os invariantes de aceite para não depender de um ID de execução que mudaria sempre que uma atualização documental disparasse novo run.

## Revisão formal dos sete critérios de promoção

A promoção de `EM ANDAMENTO` para `CONCLUÍDA` foi revisada em checkpoint próprio.

| Critério | Resultado | Evidência |
|---|---|---|
| `scripts/validate_repository.py` | PASS | etapa `Validate repository baseline and reference cases` do `Remake CI` |
| `scripts/validate_contract_v1.py` | PASS | 21 regras candidatas, 32/32 do inventário, 18 famílias |
| suíte `pytest` integral | PASS | 150 testes |
| `scripts/validate_phase3_gate.py` | PASS | `Phase 3 closure gate: PASS` |
| README + `docs/phase3-library-v1.md` coerentes | PASS | ambos promovidos para `CONCLUÍDA` no checkpoint formal |
| ausência de helper/workflow temporário | PASS | somente `main.yml`, `taxas.yml` e `remake-ci.yml` |
| nenhuma mudança de produção/consumidor misturada | PASS | diff do PR não inclui `scraper.py`, `update_taxas.py`, `dados_fiscais.json` ou `taxas_bacen.json`; WordPress/`folha-core`/H26–H29 não foram migrados |

A revisão não encontrou aresta objetiva de biblioteca, contrato, testes, documentação ou higiene que exigisse manter a Fase 3 aberta.

## Decisão

**Fase 3 — Biblioteca fiscal e testes: CONCLUÍDA.**

O gate permanece permanente no CI para impedir regressões enquanto as fases seguintes avançam.

A próxima fase é **Fase 4 — Fontes e sensores**. O fechamento da Fase 3 não autoriza antecipar publicação de releases nem migração de consumidores.
