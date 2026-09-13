# Handoff formal — Fase 2 → Fase 3

**Data:** 13/09/2026  
**Origem:** Fase 2 — Contrato Fiscal Canônico v1  
**Destino:** Fase 3 — Biblioteca fiscal e testes  
**Estado da Fase 2:** CONCLUÍDA

## 1. O que a Fase 2 entrega

A Fase 2 transforma o inventário jurídico-fiscal fechado na Fase 1 em um contrato executável e rejeitável por máquina. A Fase 3 deve consumir esse contrato; não deve reconstituir regras jurídicas a partir de páginas, textos livres ou convenções locais das calculadoras.

Entradas congeladas para a Fase 3:

- `sanida_fiscal/types_v1.py`;
- `sanida_fiscal/contract_v1.py`;
- `contracts/fiscal-contract-v1.schema.json`;
- `contracts/examples/fiscal-contract-v1.example.json`;
- `docs/rule-inventory-v1.json`;
- `docs/source-registry-v1.json`;
- `docs/contract-coverage-v1.json`;
- `tests/reference_cases/phase1_reference_cases.json`;
- `scripts/validate_contract_v1.py`;
- `tests/test_contract_v1.py`.

O inventário possui **32 regras**, todas cobertas pelo Contrato v1 em **18 famílias tipadas de payload**. O `CANDIDATE` materializa 21 regras representativas e cobre as 18 famílias; a regra `termination.vacation_proportional` foi materializada na Fase 3 para execução da matriz H29 já fechada no inventário.

## 2. Contrato de versionamento que a Fase 3 deve respeitar

### `schema_version`

SemVer da forma pública do documento JSON/Pydantic.

- **MAJOR:** alteração incompatível da forma/validação ou do significado de um campo já existente;
- **MINOR:** capacidade aditiva que preserva o significado dos documentos anteriores;
- **PATCH:** correção não incompatível de schema/metadados que não altera o significado computacional de documentos válidos.

No v1 a compatibilidade é deliberadamente **exata**: uma versão nova, mesmo minor/patch, não é aceita silenciosamente por um consumidor até que a compatibilidade seja testada e declarada.

### `contract_api_version`

SemVer da interface semântica oferecida aos consumidores.

- **MAJOR:** mudança incompatível na interpretação, seleção, contexto, lifecycle ou promessa de consumo;
- **MINOR:** capacidade semântica aditiva que preserva integralmente o comportamento anterior;
- **PATCH:** correção/clarificação que preserva os contratos de entrada e saída existentes.

No v1 o consumidor exige correspondência exata da versão da API.

### `rule_version`

SemVer de cada `rule_id`.

- **MAJOR:** muda target (`applies_to`), fórmula/método, incidência, contexto, dependência, ordem ou outra semântica incompatível da regra;
- **MINOR:** extensão compatível da regra que mantém válidos e semanticamente idênticos os casos já suportados;
- **PATCH:** mudança paramétrica, correção de vigência/proveniência/referência ou ajuste equivalente que preserve a mesma semântica computacional.

Uma nova `rule_version` nunca autoriza sobreposição ambígua de vigência no mesmo contexto.

### `release_id`

`release_id` **não é SemVer**. Releases `VALIDATED`/`PUBLISHED` usam identidade content-addressed:

```text
fiscal-v1-sha256-<sha256 do immutable payload>
```

O hash cobre o payload fiscal imutável — regras, compatibilidade, políticas e metadados canônicos relevantes — e não depende de timestamps de workflow ou de lifecycle.

## 3. Compatibilidade congelada do v1

O Contrato v1 adota uma postura fail-closed:

```text
schema_match            = exact
contract_api_match      = exact
unknown_fields          = reject
unknown_payload_types   = reject
backward_compatibility  = explicitly_tested_only
forward_compatibility   = not_assumed
unsupported_behavior    = hard_fail
```

Sem teste e declaração explícitos, a Fase 3 não deve assumir que consegue consumir versão futura nem tentar ignorar campos desconhecidos.

## 4. Imutabilidade e supersessão

Uma release `PUBLISHED` é imutável. Corrigir uma release publicada significa criar outra release.

O v1 não usa um estado mutável `SUPERSEDED` no artefato antigo. A relação é declarada pela sucessora:

```text
new_release.supersedes_release_id = previous_release.release_id
```

A release anterior permanece `PUBLISHED` e byte-a-byte preservada para auditoria. A sucessora precisa ter `release_id` distinto e só pode apontar para uma predecessora publicada.

O lifecycle de uma nova release continua:

```text
DRAFT → CANDIDATE → VALIDATED → PUBLISHED
                     ↘
                      BLOCKED
```

Mudanças estruturais publicadas exigem `HUMAN_REVIEWED`.

## 5. Resultado da auditoria final de inferência

A rodada final eliminou pontos em que um engine poderia ser obrigado a interpretar texto ou escolher uma operação não declarada. O v1 agora explicita ou tipa, entre outros:

- método de tabela progressiva: `marginal_by_bracket` versus `rate_times_base_minus_deduction`;
- unidade de escalar: `BRL` versus `BRL_per_dependent`;
- semântica completa do redutor afim: alívio integral, fórmula de phase-out e comportamento acima do limite;
- método de aquisição por limiar de dias;
- `rule_specific` por chave de competência tipada, não descrição livre;
- campos e valores admitidos em predicados de aplicabilidade;
- estágios admitidos de arredondamento;
- assertions canônicas de `policy`, sem dicionário livre de valores;
- componentes e bases da fórmula de férias;
- componentes de incidência tributária/previdenciária;
- sistema de códigos eSocial usado nas matrizes de desligamento;
- itens incluídos/excluídos no escopo parcial de H29;
- semântica do prorrateio de saldo de salário;
- `applies_to` de cada regra representativa cruzado exatamente contra o inventário fechado da Fase 1.

Campos narrativos (`description`, `notes`, `locator`) continuam possíveis para auditoria humana, mas **não são fonte de decisão computacional** para a Fase 3.

## 6. Invariantes que a Fase 3 não pode quebrar

### Gerais

- dinheiro e coeficientes monetários usam `Decimal`, nunca `float` no caminho fiscal;
- engine é determinístico e composto por funções puras sempre que possível;
- ausência de regra/parâmetro não vira zero;
- engine não consulta web, não faz scraping e não interpreta fonte oficial em tempo de cálculo;
- seleção de regra respeita contexto, vigência, qualidade e compatibilidade;
- consumidor não completa silenciosamente campo ausente nem inventa fallback fiscal.

### A01 — IRRF 2026

A memória mínima de cálculo deve preservar explicitamente:

```text
gross_taxable_income
legal_deductions
simplified_discount
irrf_tax_base
pre_reduction_irrf
reduction_input_income
reduction_amount
final_irrf
```

O redutor usa o rendimento tributável sujeito à incidência pertinente **antes das deduções do IRRF**, nunca `irrf_tax_base` por conveniência.

### 13º salário

- 13º possui apuração própria;
- regra dos 15 dias/avos permanece explícita;
- adiantamento não é modelado como `total13 * 0.5` universal;
- remuneração variável tem tratamento próprio;
- INSS e IRRF do 13º não são fundidos à folha mensal.

### A02 — férias

A contagem proporcional é ancorada no período aquisitivo, sem reset no ano civil.

Regressão congelada:

```text
01/09/2025 → 31/03/2026 = 7/12
```

### Abono pecuniário

Direito, gozo e abono são grandezas distintas. Principal e terço sobre o abono permanecem componentes tributários distintos:

```text
cash_allowance_principal:                IRRF=no  CP=no
constitutional_third_on_cash_allowance:  IRRF=yes CP=no
```

### H29 — rescisão

A Fase 3 deve preservar o escopo parcial fechado:

- mensalista ou quinzenalista;
- contrato por prazo indeterminado;
- motivos eSocial `01`, `02`, `07`, `33`;
- outros motivos = `UNSUPPORTED`;
- não transformar H29 em calculadora universal;
- saldo de salário sem divisor 30 universal;
- férias proporcionais ancoradas no período aquisitivo.

## 7. Objetivo da Fase 3

Implementar a biblioteca fiscal determinística que executa o contrato.

Primeira camada esperada:

1. primitives de `Decimal` e quantização por estágio;
2. INSS progressivo marginal;
3. IRRF pela tabela progressiva declarada;
4. redução mensal/13º/férias de 2026 conforme `AffineReductionPayload`;
5. seleção de dedução legal versus desconto simplificado no contexto aplicável;
6. avos de 13º e regra de 15 dias;
7. período aquisitivo, proporcionalidade de férias e abono;
8. saldo de salário e matrizes de elegibilidade do escopo H29;
9. composição de memória de cálculo auditável.

Casos oficiais e regressões da Fase 1 devem se tornar testes executáveis do engine. Depois, ampliar com property-based tests nas fronteiras e invariantes.

## 8. Fora do escopo da Fase 3

Continuam deliberadamente posteriores:

- collectors, snapshots reais, retry/timeouts e parsers — **Fase 4**;
- semantic diff, classificação operacional e promoção automática — **Fase 5**;
- distribuição/migração WordPress, `folha-core` e H26–H29 — **Fase 6**;
- operação ponta a ponta evergreen — **Fase 7**.

A Fase 3 pode usar fixtures e o `CANDIDATE` para testes, mas não deve fabricar evidência de produção.

## 9. Gate de entrada da Fase 3

A Fase 3 está autorizada a partir do contrato tipado e do inventário fechado. Qualquer necessidade de reinterpretar texto jurídico, inventar fórmula ou adicionar campo semântico ausente deve **interromper o engine** e voltar ao contrato/inventário como mudança explícita, em vez de ser resolvida silenciosamente dentro da função de cálculo.

O primeiro PR/commit da Fase 3 deve partir deste handoff e manter `Remake CI` verde.

## 10. Correção aditiva descoberta na Fase 3 — A02

Ao implementar férias proporcionais, a Fase 3 encontrou uma lacuna objetiva entre o inventário e a forma computacional do payload `period_rule`: o contrato dizia que a contagem era ancorada no período aquisitivo, mas não transportava para a máquina o limiar de 15 dias da fração proporcional.

Aplicando o próprio gate deste handoff, a semântica **não foi hardcoded no engine**. O Contrato v1 foi corrigido de forma aditiva:

```text
schema_version       1.0.0 -> 1.1.0
contract_api_version 1.0.0 -> 1.1.0
rule_inventory       1.0.0 -> 1.1.0
vacation.acquisition_period.rule_version 1.0.0 -> 1.1.0
```

O `PeriodRulePayload` passa a declarar explicitamente:

```text
proportional_accrual_method = one_twelfth_per_acquisition_month_or_fraction_gte_days
proportional_qualifying_days = 15
```

A política de compatibilidade continua `exact`: consumidores 1.0.0 não devem aceitar silenciosamente o documento 1.1.0. A mudança é aditiva em capacidade, mas só é consumível depois de teste explícito da versão nova.
