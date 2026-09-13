# Fase 3 — Biblioteca fiscal e testes

**Status:** EM ANDAMENTO  
**Início:** 13/09/2026  
**Base:** `docs/phase2-to-phase3-handoff.md`

## 1. Objetivo

Executar o Contrato Fiscal Canônico v1 por meio de funções puras, determinísticas e auditáveis, sem consultar web, scraping ou fontes oficiais em tempo de cálculo.

A Fase 3 não reinterpreta legislação. Se uma operação necessária não estiver expressa no contrato congelado, o trabalho deve voltar ao contrato/inventário como mudança explícita.

## 2. Primeira rodada — primitives fiscais

A primeira rodada criou a camada compartilhada inicial do engine:

- primitives de `Decimal` e quantização por política declarada;
- rejeição explícita de `float`/`bool` no caminho fiscal;
- execução de tabelas progressivas pelos dois métodos admitidos pelo Contrato v1:
  - `marginal_by_bracket`;
  - `rate_times_base_minus_deduction`;
- suporte a `cap_base` em tabela marginal;
- execução do `AffineReductionPayload`;
- composição de memória auditável do IRRF 2026;
- seleção entre deduções legais informadas e desconto simplificado;
- preservação explícita do invariante A01;
- property-based tests para monotonicidade de tabela marginal e não negatividade do redutor.

Arquivos centrais:

```text
sanida_fiscal/money.py
sanida_fiscal/engine_v1.py
tests/test_engine_v1.py
```

## 3. Segundo checkpoint — deduções por contexto e apurações separadas

O engine passa a representar explicitamente duas dimensões que não podem ser confundidas:

1. **origem do rendimento/evento** (`origin_context`);
2. **tipo de apuração de IRRF** (`income_type`).

Tipos de apuração de IRRF congelados pela Fase 1/2:

```text
monthly
thirteenth
vacation
```

`termination` **não é um quarto tipo de rendimento para IRRF**. É um contexto de origem que pode conter apurações separadas. No escopo H29 v1:

```text
termination + monthly     -> saldo de salário / apuração mensal
termination + thirteenth  -> 13º rescisório / apuração própria
termination + vacation    -> rejeitado no engine de IRRF v1
```

A última combinação é rejeitada porque as férias rescisórias suportadas por H29 são indenizadas e não devem ser convertidas silenciosamente em uma apuração tributável de férias.

### 3.1 `IrrfAssessmentIdentity`

Toda apuração de IRRF passa a carregar identidade própria:

```text
income_type
origin_context
rule_context
```

O `rule_context` é derivado do tipo de rendimento, não do evento de origem. Assim, dentro de uma rescisão, o saldo salarial seleciona regras `monthly` e o 13º seleciona regras `thirteenth`.

### 3.2 Seleção de regras por apuração

`select_irrf_rule_bundle()` seleciona deterministicamente:

```text
monthly     -> irrf.monthly.progressive_table + irrf.reduction.2026
thirteenth  -> irrf.monthly.progressive_table + thirteenth.irrf.reduction.2026
vacation    -> irrf.monthly.progressive_table + irrf.reduction.2026
```

A seleção ainda valida o `input_semantic` do redutor. Um redutor de 13º não pode ser reutilizado como mensal, e vice-versa, apenas porque os parâmetros numéricos coincidam.

### 3.3 Deduções legais tipadas por apuração

`build_irrf_legal_deductions()` cria uma memória de deduções vinculada a uma única `IrrfAssessmentIdentity`:

```text
social_security
dependent_count
dependent_unit
dependents
pension
total
```

Regras de segurança implementadas:

- dedução por dependente exige unidade `BRL_per_dependent`;
- número de dependentes deve ser inteiro não negativo;
- previdência, pensão e dependentes não podem ser negativos;
- `pension` é argumento obrigatório: o chamador precisa declarar explicitamente zero quando não houver dedução, evitando zero silencioso;
- um conjunto de deduções mensal não pode ser usado na apuração do 13º ou de férias;
- um `IrrfRuleBundle` também não pode ser reutilizado entre apurações distintas.

Isso implementa diretamente as invariantes do contrato:

```text
deductions_must_match_assessment_context
pension_must_not_be_silently_zeroed_when_applicable
monthly_thirteenth_and_vacation_are_distinct_assessments
```

## 4. Memória de IRRF

A saída `IrrfAssessmentMemory` agora preserva tanto a identidade quanto os componentes das deduções:

```text
assessment
  income_type
  origin_context

deduction_components
  social_security
  dependent_count
  dependent_unit
  dependents
  pension
  total

gross_taxable_income
legal_deductions
simplified_discount
deduction_mode
irrf_tax_base
pre_reduction_irrf
reduction_input_income
reduction_amount
final_irrf
```

O redutor continua recebendo `gross_taxable_income` como `reduction_input_income`. Ele não recebe `irrf_tax_base`.

## 5. Casos executáveis

Os cinco casos da Receita Federal fechados na Fase 1 continuam executados contra o engine:

- R$ 3.036,00;
- R$ 4.000,00;
- R$ 5.000,00;
- R$ 6.000,00 — regressão A01;
- R$ 7.607,20 — elegibilidade do redutor pela renda anterior às deduções.

O caso A01 deve produzir:

```text
renda tributável:        6000.00
deduções legais:          649.60
base IRRF:               5350.40
IR antes do redutor:      562.63
entrada do redutor:      6000.00
redutor aplicado:         179.75
IR final:                 382.88
```

Além disso, o checkpoint passa a testar explicitamente:

- composição de previdência + dependentes + pensão;
- rejeição de unidade errada para dependentes;
- rejeição de combinação incompatível entre tipo de renda e origem;
- seleção distinta de regras para mensal e 13º dentro de `termination`;
- impossibilidade de vazar deduções mensais para o 13º;
- impossibilidade de reutilizar bundle de regras entre apurações;
- memórias distintas de saldo salarial e 13º dentro da mesma rescisão;
- apuração própria do 13º com redutor específico;
- férias gozadas como apuração separada da folha mensal.

## 6. INSS progressivo

O engine marginal já suporta a família necessária ao INSS: bandas progressivas, teto opcional e arredondamento por estágio. Os testes do algoritmo são matemáticos/property-based; a Fase 3 ainda não cria uma regra INSS inexistente no `CANDIDATE` apenas para satisfazer um teste.

Quando o contrato executável disponibilizar uma instância canônica da regra `inss.employee.progressive_table`, o mesmo primitive deverá consumi-la sem duplicação de fórmula.

## 7. Limites preservados

Ainda não estão implementados integralmente:

- composição de avos e remuneração do 13º;
- férias/período aquisitivo/abono;
- saldo salarial e matriz de elegibilidade completa do H29;
- collectors/snapshots/parsers;
- publicação de releases;
- migração de WordPress/`folha-core`/H26–H29.

A separação de apurações deste checkpoint é infraestrutura para esses próximos cálculos, não sua implementação antecipada.

## 8. Próximos checkpoints da Fase 3

1. implementar avos/13º e seus testes oficiais;
2. implementar período aquisitivo, férias proporcionais e abono;
3. implementar saldo salarial e matriz H29 limitada;
4. consolidar memória de cálculo comum;
5. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.
