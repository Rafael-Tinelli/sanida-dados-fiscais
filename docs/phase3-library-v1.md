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

O engine representa explicitamente duas dimensões que não podem ser confundidas:

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

Toda apuração de IRRF carrega identidade própria:

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

A seleção valida o `input_semantic` do redutor. Um redutor de 13º não pode ser reutilizado como mensal, e vice-versa, apenas porque os parâmetros numéricos coincidam.

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
- `pension` é argumento obrigatório: o chamador precisa declarar explicitamente zero quando não houver dedução;
- um conjunto de deduções mensal não pode ser usado na apuração do 13º ou de férias;
- um `IrrfRuleBundle` também não pode ser reutilizado entre apurações distintas.

Isso implementa diretamente as invariantes do contrato:

```text
deductions_must_match_assessment_context
pension_must_not_be_silently_zeroed_when_applicable
monthly_thirteenth_and_vacation_are_distinct_assessments
```

## 4. Terceiro checkpoint — 13º salário

O núcleo específico do 13º vive em:

```text
sanida_fiscal/thirteenth_v1.py
tests/test_thirteenth_v1.py
```

Ele consome os payloads já congelados pelo Contrato v1, sem introduzir regra jurídica nova.

### 4.1 Avos e regra dos 15 dias

`qualifies_thirteenth_month()` executa o limiar declarado em `ThresholdAccrualPayload`. `calculate_thirteenth_accrual()` conta meses do ano de referência por dias efetivos no intervalo informado pelo chamador.

A função:

- considera dias de serviço de forma inclusiva;
- limita o total a `max_units` do contrato;
- reproduz os casos oficiais fechados na Fase 1: 14 dias = 0 avo; 15 dias = 1 avo;
- não infere projeção de aviso, suspensão ou ajuste de dias que não esteja explicitamente informado pelo chamador.

### 4.2 Referência remuneratória

`select_thirteenth_reference_remuneration()` aplica a semântica do `RemunerationReferencePayload`:

```text
13º anual        -> remuneração devida em dezembro
13º rescisório   -> remuneração do mês da extinção
```

A parcela variável pré-calculada só pode entrar com `VariableComponentMode.EXTERNAL_PRECOMPUTED`, em linha com `VariableRemunerationPayload.precomputed_average_allowed_only_as_external_input`.

O engine **não inventa** a apuração canônica completa da remuneração variável enquanto o contrato não expuser semântica computacional suficiente para isso.

### 4.3 Valor bruto proporcional

`calculate_thirteenth_gross()` aplica os avos ao total de referência usando o numerador/denominador do `ThresholdAccrualPayload` e exige uma `RoundingPolicy` explícita.

Nenhum arredondamento monetário implícito é aceito.

### 4.4 Adiantamento

`calculate_thirteenth_advance()` executa a branch simples coberta pelo caso oficial da Fase 1:

```text
referência fixa = salário do mês anterior
fração          = 1/2
janela           = fevereiro a novembro
```

O caso de R$ 4.000 produz R$ 2.000.

Casos de admissão no ano e remuneração variável são deliberadamente rejeitados neste estágio. O engine não volta à aproximação incorreta `total13 * 0.5`.

### 4.5 Apuração previdenciária própria

`assess_thirteenth_social_security()` exige uma `IrrfAssessmentIdentity` de tipo `thirteenth` e calcula a contribuição sobre a base do 13º separadamente da folha mensal.

A tabela previdenciária entra como `ProgressiveTablePayload` tipado. Isso preserva a arquitetura do contrato sem hardcodar no engine uma tabela INSS que o `CANDIDATE` ainda não materializa como `inss.employee.progressive_table`.

### 4.6 IRRF próprio do 13º

`assess_thirteenth_fiscal_2026()` compõe:

```text
base própria do 13º
    ↓
INSS próprio do 13º
    ↓
dependentes/pensão do 13º
    ↓
comparação legal x simplificado da própria apuração
    ↓
tabela progressiva de IR
    ↓
thirteenth.irrf.reduction.2026
    ↓
memória fiscal do 13º
```

Dentro de `termination`, o resultado continua com `origin_context=termination`, mas `income_type=thirteenth` e `rule_context=thirteenth`.

Isso fecha a regressão estrutural em que H29 zerava pensão no 13º e impede que INSS/deduções mensais sejam reaproveitados silenciosamente.

## 5. Memória de IRRF

A saída `IrrfAssessmentMemory` preserva tanto a identidade quanto os componentes das deduções:

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

## 6. Casos executáveis

Os cinco casos da Receita Federal fechados na Fase 1 continuam executados contra o engine:

- R$ 3.036,00;
- R$ 4.000,00;
- R$ 5.000,00;
- R$ 6.000,00 — regressão A01;
- R$ 7.607,20 — elegibilidade do redutor pela renda anterior às deduções.

O caso A01 continua produzindo:

```text
renda tributável:        6000.00
deduções legais:          649.60
base IRRF:               5350.40
IR antes do redutor:      562.63
entrada do redutor:      6000.00
redutor aplicado:         179.75
IR final:                 382.88
```

Casos específicos de 13º agora executados:

- 14 dias = 0 avo;
- 15 dias = 1 avo;
- referência anual pela remuneração de dezembro;
- referência rescisória pela remuneração do mês da extinção;
- 7/12 de R$ 3.600 = R$ 2.100 com rounding explícito;
- adiantamento simples: R$ 4.000 → R$ 2.000;
- rejeição do atalho de adiantamento em admissão no ano/remuneração variável;
- INSS do 13º em memória separada;
- redutor específico `thirteenth.irrf.reduction.2026`;
- pensão e dependentes próprios da apuração do 13º;
- 13º rescisório mantendo contexto fiscal de `thirteenth`.

Property-based tests cobrem a fronteira única dos 15 dias e garantem 0–12 avos no ano de referência.

## 7. INSS progressivo

O engine marginal já suporta a família necessária ao INSS: bandas progressivas, teto opcional e arredondamento por estágio.

No checkpoint de 13º, a apuração previdenciária separada já consome um `ProgressiveTablePayload`. Os testes usam uma tabela sintética para provar a arquitetura de separação sem fabricar uma instância oficial inexistente no `CANDIDATE`.

Quando o contrato executável disponibilizar a instância canônica de `inss.employee.progressive_table`, o mesmo primitive deverá consumi-la sem duplicação de fórmula.

## 8. Estado do CI

Após o checkpoint de 13º:

```text
Repository baseline       PASS
Fiscal Contract v1        PASS
Fiscal engine             PASS
Property-based tests      PASS

89 passed
```

Run de referência: `34766418032`.

## 9. Limites preservados

Ainda não estão implementados integralmente:

- cálculo canônico interno completo da remuneração variável do 13º;
- branches especiais de adiantamento para admissão no ano/remuneração variável;
- férias/período aquisitivo/abono;
- saldo salarial e matriz de elegibilidade completa do H29;
- collectors/snapshots/parsers;
- publicação de releases;
- migração de WordPress/`folha-core`/H26–H29.

Esses limites são fail-closed: o engine rejeita os casos não modelados em vez de convertê-los silenciosamente em aproximações.

## 10. Próximos checkpoints da Fase 3

1. implementar período aquisitivo, férias proporcionais e abono;
2. implementar saldo salarial e matriz H29 limitada;
3. consolidar memória de cálculo comum;
4. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.
