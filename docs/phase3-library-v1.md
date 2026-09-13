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

## 8. Quarto checkpoint — período aquisitivo, férias proporcionais e abono

O núcleo específico de férias vive em:

```text
sanida_fiscal/vacation_v1.py
tests/test_vacation_v1.py
```

### 8.1 Emenda explícita do Contrato v1 para A02

Ao iniciar a implementação, o gate do handoff detectou uma lacuna objetiva: `vacation.acquisition_period` já declarava período de 12 meses, âncora no vínculo e ausência de reset no ano civil, mas não transportava para a máquina o limiar de 15 dias da fração proporcional. Hardcodar `15` no engine violaria a regra de não inferência da Fase 2.

A correção foi feita no próprio contrato, de forma aditiva e versionada:

```text
schema_version       1.0.0 -> 1.1.0
contract_api_version 1.0.0 -> 1.1.0
rule_inventory       1.0.0 -> 1.1.0
vacation.acquisition_period.rule_version 1.0.0 -> 1.1.0
```

`PeriodRulePayload` agora carrega:

```text
proportional_accrual_method = one_twelfth_per_acquisition_month_or_fraction_gte_days
proportional_qualifying_days = 15
```

A compatibilidade continua `exact`: um leitor 1.0.0 não aceita silenciosamente o contrato 1.1.0.

### 8.2 Período aquisitivo e A02

`acquisition_period_for_date()` localiza o período corrente a partir de `employment_start_anniversary`. `calculate_proportional_vacation_accrual()` subdivide esse período em doze fatias aquisitivas sucessivas e conta 1/12 por fatia integral ou fração que alcance o limiar declarado no contrato.

A contagem **não usa meses civis como substituto do período aquisitivo**. Portanto vínculos iniciados no meio do mês continuam ancorados naquele dia.

Regressão A02 congelada e executável:

```text
admissão      01/09/2025
desligamento  31/03/2026
período       01/09/2025 .. 31/08/2026
avos           7/12
```

A fronteira de fração também está testada: 14 dias não geram avo; 15 dias geram 1/12.

### 8.3 Direito em dias por faltas

`vacation_entitlement_from_absences()` executa `EntitlementBandsPayload` sem reconstituir faixas no código:

```text
0–5 faltas   -> 30 dias
6–14         -> 24 dias
15–23        -> 18 dias
24–32        -> 12 dias
```

Valor fora das faixas suportadas falha fechado; não há extrapolação silenciosa.

### 8.4 Abono pecuniário

`calculate_cash_allowance_days()` aplica o `FractionPayload` de 1/3 sobre **entitled_days**, nunca sobre uma quantidade arbitrária de dias escolhidos para gozo.

Casos suportados ficam exatos:

```text
30 -> 10 de abono + 20 restantes
24 ->  8 de abono + 16 restantes
18 ->  6 de abono + 12 restantes
12 ->  4 de abono +  8 restantes
```

Se uma entrada fora do universo contratual produzir fração não inteira, o engine não inventa arredondamento estatutário.

### 8.5 Principal e terço constitucional não são fundidos

`resolve_cash_allowance_tax_treatment()` exige os dois componentes canônicos distintos:

```text
cash_allowance_principal:                IRRF=no  CP=no
constitutional_third_on_cash_allowance:  IRRF=yes CP=no
```

`cash_allowance_tax_bases()` preserva essa separação até a formação das bases. Exemplo com principal de R$ 1.000 e terço de R$ 333,33:

```text
base IRRF = 333.33
base CP   = 0.00
```

Perfis trocados ou componentes fundidos são rejeitados. Isso elimina estruturalmente o defeito de colocar `abono + 1/3` inteiro em um vetor isento de IR.

### 8.6 Limite deliberado

O checkpoint não fabrica uma fórmula monetária específica para transformar dias de abono em principal/terço quando essa fórmula não estiver expressa como payload computacional correspondente. O engine já executa direito em dias e incidências; qualquer semântica monetária adicional precisa entrar explicitamente no contrato antes de ser calculada.

## 9. Quinto checkpoint — saldo salarial e matriz H29 limitada

O núcleo rescisório limitado vive em:

```text
sanida_fiscal/termination_v1.py
tests/test_termination_v1.py
```

O objetivo não é transformar H29 em calculadora universal de rescisão. O engine executa apenas o subconjunto fechado na Fase 1/2 e devolve explicitamente `partial_estimate`.

### 9.1 Matriz de motivos suportados

`select_termination_rule_bundle()` cruza três fontes computacionais do próprio contrato:

```text
termination.reason_scope
termination.thirteenth_proportional
termination.vacation_proportional
```

A regra de férias proporcionais já existia no inventário/coverage, mas ainda não estava materializada no `CANDIDATE`. Ela foi adicionada como 21ª regra representativa, usando a família já existente `code_eligibility`; nenhuma família de schema nova foi criada.

A matriz executada é:

```text
01  justa causa pelo empregador
    saldo salarial             sim
    13º proporcional           não
    férias proporcionais       não

02  sem justa causa
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim

07  pedido de demissão
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim

33  acordo art. 484-A
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim
```

Motivo fora desse conjunto é `UNSUPPORTED`. O bundle exige ainda que as regras específicas de 13º e férias cubram exatamente os mesmos quatro códigos e reproduzam os mesmos booleans da matriz. Divergência contratual falha antes do cálculo.

### 9.2 Limite de vínculo suportado

A aplicabilidade de `termination.reason_scope` é executada:

```text
employment_regime ∈ {monthly, biweekly}
contract_term      = indefinite
```

No caminho monetário, `monthly_base_salary` é a base mensal normalizada declarada pelo `ProrationPayload`. Horista, diarista, semanalista e contrato a prazo determinado não são convertidos silenciosamente para esse modelo.

### 9.3 Saldo salarial

`calculate_salary_balance()` executa:

```text
monthly_base_salary
× days_counted_through_termination
÷ calendar_days_in_month
```

O denominador vem do calendário do próprio mês de desligamento. Não existe divisor 30 universal. O caso de referência fica executável:

```text
3100.00 / 31 × 10 = 1000.00
```

O mesmo numerador em abril de 2026 produz `1033.33`, demonstrando que a diferença 30/31 não é apagada. O numerador não pode exceder o dia do desligamento e o resultado não pode ultrapassar a base mensal.

### 9.4 13º proporcional e férias proporcionais permanecem calendários distintos

Para motivos `02/07/33`, `calculate_h29_limited_estimate()` reutiliza os núcleos já testados:

- 13º: `calculate_thirteenth_accrual()` + referência remuneratória rescisória + fórmula proporcional do 13º;
- férias: `calculate_proportional_vacation_accrual()` ancorada no período aquisitivo.

No mesmo caso:

```text
admissão       01/09/2025
desligamento   31/03/2026

13º rescisório        3/12  (jan-mar/2026)
férias proporcionais  7/12  (set/2025-mar/2026)
```

O engine não reutiliza a contagem de um direito como se fosse a do outro. No motivo `01`, os cálculos proporcionais nem são executados. A fronteira de 15 dias também foi integrada.

### 9.5 A saída continua parcial

`H29LimitedEstimate` carrega:

```text
result_promise = partial_estimate
user_disclosure_required = true
reason
salary_balance
thirteenth_proportional
vacation_proportional
acquired_vacation_if_due
included_items
excluded_items
```

Neste checkpoint, `acquired_vacation_if_due` é a elegibilidade da matriz; o cálculo monetário de períodos adquiridos/vencidos não foi expandido a partir do antigo checkbox.

Continuam fora do total, conforme `ScopeDeclarationPayload`:

- aviso prévio ou desconto de aviso;
- multa e saque do FGTS;
- seguro-desemprego;
- indenizações de estabilidade;
- verbas específicas de CCT;
- regras de contrato por prazo determinado;
- rescisão indireta sem contexto judicial resolvido;
- itens variáveis rescisórios não explicitamente modelados.

A saída não pode ser apresentada como “total universal da rescisão”.

## 10. Sexto checkpoint — memória comum, invariantes e gate de fechamento

O checkpoint consolida a **representação auditável** dos cálculos já implementados sem criar uma camada que misture semânticas jurídicas distintas.

Arquivos permanentes adicionados:

```text
sanida_fiscal/memory_v1.py
tests/test_memory_v1.py
tests/test_phase3_invariants.py
scripts/validate_phase3_gate.py
docs/phase3-closure-gate.md
```

### 10.1 Memória comum sem fusão semântica

`CalculationMemory` é um envelope comum para auditoria. Cada nó declara `calculation_type`, `origin_context`, tipo de apuração quando aplicável, fatos ordenados e memórias-filhas.

Cada `CalculationFact` carrega:

```text
key
value
unit
role
rule_ids
```

Valores monetários chegam à memória como `Decimal` e são serializados como texto canônico. A camada não converte os cálculos para `float`.

A normalização é apenas de representação: mensal, 13º, férias e rescisão continuam com identidades, bases e regras próprias. No H29, por exemplo, saldo salarial, 13º proporcional e férias proporcionais aparecem como filhos separados; o motivo `01` contém apenas o filho de saldo salarial.

### 10.2 Reconciliação e proveniência mínima

O adaptador de IRRF preserva a memória A01 completa e associa os fatos críticos aos `rule_ids` executados. O caso de R$ 6.000 continua registrando simultaneamente:

```text
gross_taxable_income     6000.00
irrf_tax_base            5350.40
reduction_input_income   6000.00
final_irrf                382.88
```

O envelope também cobre a apuração fiscal do 13º, saldo salarial, 13º proporcional de H29, férias proporcionais, bases tributárias do abono e composição H29 limitada.

### 10.3 Expansão das invariantes/property-based tests

O novo conjunto cobre adicionalmente:

- fronteiras do redutor de 2026 em `4999.99 / 5000.00 / 5000.01` e `7349.99 / 7350.00 / 7350.01`;
- saldo salarial usando o número real de dias civis nos doze meses;
- monotonicidade dos avos de férias dentro de um período aquisitivo;
- monotonicidade dos avos de 13º dentro do ano de referência;
- totalidade da matriz H29 exatamente sobre `01/02/07/33`;
- impossibilidade de o principal do abono vazar para a base de IRRF ou contribuição previdenciária.

### 10.4 Gate executável de fechamento

`scripts/validate_phase3_gate.py` passou a rodar no `Remake CI` depois da suíte completa. Ele valida artefatos obrigatórios, higiene de workflows, A01 e sua memória comum, H29 limitado (`01` e `02`), calendários 3/12 versus 7/12 e separação principal/terço do abono.

O gate não amplia escopo: casos deliberadamente não modelados permanecem fail-closed e estão registrados em `docs/phase3-closure-gate.md`.

## 11. Estado do CI

Após o checkpoint de memória comum, invariantes e gate de fechamento:

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

Run de validação do checkpoint: `34775296512`.

## 12. Limites preservados

Ainda não estão implementados integralmente:

- cálculo canônico interno completo da remuneração variável do 13º;
- branches especiais de adiantamento para admissão no ano/remuneração variável;
- fórmula monetária adicional do abono além das grandezas e incidências já tipadas, se necessária, até que exista payload computacional explícito;
- cálculo monetário de períodos integrais adquiridos/vencidos na rescisão além da elegibilidade já exposta;
- incidências e memória fiscal final do saldo salarial no orquestrador H29, que serão compostas a partir dos primitives já existentes sem ampliar o escopo de verbas;
- collectors/snapshots/parsers;
- publicação de releases;
- migração de WordPress/`folha-core`/H26–H29.

Esses limites são fail-closed: o engine rejeita os casos não modelados em vez de convertê-los silenciosamente em aproximações.

## 13. Próximos checkpoints da Fase 3

1. revisar formalmente o gate de fechamento da Fase 3 no mesmo head limpo;
2. se repositório, contrato, suíte integral, gate, documentação e higiene estiverem verdes, promover a Fase 3 para `CONCLUÍDA` em checkpoint próprio;
3. somente depois iniciar a Fase 4 — Fontes e sensores, sem antecipar publicação ou migração de consumidores.
