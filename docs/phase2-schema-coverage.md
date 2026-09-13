# Fase 2 — cobertura integral do inventário pelo schema v1

**Status:** 32/32 regras da Fase 1 mapeadas para famílias tipadas do Contrato Fiscal Canônico v1.

Este documento é a leitura humana de `docs/contract-coverage-v1.json`. O JSON é o gate machine-readable; esta página explica a decisão arquitetural.

## Resultado

- regras fechadas na Fase 1: **32**;
- regras cobertas pelo mapa: **32**;
- famílias de payload necessárias: **18**;
- nenhuma regra depende de payload desconhecido;
- todas as classes de regra da Fase 1 são representáveis no enum `RuleClass`;
- o exemplo `CANDIDATE` materializa pelo menos uma regra de cada família de payload;
- regras estruturais continuam declarativas: funções de cálculo pertencem à Fase 3.

## Famílias de payload e regras cobertas

### `affine_reduction`

- `irrf.reduction.2026`
- `thirteenth.irrf.reduction.2026`
- `vacation.irrf.reduction.2026`

### `code_eligibility`

- `termination.thirteenth_proportional`
- `termination.vacation_proportional`

### `component_formula`

- `vacation.remuneration_and_constitutional_third`

### `eligibility_matrix`

- `termination.reason_scope`

### `entitlement_bands`

- `vacation.entitlement_days_by_absences`

### `fraction`

- `vacation.abono_pecuniario`

### `incidence_profile`

- `vacation.abono.ir_exemption`
- `vacation.abono_constitutional_third.ir_incidence`
- `vacation.inss.enjoyed`
- `termination.vacation_indemnity_tax_treatment`

### `period_rule`

- `vacation.acquisition_period`

### `period_state`

- `termination.acquired_and_overdue_vacation`

### `policy`

O payload `policy` deixa de ser um dicionário sem tipo e passa a exigir `policy_kind` enumerado.

- `irrf.deductions_by_income_type` → `deductions_by_income_type`
- `irrf.income_type` → `income_type_partition`
- `thirteenth.inss.separate_assessment` → `separate_social_security_assessment`
- `thirteenth.irrf.exclusive_assessment` → `exclusive_irrf_assessment`
- `vacation.irrf.separate_assessment` → `separate_vacation_irrf_assessment`
- `technical.money_decimal_and_rounding` → `money_decimal_and_rounding`
- `technical.contract_vigency_and_quality` → `contract_vigency_and_quality`

### `progressive_table`

- `inss.employee.progressive_table`
- `irrf.monthly.progressive_table`

### `proration`

- `termination.salary_balance`

### `remuneration_reference`

- `thirteenth.reference_remuneration`

### `scalar`

- `irrf.dependent_deduction`
- `irrf.simplified_monthly_discount`

### `scope_declaration`

- `termination.partial_output_scope`

### `thirteenth_advance`

- `thirteenth.advance`

### `threshold_accrual`

- `thirteenth.accrual.twelfths`

### `variable_remuneration`

- `thirteenth.variable_remuneration`

## Critério adotado

O objetivo desta rodada não foi transformar o contrato em uma rules engine. O schema deve preservar semântica suficiente para que a Fase 3 implemente funções puras sem reconstruir significado jurídico.

Por isso:

- parâmetros numéricos usam payloads numéricos tipados;
- tabelas progressivas usam `progressive_table`;
- regras de incidência usam `incidence_profile`;
- elegibilidade de desligamento usa matrizes/códigos tipados;
- períodos, prorrateios, componentes e escopo possuem payloads próprios;
- políticas sem fórmula numérica usam `policy`, mas com `policy_kind` enumerado.

## Endurecimentos simultâneos

### Proveniência

- `retrieval_method` passa a ser explícito;
- `parser_id` e `parser_version` são um par obrigatório;
- fonte indisponível/incompatível não pode carregar snapshot;
- `snapshot_path` exige `snapshot_sha256`;
- o validador proíbe fonte não autorizada pelo inventário da Fase 1;
- papel da fonte continua sendo cruzado com `source-registry-v1.json`.

### Competência

- a política de competência permanece obrigatória por regra;
- `context_overrides` permite bases diferentes por contexto sem texto livre;
- override para contexto não declarado é rejeitado;
- `rule_specific` exige descrição explícita.

### Lifecycle de release

O contrato distingue e valida `DRAFT`, `CANDIDATE`, `VALIDATED`, `PUBLISHED`, `SUPERSEDED` e `BLOCKED` com metadados próprios de lifecycle.

`PUBLISHED` exige validação, data de publicação, modo de aprovação e snapshots hashados. Release que contém mudança estrutural só pode ser publicada com `HUMAN_REVIEWED`.

### Testes negativos

A suíte cobre falhas de proveniência, competência, lifecycle, dependências, sobreposição de vigência, payloads estruturais e invariantes A01/H28/H29.

## O que isto fecha

Esta rodada fecha a pergunta: **o schema v1 consegue representar todas as famílias necessárias do inventário jurídico-fiscal da Fase 1 sem obrigar o consumidor a inferir semântica?**

A resposta passa a ser verificável por CI: `docs/contract-coverage-v1.json` precisa cobrir exatamente o conjunto de `rule_id` do inventário, e o conjunto de famílias usadas pelo mapa precisa coincidir exatamente com a união de payloads admitida pelo schema.

Isso ainda não encerra a Fase 2. Antes do handoff para a Fase 3, resta consolidar a cobertura de proveniência/competência no exemplo e fechar a política de release/versionamento como contrato estável.
