# Fase 3 — Biblioteca fiscal e testes

**Status:** EM ANDAMENTO  
**Início:** 13/09/2026  
**Base:** `docs/phase2-to-phase3-handoff.md`

## 1. Objetivo

Executar o Contrato Fiscal Canônico v1 por meio de funções puras, determinísticas e auditáveis, sem consultar web, scraping ou fontes oficiais em tempo de cálculo.

A Fase 3 não reinterpreta legislação. Se uma operação necessária não estiver expressa no contrato congelado, o trabalho deve voltar ao contrato/inventário como mudança explícita.

## 2. Primeira rodada

Esta rodada cria a primeira camada compartilhada do engine:

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

## 3. Memória mínima de IRRF implementada

A saída `IrrfAssessmentMemory` preserva os campos congelados no handoff:

```text
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

O redutor recebe `gross_taxable_income` como `reduction_input_income`. Ele não recebe `irrf_tax_base`.

## 4. Casos executáveis desta rodada

Os cinco casos da Receita Federal fechados na Fase 1 passam a ser executados contra o engine:

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

## 5. INSS progressivo nesta rodada

O engine marginal já suporta a família necessária ao INSS: bandas progressivas, teto opcional e arredondamento por estágio. Nesta rodada os testes do algoritmo são matemáticos/property-based; a Fase 3 ainda não cria uma regra INSS inexistente no `CANDIDATE` apenas para satisfazer um teste.

Quando o contrato executável disponibilizar uma instância canônica da regra `inss.employee.progressive_table`, o mesmo primitive deverá consumi-la sem duplicação de fórmula.

## 6. Limites preservados

Ainda não são implementados nesta rodada:

- composição integral do 13º;
- férias/período aquisitivo/abono;
- rescisão H29;
- collectors/snapshots/parsers;
- publicação de releases;
- migração de WordPress/`folha-core`/H26–H29.

## 7. Próximos checkpoints da Fase 3

Após esta primeira rodada ficar verde:

1. completar primitives de deduções por contexto e apurações separadas;
2. implementar avos/13º e seus testes oficiais;
3. implementar período aquisitivo, férias proporcionais e abono;
4. implementar saldo salarial e matriz H29 limitada;
5. consolidar memória de cálculo comum;
6. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.
