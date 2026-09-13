# Inventário jurídico-fiscal v1 — H26 a H29

**Status:** Fase 1 CONCLUÍDA  
**Data de corte:** 13/09/2026  
**Escopo:** H26 — salário líquido; H27 — 13º salário; H28 — férias; H29 — rescisão.  
**Handoff:** `docs/phase1-closure.md`

## 1. Função deste documento

Este arquivo é a leitura humana do inventário jurídico-computacional que antecede o **Contrato Fiscal Canônico v1**.

A representação machine-readable está em:

- `docs/rule-inventory-v1.json` — regras, escopo e decisões;
- `docs/source-registry-v1.json` — fontes oficiais e papéis de evidência;
- `tests/reference_cases/phase1_reference_cases.json` — casos oficiais, limites e regressões.

A Fase 2 não deve voltar a inferir significado jurídico a partir de números soltos. O schema e o engine deverão materializar as decisões congeladas aqui.

## 2. Conclusões fechadas

1. **A01 é defeito semântico, não apenas numérico.** A redução de IR de 2026 usa o rendimento tributável sujeito à incidência mensal como variável de entrada, e não a base de IR após deduções.
2. **O redutor de 2026 também se aplica ao 13º.** A Lei 15.270/2025 determina isso expressamente.
3. **Férias tributáveis recebem apuração própria de IR e entram na sistemática mensal de 2026.** O inventário registra ligação normativa direta, embora não tenha sido localizado exemplo numérico oficial específico de férias com o redutor.
4. **Mensal, férias e 13º são contextos distintos de IR.** Deduções e desconto simplificado precisam ser avaliados dentro da respectiva apuração.
5. **H27 não pode usar `total13 * 0.5` como primeira parcela universal.** O adiantamento possui referência e regras próprias.
6. **Remuneração variável do 13º precisa de regra canônica própria.** A média pronta informada pelo usuário pode existir apenas como compatibilidade explicitamente marcada.
7. **A02 está confirmado.** Férias proporcionais seguem o período aquisitivo e não reiniciam no ano civil.
8. **Direito, gozo e abono de férias são variáveis diferentes.** `Math.round(dias / 3)` não representa o art. 143 da CLT.
9. **O principal do abono e o terço incidente sobre ele não têm o mesmo tratamento tributário.** O principal do abono é não tributável pelo IR; o terço constitucional incidente sobre o abono, pago durante o contrato, sofre IR. Ambos ficam fora da contribuição previdenciária nas fontes operacionais adotadas.
10. **H29 precisa de motivo do desligamento.** O Contrato v1 suportará inicialmente os códigos eSocial `01`, `02`, `07` e `33`; demais motivos serão `UNSUPPORTED` até especificação própria.
11. **H29 continuará parcial.** Aviso, multa/saque FGTS, seguro-desemprego, estabilidade, regras de contrato a termo, instrumentos coletivos e demais itens não modelados não compõem um “total da rescisão”.
12. **Saldo de salário não terá divisor 30 universal.** Para o escopo v1 de mensalista/quinzenalista, a referência operacional é salário-base dividido pelos dias do mês e multiplicado pelos dias considerados no desligamento, salvo override tipado com proveniência.
13. **Vigência e qualidade são precondições do cálculo.** `generated_at` não substitui `effective_from`, competência, proveniência ou estado de qualidade.
14. **Mudança estrutural não é autopublicável.** Deve resultar em `REVIEW_REQUIRED`.

## 3. Matriz final de regras

| Rule ID | Consumidores | Classe | Estado de entrada na Fase 2 | Decisão principal |
|---|---|---|---|---|
| `inss.employee.progressive_table` | H26–H29 | parametrizável | fechado | Faixas/teto com vigência e proveniência obrigatórias. |
| `irrf.monthly.progressive_table` | H26/H28/H29 | parametrizável | fechado | Base pós-deduções é estágio distinto do rendimento usado pelo redutor. |
| `irrf.dependent_deduction` | H26–H29 | parâmetro | fechado | Dedução tipada por apuração e vigência. |
| `irrf.simplified_monthly_discount` | H26–H29 | parametrizável | fechado | Comparação separada em mensal, férias e 13º. |
| `irrf.deductions_by_income_type` | H26–H29 | estrutural | fechado | Previdência/pensão/deduções vinculadas ao tipo de rendimento. |
| `irrf.reduction.2026` | H26/H28/H29 | parametrizável | **A01** | Target = rendimento tributável antes das deduções do IR. |
| `irrf.income_type` | H26–H29 | estrutural | fechado | `monthly`, `vacation`, `thirteenth`. |
| `thirteenth.accrual.twelfths` | H27/H29 | estrutural | fechado | 15+ dias gera 1/12. |
| `thirteenth.reference_remuneration` | H27/H29 | estrutural | fechado | Separar anual normal e rescisório. |
| `thirteenth.variable_remuneration` | H27/H29 | estrutural | fechado | Modelar regra normativa e revisão posterior. |
| `thirteenth.advance` | H27 | estrutural | fechado | Não usar metade do total anual estimado como regra universal. |
| `thirteenth.inss.separate_assessment` | H27/H29 | estrutural | fechado | INSS do 13º separado do mensal. |
| `thirteenth.irrf.exclusive_assessment` | H27/H29 | estrutural | fechado | Contexto e deduções próprias do 13º. |
| `thirteenth.irrf.reduction.2026` | H27/H29 | parametrizável | fechado | Lei manda aplicar redutor também ao 13º. |
| `vacation.acquisition_period` | H28/H29 | estrutural | **A02** | Período aquisitivo contínuo, inclusive entre anos civis. |
| `vacation.entitlement_days_by_absences` | H28 | estrutural | fechado | Art. 130 vira regra canônica do direito em dias. |
| `vacation.remuneration_and_constitutional_third` | H28/H29 | estrutural | fechado | Natureza `enjoyed` e `indemnified` é obrigatória. |
| `vacation.abono_pecuniario` | H28 | estrutural | defeito atual | Abono = 1/3 do direito, não dos dias escolhidos para gozo. |
| `vacation.abono.ir_exemption` | H28 | estrutural | fechado | Principal do abono: IR não / CP não. |
| `vacation.abono_constitutional_third.ir_incidence` | H28 | estrutural | **novo P0** | Terço sobre abono: IR sim / CP não. |
| `vacation.irrf.separate_assessment` | H28 | estrutural | fechado | Férias gozadas têm apuração própria no pagamento. |
| `vacation.irrf.reduction.2026` | H28 | parametrizável | fechado | Redutor integra a apuração separada das férias tributáveis. |
| `vacation.inss.enjoyed` | H28 | estrutural | fechado | Férias gozadas + terço entram na base previdenciária. |
| `termination.reason_scope` | H29 | estrutural | contexto ausente | Motivo é input obrigatório; v1 suporta 01/02/07/33. |
| `termination.partial_output_scope` | H29 | produto | fechado | Saída permanece estimativa parcial. |
| `termination.salary_balance` | H29 | estrutural | fechado | Sem divisor 30 universal; regra padrão v1 segue escopo mensalista/quinzenalista. |
| `termination.thirteenth_proportional` | H29 | estrutural | fechado | Devido em 02/07/33; não em 01. |
| `termination.vacation_proportional` | H29 | estrutural | **A02** | Devida em 02/07/33; não em 01; contar período aquisitivo. |
| `termination.acquired_and_overdue_vacation` | H29 | estrutural | simplificação atual | Diferenciar integral adquirida, vencida e eventual dobra. |
| `termination.vacation_indemnity_tax_treatment` | H29 | estrutural | fechado | Natureza indenizatória explícita, sem herdar regra de férias gozadas. |
| `technical.money_decimal_and_rounding` | H26–H29 | técnico | fechado p/ design | `Decimal`; política de quantização por fórmula/estágio. |
| `technical.contract_vigency_and_quality` | H26–H29 | técnico | ausente hoje | Gate obrigatório de vigência/proveniência/qualidade. |

A estrutura completa, fontes por regra e matriz de rescisão estão em `docs/rule-inventory-v1.json`.

## 4. Fontes oficiais canônicas da Fase 1

A lista canônica está em `docs/source-registry-v1.json` e distingue:

- `normative_primary` — Lei, Decreto, CLT;
- `administrative_norm` — IN/Solução de Consulta da autoridade competente;
- `official_operational` — Receita, INSS, MTE e eSocial para execução/classificação operacional;
- `official_reference_case` — exemplo numérico oficial transformado em teste de regressão.

Fontes centrais:

- Lei 15.270/2025;
- tabela IRRF 2026 e exemplos oficiais da Receita;
- IN RFB 1.500/2014 em versão anotada;
- SC Cosit 209/2021;
- tabela de incidência previdenciária da Receita;
- tabela INSS 2026;
- CLT;
- Leis 4.090/1962 e 4.749/1965;
- Decreto 10.854/2021;
- Tabelas e manuais do eSocial;
- FAQ oficial do MTE sobre verbas rescisórias.

## 5. Defeitos bloqueadores convertidos em requisitos

### 5.1. A01 — IRRF 2026

Campos mínimos da memória de cálculo:

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

Caso oficial congelado:

```text
rendimento tributável = R$ 6.000,00
base do IR            = R$ 5.350,40
IR pré-redução        = R$ 562,63
entrada do redutor    = R$ 6.000,00
redução               = R$ 179,75
IR final              = R$ 382,88
```

### 5.2. A02 — férias proporcionais

A contagem deve partir do período aquisitivo:

```text
acquisition_period_start
reference_or_termination_date
qualifying_month_fractions
proportional_twelfths
```

Regressão obrigatória:

```text
01/09/2025 → 31/03/2026 = 7/12
```

### 5.3. Abono — direito/gozo

Campos distintos:

```text
vacation_entitled_days
vacation_enjoyed_days
vacation_cash_allowance_days
```

Com direito de 30 dias e opção válida:

```text
cash_allowance_days = 10
```

### 5.4. Novo P0 — terço constitucional sobre o abono

Não é permitido um único “vetor indenizatório” contendo principal e terço com o mesmo tratamento de IR.

```text
cash_allowance_principal:
  IR = false
  CP = false

constitutional_third_on_cash_allowance:
  IR = true
  CP = false
```

## 6. Escopo inicial fechado de H29

### População

- mensalista ou quinzenalista;
- contrato por prazo indeterminado;
- motivo eSocial suportado.

### Motivos

| Código | Modalidade | 13º prop. | Férias prop. |
|---|---|---:|---:|
| `01` | justa causa pelo empregador | não | não |
| `02` | sem justa causa pelo empregador | sim | sim |
| `07` | pedido de demissão | sim | sim |
| `33` | acordo art. 484-A | sim | sim |

Saldo de salário e férias integrais/vencidas, quando devidas, permanecem parcelas distintas.

### Fora do “total” v1

- aviso prévio;
- multa/saque FGTS;
- seguro-desemprego;
- estabilidade;
- instrumentos coletivos;
- contrato a termo e regimes especiais;
- rescisão indireta não resolvida;
- variáveis rescisórias não modeladas.

## 7. Casos de referência congelados

`tests/reference_cases/phase1_reference_cases.json` contém:

- exemplos oficiais de IR 2026 e regressão A01;
- fronteira 14/15 dias do 13º;
- caso simples do adiantamento fixo;
- regressão A02 atravessando ano civil;
- limites do direito de férias por faltas;
- abono de 10 dias sobre direito de 30;
- tratamento tributário separado do principal e do terço sobre abono;
- matriz de motivos 01/02/07/33;
- saldo de salário de mensalista em mês com 31 dias.

Na Fase 2/3 esses casos serão ligados aos `rule_id` e convertidos em testes executáveis do engine.

## 8. Política de mudança congelada

```text
SOURCE_REFRESH_NO_CHANGE
PARAMETER_CHANGE
EFFECTIVE_DATE_CHANGE
STRUCTURAL_CHANGE
RULE_ADDED
RULE_REMOVED
SOURCE_UNAVAILABLE
PARSER_INCOMPATIBLE
```

- parâmetro conhecido: pode chegar à autopublicação somente após validação;
- mudança estrutural: `REVIEW_REQUIRED`;
- fonte indisponível/parser incompatível: nunca fabricar contrato corrente;
- `last-good`: somente enquanto sua vigência permanecer válida.

## 9. Gate de entrada para o Contrato Fiscal Canônico v1

A Fase 2 deverá materializar, no mínimo:

```text
rule_id
schema_version
jurisdiction
domain
rule_class
description
applies_to
assessment_context
applicability predicates
parameters
units
dependencies
calculation_order
rounding_policy
effective_from
effective_until
competence
source_ids
source snapshot/hash
observed_at
quality_status
change_class
reference_case_ids
last_good_policy
consumer compatibility/version
```

## 10. Critério de saída

- [x] significado e fonte definidos para as regras do escopo H26–H29;
- [x] regras estruturais separadas de parâmetros;
- [x] fontes oficiais registradas;
- [x] A01/A02 e abono convertidos em regressões;
- [x] novo P0 do terço sobre abono registrado;
- [x] H29 com motivos e limites explícitos;
- [x] vigência/proveniência/qualidade definidas como gates;
- [x] CI do remake existente;
- [x] handoff formal para Fase 2 documentado.

## 11. Próxima ação

Iniciar a **Fase 2 — Contrato Fiscal Canônico v1**, transformando este inventário em Pydantic, JSON Schema, invariantes executáveis e política de seleção/versionamento, sem reabrir decisões jurídicas da Fase 1 sem evidência oficial nova.
