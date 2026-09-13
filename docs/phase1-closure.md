# Fechamento da Fase 1 — Inventário jurídico-fiscal

**Status:** CONCLUÍDA  
**Data de fechamento:** 13/09/2026  
**Escopo:** H26 — salário líquido; H27 — 13º salário; H28 — férias; H29 — rescisão.

## 1. Resultado da fase

A Fase 1 deixa de ser apenas um levantamento editorial e passa a constituir a **especificação jurídica e semântica de entrada** para o Contrato Fiscal Canônico v1.

O fechamento produz quatro artefatos complementares:

- `docs/inventario-juridico-fiscal-v1.md` — leitura humana do diagnóstico e das decisões;
- `docs/rule-inventory-v1.json` — inventário machine-readable das regras e decisões congeladas;
- `docs/source-registry-v1.json` — registro canônico das fontes oficiais aceitas nesta fase;
- `tests/reference_cases/phase1_reference_cases.json` — casos oficiais, limites e regressões que o Contrato/engine deverá preservar.

Nenhum desses arquivos substitui a legislação ou a fonte oficial. Eles registram **como o projeto interpretará e versionará computacionalmente as regras dentro do escopo declarado**.

## 2. Decisões fechadas para a Fase 2

### 2.1. IRRF: rendimento, base e redução são conceitos distintos

O Contrato v1 deverá distinguir pelo menos:

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

A redução instituída para 2026 é determinada pelo **rendimento tributável sujeito à incidência mensal**, e não pela base de cálculo já reduzida por INSS, dependentes, pensão ou desconto simplificado.

O caso oficial de R$ 6.000 fica congelado como regressão A01:

```text
rendimento tributável = 6.000,00
base do IR            = 5.350,40
IR antes da redução   = 562,63
entrada do redutor    = 6.000,00
redução               = 179,75
IR final              = 382,88
```

### 2.2. IRRF precisa de contexto de rendimento

O engine não poderá tratar mensal, férias e 13º como uma única apuração genérica sem contexto.

O Contrato v1 deverá possuir um discriminador equivalente a:

```text
income_type = monthly | vacation | thirteenth
```

As deduções legais, o desconto simplificado e as incidências serão resolvidos dentro da apuração correspondente.

### 2.3. Redução de 2026 no 13º

A Lei 15.270/2025 determina expressamente a aplicação da redução ao imposto cobrado exclusivamente na fonte sobre o 13º salário.

O 13º terá target semântico próprio:

```text
thirteenth_taxable_income_before_irrf_deductions
```

### 2.4. Redução de 2026 nas férias tributáveis

A Fase 1 fecha a regra para o Contrato v1: a redução integra a apuração separada das **férias tributáveis pagas no curso do contrato**.

Base do fechamento:

- a Lei 15.270/2025 alcança rendimentos tributáveis sujeitos à incidência mensal;
- a IN RFB 1.500 trata as férias como apuração no mês do pagamento, separada dos demais rendimentos, usando a sistemática mensal;
- a Receita mantém férias como rendimento sujeito à tributação no pagamento;
- o eSocial separa mensal, 13º e férias como contextos próprios de IR.

Não foi localizado caso numérico oficial específico de férias aplicando o redutor. Por isso o inventário registra explicitamente o nível de evidência como **ligação normativa direta, sem exemplo oficial trabalhado específico**. Isso não bloqueia o Contrato v1, mas impede apresentar um exemplo inventado como caso oficial.

### 2.5. 13º — avos e fronteira dos 15 dias

Para o direito ao 13º anual/rescisório:

```text
fração < 15 dias  → não gera avo
fração >= 15 dias → gera 1/12
```

Essa regra não deve ser confundida com condições específicas do adiantamento do 13º.

### 2.6. 13º — primeira parcela

A fórmula atual `total13 * 0.5` deixa de ser aceita como regra universal.

Para remuneração fixa, o adiantamento legal parte da metade do salário recebido no mês anterior, observadas as regras de período de pagamento e situações específicas previstas na legislação.

O Contrato v1 deverá separar:

```text
annual_entitlement
advance_reference_remuneration
advance_amount
final_settlement
```

### 2.7. 13º — remuneração variável

O campo atual de “variáveis” não é a regra canônica.

O Contrato v1 deverá modelar a metodologia prevista no Decreto 10.854/2021 para remuneração variável e sua revisão posterior. Uma média previamente calculada pelo usuário poderá existir apenas como **entrada externa de compatibilidade**, identificada como tal, sem ser confundida com cálculo normativo executado pelo engine.

### 2.8. Férias — período aquisitivo

O período aquisitivo é contínuo e não reinicia em 1º de janeiro.

Regressão A02 obrigatória:

```text
admissão:     01/09/2025
desligamento: 31/03/2026
resultado:    7/12
```

### 2.9. Férias — direito em dias

O Contrato v1 deverá representar o direito decorrente do art. 130 da CLT, inclusive as faixas de faltas injustificadas:

```text
0–5 faltas   → 30 dias
6–14         → 24 dias
15–23        → 18 dias
24–32        → 12 dias
```

Um seletor arbitrário de dias não poderá ser tratado como sinônimo de direito adquirido.

### 2.10. Férias — direito, gozo e abono são variáveis diferentes

O Contrato v1 deverá separar:

```text
vacation_entitled_days
vacation_enjoyed_days
vacation_cash_allowance_days
```

O abono pecuniário corresponde à conversão de **1/3 do período a que o empregado tem direito**. Para direito de 30 dias, o caso de referência é 10 dias de abono. É proibido derivar o abono por `round(enjoyed_days / 3)`.

### 2.11. Férias — principal do abono e terço sobre o abono têm incidências distintas

Este fechamento identificou um P0 adicional na implementação atual.

O principal do abono pecuniário do art. 143:

```text
IR: não
CP: não
```

O terço constitucional incidente sobre esse abono, pago durante o contrato:

```text
IR: sim
CP: não
```

Portanto, o engine não poderá colocar `abono + 1/3 do abono` em um único vetor integralmente indenizatório/isento. Os componentes devem existir como rubricas semânticas distintas.

### 2.12. Férias gozadas x férias indenizadas

A natureza da verba será obrigatória no contrato:

```text
vacation_nature = enjoyed | indemnified
```

Férias gozadas e o respectivo terço entram na apuração própria de IR e na base previdenciária pertinente. Férias convertidas em pecúnia na extinção do contrato não podem herdar automaticamente o tratamento das férias gozadas.

## 3. Escopo fechado de H29 para o Contrato v1

H29 continuará sendo uma **estimativa parcial de verbas suportadas**, não uma calculadora universal do total da rescisão.

### 3.1. População-base suportada

No primeiro contrato canônico:

- empregado mensalista ou quinzenalista;
- contrato por prazo indeterminado;
- uma das quatro modalidades de desligamento explicitamente suportadas.

Horista, diarista, semanalista, contrato a termo, aprendizagem, intermitente e demais regimes não serão inferidos como equivalentes.

### 3.2. Motivos suportados

O contrato usará como identificadores canônicos os códigos da Tabela 19 do eSocial:

| Código | Modalidade | Saldo | 13º proporcional | Férias proporcionais | Férias integrais/vencidas |
|---|---|---:|---:|---:|---:|
| `01` | Justa causa pelo empregador | sim | não | não | sim, quando devidas |
| `02` | Sem justa causa pelo empregador | sim | sim | sim | sim, quando devidas |
| `07` | Pedido de demissão | sim | sim | sim | sim, quando devidas |
| `33` | Acordo do art. 484-A | sim | sim | sim | sim, quando devidas |

Outros motivos serão retornados como `UNSUPPORTED` até ganharem regra própria.

### 3.3. Saldo de salário

A Fase 1 rejeita um `divisor=30` universal.

Para o escopo padrão de mensalista/quinzenalista adotado no v1, o método operacional de referência será o usado pelo eSocial simplificado:

```text
salário-base / total de dias do mês × dias considerados no desligamento
```

Se contrato, norma coletiva ou regime específico exigir outra base, isso só poderá entrar como override explícito, tipado e com proveniência. A UI não deve apresentar “30 ou dias reais” como se fossem duas regras jurídicas intercambiáveis sem contexto.

### 3.4. Itens explicitamente fora do total de H29 v1

Enquanto não houver modelagem própria, permanecem fora do valor calculado:

- aviso prévio pago ou desconto de aviso;
- multa rescisória do FGTS;
- saque do FGTS;
- seguro-desemprego;
- indenizações de estabilidade;
- verbas específicas de convenção/acordo coletivo;
- regras de contrato por prazo determinado;
- rescisão indireta sem contexto juridicamente resolvido;
- componentes variáveis rescisórios não modelados.

A resposta ao usuário deverá expor esses limites de forma inequívoca.

## 4. Fontes e níveis de evidência

O Contrato v1 não deverá usar uma única noção genérica de “fonte”. A Fase 1 fecha quatro papéis:

```text
normative_primary       → lei, decreto, CLT
administrative_norm     → IN/Solução de Consulta da autoridade competente
official_operational    → eSocial, MTE, INSS, tabelas operacionais da Receita
official_reference_case → exemplo numérico oficial transformado em regressão
```

`docs/source-registry-v1.json` passa a ser o registro de fontes de entrada para o desenho do schema.

## 5. Classes de mudança

O Contrato v1 deverá permitir classificar, no mínimo:

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

Política fechada:

- mudança puramente paramétrica pode chegar a autopublicação somente após validação;
- mudança estrutural exige `REVIEW_REQUIRED`;
- ausência/incompatibilidade da fonte não pode gerar contrato corrente fictício;
- `last-good` só é utilizável enquanto sua vigência permanecer defensável.

## 6. Campos mínimos que a Fase 2 precisa materializar

Sem congelar ainda os nomes finais das classes Pydantic, o Contrato Fiscal Canônico v1 deverá ser capaz de representar:

```text
rule_id
schema_version
jurisdiction
domain
rule_class
description
applies_to
income_type / assessment_context
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
source snapshots / hashes
observed_at
quality_status
change_class
reference_case_ids
last_good_policy
consumer compatibility/version
```

Esses requisitos são a passagem formal da especificação jurídica para a especificação de software.

## 7. Questões que deixam de bloquear a Fase 2

Estão fechados nesta fase:

- target semântico do redutor de 2026;
- redutor no 13º;
- tratamento do redutor nas férias tributáveis;
- separação mensal/férias/13º para IR;
- regra de avos do 13º;
- rejeição da primeira parcela como simples metade do total estimado;
- tratamento canônico de remuneração variável do 13º;
- período aquisitivo de férias;
- dias de direito por faltas;
- abono como fração do direito;
- incidência distinta do principal e do terço do abono;
- escopo inicial de H29 e seus motivos suportados;
- política do saldo de salário para o escopo mensalista/quinzenalista;
- limites explícitos da estimativa de rescisão.

## 8. Questões legitimamente transferidas para a Fase 2

Permanecem abertas apenas decisões de **design de software e operação**, não lacunas jurídicas P0 da Fase 1:

- nomes e composição exata das classes Pydantic;
- `$id` e modularização dos JSON Schemas;
- organização de diretórios;
- granularidade física de releases e contratos;
- formato de snapshots e política de retenção;
- política exata de versionamento de schema/release;
- formato de distribuição para WordPress/SFA;
- política temporal específica de `last-good` por regra;
- mecanismo de semantic diff;
- estratégia de compatibilidade durante a migração dos consumidores.

## 9. Critério de aceite da Fase 1

- [x] regras necessárias a H26–H29 inventariadas dentro do escopo declarado;
- [x] fontes oficiais registradas e classificadas por papel;
- [x] A01 convertido em requisito e caso oficial de regressão;
- [x] A02 convertido em requisito e caso de regressão;
- [x] modelo de direito/gozo/abono de férias especificado;
- [x] incidência do terço sobre o abono separada do principal;
- [x] 13º anual, adiantamento, variável e rescisório separados semanticamente;
- [x] H29 recebeu matriz de motivos suportados e lista explícita de exclusões;
- [x] vigência, proveniência e qualidade definidas como precondições do consumidor;
- [x] mudança estrutural separada de mudança paramétrica;
- [x] CI do remake existente para impedir regressões documentais/estruturais.

## 10. Handoff

A próxima etapa é **Fase 2 — Contrato Fiscal Canônico v1**.

A Fase 2 não deve reabrir as decisões acima sem evidência oficial nova ou contradição objetiva. Seu trabalho é transformar este inventário em tipos, schemas, invariantes executáveis e política de seleção/versionamento de contratos.
