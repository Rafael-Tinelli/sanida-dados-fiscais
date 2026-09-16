# Fase 6 — C6.6 — H29 — rescisão CLT sobre o contrato fiscal v1.2

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** migração da calculadora H29 (`/financas/calculadoras/rescisao-clt/`) para a release fiscal v1.2 PUBLISHED/HUMAN_REVIEWED, preservando a promessa contratual de `partial_estimate`.

## 1. O H29 não promete o total da rescisão

C6.6 não amplia o contrato para uma calculadora universal. A saída continua explicitamente:

```text
result_promise = partial_estimate
user_disclosure_required = true
```

O frontend não cria KPI de total final. Ele apresenta separadamente o saldo salarial, o 13º proporcional quando devido, os avos de férias proporcionais quando devidos e a sinalização de férias adquiridas/vencidas sem monetizá-las automaticamente.

## 2. Motivo eSocial é input estrutural

A UI exige um dos quatro códigos fechados pelo contrato:

```text
01  Justa causa pelo empregador
02  Sem justa causa pelo empregador
07  Pedido de demissão
33  Acordo do art. 484-A
```

A matriz executável determina:

```text
01  saldo sim | 13º não | férias proporcionais não | adquiridas/vencidas se devidas
02  saldo sim | 13º sim | férias proporcionais sim | adquiridas/vencidas se devidas
07  saldo sim | 13º sim | férias proporcionais sim | adquiridas/vencidas se devidas
33  saldo sim | 13º sim | férias proporcionais sim | adquiridas/vencidas se devidas
```

`termination.thirteenth_proportional` e `termination.vacation_proportional` precisam reproduzir a mesma matriz. Divergência falha fechado.

## 3. Escopo de vínculo

O H29 v1 executa apenas o subconjunto declarado em `termination.reason_scope`:

- regime `monthly` ou `biweekly`;
- contrato `indefinite`.

Horista e contrato por prazo determinado não são convertidos por aproximação.

## 4. Saldo salarial usa dias civis reais

`termination.salary_balance` declara:

```text
monthly_base_salary
× days_counted_through_termination
÷ calendar_days_in_month
```

A propriedade `universal_fixed_denominator = false` é validada pelo consumidor. Assim:

```text
3100.00 / 31 × 10 = 1000.00  em março
3100.00 / 30 × 10 = 1033.33  em abril
```

Os dias computados são entrada explícita e não podem superar o dia do desligamento.

## 5. 13º e férias não compartilham calendário

Para `02/07/33`, o 13º rescisório usa o ano civil da extinção e a remuneração do mês da extinção. O limiar de 15 dias vem de `thirteenth.accrual.twelfths`.

As férias proporcionais usam `vacation.acquisition_period`, ancorado na admissão, sem reset em janeiro. A regressão A02 fica congelada:

```text
admissão       01/09/2025
desligamento   31/03/2026
13º            3/12
férias         7/12
```

A fronteira de 15 dias também é testada: 14 dias = 0/12; 15 dias = 1/12 nos respectivos cálculos.

## 6. Férias adquiridas/vencidas permanecem sinalização

`termination.acquired_and_overdue_vacation` declara os estados de período e exige regra explícita para eventual dobra. O contrato permite que o consumidor inicial limite o tratamento a um período se isso for divulgado.

Por isso C6.6 não inventa valor para períodos integrais/vencidos e não converte o antigo checkbox em fórmula monetária. A saída registra:

```text
monetary_calculation = not_automated_in_h29_v1
overdue_double       = requires_explicit_rule
```

## 7. Itens fora do cálculo automático

O payload `termination.partial_output_scope` continua excluindo, entre outros:

- aviso prévio ou desconto de aviso;
- multa rescisória do FGTS;
- saque do FGTS;
- seguro-desemprego;
- indenizações de estabilidade;
- verbas específicas de negociação coletiva;
- regras de contrato por prazo determinado;
- rescisão indireta sem contexto judicial resolvido;
- verbas rescisórias variáveis não explicitamente modeladas.

Nenhum desses itens é estimado localmente pelo JavaScript.

## 8. Caso E2E de controle

Com:

```text
motivo eSocial                    = 02
regime                            = monthly
prazo                             = indefinite
admissão                          = 01/09/2025
desligamento                      = 31/03/2026
salário-base mensal               = 3100.00
dias computados no mês            = 10
remuneração do mês da extinção    = 3600.00
```

resultado automatizado:

```text
saldo salarial        = 1000.00  (10/31)
13º proporcional      = 3/12 = 900.00
férias proporcionais  = 7/12
período aquisitivo    = 01/09/2025 a 31/08/2026
resultado              = partial_estimate
```

Não é calculado um “total da rescisão”, pois férias proporcionais e adquiridas/vencidas não possuem monetização completa no núcleo H29 limitado e diversos itens permanecem excluídos.

## 9. Auditabilidade e fail-closed

O resultado registra `release_id`, `rule_id` e `rule_version` das regras efetivamente selecionadas. C6.6 rejeita:

- motivo eSocial fora de `01/02/07/33`;
- regime ou prazo contratual fora da aplicabilidade publicada;
- dias computados acima do dia de desligamento;
- valores monetários negativos;
- matriz de motivo divergente das regras específicas de 13º/férias;
- tentativa de usar divisor mensal universal;
- período aquisitivo que declare reset por ano civil;
- release que não seja PUBLISHED, 32/32 e compatível exatamente com H29.

## 10. Arquivos versionados

```text
consumers/frontend/folha-termination.js
consumers/frontend/rescisao-clt.js
consumers/frontend/rescisao-clt/index.php
consumers/frontend/rescisao-clt/parts/*
tests/js/phase6_c66_h29_runtime.cjs
tests/test_h29_c66.py
scripts/validate_phase6_c66_gate.py
tests/test_phase6_c66_gate.py
```

## 11. Gate permanente e fronteira

O gate C6.6 executa o runtime JavaScript real contra a release PUBLISHED, compara o caso canônico com o engine Python, verifica os quatro motivos, a regressão 3/12 versus 7/12, divisor 31/30, fronteira 14/15 dias, escopo parcial, exclusões e audit trail.

C6.6 **não afirma implantação no HostGator**. Também não remove ainda os adaptadores legados compartilhados.

Próximo checkpoint: **C6.7 — remoção controlada do legado e fechamento da Fase 6**.
