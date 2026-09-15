# Fase 6 — C6.5 — H28 — férias CLT sobre o contrato fiscal v1.2

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** migração da calculadora H28 (`/financas/calculadoras/ferias-clt/`) para a release fiscal v1.2 PUBLISHED/HUMAN_REVIEWED e remoção da ambiguidade entre direito adquirido, dias de gozo e abono pecuniário.

## 1. Defeito de origem corrigido

O frontend anterior continha lógica equivalente a:

```text
diasAbono = vender ? Math.round(dias / 3) : 0
```

Isso confundia a escolha de dias de gozo com o período a que o empregado tinha direito. Uma seleção de 20 dias podia produzir sete dias de abono; 15 podia produzir cinco, embora a interface anunciasse venda de dez dias.

C6.5 elimina essa relação. A sequência canônica passa a ser:

```text
faltas injustificadas no período aquisitivo
  -> vacation.entitlement_days_by_absences
  -> direito adquirido (30 / 24 / 18 / 12 no escopo vigente)
  -> vacation.abono_pecuniario, se solicitado
  -> 1/3 exato do direito adquirido
  -> dias de gozo = direito - dias convertidos
```

Como as faixas modeladas são divisíveis por três, 30→10, 24→8, 18→6 e 12→4. Se um sucessor produzir uma fração não integral sem regra executável de arredondamento estatutário, o runtime falha fechado; não usa `Math.round`.

## 2. Base monetária: fronteira explícita

H28 não inventa média de comissão, horas extras, adicional noturno ou outra composição remuneratória. A entrada monetária é a **base integral das férias já apurada para todo o período adquirido, sem o terço constitucional**.

A partir dela, a calculadora executa apenas transformações declaradas pelas regras de férias:

- principal das férias gozadas;
- terço constitucional sobre o principal gozado;
- principal do abono, quando existente;
- terço constitucional sobre o abono.

Essa fronteira é deliberada: o contrato v1.2 não contém uma regra executável que autorize o navegador a reconstruir médias trabalhistas ou a presumir uma composição universal da base.

## 3. Mudança contratual publicada antes do frontend

C6.5 depende da sucessora aprovada na Issue #36 e publicada como release fiscal canônica. O sucessor preserva o inventário fechado de 32 regras e amplia semanticamente duas regras existentes:

- `vacation.abono_pecuniario` passa a alcançar o período devido e os componentes remuneratórios correspondentes;
- `vacation.remuneration_and_constitutional_third` passa a ser selecionável também em `vacation_cash_allowance`, mantendo principal e terço como componentes distintos.

A publicação foi HUMAN_REVIEWED; o frontend nunca consome o pacote candidato de revisão.

## 4. Incidências por componente

A calculadora não aplica um rótulo tributário único a todo o pagamento de férias.

### Férias gozadas

`vacation.inss.enjoyed` declara que:

- principal das férias gozadas: INSS `yes`, IRRF `yes`;
- terço constitucional sobre férias gozadas: INSS `yes`, IRRF `yes`.

Esses dois componentes formam a base previdenciária usada pela tabela progressiva do INSS.

### Abono pecuniário

`vacation.abono.ir_exemption` declara para o principal do abono:

- INSS `no`;
- IRRF `no`.

`vacation.abono_constitutional_third.ir_incidence` declara para o terço sobre o abono:

- INSS `no`;
- IRRF `yes`.

Portanto o terço do abono não é silenciosamente agrupado com a isenção do principal.

## 5. IRRF de férias é uma avaliação separada

O fluxo executado por H28 é:

```text
rendimento tributável de férias
  = principal gozado
  + terço do gozo
  + terço do abono

base previdenciária
  = principal gozado
  + terço do gozo

INSS
  -> inss.employee.progressive_table / vacation_enjoyed

IRRF
  -> vacation.irrf.separate_assessment
  -> irrf.monthly.progressive_table
  -> deduções próprias desta avaliação
  -> vacation.irrf.reduction.2026
```

O principal do abono não entra no rendimento tributável de férias. O salário normal do restante do mês também não é somado a essa avaliação.

O engine Python foi alinhado previamente para selecionar `vacation.irrf.reduction.2026` no income type `vacation`. O runtime JavaScript de C6.5 seleciona o mesmo rule_id explicitamente e rejeita a semântica genérica mensal.

## 6. Caso E2E canônico

Em 15/09/2026, com:

```text
base integral das férias, sem 1/3 = 4000.00
faltas injustificadas              = 0
venda de 1/3                       = sim
dependentes                        = 0
pensão                              = 0.00
```

resultado estrutural:

```text
direito adquirido                  = 30 dias
dias de abono                      = 10 dias
dias de gozo                       = 20 dias
principal gozado                   = 2666.67
terço do gozo                      = 888.89
principal do abono                 = 1333.33
terço do abono                     = 444.44
total bruto das férias             = 5333.33
base previdenciária                = 3555.56
INSS                               = 315.27
rendimento tributável de férias    = 4000.00
IRRF final                         = 0.00
líquido estimado                   = 5018.06
```

O caso é executado em Node sobre o mesmo artefato apontado por `releases/fiscal-v1/current.json` e comparado com o engine Python.

## 7. Auditabilidade

O resultado carrega:

- `release_id`;
- `rule_id` e `rule_version` de cada regra efetivamente usada;
- data fiscal;
- faltas injustificadas;
- direito adquirido;
- dias de gozo e de abono;
- quatro componentes remuneratórios separados;
- base previdenciária e INSS;
- rendimento tributável de férias;
- base do IRRF após dedução;
- IRRF antes da redução;
- rendimento usado pelo redutor;
- redução aplicada e IRRF final.

## 8. Fail-closed

H28 rejeita, entre outros:

- base de férias negativa ou ausente;
- faltas negativas ou fora das faixas automatizadas;
- divisão do abono que exigiria arredondamento de dias não modelado;
- pensão omitida em vez de explicitamente informada, inclusive zero;
- regra de incidência cujo componente/flag divergir do esperado;
- ausência da apuração separada de férias;
- ausência do redutor dedicado `vacation.irrf.reduction.2026` ou semântica de input incompatível;
- release que não seja PUBLISHED, 32/32 e compatível exatamente com H28.

## 9. Arquivos versionados

```text
consumers/frontend/folha-vacation.js
consumers/frontend/ferias-clt.js
consumers/frontend/ferias-clt/index.php
consumers/frontend/ferias-clt/parts/*
tests/js/phase6_c65_h28_runtime.cjs
tests/test_h28_c65.py
scripts/validate_phase6_c65_gate.py
tests/test_phase6_c65_gate.py
```

## 10. Gate permanente e fronteira

O gate C6.5 executa o runtime JavaScript real contra a release PUBLISHED, confere o caso E2E com o engine Python, verifica as faixas 5/6/15/24/32 faltas, rejeição acima do escopo, incidências do abono, redutor dedicado, audit trail, ausência da fórmula legada e integridade da página versionada.

C6.5 **não afirma implantação no HostGator**. Também não migra H29 e não autoriza remoção dos adaptadores legados compartilhados. O salário ordinário do restante do mês permanece fora da avaliação fiscal das férias.

Próximo checkpoint: **C6.6 — H29 — rescisão CLT**.
