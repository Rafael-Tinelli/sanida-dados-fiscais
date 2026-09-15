# Fase 6 — C6.2 — `folha-core` sobre o contrato fiscal v1.2

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** runtime fiscal compartilhado do frontend. Não inclui ainda a adaptação específica das calculadoras H26–H29, que permanece nos checkpoints C6.3–C6.6.

## 1. Baseline migrado

O `folha-core.js` efetivamente publicado antes desta etapa mantinha duas autoridades concorrentes:

```text
/blog/wp-json/sfa/v1/folha
raw.githubusercontent.com/.../main/dados_fiscais.json
```

Além disso, reconstruía no browser fórmulas de INSS, tabela de IRRF, redutor de 2026, férias proporcionais e divisor de rescisão sem preservar identidade da regra ou da release.

C6.2 versiona a nova fonte canônica em:

```text
consumers/frontend/folha-core.js
```

Esse arquivo corresponde ao ativo de produção `/financas/calculadoras/assets/folha-core.js` quando a futura implantação controlada ocorrer.

## 2. Fonte única de dados fiscais

O core passa a buscar exclusivamente:

```text
/blog/wp-json/sfa/v1/fiscal-release
```

Esse endpoint pertence à fronteira C6.1 e só entrega uma release já verificada pelo WordPress contra `releases/fiscal-v1/current.json`, o artefato imutável e seu SHA-256.

O frontend não possui fallback para `dados_fiscais.json`, para raw GitHub nem para `/sfa/v1/folha`.

## 3. Compatibilidade e fail-closed

Antes de usar uma release, o core exige:

- `contract_id = br.sanida.fiscal`;
- schema `1.2.0`;
- Contract API `1.2.0`;
- `status = PUBLISHED`;
- compatibilidade exata e `unsupported_behavior = hard_fail`;
- consumidores `H26/H27/H28/H29`;
- lifecycle final sem `block_reasons`;
- exatamente 32 regras, sem `rule_id` duplicada;
- `rule_version` SemVer;
- `quality.status = VALIDATED`;
- contextos e janelas de vigência válidos;
- payload pertencente às 18 famílias conhecidas.

Regra ausente, regra duplicada, vigência incompatível, contexto incompatível, payload desconhecido ou release não publicada falham fechados.

## 4. Seleção de regra

`selectRule()` centraliza a seleção por:

```text
release_id
+ rule_id
+ consumer
+ assessment context
+ target date / vigency
```

A seleção deve retornar exatamente uma regra. O resultado carrega a memória mínima auditável:

- `release_id`;
- `rule_id`;
- `rule_version`;
- contexto;
- data-alvo;
- início/fim de vigência.

`competenceBasisFor()` respeita o `CompetencePolicy` da própria regra e seus `context_overrides`.

## 5. Assessment identity

O IRRF não é selecionado apenas por “origem da verba”. O core replica, sob teste de paridade com o engine Python, a identidade fechada na Fase 3:

- `monthly`;
- `thirteenth`;
- `vacation`.

`termination` é contexto de origem e pode conter assessment mensal ou de 13º; não vira um quarto tipo de IRRF. A identidade determina o `rule_context`, a regra de redução e o `input_semantic` esperado.

Qualquer combinação fora da matriz fechada é rejeitada.

## 6. Dinheiro decimal

O core não aceita `Number` fracionário no caminho fiscal. `DecimalValue` usa coeficiente `BigInt` + escala decimal e implementa:

- soma/subtração;
- multiplicação;
- comparação/min/max;
- quantização explícita;
- `ROUND_HALF_UP`, `ROUND_HALF_EVEN`, `ROUND_DOWN`, `ROUND_FLOOR` e `ROUND_CEILING`.

Strings decimais vindas do contrato permanecem decimais até a fronteira de apresentação. Entrada monetária textual é normalizada para string decimal; float binário é rejeitado.

## 7. Primitives executáveis

C6.2 não cria novas regras jurídicas. Os primitives executam apenas payloads tipados existentes:

- `executeProgressive()` executa `progressive_table` respeitando `calculation_method`, `cap_base` e `rounding_policy`;
- `executeAffineReduction()` executa `affine_reduction` diretamente do payload;
- `scalarValue()` lê `scalar` com unidade esperada;
- `assessInss()` seleciona e executa `inss.employee.progressive_table`;
- `selectIrrfBundle()` seleciona tabela, redutor, dependente e desconto simplificado para um assessment;
- `assessIrrf()` aplica a sequência fechada pelo engine e preserva memória/audit trail.

A semântica é confrontada com o engine Python em teste de paridade usando a primeira release PUBLISHED real. O objetivo do JS é ser implementação consumidora do contrato, nunca autoridade normativa independente.

## 8. Lógica legada removida do core

O novo core não preserva como compatibilidade:

- `SFA.endpoints` com raw GitHub;
- `calcINSS(base, faixas)`;
- `calcIR(params)` sobre o payload legado;
- `startOfCurrentVacationPeriod` / `avosFeriasProporcionais`;
- `daysInTerminationMonth` com divisor 30 alternativo;
- valores de tabela/limites fiscais hardcoded.

Essas funções seriam uma segunda interpretação concorrente. As páginas H26–H29 serão migradas para os primitives contratuais nos checkpoints seguintes.

## 9. Testes e gate permanente

A suíte C6.2 executa Node contra a release apontada por `releases/fiscal-v1/current.json` e compara o resultado com o engine Python da Fase 3.

A paridade cobre, no mínimo:

- aritmética decimal exata e rejeição de float;
- seleção fail-closed e auditável;
- INSS progressivo;
- IRRF mensal sobre o mesmo bundle e rounding do engine;
- `release_id`, `rule_id` e `rule_version` preservados.

O Remake CI executa também:

```text
python scripts/validate_phase6_c62_gate.py
```

O gate faz `node --check`, rejeita sinais do consumidor fiscal legado e exige a integração do runtime/testes/documentação ao repositório.

## 10. Fronteira com C6.3–C6.6

C6.2 entrega o runtime compartilhado. Ainda não declara H26, H27, H28 ou H29 migradas.

Próximo checkpoint: **C6.3 — H26 — salário líquido CLT**, que deve adaptar a página/JS da calculadora ao novo core e provar o caso A01 completo, inclusive base `5350.40`, redutor sobre `6000.00`, IRRF final `382.88` e `release_id` exibível/auditável.

## 11. Implantação

Este fechamento consolida a fonte canônica do novo `folha-core` no repositório. Ele **não afirma implantação no HostGator** e não deve ser instalado isoladamente antes de seus consumidores H26–H29 serem migrados para a nova API nos checkpoints seguintes.
