# Fase 6 — C6.3 — H26 — salário líquido CLT

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** migração da calculadora H26 (`/financas/calculadoras/salario-liquido-clt/`) para o runtime fiscal C6.2 e a release canônica v1.2 publicada.

## 1. Baseline real migrado

O baseline foi extraído da cópia de produção fornecida no projeto, em:

```text
financas/calculadoras/salario-liquido-clt/index.php
financas/calculadoras/assets/salario-liquido.js
```

A interface existente foi preservada: salário bruto, variáveis habituais, dependentes, pensão alimentícia e outros descontos continuam sendo os inputs do H26.

O JavaScript anterior chamava a API legada do `folha-core`:

```text
fetchData()
calcINSS()
calcIR()
```

C6.3 remove esse caminho do consumidor H26. A fonte canônica versionada passa a ser:

```text
consumers/frontend/salario-liquido.js
consumers/frontend/salario-liquido-clt/index.php
```

## 2. Cadeia fiscal do H26

O consumidor carrega exclusivamente uma release validada pelo C6.2:

```text
SFA.fetchRelease({ consumer: 'H26' })
```

E executa:

1. remuneração bruta = salário mensal + variáveis habituais;
2. `SFA.assessInss()` no contexto `monthly`;
3. `SFA.assessIrrf()` com `incomeType = monthly` e `originContext = monthly`;
4. pensão como dedução legal explícita do assessment de IRRF e também como saída do líquido;
5. outros descontos somente depois da apuração fiscal, sem alterar INSS ou IRRF;
6. líquido = bruto − INSS − IRRF − pensão − outros descontos.

Nenhuma faixa, alíquota, limite ou dedução fiscal é hardcoded no H26.

## 3. Data de referência e fail-closed

O H26 continua sendo uma calculadora das regras correntes. Na interface, a data civil local da execução é usada como `targetDate` e fica visível na memória do resultado.

A seleção de regra continua pertencendo ao `folha-core`: `rule_id + consumer + assessment context + targetDate/vigency` deve resolver exatamente uma regra validada.

Se a release não estiver disponível, for incompatível ou não possuir regra vigente para a data, o H26 não tenta reinterpretar a legislação nem reutilizar número hardcoded: o cálculo falha fechado.

Isso é especialmente relevante para `irrf.reduction.2026`, cuja release corrente declara fim de vigência em 2026-12-31. Em 2027, o consumidor precisa de uma release sucessora compatível; a ausência dela é erro, não autorização para continuar usando 2026.

## 4. A01 — reconciliação do vetor histórico com o H26 E2E

O handoff anterior registrava um vetor A01 com:

```text
rendimento tributável = 6000.00
INSS fornecido = 649.60
base IRRF = 5350.40
IRRF final = 382.88
```

Esse vetor continua válido **como regressão isolada do assessment de IRRF**: com dedução previdenciária explicitamente fornecida de `649.60`, o redutor recebe `6000.00`, não `5350.40`, e o resultado final é `382.88`.

Ele, porém, não pode ser usado como resultado E2E do H26 atual porque a primeira release PUBLISHED v1.2 contém a tabela progressiva INSS 2026 canônica. Para remuneração de `6000.00`, essa tabela produz:

```text
INSS = 641.51
base IRRF = 5358.49
IRRF antes da redução = 564.85
rendimento testado no redutor = 6000.00
redução = 179.75
IRRF final = 385.10
salário líquido, sem outros descontos = 4973.39
```

Portanto C6.3 preserva duas regressões distintas:

- **A01 isolado:** prova a semântica do redutor e continua retornando `382.88` quando o INSS de entrada é `649.60`;
- **H26 E2E canônico:** usa a própria regra `inss.employee.progressive_table` da release e retorna `385.10` de IRRF para `6000.00`.

Forçar `382.88` no H26 completo exigiria substituir o INSS produzido pela release por um número externo e violaria a fonte única consolidada nos C6.0–C6.2.

## 5. Memória exibida ao usuário

A área de detalhes já existente foi preservada e ampliada para mostrar:

- INSS;
- IRRF final;
- pensão;
- outros descontos;
- base usada no IRRF;
- IRRF antes da redução;
- rendimento tributável testado no redutor;
- modo de dedução (`legal` ou `simplified`);
- redução aplicada;
- teto da base INSS declarado pela regra;
- ano fiscal;
- data de referência;
- `release_id` efetivamente usada.

O objeto de cálculo também carrega os audit records das regras executadas com `release_id`, `rule_id` e `rule_version`.

## 6. Dinheiro e inputs

O H26 não converte valores fiscais em `Number` fracionário. Salário, variáveis, pensão, outros descontos, bases e tributos permanecem em `SFA.Decimal`, implementado em C6.2 com `BigInt` + escala.

`dependentCount` é o único input numérico inteiro. Valores monetários negativos e quantidade de dependentes inválida são rejeitados.

## 7. Outros descontos não são nova regra fiscal

O campo já existente de “outros descontos” continua representando vale, adiantamento, coparticipação e abatimentos informados pelo usuário. Ele é aplicado **somente ao líquido**.

O teste C6.3 prova que adicionar `100.05` nesse campo:

- não altera o INSS;
- não altera a memória do IRRF;
- reduz o líquido exatamente em `100.05`.

Isso evita transformar um desconto administrativo informado pelo usuário em dedução tributária sem suporte contratual.

## 8. Testes e gate permanente

A suíte C6.3 executa o `folha-core.js` e o consumidor H26 no Node contra a release apontada por `releases/fiscal-v1/current.json` e confronta o resultado com o engine Python.

Ela cobre:

- H26 E2E com a release PUBLISHED real;
- INSS `641.51` em `6000.00`;
- base IRRF `5358.49`;
- redutor recebendo `6000.00`;
- IRRF final `385.10`;
- líquido `4973.39` sem outros descontos;
- vetor A01 isolado `649.60 → 5350.40 → 382.88`;
- outros descontos fora das bases fiscais;
- `release_id`, `rule_id` e `rule_version` auditáveis;
- vigência sem regra sucessora falhando fechada;
- inputs monetários negativos rejeitados;
- ausência das chamadas legadas `fetchData/calcINSS/calcIR` no H26.

O Remake CI executa permanentemente:

```text
python scripts/validate_phase6_c63_gate.py
```

## 9. Implantação

C6.3 consolida o H26 migrado no repositório. **Não afirma implantação no HostGator.**

O novo `folha-core` não deve substituir o ativo compartilhado em produção enquanto H27, H28 e H29 ainda dependerem da API legada removida em C6.2. A implantação conjunta fica condicionada à migração dos consumidores restantes.

Próximo checkpoint: **C6.4 — H27 — décimo terceiro**.
