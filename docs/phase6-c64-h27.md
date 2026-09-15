# Fase 6 — C6.4 — H27 — décimo terceiro sobre o contrato fiscal v1.2

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** migração da calculadora H27 (`/financas/calculadoras/decimo-terceiro/`) para a release fiscal v1.2 PUBLISHED e para o runtime compartilhado fechado em C6.2/C6.3.

## 1. Baseline real migrado

O JavaScript efetivamente publicado antes desta etapa calculava o 13º com lógica local equivalente a:

```text
base = salário + variáveis
total13 = base * (avos / 12)
primeira = total13 * 0.5
segunda = total13 - primeira
```

Depois chamava a API fiscal legada do `folha-core`. Esse desenho tinha dois problemas materiais: a 1ª parcela era tratada como metade universal do 13º estimado e INSS/IRRF do 13º não carregavam a memória estrutural própria de apurações separadas.

C6.4 passa a versionar:

```text
consumers/frontend/folha-thirteenth.js
consumers/frontend/decimo-terceiro.js
consumers/frontend/decimo-terceiro-clt/index.php
consumers/frontend/decimo-terceiro-clt/parts/*
```

A página foi mantida em partes PHP para preservar o conteúdo editorial/SEO existente sem misturá-lo ao runtime fiscal.

## 2. Avos e regra dos 15 dias

O H27 não contém `15` como regra jurídica local. O runtime executa `thirteenth.accrual.twelfths`, cujo payload define um avo por mês quando os dias de serviço atingem o limiar declarado, limitado ao máximo contratual.

Regressões permanentes:

- 14 dias de serviço no mês → `0/12`;
- 15 dias de serviço no mês → `1/12`;
- ano completo → máximo de `12/12`.

O usuário pode informar avos manualmente ou pedir contagem por datas. O modo por datas recorta o intervalo ao ano de referência e conta dias civis inclusivamente em cada mês.

## 3. Remuneração de referência e variáveis

Para o 13º anual, `thirteenth.reference_remuneration` determina a remuneração devida em dezembro como referência fixa.

`thirteenth.variable_remuneration` não autoriza o navegador a inventar uma média. A calculadora recebe a média de variáveis como entrada externa já apurada e a identifica dessa forma. Essa fronteira evita criar uma segunda interpretação normativa no frontend.

## 4. A 1ª parcela não é `total13 * 0.5`

`thirteenth.advance` define, no ramo padrão suportado:

- referência no **salário do mês anterior ao adiantamento**;
- fração `1/2`;
- janela de pagamento de fevereiro a novembro.

Caso de referência fechado:

```text
salário do mês anterior = 4000.00
adiantamento padrão     = 2000.00
```

Isso não equivale a assumir metade do 13º anual estimado em todos os cenários.

A própria regra declara tratamento especial para:

- admissão no ano; e
- remuneração variável.

Nesses cenários, o H27 não inventa o adiantamento: retorna `UNSUPPORTED` para o cálculo automático e exige que o usuário informe a 1ª parcela efetivamente paga para estimar a quitação final.

## 5. Arredondamento monetário sem contaminar a regra jurídica

`thirteenth.accrual.twelfths` e `thirteenth.advance` continuam descrevendo a semântica jurídico-fiscal e não recebem uma política técnica artificial de arredondamento.

Quando essas regras produzem componentes monetários que não possuem política específica, o H27 seleciona separadamente `technical.money_decimal_and_rounding`, no contexto `technical`, e aplica sua política `2 casas + ROUND_HALF_UP + per_component`. A identidade dessa regra técnica também entra no audit trail. Se a política técnica estiver ausente ou incompatível, o consumidor falha fechado.

## 6. INSS e IRRF do 13º são avaliações próprias

O fluxo fiscal agora é:

```text
13º bruto
  -> thirteenth.inss.separate_assessment
  -> inss.employee.progressive_table no contexto thirteenth
  -> thirteenth.irrf.exclusive_assessment
  -> tabela progressiva declarada como dependência
  -> deduções vinculadas à própria apuração
  -> thirteenth.irrf.reduction.2026
```

A tabela progressiva do IRRF é acessada somente porque `thirteenth.irrf.exclusive_assessment` a declara como dependência. O consumidor não amplia silenciosamente a elegibilidade direta da regra.

Pensão e dependentes pertencem à memória da apuração exclusiva do 13º; não são herdados de uma folha mensal fictícia.

## 7. Caso E2E canônico

Com a release PUBLISHED atual, em `2026-12-20`, remuneração de dezembro de `4000.00`, 12/12, sem variáveis, sem dependentes, sem pensão, e adiantamento padrão em `2026-11-30` sobre salário do mês anterior de `4000.00`:

```text
13º total              = 4000.00
1ª parcela             = 2000.00
INSS do 13º            = 368.60
IRRF final do 13º      = 0.00
2ª parcela bruta       = 2000.00
2ª parcela líquida     = 1631.40
```

Esse vetor é executado em JavaScript sobre a release real e comparado com o engine Python.

## 8. Auditabilidade e fail-closed

O resultado mantém:

- `release_id`;
- `rule_id`;
- `rule_version`;
- data fiscal usada;
- avos;
- origem/status do adiantamento;
- política técnica de arredondamento efetivamente usada;
- INSS separado;
- base do IRRF;
- IRRF antes do redutor;
- rendimento usado pelo redutor;
- redução aplicada;
- IRRF final.

O runtime rejeita, entre outros:

- dinheiro negativo;
- datas civis inválidas;
- avos fora do limite da regra;
- ausência do salário do mês anterior quando se pede cálculo automático padrão do adiantamento;
- data de adiantamento fora da janela contratual;
- política técnica de arredondamento ausente ou incompatível;
- regra não vigente ou não VALIDATED;
- dependência fiscal não declarada;
- cenário especial tratado como se fosse o ramo padrão.

## 9. Testes e gate permanente

C6.4 adiciona:

```text
tests/js/phase6_c64_h27_runtime.cjs
tests/test_h27_c64.py
tests/test_phase6_c64_gate.py
scripts/validate_phase6_c64_gate.py
```

O gate exige a release PUBLISHED 32/32, executa o H27 real em Node, compara os valores centrais com o engine Python, verifica as fronteiras de 14/15 dias, casos especiais de adiantamento, política técnica de arredondamento, audit trail, ausência da lógica fiscal legada e integridade da página versionada.

## 10. Fronteira

C6.4 não migra férias (H28) nem rescisão (H29), não remove ainda os adaptadores legados restantes e **não afirma implantação no HostGator**.

A substituição dos assets compartilhados em produção permanece bloqueada até que H28 e H29 sejam migrados e C6.7 autorize a remoção controlada do legado.

Próximo checkpoint: **C6.5 — H28 — férias CLT**.
