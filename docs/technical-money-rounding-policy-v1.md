# Política técnica de valores monetários e arredondamento — v1

## Escopo

Este documento é evidência de governança interna para a regra `technical.money_decimal_and_rounding`. Ele não constitui autoridade jurídico-fiscal externa e não altera regras legais de incidência, base, elegibilidade ou competência.

## Política canônica

Quando uma fórmula monetária do consumidor precisa quantizar um componente em BRL e a regra jurídico-fiscal específica não declara uma política de arredondamento mais específica, o contrato técnico deve fornecer explicitamente a política abaixo:

```text
decimal_places = 2
mode           = ROUND_HALF_UP
stage          = per_component
```

A política é um fallback **técnico explícito do contrato**, não um fallback local do consumidor.

Regras de precedência:

1. uma `rounding_policy` declarada pela regra jurídico-fiscal específica prevalece para a etapa que ela governa;
2. na ausência de política específica, o consumidor pode usar a `rounding_policy` da regra `technical.money_decimal_and_rounding` somente para quantização monetária de componente;
3. se nenhuma política aplicável estiver presente na release PUBLISHED, o consumidor deve falhar fechado;
4. consumidores não podem recriar silenciosamente `2 casas / ROUND_HALF_UP` em JavaScript, PHP ou outro runtime;
5. a política técnica não pode ser apresentada como se derivasse de fonte legal ou fiscal externa.

## Aplicação ao 13º salário

`thirteenth.accrual.twelfths` define a fração legal de avos e não deve receber proveniência técnica interna. Para calcular o valor bruto proporcional do 13º, o runtime usa:

- numerador, denominador e avos de `thirteenth.accrual.twelfths`;
- remuneração de referência das regras próprias do 13º;
- política monetária da regra técnica `technical.money_decimal_and_rounding`, quando a regra estrutural de avos não declarar política monetária própria.

`thirteenth.advance` continua usando sua própria `rounding_policy` declarada no contrato; ela não é substituída pela política técnica geral.

## Governança

Qualquer alteração de `decimal_places`, `mode`, `stage` ou da precedência acima é mudança estrutural da regra técnica e exige o fluxo de revisão humana aplicável às regras estruturais do Contrato Fiscal.
