# Fiscal v1.2 — fontes oficiais alternativas às normas do Planalto

## Objetivo

Manter o contrato fiscal operante quando o Planalto bloqueia o acesso feito
pelo GitHub Actions, sem enfraquecer a identificação jurídica, o histórico de
evidências ou a revisão humana. O PR #125 já havia selecionado a CLT atualizada
da Câmara. Esta extensão cobre as quatro normas remanescentes que ainda eram
referenciadas pelo inventário fiscal.

## Resultados reais — GitHub Actions, 11/11/2026

Execução independente, de leitura, com o `HttpCollectorV1`, checagem de
marcadores materiais, URL de origem e SHA-256:

https://github.com/Rafael-Tinelli/sanida-dados-fiscais/actions/runs/43116447349

| Fonte Câmara | Edição | HTTP | Bytes | SHA-256 |
|---|---|---:|---:|---|
| `CAMARA_DECRETO_10854_2021` | Atualizada | 200 | 263116 | `ed345d82746b9cb3cf83aad3886966c2db97812a0d8adb5fb9c842d3ebd6e086` |
| `CAMARA_LEI_4090_1962` | Atualizada | 200 | 32007 | `4f9547e5b93ad3241055980b0929e8e69544c5a386cd5bd1bf342cf1c8da57a6` |
| `CAMARA_LEI_4749_1965` | Original | 200 | 43131 | `57e4aa663b83a004cac3dde7dc71a2370d182c9813f8e32ef412e6caddaa11a0` |
| `CAMARA_LEI_15270_2025` | Original | 200 | 78475 | `5ef299f1300f427141b90cd5ddddad2e09467fcbfea615ca5bcf2317b6d6379d` |

Hashes são evidência pontual daquela coleta e **não** valores fixos a serem
aceitos em todas as execuções futuras. A mudança material no texto deve
desencadear revisão, não ser ignorada por uma lista de hashes permitidos.

## Seleção jurídica: não trocar a norma junto com o endereço

- Regras antes selecionadas de `PLANALTO_DECRETO_10854_2021` passam ao
  `CAMARA_DECRETO_10854_2021`.
- Regras antes selecionadas de `PLANALTO_LEI_15270_2025` passam ao
  `CAMARA_LEI_15270_2025`.
- `CAMARA_LEI_4090_1962` e `CAMARA_LEI_4749_1965` são autorizadas e
  verificadas para conferência, mas **não** passam a substituir o Decreto
  10.854/2021 na escolha automática vigente.
- As entradas `PLANALTO_*` continuam no registro para rastreabilidade;
  nenhuma é selecionada no contrato fiscal atual.

As quatro URLs completas estão no `docs/source-registry-v1.json`.
As fontes da Câmara usam `source_id`, autoridade, observação, URL
e SHA-256 próprios: jamais registrar um documento da Câmara como Planalto.

## Restrições operacionais e jurídicas

1. A Lei 4.749/1965 e a Lei 15.270/2025 foram obtidas como **publicação
   original**. Isso não comprova, isoladamente, que não houve alterações
   posteriores; esse risco deve continuar visível no processo de revisão.
2. A mudança de fonte pode gerar diferenças de apresentação e de texto.
   A revisão de versões e o gate de aprovação humana permanecem ativos.
   Não converter alterações de fonte em aprovação automática.
3. O PR não altera parâmetros, fórmulas, vigências, `current.json`,
   regras de negócio ou políticas de promoção.
4. Respostas de WAF e páginas de navegação não devem ser aceitas como
   norma: a identidade legal e a integridade são obrigatórias.
