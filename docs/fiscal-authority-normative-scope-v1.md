# Fiscal v1.2 — norma aplicável, não documento inteiro (controle N114)

## Delimitação fechada

A autoridade oficial `PLANALTO_CLT` é arquivada **integralmente**, com SHA-256 bruto, proveniência e data de coleta. Para a decisão específica de **dispensar uma nova publicação** por ausência de delta material, o contrato atual utiliza a seção contínua **arts. 129–147** da CLT, conforme `docs/source-registry-v1.json`. Ela é delimitada pelos marcadores oficiais únicos `art129` (inclusivo) e `art148` (exclusivo), preservando os parágrafos, subdivisões, notas, links e texto. Alterações em qualquer parte do trecho resultam em revisão fiscal, não em dispensa.

A correção não modifica a lei, os parâmetros do contrato, as tabelas anuais nem qualquer fórmula das calculadoras H26–H29. O trecho é selecionado *somente* para `PLANALTO_CLT` e *somente* no gate de equivalência da atualização documental. Os demais documentos continuam sendo comparados pelas políticas vigentes.

## Motivação (Issue #114, 09/10/2026)

Entre os snapshots `9f9e5356…` e `d14abfc4…`, foram acrescentadas quatro notas `Vide ADC 80` nos arts. 790 e 790-B, relativos a justiça gratuita processual, **fora** da cobertura registrada das calculadoras. Os dispositivos 129–147 permaneceram equivalentes. Esse é um delta jurídico real no documento completo, mas não altera os dispositivos atualmente representados pelas regras H26–H29.

Nunca classificar genericamente essas notas como alterações cosméticas: a saída operacional distingue `out_of_scope_legal_sources` de `source presentation noise`, com os hashes brutos dos dois documentos, a identidade do trecho verificado e o recorte utilizado no estado `state/fiscal-release-v12-last-attempt.json`. Esse relatório mantém trilha sem criar nova release quando o contrato fiscal não sofreu alteração pertinente.

## Condições cumulativas do gate de dispensa

- Evidência imutável anterior e atual devem existir e passar SHA-256.
- Mesmo conjunto exato de 32 regras; sem alteração de estrutura de contrato, parâmetros, vigência ou metadados de parser.
- Diferenças por regra limitadas à proveniência, data de validação e incremento de versão por `SOURCE_REFRESH_NO_CHANGE`.
- Marcadores oficiais únicos, ordenados e com limites válidos; se ocorrer renumeração, ausência ou alteração estrutural, **falhar fechado**.
- Texto visível, anotações e destinos de links *dentro* dos arts. 129–147 devem ser idênticos. Divergências obrigam revisão.
- A reclassificação **não autoriza** `/approve`, não altera `current.json` nem gera release. Para alterações em dispositivos fora do escopo, manter evidência integral arquivada e decisão explicitada no relatório.

## Mudança futura do escopo

Se outra regra passar a utilizar dispositivos da CLT fora dos arts. 129–147 — inclusive art. 148 ou tópicos processuais —, atualizar explicitamente o registro de fontes, o extrator, os testes e a fundamentação jurídica por PR próprio. Na dúvida, preservar revisão humana. A política aqui não define nem antecipa parâmetros de 2027.
