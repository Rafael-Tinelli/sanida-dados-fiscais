# Fase 4 — Gate formal de fechamento

**Status:** APROVADO CONDICIONALMENTE AO CI DO HEAD FINAL  
**Data:** 13/09/2026  
**Branch:** `refactor/sources-sensors-v1`

## Decisão

A Fase 4 pode ser encerrada quando o `Remake CI` estiver verde no head final contendo este gate.

A revisão final da fronteira registrou A01–A05 como `CORRECTED` em `docs/phase4-final-boundary-audit-v1.json`.

## A05 — decisão final

Política canônica:

```text
preserve_auditable_block_new_consumption
```

Quando uma coleta financeira corrente obtém bytes, mas o parser registrado retorna `PARSER_INCOMPATIBLE`:

1. o novo raw snapshot e o estado de falha são preservados;
2. os ponteiros do último snapshot/candidato `PARSED` continuam preservados para auditoria;
3. os bytes do `taxas_bacen.json` previamente validado não são reescritos;
4. o last-good preservado pode ser verificado apenas pelo caminho de auditoria explícito;
5. a verificação normal de evidência para **novo consumo** falha fechada;
6. nenhum novo `taxas_bacen.json` pode ser formado a partir do candidato anterior;
7. nenhum novo `dados_fiscais.json` pode incorporar esse last-good financeiro em quarentena;
8. `generated_at_utc` não pode ser renovado e o valor anterior não pode ser relabelado como corrente;
9. o consumo normal só é retomado depois que uma coleta corrente volta a produzir `PARSED` com evidência persistida válida.

Essa decisão separa duas propriedades que não podem ser confundidas:

- **preservação/auditabilidade do last-good**;
- **autorização para consumo em um artefato novo**.

`PARSER_INCOMPATIBLE` mantém a primeira e bloqueia a segunda.

## Prova executável

A implementação vive em `sanida_fiscal/financial_evidence_v1.py`.

O teste `test_A05_parser_incompatible_preserves_last_good_but_blocks_new_consumption` comprova que:

- a falha corrente não destrói hash/caminho do candidato last-good;
- o verificador audit-only continua resolvendo os bytes preservados;
- `verify_financial_artifact_evidence()` rejeita o mesmo artefato enquanto o estado corrente for `PARSER_INCOMPATIBLE`.

`scripts/validate_phase4_closure_gate.py` cruza a decisão documental, a implementação, o teste, a política de persistência e a presença dos gates no CI.

## Critérios de fechamento

Todos devem estar verdadeiros no mesmo head:

- A01–A05 `CORRECTED`;
- fundação da Fase 4 verde;
- fronteira de produção verde;
- teste A05 verde;
- gate formal de closure verde;
- suíte integral verde;
- Fase 3 permanece verde;
- nenhuma migração de consumidores da Fase 6 foi antecipada;
- nenhum semantic diff/promotion da Fase 5 foi antecipado.

## Fronteira pós-fechamento

O merge da Fase 4 ativa a infraestrutura de fontes/sensores, não a migração das calculadoras.

A ordem operacional inicial em `main` deve ser:

1. `taxas.yml` produzir o primeiro `taxas_bacen.json` 1.4.0 com evidência durável;
2. somente depois `main.yml` pode produzir novo `dados_fiscais.json` com a cadeia RFB + INSS + Selic + CDI comprovada;
3. semantic diff e política de promoção pertencem à Fase 5;
4. WordPress/plugin, `folha-core` e H26–H29 pertencem à Fase 6.

O fechamento da Fase 4 não autoriza relabelagem de artefatos legados nem consumo de estados incompatíveis.
