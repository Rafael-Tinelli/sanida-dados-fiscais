# Fase 6 — C6.7 — remoção controlada do legado e fechamento formal

**Data:** 2026-09-16  
**Status:** CONCLUÍDO  
**Escopo:** retirar a autoridade fiscal legada restante dos consumidores, provar a cadeia única de H26–H29 e encerrar formalmente a Fase 6 sem afirmar implantação no HostGator.

## 1. Estado recebido

C6.7 começa somente depois dos gates permanentes C6.1–C6.6. H26, H27, H28 e H29 já executam sobre uma release fiscal v1.2 `PUBLISHED`, 32/32 e auditável. A release canônica é sempre a apontada por `releases/fiscal-v1/current.json`; este checkpoint não fixa uma release histórica como autoridade futura.

A cadeia fiscal final da Fase 6 é:

```text
releases/fiscal-v1/current.json
        ↓
release imutável v1.2 + SHA-256
        ↓
WordPress/cache + /wp-json/sfa/v1/fiscal-release
        ↓
folha-core
        ↓
H26 / H27 / H28 / H29
```

## 2. REST legado encerrado

`/wp-json/sfa/v1/folha` deixa de transportar tabelas ou parâmetros fiscais. A rota permanece apenas como tombstone HTTP `410 Gone`, com código `sfa_legacy_folha_retired` e indicação de `/wp-json/sfa/v1/fiscal-release`.

O endpoint canônico continua entregando somente a release fiscal íntegra já validada pelo plugin.

## 3. Bootstrap e calculadoras antigas do plugin

O shortcode `sfa_bootstrap_folha` não publica mais shape fiscal legado. Ele produz apenas um marcador de aposentadoria, sem números, tabelas ou regras.

Os shortcodes antigos de cálculo também deixam de ser autoridades computacionais:

- `calc_salario_liquido` não calcula mais INSS/IRRF; apenas encaminha para `/financas/calculadoras/salario-liquido-clt/` (H26);
- `sanida_calculadora_13` e `sfa_calc13_assets` não injetam mais cálculo de 13º; apenas encaminham para `/financas/calculadoras/decimo-terceiro/` (H27).

Assim, fórmulas duplicadas como INSS/IRRF local, redutor local e `total13 / 2` deixam de existir no plugin.

## 4. Único adaptador temporário preservado

Permanece temporariamente `wordpress_table_shortcodes_v1`, exclusivamente para apresentação dos shortcodes:

```text
ano_ref
inss_tabela
irrf_tabela
```

Ele lê a release canônica já validada e apenas projeta ano/faixas para HTML. Não atende calculadoras, não alimenta REST fiscal, não expõe redutor, dependentes ou regras de cálculo e não é uma autoridade alternativa.

Prazo de retirada declarado:

```text
C7.1-before-production-deployment
```

Esse adaptador deve ser removido ou substituído por apresentação direta da release antes da implantação do bundle da Fase 7.

## 5. `dados_fiscais.json` não é apagado do produtor

C6.7 não remove `dados_fiscais.json` do repositório porque ele ainda pertence à fronteira histórica/compatível de produção e evidência das Fases anteriores. A remoção controlada aqui é de **autoridade de consumo fiscal**.

O gate prova que H26–H29, `folha-core` e suas extensões não consomem:

- `dados_fiscais.json`;
- `raw.githubusercontent.com/.../main/dados_fiscais.json`;
- `/sfa/v1/folha`;
- constantes fiscais antigas como substituto do contrato.

## 6. Invariantes preservados

C6.7 mantém:

- release `PUBLISHED` e 32/32;
- compatibilidade exata schema/API 1.2.0;
- verificação de `release_id` e SHA-256 no WordPress;
- last-good apenas sob a política explicitamente autorizada;
- falha fechada quando release/regra/vigência não é consumível;
- `release_id`, `rule_id` e `rule_version` nas memórias H26–H29;
- regressões A01/A02 e os limites específicos de H27/H28/H29;
- separação de `payroll_fiscal` e `financial_reference`.

## 7. Critérios de saída da Fase 6

No mesmo head de fechamento devem estar provados:

1. release fiscal v1.2 PUBLISHED 32/32;
2. manifest + artefato imutável e íntegro;
3. WordPress consumindo a release e não `dados_fiscais.json` como autoridade fiscal;
4. `folha-core` sem fallback/tabelas fiscais hardcoded;
5. H26–H29 sobre a mesma release canônica;
6. A01/A02 e regressões H27/H28/H29 verdes;
7. falha de release/cache fail-closed;
8. audit trail de release/regras;
9. C6.1–C6.7 permanentes no Remake CI;
10. README e documentação sincronizados.

## 8. Gate formal

`scripts/validate_phase6_c67_gate.py` verifica a release atual, integridade do artefato, ausência de autoridades/fórmulas legadas nos consumidores ativos, aposentadoria do REST/bootstrap/calculadoras antigas do plugin, delimitação do único adaptador temporário e presença de todos os gates da Fase 6 no CI.

`tests/test_phase6_c67_gate.py` torna esse fechamento parte da suíte normal.

## 9. Fronteira operacional

C6.7 **não afirma implantação no HostGator**. O repositório fecha a migração e fica pronto para a Fase 7, que deverá realizar fechamento ponta a ponta, operação evergreen e implantação controlada.

Antes da implantação de produção na Fase 7, o adaptador `wordpress_table_shortcodes_v1` deve cumprir o prazo `C7.1-before-production-deployment`.

## 10. Resultado

Com C6.7 verde, a **Fase 6 — Migração dos consumidores** está formalmente **CONCLUÍDA**.

Próxima etapa: **Fase 7 — fechamento E2E e operação evergreen**.
