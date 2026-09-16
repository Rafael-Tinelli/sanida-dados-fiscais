# Deployments de manutenção após o remake — health em duas etapas

Este documento rege **deployments futuros de manutenção** dos arquivos gerenciados das calculadoras H26–H29. Ele não reescreve nem invalida o registro histórico C7.4/C7.5 concluído em 16/09/2026.

## Por que existe

O fechamento C7.5 provou que bytes corretos na origem podem coexistir com bytes antigos no Cloudflare quando URLs estáveis possuem cache longo. Por isso, `HTTP 200`, REST saudável e igualdade de bytes no HostGator não são suficientes para declarar a entrega pública final saudável.

O estado histórico `APPLIED_HEALTHY` do primeiro deployment permanece preservado como evidência do que o executor C7.4 registrou naquele momento. **Para manutenção futura, porém, esse nome não é estado final saudável.**

## Fluxo obrigatório para manutenção

1. Gerar e autorizar o bundle corrente com os mesmos vínculos fail-closed de bundle, C7.3 e autorização single-use.
2. Executar `scripts/run_phase7_c74_controlled_deploy_v2.py`, informando também o commit autorizado com `--authorized-commit`.
3. Se escrita, verificação local, dependências e health de origem/REST passarem, o estado persistido deve ser:

   `APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL`

   Nesse ponto `production_deployed=true`, mas `external_delivery_verified=false` e `final_health_status=PENDING_EXTERNAL_PROOF`.
4. Executar a prova pública C7.5 contra os URLs canônicos. Se algum asset estiver stale, manter o deployment pendente, corrigir cache por versionamento de URL/cache-key ou purga seletiva exata e repetir a prova.
5. Somente uma evidência C7.5 `PASS`, vinculada ao mesmo commit autorizado, com `assets_matching == assets_expected`, hashes públicos iguais aos esperados e `block_reasons=[]`, pode ser fornecida a `scripts/finalize_phase7_c75_external_health_v2.py`.
6. Apenas então o journal pode chegar a:

   `APPLIED_EXTERNALLY_HEALTHY`

   com `external_delivery_verified=true` e `final_health_status=HEALTHY`.

## Invariantes

- C7.4 prova aplicação e saúde da **origem**; não prova entrega pública final.
- C7.5 prova os **bytes recebidos pelo cliente externo**.
- Evidência externa `BLOCKED`, incompleta, de outro commit ou com qualquer mismatch não promove o estado.
- O finalizador C7.5 altera somente o journal/evidência; não reescreve arquivos de produção.
- O histórico do deployment de 16/09/2026 continua imutável. Gates históricos devem provar os vínculos entre autorização, registro de produção e estado preservado, e não tentar reconstruir o hash daquele bundle a partir do código corrente.

## Relação com o CI

O bundle atual continua sendo validado por C7.1 e pelas simulações correntes. O gate C7.4 histórico valida a integridade do evento passado por referências cruzadas imutáveis. Assim, uma correção legítima posterior em arquivos gerenciados não torna impossível manter o CI verde e, ao mesmo tempo, não falsifica a evidência do deployment anterior.
