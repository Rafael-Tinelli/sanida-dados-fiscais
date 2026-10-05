# Deployments de manutenção após o remake — registro histórico e ownership atual

> **Status operacional: SUPERADO PARA NOVOS DEPLOYS.** Este documento preserva o desenho e as garantias do fluxo fiscal-side utilizado após C7.5. A partir da extração definitiva do frontend, nenhum workflow ativo de `sanida-dados-fiscais` publica H26–H29 no HostGator. O recipe correspondente permanece arquivado em `ops/workflows/fiscal-runtime-production.yml` para auditoria, testes isolados e reconstrução histórica.

## Ownership vigente

- `sanida-dados-fiscais`: regras, releases, motores fiscais, evidências oficiais, testes semânticos e artefatos imutáveis.
- `sanida-financas-frontend`: integração dos motores aprovados, adapters/templates, bundle, preflight, backup/rollback, implantação e prova pública/CDN.
- uma alteração de motor fiscal somente se torna implantável depois de ser pinada pelo commit/release exato no cutover do frontend e passar os testes cruzados.
- merge na `main` fiscal não é autorização operacional de escrita no HostGator.

## Garantias históricas preservadas

O fechamento C7.5 demonstrou que saúde da origem e entrega pública são fronteiras distintas, especialmente com Cloudflare. Permanecem válidos como requisitos do cutover atual: igualdade byte a byte, política de cache compatível, backup/rollback, health fiscal, H26–H29 sem erro e prova externa antes de declarar `APPLIED_EXTERNALLY_HEALTHY`.

O script `scripts/deploy_fiscal_runtime_assets.py`, a política `ops/fiscal-runtime-assets.htaccess` e o workflow arquivado documentam o mecanismo anterior. Eles não podem ser reativados em `.github/workflows/` deste repositório sem reabrir formalmente a decisão de arquitetura e os gates de extração do frontend.

## Fluxo obrigatório para manutenção


1. No `sanida-financas-frontend`, reconstruir o bundle corrente pinando o commit/release fiscal aprovado e manter os mesmos vínculos fail-closed de bundle, preflight e autorização.
2. Executar o cutover controlado pelo fluxo de frontend; os scripts históricos deste repositório podem ser usados somente para auditoria/reprodução isolada, não como caminho ativo de produção.
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

O `Remake CI` fiscal valida regras, motores, evidências e contratos; não publica o frontend. O CI do `sanida-financas-frontend` valida a integração e o bundle de implantação. Os gates C7.4/C7.5 históricos continuam servindo como evidência imutável do evento passado e não devem ser reinterpretados como autorização automática para novos deploys.