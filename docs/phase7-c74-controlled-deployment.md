# Fase 7 — C7.4 — Autorização e implantação controlada

**Status:** CONCLUÍDO  
**Autorização:** single-use consumida com sucesso.  
**Produção:** implantada e validada em `2026-09-16T17:21:52.270428+00:00`.

## 1. Resultado

C7.4 converteu o `GO` técnico de C7.3 em uma única implantação de produção controlada, auditável e recuperável.

Resultado final observado no HostGator:

- authorization id: `c74-20260916-a741aa78-843dca3e`;
- commit autorizado: `ccc5a31c3da7c1c93570df0337e553e5a06404ac`;
- status do executor: `APPLIED_HEALTHY`;
- `production_deployed=true`;
- `post_deploy_validated=true`;
- `rollback_performed=false`;
- 32/32 arquivos gerenciados aplicados;
- quatro diretórios planejados criados;
- bundle manifest SHA-256: `843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1`;
- evidência C7.3 SHA-256: `e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7`;
- `deployment-state.json` SHA-256: `9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c`.

A release efetivamente servida após o deploy é:

`fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e`.

## 2. Trilha de autorização

O registro pré-deploy original foi preservado em `docs/phase7-c74-authorization-record.json`.

Ele comprova que a autorização:

- era `single_use_authorization=true`;
- estava vinculada ao bundle C7.1 reconstruído deterministicamente;
- estava vinculada à evidência remota C7.3 validada;
- autorizava exatamente a release acima;
- exigia pré-flight fresco, snapshot exato e rollback automático;
- ainda declarava `production_deployed=false` antes da execução.

O estado corrente em `state/phase7-c74-deployment.json` marca a autorização como consumida e C7.4 como `CONCLUÍDO`.

## 3. Pré-flight fresco e ausência de drift

Antes da primeira escrita, o executor reconstruiu o bundle autorizado, verificou seu SHA-256, executou PHP lint no ambiente real, validou o registro de autorização e refez o pré-flight.

A execução só alcançou a fase de aplicação porque o estado remoto continuava compatível com a evidência C7.3. Qualquer drift em dependências, destinos gerenciados, diretórios planejados, release ou hashes teria produzido `BLOCKED_BEFORE_WRITE`.

## 4. Snapshot e journal

O journal single-use foi criado fora dos roots de produção em:

`~/c74-deployments/c74-20260916-a741aa78-843dca3e/`

Antes da aplicação foram preservados o pré-flight fresco, autorização, evidência C7.3, `bundle-manifest.json`, snapshot dos 32 destinos, backups dos arquivos existentes e hashes das 11 dependências.

O estado final do journal é preservado pelo SHA-256:

`9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c`.

O registro resumido e versionado dessa evidência está em `docs/phase7-c74-production-deployment-record.json`.

## 5. Aplicação

O executor aplicou somente os 32 records autorizados, em ordem controlada:

1. includes do plugin;
2. assets JavaScript;
3. `parts/` das calculadoras;
4. páginas `index.php`;
5. arquivo principal do plugin por último.

Cada arquivo foi verificado contra o SHA do bundle antes e depois da escrita atômica. Foram criados exatamente os quatro diretórios planejados pelo pré-flight.

Nenhum rollback foi necessário.

## 6. Health pós-deploy

O health imediato de produção fechou integralmente:

- `/blog/wp-json/sfa/v1/fiscal-health` → HTTP 200, `status=healthy`, release autorizada;
- `/blog/wp-json/sfa/v1/fiscal-release` → HTTP 200, mesma release;
- `/blog/wp-json/sfa/v1/folha` → HTTP 410;
- H26 salário líquido → HTTP 200;
- H27 décimo terceiro → HTTP 200;
- H28 férias CLT → HTTP 200;
- H29 rescisão CLT → HTTP 200.

Tamanhos observados na resposta do primeiro health:

- H26: 74.789 bytes;
- H27: 95.137 bytes;
- H28: 13.668 bytes;
- H29: 14.448 bytes.

O executor terminou com `error=null` e exit code `0`.

## 7. Rollback boundary preservada

Embora não utilizada, a fronteira de rollback continua válida e versionada:

- restaura bytes exatos dos arquivos preexistentes;
- remove somente arquivos gerenciados que eram ausentes;
- usa `rmdir` somente para diretórios criados pelo deploy e vazios;
- preserva conteúdo não gerenciado;
- proíbe deleção recursiva;
- possui recuperação manual por `scripts/run_phase7_c74_rollback.py`.

A autorização consumida não pode ser reutilizada para outro deploy.

## 8. Evidência versionada

Artefatos de fechamento:

- `docs/phase7-c74-authorization-record.json` — autorização original pré-deploy;
- `docs/phase7-c74-production-deployment-record.json` — resultado observado em produção;
- `state/phase7-c74-deployment.json` — estado corrente concluído;
- `scripts/run_phase7_c74_controlled_deploy.py` — executor;
- `scripts/run_phase7_c74_rollback.py` — rollback manual;
- `scripts/simulate_phase7_c74_controlled_deployment.py` — provas de apply e rollback;
- `scripts/validate_phase7_c74_gate.py` — gate permanente de fechamento;
- `tests/test_phase7_c74.py` — regressões C7.4.

A evidência completa de host permanece no journal fora de produção; o repositório não inventa campos não copiados do host e registra o SHA imutável do estado final.

## 9. Critério de fechamento

Todos os critérios definidos antes do deploy foram satisfeitos:

- `status=APPLIED_HEALTHY`;
- `production_deployed=true`;
- `rollback_performed=false`;
- release autorizada servida;
- health REST íntegro;
- legado `/folha` em 410;
- H26–H29 em HTTP 200;
- 32 arquivos aplicados;
- quatro diretórios planejados criados;
- journal preservado fora de produção;
- SHA-256 do `deployment-state.json` registrado no repositório.

**C7.4 está CONCLUÍDO.**

## 10. Próxima fronteira

A próxima etapa é o fechamento pós-implantação da Fase 7: verificar estabilidade após a janela inicial, consolidar evidência final do remake e declarar o encerramento formal sem reimplantar o bundle C7.4.
