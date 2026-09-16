# Fase 7 — C7.4 — Autorização e implantação controlada

**Status:** EM ANDAMENTO  
**Autorização:** registrada para uma única implantação do candidato vinculado ao C7.3.  
**Produção:** ainda não implantada enquanto este checkpoint estiver apenas no repositório.

## 1. Objetivo

C7.4 converte o `GO` técnico de C7.3 em uma única implantação de produção controlada, auditável e recuperável. A autorização não é genérica: ela está vinculada ao SHA-256 do `bundle-manifest.json`, à evidência remota C7.3 e à release fiscal já validada.

Candidato autorizado:

- authorization id: `c74-20260916-a741aa78-843dca3e`;
- bundle manifest SHA-256: `843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1`;
- C7.3 evidence SHA-256: `e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7`;
- release: `fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e`;
- 32 arquivos gerenciados;
- 11 dependências somente leitura;
- quatro diretórios previamente planejados para eventual criação.

## 2. Fronteira de autorização

`state/phase7-c74-deployment.json` é o registro machine-readable da autorização. Ela é `single_use_authorization=true` e só vale para o candidato acima.

A implantação é bloqueada antes da primeira escrita se qualquer vínculo divergir:

- SHA do bundle;
- SHA da evidência C7.3;
- release id;
- inventário 32/11;
- estado atual dos arquivos/dependências em relação à evidência C7.3;
- conjunto de diretórios planejados;
- resultado do pré-flight fresco.

Publicação evergreen posterior no backend não muda automaticamente os bytes autorizados para deploy.

## 3. Pré-flight fresco e detecção de drift

Imediatamente antes do snapshot o executor C7.4 roda novamente `run_phase7_c73_host_preflight.py` em memória.

Além de exigir `PASS/GO`, C7.4 compara a inspeção nova com o JSON C7.3 preservado no HostGator:

- as 11 dependências precisam manter existência, tipo, ausência de symlink e SHA-256;
- cada um dos 32 destinos precisa manter o mesmo estado `exists` e, quando existente, o mesmo SHA-256;
- o journal de `planned_directory_creations` precisa ser idêntico.

Qualquer drift produz `BLOCKED_BEFORE_WRITE`.

## 4. Journal e snapshot

O diretório do journal:

- fica fora dos roots de produção;
- tem nome exatamente igual ao `authorization_id`;
- não pode existir antes da execução;
- torna a autorização efetivamente single-use no host.

Antes da primeira escrita, são preservados:

- pré-flight fresco;
- autorização;
- evidência C7.3;
- `bundle-manifest.json`;
- snapshot JSON dos 32 destinos;
- bytes exatos dos arquivos gerenciados preexistentes;
- SHA-256 das 11 dependências.

O backup é verificado antes da primeira mutação.

## 5. Ordem e atomicidade da aplicação

A aplicação usa somente os 32 records do bundle e evita glob/cópia de árvore inteira.

Ordem:

1. includes do plugin;
2. assets JavaScript;
3. `parts/` das calculadoras;
4. páginas `index.php`;
5. arquivo principal do plugin por último.

Cada arquivo:

- tem SHA-256 do bundle verificado antes da escrita;
- é escrito em arquivo temporário no mesmo filesystem;
- é promovido por `os.replace`;
- preserva o mode anterior quando já existia, ou usa `0644` quando é novo;
- tem SHA-256 verificado depois da promoção.

Somente os quatro diretórios aprovados pelo pré-flight podem ser criados e cada criação entra imediatamente no journal.

## 6. Health check pós-write

Depois que os 32 hashes no filesystem conferirem, o executor exige:

1. `/blog/wp-json/sfa/v1/fiscal-health` → HTTP 200, `status=healthy` e release autorizada;
2. `/blog/wp-json/sfa/v1/fiscal-release` → HTTP 200 e mesma release;
3. `/blog/wp-json/sfa/v1/folha` → HTTP 410;
4. H26, H27, H28 e H29 → HTTP 200 sem `Fatal error`/`Parse error`;
5. nova verificação dos 32 hashes;
6. nova verificação das 11 dependências.

Em C7.4, `degraded_last_good` não é aceito como sucesso de implantação: a primeira validação de produção exige `healthy`.

## 7. Rollback automático

Qualquer exceção depois do início da aplicação — inclusive `SIGINT`/`SIGTERM`, divergência de SHA ou health check — dispara rollback automático:

1. restaura bytes exatos dos arquivos que existiam;
2. remove somente arquivos gerenciados que eram ausentes;
3. remove apenas diretórios criados por este deploy e somente com `rmdir`;
4. preserva diretório não vazio e registra escalonamento;
5. verifica restauração dos 32 destinos e das 11 dependências.

`rm -rf`/deleção recursiva não integra a implementação.

## 8. Recuperação manual

Uma interrupção não capturável, como encerramento forçado do processo, pode deixar o journal em estado intermediário. Nesse caso usar `scripts/run_phase7_c74_rollback.py` apontando para o mesmo journal. O rollback não depende da memória do processo original; ele usa `snapshot.json`, backup e `created_directories` já persistidos.

O journal deve ser preservado mesmo após rollback.

## 9. Evidência de fechamento

C7.4 só muda para `CONCLUÍDO` depois de uma execução real com:

- `status=APPLIED_HEALTHY`;
- `production_deployed=true`;
- `rollback_performed=false`;
- release id autorizada;
- health REST íntegro;
- `/folha` 410;
- H26–H29 HTTP 200;
- 32/32 hashes do bundle no filesystem;
- 11/11 dependências inalteradas;
- journal preservado fora de produção;
- SHA-256 do `deployment-state.json` registrado no repositório.

Se a execução terminar em `ROLLED_BACK`, produção volta ao estado anterior e uma nova tentativa exige nova autorização explícita; o authorization id não deve ser reutilizado.

## 10. Artefatos

- `state/phase7-c74-deployment.json`;
- `scripts/run_phase7_c74_controlled_deploy.py`;
- `scripts/run_phase7_c74_rollback.py`;
- `scripts/simulate_phase7_c74_controlled_deployment.py`;
- `scripts/validate_phase7_c74_gate.py`;
- `tests/test_phase7_c74.py`;
- este runbook;
- gate permanente no `Remake CI`.

## 11. Próxima ação

Depois que este conjunto passar pelo `Remake CI` e for incorporado ao `main`, executar no HostGator a única implantação autorizada. Não substituir o executor por `cp`, extração de ZIP ou criação manual dos diretórios.
