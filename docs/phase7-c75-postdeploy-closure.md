# Fase 7 — C7.5 — Validação pós-deploy e fechamento formal

**Status:** CONCLUÍDO  
**Fase 7:** CONCLUÍDA  
**Remake:** CONCLUÍDO  
**Produção:** implantada por C7.4; C7.5 não reimplantou o bundle.  
**Modo de C7.5:** validação read-only da origem + validação externa da entrega pública, com remediação seletiva apenas do cache de borda.

## 1. Objetivo e resultado

C7.5 encerrou a Fase 7 ao provar que o estado implantado por C7.4 permaneceu íntegro depois de uma janela separada da implantação e que os consumidores públicos receberam os mesmos bytes do bundle autorizado.

C7.4 permaneceu `APPLIED_HEALTHY`, com `production_deployed=true`, `rollback_performed=false` e SHA-256 do `deployment-state.json`:

`9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c`

A release observada permaneceu:

`fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e`

## 2. Janela pós-deploy e integridade da origem

A primeira evidência HostGator foi coletada mais de 1.900 segundos após o deploy, acima da janela mínima de 600 segundos.

Ela provou:

- journal C7.4 com SHA imutável;
- bundle manifest idêntico ao candidato autorizado;
- evidência C7.3 idêntica ao baseline registrado;
- 32/32 arquivos gerenciados iguais ao bundle C7.4;
- 11/11 dependências pré-existentes iguais ao baseline C7.3;
- quatro diretórios criados por C7.4 ainda presentes;
- nenhum temporário `.c74-*` restante;
- `current.json` e artefato fiscal canônico íntegros;
- duas rodadas independentes de health/release/folha/H26–H29;
- H26–H29 HTTP 200;
- `/sfa/v1/folha` HTTP 410;
- `production_mutated=false`.

A evidência HostGator original permanece em:

`~/c75-evidence/c75-hostgator-postdeploy-20260916T175405Z.json`

SHA-256:

`996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447`

Ela foi formalmente revalidada pelo commit `60d196271d6992bb391a485bea755eb541aec3e1` como `host_origin_validation=PASS`.

## 3. Primeiro BLOCKED e separação de fronteiras

A primeira execução real de C7.5 fechou em `BLOCKED` porque apenas 5/8 JavaScript observados via HostGator → hostname público batiam com o bundle.

Os únicos bloqueios eram:

- `folha-core.js`;
- `ferias-clt.js`;
- `rescisao-clt.js`.

O HostGator recebia HTTP 403 do Cloudflare ao acessar esses caminhos públicos, então esse caminho de rede não era uma sonda válida de cliente externo. O resultado foi preservado, não reclassificado retroativamente.

O registro histórico está em `docs/phase7-c75-initial-host-observation.json`.

## 4. Primeira sonda externa e cache obsoleto

Uma sonda PowerShell executada de um cliente externo comparou os oito JS públicos com os arquivos do commit C7.4 autorizado:

`ccc5a31c3da7c1c93570df0337e553e5a06404ac`

A primeira sonda externa também fechou em `BLOCKED`, com 5/8 assets corretos.

SHA-256 dessa evidência:

`908551d36f340d97fbaa1277768fd1330651c1a555f7312c286003300843fd70`

Os mesmos três assets divergiam. O diagnóstico de headers provou que as URLs canônicas estavam sendo entregues como objetos Cloudflare `HIT` antigos, com:

`Cache-Control: max-age=31536000`

Em contraste, URLs com cache-key nova retornaram `CF-Cache-Status: MISS`, `last-modified=Wed, 16 Sep 2026 17:21:48 GMT` e os SHA-256 exatos do bundle C7.4.

Portanto, a causa não era drift no HostGator nem erro de deploy: era cache público obsoleto sobre URLs estáveis.

## 5. Remediação

Foi executada purga seletiva apenas destes três URLs públicos:

- `/financas/calculadoras/assets/folha-core.js`;
- `/financas/calculadoras/assets/ferias-clt.js`;
- `/financas/calculadoras/assets/rescisao-clt.js`.

Não houve:

- reexecução de C7.4;
- nova escrita dos arquivos em `public_html`;
- alteração das 11 dependências;
- alteração da release fiscal;
- rollback;
- `Purge Everything`.

A remediação foi restrita à camada de cache de borda.

## 6. Evidência externa final

Depois da purga seletiva, a mesma sonda externa fechou em:

- `status=PASS`;
- `assets=8/8`;
- todos os oito URLs públicos HTTP 200;
- SHA público igual ao SHA do arquivo correspondente no commit C7.4 autorizado;
- `block_reasons=[]`;
- `production_mutated=false`;
- exit code 0.

Observação final:

`2026-09-16T18:27:35.0648819Z`

SHA-256 da evidência externa final:

`e162b1f7d60427bc9fd2679bdce683adac566ecdfcd349637dfef883cb981284`

O registro consolidado está em:

`docs/phase7-c75-final-production-validation-record.json`

## 7. Invariante operacional de cache

C7.5 revelou uma fronteira operacional que passa a ser permanente:

> Um asset estático público com URL estável e cache de longa duração não pode ter seus bytes substituídos em produção sem invalidação explícita do cache ou versionamento da URL/cache-key.

Para qualquer deploy futuro que altere bytes de assets públicos gerenciados:

1. versionar a URL/cache-key **ou** purgar exatamente os URLs afetados;
2. não usar purga global por padrão;
3. executar prova externa de bytes depois da invalidação;
4. só declarar o deploy saudável quando os SHA-256 públicos coincidirem com o artefato implantado.

Esse requisito decorre diretamente do caso observado em C7.5, no qual a origem estava correta enquanto a borda servia bytes antigos por `max-age=31536000`.

## 8. Não-mutação da origem

C7.5 não reimplantou nem reescreveu produção. O estado final mantém:

- `production_deployed=true`;
- `production_files_mutated_by_c75=false`;
- `edge_cache_remediation_performed=true`;
- `c74_redeployment_performed=false`.

A distinção é deliberada: houve remediação operacional do cache público, não mutação dos arquivos implantados.

## 9. Critério formal de fechamento atendido

A Fase 7 foi promovida a `CONCLUÍDA` porque:

- C7.1–C7.4 permanecem concluídos;
- a evidência HostGator existente passou no validador de origem;
- a evidência externa final provou os oito JavaScript públicos;
- os SHA-256 das evidências ficaram registrados;
- `production_deployed=true` permanece verdadeiro;
- `production_files_mutated_by_c75=false`;
- a causa de cache obsoleto foi diagnosticada e remediada de forma seletiva;
- o invariante para futuros deploys foi incorporado à governança;
- nenhum bloqueio técnico do remake permanece aberto;
- o Remake CI deve permanecer verde no mesmo head de fechamento.

Depois desse fechamento, mudanças futuras são manutenção evergreen ou nova evolução de produto. O remake não deve ser reaberto sem evidência objetiva de novo defeito ou mudança de escopo.

## 10. Artefatos

- `state/phase7-c75-closure.json`;
- `docs/phase7-c75-initial-host-observation.json`;
- `docs/phase7-c75-final-production-validation-record.json`;
- `scripts/run_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_remote_evidence.py`;
- `scripts/run_phase7_c75_external_delivery_probe.ps1`;
- `scripts/validate_phase7_c75_external_delivery_evidence.py`;
- `scripts/simulate_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_gate.py`;
- `tests/test_phase7_c75.py`;
- este documento.
