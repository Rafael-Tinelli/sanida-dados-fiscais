# Fase 7 — C7.3 — Runbook, observabilidade e pré-flight de produção

**Status:** EM REVISÃO  
**Fronteira:** nenhum deploy é executado neste checkpoint; `production_deployed=false` permanece obrigatório.

## 1. Objetivo

C7.3 transforma as provas de C7.1 e C7.2 em procedimento operacional reproduzível para produção. O checkpoint cobre:

- pré-flight read-only dos dois roots reais;
- verificação das 11 dependências pré-existentes e dos 32 destinos gerenciados;
- planejamento explícito dos diretórios gerenciados que ainda não existirem;
- critérios objetivos de `GO` / `NO_GO`;
- backup exato obrigatório antes da primeira escrita;
- journal dos diretórios criados pelo deploy e rollback seguro;
- health check operacional do WordPress;
- sinais de observabilidade e sequência pós-deploy.

C7.3 **não implanta** o bundle e **não autoriza automaticamente** implantação. Um `GO` técnico é condição necessária, mas a autorização de deploy continua explícita e pertence a checkpoint posterior.

## 2. Evidência real já necessária

O pré-flight deve consumir o `bundle-manifest.json` produzido pelo mesmo bundle C7.1 que será candidato a deploy. Requisitos invariantes:

- `checkpoint=C7.1`;
- `production_deployed=false`;
- 32 arquivos gerenciados;
- 11 dependências pré-existentes;
- release fiscal `PUBLISHED`, schema/API 1.2.0 e inventário 32/32;
- SHA-256 de todos os bytes gerenciados.

## 3. Pré-flight read-only no HostGator

Executar:

```bash
python3 scripts/run_phase7_c73_host_preflight.py \
  --bundle-manifest /caminho/bundle-manifest.json \
  --site-root "$SITE_ROOT" \
  --wordpress-plugin-dir "$WP_PLUGIN_DIR" \
  --evidence-out /fora/dos/roots/c73-hostgator-preflight.json
```

O arquivo de evidência deve ficar fora dos roots examinados. O script não cria, altera ou remove arquivo ou diretório de produção.

### 3.1. O que é verificado

1. ambos os roots existem, são diretórios reais, legíveis e atravessáveis;
2. as 11 dependências existem como arquivos regulares, legíveis e não-symlink;
3. cada parent dos 32 destinos está em uma destas condições:
   - já existe como diretório real, legível, atravessável e gravável; ou
   - ainda não existe, mas toda a cadeia faltante pode ser criada a partir de um ancestral existente, real, legível, atravessável e gravável;
4. todos os diretórios a criar são registrados em `planned_directory_creations`;
5. destinos existentes são arquivos regulares, legíveis, graváveis e não-symlink;
6. SHA-256, tamanho e mode dos destinos existentes são registrados;
7. há espaço para staging do bundle mais backup dos bytes existentes;
8. PHP CLI está disponível;
9. qualquer condição insegura produz `NO_GO`.

## 4. Diretórios gerenciados ausentes

A inspeção real de 16/09/2026 mostrou um caso legítimo que o contrato C7.3 original tratava de forma conservadora demais: `parts/` de calculadoras e `includes/` do plugin ainda não existiam porque os respectivos arquivos ainda não haviam sido implantados.

A política corrigida não manda criar diretórios manualmente antes do deploy. Em vez disso:

- o pré-flight continua read-only;
- a lista exata de diretórios ausentes entra no journal de planejamento;
- o deploy futuro poderá criar **somente** esses diretórios gerenciados;
- cada diretório efetivamente criado deve ser registrado em journal de aplicação;
- rollback remove arquivos novos primeiro;
- depois usa apenas `rmdir` nos diretórios criados por aquele deploy, do mais profundo para o mais raso;
- deleção recursiva (`rm -rf`, `shutil.rmtree` ou equivalente) é proibida no rollback de produção;
- diretório criado pelo deploy que tiver conteúdo não gerenciado no momento do rollback **não é removido**; o conteúdo é preservado e o caso é escalado manualmente.

Essa extensão foi simulada separadamente em `scripts/simulate_phase7_c73_directory_rollback.py` e não reescreve a evidência histórica de C7.1.

## 5. Critério técnico `GO` / `NO_GO`

O JSON usa:

- `status=PASS` + `technical_go_no_go=GO`; ou
- `status=BLOCKED` + `technical_go_no_go=NO_GO`.

`GO` exige simultaneamente:

- zero `block_reasons`;
- 11/11 dependências verificadas;
- 32/32 destinos examinados;
- todos os parents existentes **ou seguramente criáveis**;
- journal completo em `planned_directory_creations` para parents ausentes;
- permissões para leitura, backup, escrita e eventual criação dos diretórios planejados;
- PHP CLI disponível;
- espaço livre suficiente;
- nenhuma mutação dos roots pelo pré-flight.

**`GO` não executa deploy e não equivale a autorização humana.**

## 6. Backup exato antes da primeira escrita

Depois de `GO` e somente no checkpoint que autorizar implantação:

1. congelar o SHA do `main` e digest do bundle;
2. registrar os 32 destinos antes da escrita;
3. salvar bytes exatos e SHA-256 de cada arquivo existente;
4. registrar `existed=false` para cada arquivo ausente;
5. preservar o journal de `planned_directory_creations`;
6. guardar snapshot e journals fora dos roots gerenciados;
7. verificar o backup;
8. somente então permitir a primeira mutação.

As 11 dependências são somente leitura e nunca entram como arquivos a substituir.

## 7. Aplicação futura

A aplicação deve obedecer ao manifesto sem glob genérico.

Antes de escrever cada arquivo:

1. criar, quando necessário, somente a cadeia de diretórios previamente aprovada pelo pré-flight;
2. registrar imediatamente cada diretório que realmente foi criado;
3. não registrar como criado diretório que já existia;
4. validar SHA-256 do byte do bundle;
5. escrever somente `target_root + target_path` declarado;
6. validar SHA-256 após a escrita;
7. interromper na primeira divergência.

Nenhum arquivo não gerenciado pode ser apagado ou substituído.

## 8. Observabilidade WordPress

C7.3 adiciona:

```text
GET /wp-json/sfa/v1/fiscal-health
```

Estados:

- `healthy` — pacote fiscal válido disponível;
- `degraded_last_good` — fonte canônica indisponível, mas `last_good` validado permitido;
- `blocked_known_successor` — sucessora conhecida ainda não verificada; consumo bloqueado;
- `unavailable` — nenhum pacote consumível disponível.

HTTP 200 para `healthy`/`degraded_last_good`; HTTP 503 para `blocked_known_successor`/`unavailable`. A rota usa `Cache-Control: no-store`, `X-Sanida-Fiscal-Status` e, quando aplicável, `X-Sanida-Fiscal-Release`. A resposta pública não expõe `body_sample` nem conteúdo bruto de coleta.

## 9. Health checks pós-deploy

Após implantação futura:

1. `GET /wp-json/sfa/v1/fiscal-health`;
2. exigir `healthy`, ou `degraded_last_good` somente quando a indisponibilidade canônica estiver compreendida;
3. conferir `X-Sanida-Fiscal-Release` contra a release do bundle;
4. conferir `/wp-json/sfa/v1/fiscal-release`;
5. confirmar `/wp-json/sfa/v1/folha` como 410;
6. testar H26, H27, H28 e H29 em casos canônicos;
7. verificar `[fiscais_debug]` para origem/cache/latch.

`blocked_known_successor`, `unavailable`, release divergente, erro PHP, asset ausente ou quebra H26–H29 são gatilhos de rollback.

## 10. Rollback

Gatilhos incluem SHA divergente, 5xx inesperado, health bloqueado/indisponível, release errada, `/folha` diferente de 410, falha H26–H29, PHP fatal, mutação de dependência ou de arquivo não gerenciado.

Sequência:

1. restaurar bytes exatos dos arquivos que existiam;
2. remover somente arquivos gerenciados que eram ausentes;
3. percorrer o journal de diretórios criados em profundidade decrescente;
4. executar apenas `rmdir` em cada diretório criado pelo deploy;
5. se `rmdir` falhar porque o diretório não está vazio, preservar o diretório e seu conteúdo e registrar escalonamento manual;
6. nunca tocar nas 11 dependências nem em arquivos não gerenciados.

A prova automatizada exige tanto restauração exata quando os diretórios estão vazios quanto preservação de conteúdo não gerenciado em diretório criado pelo deploy.

## 11. Evidência necessária para fechar C7.3

C7.3 só muda para `CONCLUÍDO` quando forem preservados:

1. JSON do pré-flight remoto real;
2. `status=PASS` e `technical_go_no_go=GO`;
3. identificação dos dois roots sem credenciais;
4. 11 dependências verificadas;
5. 32 destinos examinados;
6. todos os parents existentes ou seguramente criáveis;
7. journal `planned_directory_creations` íntegro;
8. `production_mutated=false`;
9. SHA do `bundle-manifest.json` usado;
10. release vinculada ao bundle;
11. validação do JSON pelo validador C7.3.

Sem essa evidência, o estado correto permanece **EM REVISÃO / NO_GO**.

## 12. Artefatos C7.3

- `docs/phase7-c73-preflight-contract-v1.json`;
- `scripts/run_phase7_c73_host_preflight.py`;
- `scripts/simulate_phase7_c73_preflight.py`;
- `scripts/simulate_phase7_c73_directory_rollback.py`;
- `scripts/validate_phase7_c73_remote_evidence.py`;
- `tests/php/phase7_c73_health.php`;
- `tests/test_phase7_c73.py`;
- `scripts/validate_phase7_c73_gate.py`;
- este runbook;
- etapas permanentes no `Remake CI`.

## 13. Próxima ação

Executar novamente o pré-flight read-only no HostGator real com esta política. Se o JSON resultar em `PASS/GO`, validar e preservar a evidência. Somente então C7.3 pode ser encerrado. Nenhum deploy é realizado por essa ação.
