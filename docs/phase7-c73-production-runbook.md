# Fase 7 — C7.3 — Runbook, observabilidade e pré-flight de produção

**Status:** EM REVISÃO  
**Fronteira:** nenhum deploy é executado neste checkpoint; `production_deployed=false` permanece obrigatório.

## 1. Objetivo

C7.3 transforma as provas de C7.1 e C7.2 em procedimento operacional reproduzível para produção. O checkpoint define:

- pré-flight read-only dos dois roots de destino;
- verificação das 11 dependências pré-existentes e dos 32 destinos gerenciados;
- critérios objetivos de `GO` / `NO_GO`;
- backup exato obrigatório antes da primeira escrita;
- rollback mecânico e seus gatilhos;
- health check operacional do WordPress;
- sinais de observabilidade e sequência pós-deploy;
- evidência mínima necessária para autorizar um deploy posterior.

C7.3 **não implanta** o bundle e **não autoriza automaticamente** implantação. Um `GO` técnico é condição necessária, mas a autorização de deploy continua sendo explícita e pertence a checkpoint posterior.

## 2. Estado atual e motivo de `EM REVISÃO`

O repositório não contém credencial, segredo ou mecanismo SSH para o HostGator. Portanto é possível construir, testar e versionar o pré-flight, mas não é correto afirmar que o filesystem real da hospedagem foi inspecionado a partir deste ambiente.

O checkpoint só poderá ser promovido a `CONCLUÍDO` quando existir evidência remota read-only, produzida por `scripts/run_phase7_c73_host_preflight.py`, contra os roots reais de produção e validada pelo gate correspondente.

Até lá:

- implementação do pré-flight: pronta;
- simulação positiva/negativa e prova de não-mutação: automatizadas;
- observabilidade WordPress: implementada no bundle;
- runbook: definido;
- inspeção remota real das 11 dependências: **pendente**;
- `technical_go_no_go` de produção: **NO_GO por ausência de evidência remota**, não por falha conhecida do ambiente.

## 3. Artefato de referência

O único pacote candidato a deploy é o bundle C7.1 construído por:

```bash
python scripts/build_phase7_c71_bundle.py --output-dir /tmp/c71-deployment-bundle
```

O pré-flight deve consumir **o `bundle-manifest.json` gerado no mesmo bundle que se pretende implantar**. Não se aceita reconstruir manualmente a lista de arquivos.

Requisitos invariantes do bundle:

- `checkpoint=C7.1`;
- `production_deployed=false`;
- 32 arquivos gerenciados;
- 11 dependências pré-existentes;
- release fiscal `PUBLISHED`, schema/API 1.2.0 e inventário 32/32;
- hashes SHA-256 de todos os bytes gerenciados.

## 4. Pré-flight read-only no HostGator

Com o bundle já construído, executar a partir de um ambiente que consiga enxergar os paths reais da hospedagem:

```bash
python3 scripts/run_phase7_c73_host_preflight.py \
  --bundle-manifest /caminho/para/c71-deployment-bundle/bundle-manifest.json \
  --site-root "$SITE_ROOT" \
  --wordpress-plugin-dir "$WP_PLUGIN_DIR"
```

Para preservar a prova read-only, a saída deve ser capturada fora dos dois roots de destino, por redirecionamento do shell ou pelo chamador SSH. O script não cria, altera ou remove arquivos nos roots verificados.

### 4.1. O que o pré-flight verifica

1. ambos os roots existem, são diretórios, legíveis e atravessáveis;
2. as 11 dependências pré-existentes existem como arquivos regulares, legíveis e não-symlink;
3. os diretórios-pai de todos os 32 destinos gerenciados **já existem**;
4. esses diretórios-pai são legíveis, atravessáveis e graváveis pelo usuário de deploy;
5. destinos já existentes são arquivos regulares, legíveis, graváveis e não-symlink;
6. SHA-256, tamanho e mode de cada destino existente são registrados;
7. o espaço livre é suficiente para staging do bundle mais backup dos bytes existentes;
8. PHP CLI está disponível;
9. nenhuma condição negativa é convertida em warning permissivo.

### 4.2. Por que diretórios-pai ausentes são `NO_GO`

C7.1 provou rollback exato de **arquivos**: restaura bytes anteriores e remove arquivos que não existiam. O simulador não define rollback de diretórios recém-criados. Para não ampliar silenciosamente a superfície de mutação, C7.3 exige que toda árvore de diretórios necessária já exista antes do deploy.

Se qualquer parent faltar, o deploy permanece bloqueado até o plano de rollback ser explicitamente ampliado e testado.

## 5. Critério técnico `GO` / `NO_GO`

O JSON de pré-flight usa:

- `status=PASS` + `technical_go_no_go=GO`; ou
- `status=BLOCKED` + `technical_go_no_go=NO_GO`.

`GO` exige simultaneamente:

- zero `block_reasons`;
- 11/11 dependências verificadas;
- 32/32 destinos pré-flighted;
- todos os parents preexistentes;
- permissões necessárias para leitura/backup e substituição;
- PHP CLI disponível;
- espaço livre suficiente;
- bundle ainda marcado `production_deployed=false`;
- nenhuma mutação dos roots causada pelo próprio pré-flight.

Qualquer falha produz `NO_GO`.

**`GO` não executa deploy e não equivale a autorização humana.**

## 6. Backup obrigatório antes da primeira escrita

Depois de um `GO` técnico e somente no checkpoint que efetivamente autorizar implantação:

1. congelar o SHA do commit `main` e o digest do bundle que será usado;
2. capturar estado dos 32 destinos antes de qualquer escrita;
3. para cada arquivo existente, salvar os bytes exatos e SHA-256;
4. para cada arquivo ausente, registrar explicitamente `existed=false`;
5. guardar o snapshot fora dos destinos gerenciados;
6. verificar o backup antes da aplicação;
7. só então iniciar a substituição dos arquivos gerenciados.

As 11 dependências pré-existentes são somente leitura e **não entram como arquivos a substituir**.

## 7. Aplicação futura

A aplicação de produção deve obedecer ao manifesto do bundle, sem glob genérico e sem copiar diretórios inteiros por conveniência.

Para cada um dos 32 records:

1. verificar novamente SHA-256 do byte do bundle;
2. escrever somente o `target_root + target_path` declarado;
3. verificar SHA-256 após a escrita;
4. interromper imediatamente em primeira divergência.

Nenhum arquivo não gerenciado pode ser apagado ou substituído.

## 8. Observabilidade WordPress

C7.3 adiciona:

```text
GET /wp-json/sfa/v1/fiscal-health
```

A rota não publica tabela fiscal. Ela expõe somente estado operacional sanitizado.

### Estados

- `healthy` — pacote fiscal válido disponível; transient/304/release verificada são estados normais;
- `degraded_last_good` — fonte canônica não pôde ser consultada, mas a política permite servir o `last_good` validado;
- `blocked_known_successor` — existe sucessora conhecida ainda não verificada; consumo fica bloqueado;
- `unavailable` — nenhum pacote consumível está disponível.

### HTTP

- `healthy` e `degraded_last_good`: HTTP 200;
- `blocked_known_successor` e `unavailable`: HTTP 503;
- `Cache-Control: no-store` na resposta saudável/degradada;
- `X-Sanida-Fiscal-Status` informa o estado;
- `X-Sanida-Fiscal-Release` informa a release quando houver pacote consumível.

A resposta pública não expõe `body_sample`, URL interna de erro ou conteúdo bruto de coleta.

O shortcode administrativo `[fiscais_debug]` mantém detalhes adicionais para operador autenticado e passa a incluir `fiscais_health`.

## 9. Health checks pós-deploy

Após implantação futura, executar na ordem:

1. `GET /wp-json/sfa/v1/fiscal-health`;
2. exigir HTTP 200 e `status=healthy` ou, apenas durante indisponibilidade canônica já compreendida, `degraded_last_good`;
3. conferir `X-Sanida-Fiscal-Release` contra o `release_id` do bundle;
4. `GET /wp-json/sfa/v1/fiscal-release` e conferir o mesmo `release_id`;
5. confirmar `/wp-json/sfa/v1/folha` como `410 Gone`;
6. testar H26, H27, H28 e H29 em casos canônicos;
7. verificar no debug administrativo origem, known-successor e release ativa.

`blocked_known_successor`, `unavailable`, release divergente, erro PHP, asset ausente ou quebra em qualquer H26–H29 são gatilhos de rollback.

## 10. Gatilhos de rollback

Rollback imediato se ocorrer qualquer um destes eventos após a primeira escrita:

- SHA pós-write divergente do bundle;
- HTTP 5xx inesperado nas páginas/calculadoras;
- `fiscal-health` em `blocked_known_successor` ou `unavailable`;
- release servida diferente da release vinculada ao bundle;
- `/sfa/v1/folha` deixar de responder 410;
- H26–H29 falharem caso canônico;
- PHP fatal/syntax error;
- dependência pré-existente tiver sido alterada;
- arquivo não gerenciado tiver sido alterado.

O rollback restaura bytes exatos dos destinos que existiam e remove os arquivos gerenciados que eram ausentes no snapshot. Não deve tocar nas 11 dependências nem em arquivos não gerenciados.

## 11. Observação contínua pós-deploy

No período inicial pós-deploy, o operador deve acompanhar conjuntamente:

- `fiscal-health`;
- `fiscal-release` e `release_id`;
- `[fiscais_debug]` para origem/cache/latch;
- workflow `Fiscal Contract v1.2 Publication`;
- `current.json` canônico;
- Remake CI do commit implantado.

Uma publicação evergreen válida pode mudar o `release_id` sem novo deploy de PHP/JS, desde que schema/API e contrato de consumidor permaneçam compatíveis. Mudança estrutural continua presa à revisão humana já definida na Fase 5/C6.0a.

## 12. Evidência necessária para fechar C7.3

C7.3 só muda para `CONCLUÍDO` quando forem preservados:

1. JSON do pré-flight remoto real;
2. `status=PASS` e `technical_go_no_go=GO`;
3. identificação dos dois roots examinados sem incluir credenciais;
4. 11 dependências verificadas;
5. 32 destinos examinados;
6. prova de parents preexistentes;
7. `production_mutated=false`;
8. SHA do `bundle-manifest.json` usado;
9. release vinculada ao bundle;
10. validação do JSON pelo gate C7.3.

Sem essa evidência, o estado correto é **EM REVISÃO / NO_GO**, ainda que todos os testes locais e de CI estejam verdes.

## 13. Artefatos C7.3

- `docs/phase7-c73-preflight-contract-v1.json`;
- `scripts/run_phase7_c73_host_preflight.py`;
- `scripts/simulate_phase7_c73_preflight.py`;
- `tests/php/phase7_c73_health.php`;
- `tests/test_phase7_c73.py`;
- `scripts/validate_phase7_c73_gate.py`;
- este runbook;
- etapas permanentes no `Remake CI`.

## 14. Próxima ação dentro do próprio C7.3

Executar o pré-flight read-only contra o HostGator real e guardar o JSON retornado. Somente depois dessa evidência C7.3 pode ser encerrado e o projeto pode avançar para autorização de implantação controlada.
