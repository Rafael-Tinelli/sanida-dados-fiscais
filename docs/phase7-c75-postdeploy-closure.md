# Fase 7 — C7.5 — Validação pós-deploy e fechamento formal

**Status:** EM ANDAMENTO  
**Produção:** implantada por C7.4 e não deve ser reimplantada em C7.5.  
**Modo de C7.5:** validação estritamente read-only nos roots de produção.

## 1. Objetivo

C7.5 encerra a Fase 7 somente se o estado implantado por C7.4 permanecer íntegro depois de uma janela separada da implantação. O checkpoint não aplica, repara, atualiza nem reexecuta o bundle.

C7.4 terminou em `APPLIED_HEALTHY`, com `production_deployed=true`, `rollback_performed=false` e SHA-256 do `deployment-state.json`:

`9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c`

C7.5 trata esse journal como âncora histórica e verifica o estado vivo.

## 2. Janela separada

A evidência C7.5 só pode ser `PASS` se a validação ocorrer pelo menos 600 segundos após `completed_at_utc` do deployment C7.4.

Esse requisito evita que o fechamento da Fase 7 seja apenas uma segunda leitura do mesmo instante do deploy.

## 3. Integridade do filesystem

O validador read-only exige:

- SHA do `deployment-state.json` exatamente igual ao registrado em C7.4;
- SHA do `bundle-manifest.json` exatamente igual ao candidato autorizado;
- SHA da evidência C7.3 exatamente igual ao baseline registrado;
- 32/32 arquivos gerenciados ainda iguais ao bundle C7.4;
- 11/11 dependências pré-existentes ainda iguais ao baseline C7.3;
- quatro diretórios criados por C7.4 ainda como diretórios regulares, sem symlink;
- nenhum arquivo temporário `.c74-*` restante nos parents gerenciados.

C7.5 não corrige drift. Qualquer divergência produz `BLOCKED`.

## 4. Cadeia evergreen canônica

A validação consulta diretamente `releases/fiscal-v1/current.json` no `main` e o artefato imutável apontado por ele.

São requisitos:

- `current.json` HTTP 200 e JSON válido;
- contrato/schema `1.2.0`;
- artefato imutável HTTP 200;
- SHA-256 do artefato igual ao declarado no pointer;
- `release_id` do artefato igual ao pointer.

Assim, C7.5 valida a cadeia fonte canônica → WordPress, e não apenas a release que existia no instante do deploy.

## 5. Produção pública

São executadas pelo menos duas rodadas independentes de HTTP probes.

Cada rodada exige:

1. `/blog/wp-json/sfa/v1/fiscal-health` → HTTP 200, `healthy`, release canônica corrente, `Cache-Control: no-store`, `X-Sanida-Fiscal-Status: healthy` e `X-Sanida-Fiscal-Release` correto;
2. `/blog/wp-json/sfa/v1/fiscal-release` → HTTP 200 e mesma release no body/header;
3. `/blog/wp-json/sfa/v1/folha` → HTTP 410;
4. H26, H27, H28 e H29 → HTTP 200, sem `Fatal error`/`Parse error` e com referências aos assets esperados.

## 6. Cache público dos JavaScript

C7.4 provou os bytes no filesystem e o HTTP 200 das páginas. C7.5 adiciona uma prova que faltava: os oito JavaScript gerenciados são baixados pelas URLs públicas e comparados por SHA-256 com o bundle C7.4.

Isso detecta, entre outros problemas, um arquivo antigo ainda servido por cache/CDN mesmo quando o arquivo correto já existe no HostGator.

Qualquer divergência de asset público bloqueia o fechamento.

## 7. Não-mutação

`scripts/run_phase7_c75_postdeploy_validation.py` não contém operação de apply ou rollback. O único write permitido é o JSON de evidência, obrigatoriamente fora de `site_root` e `wordpress_plugin_dir`.

C7.5 não:

- reexecuta `run_phase7_c74_controlled_deploy.py`;
- altera cache WordPress;
- limpa ETag/transient;
- modifica arquivos gerenciados;
- modifica as 11 dependências;
- remove diretórios ou temporários.

Problema encontrado significa `BLOCKED`, não reparo implícito.

## 8. Evidência e validador formal

A execução real deve gerar um JSON C7.5 e passá-lo por:

`scripts/validate_phase7_c75_remote_evidence.py`

O validador formal exige, entre outros pontos:

- `status=PASS`;
- `phase7_close_recommended=true`;
- `production_mutated=false`;
- janela mínima atendida;
- 32/32 arquivos;
- 11/11 dependências;
- 4/4 diretórios;
- 8/8 JS públicos;
- pelo menos duas rodadas HTTP;
- REST saudável e consistente com a fonte canônica;
- legado 410;
- H26–H29 em 200.

O SHA-256 do JSON final de C7.5 deve ser registrado no repositório antes do fechamento.

## 9. Critério formal de fechamento da Fase 7

A Fase 7 só muda para `CONCLUÍDA` quando:

- C7.1–C7.4 permanecem concluídos;
- evidência real C7.5 passa no validador formal;
- o SHA da evidência C7.5 é preservado;
- `production_deployed=true` permanece verdadeiro;
- `production_mutated_by_c75=false`;
- nenhuma pendência técnica do remake permanece aberta.

Depois desse fechamento, mudanças futuras são operação/manutenção evergreen ou uma nova evolução de produto; não reabrem o remake sem evidência objetiva.

## 10. Artefatos

- `state/phase7-c75-closure.json`;
- `scripts/run_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_remote_evidence.py`;
- `scripts/simulate_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_gate.py`;
- `tests/test_phase7_c75.py`;
- este documento;
- evidência real preservada fora dos roots de produção e vinculada por SHA-256 no fechamento.
