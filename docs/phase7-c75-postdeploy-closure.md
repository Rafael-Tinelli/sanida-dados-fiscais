# Fase 7 — C7.5 — Validação pós-deploy e fechamento formal

**Status:** EM ANDAMENTO  
**Produção:** implantada por C7.4 e não deve ser reimplantada em C7.5.  
**Modo de C7.5:** validação read-only, com evidências separadas de origem e de entrega pública ao cliente.

## 1. Objetivo

C7.5 encerra a Fase 7 somente se o estado implantado por C7.4 permanecer íntegro depois de uma janela separada da implantação. O checkpoint não aplica, repara, atualiza nem reexecuta o bundle.

C7.4 terminou em `APPLIED_HEALTHY`, com `production_deployed=true`, `rollback_performed=false` e SHA-256 do `deployment-state.json`:

`9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c`

## 2. Janela separada

A evidência de origem C7.5 só é admissível quando observada pelo menos 600 segundos após `completed_at_utc` do deployment C7.4. A primeira execução real ocorreu mais de 1.900 segundos depois do deploy.

## 3. Integridade da origem

A validação HostGator exige:

- journal C7.4 com SHA imutável;
- bundle manifest idêntico ao candidato autorizado;
- evidência C7.3 idêntica ao baseline registrado;
- 32/32 arquivos gerenciados iguais ao bundle C7.4;
- 11/11 dependências pré-existentes iguais ao baseline C7.3;
- quatro diretórios criados por C7.4 ainda presentes como diretórios regulares;
- nenhum temporário `.c74-*` nos parents gerenciados;
- `current.json` e artefato fiscal canônico íntegros;
- duas rodadas independentes de health/release/folha/H26–H29;
- `production_mutated=false`.

Qualquer drift nessa camada bloqueia o fechamento.

## 4. Primeira observação real e descoberta de fronteira

A primeira execução real de C7.5 gerou:

- `status=BLOCKED`;
- SHA-256 da evidência `996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447`;
- 32/32 arquivos gerenciados corretos;
- 11/11 dependências corretas;
- quatro diretórios persistentes;
- zero temporários C7.4;
- fonte canônica íntegra;
- duas rodadas REST/páginas saudáveis;
- 5/8 JavaScript observados como idênticos pela chamada HostGator → hostname público.

Os três bloqueios foram exclusivamente:

- `ferias-clt.js`;
- `folha-core.js`;
- `rescisao-clt.js`.

O diagnóstico subsequente mostrou HTTP 403 do Cloudflare para os três URLs, tanto direto quanto com cache-buster, quando a chamada parte do próprio HostGator. Portanto os corpos comparados eram respostas de negação da borda, não JavaScript.

Esse resultado não é reclassificado como PASS por conveniência. Ele é preservado em `docs/phase7-c75-initial-host-observation.json` e levou à separação formal das fronteiras.

## 5. Fronteira de entrega pública ao cliente

Servidor de origem não é um cliente externo quando a camada Cloudflare impede o loop de saída do HostGator para o hostname público.

C7.5 passa a exigir duas provas independentes:

1. **Host/origem** — filesystem, journal, dependências, cadeia fiscal e páginas/REST;
2. **cliente externo** — os oito JavaScript públicos precisam ser baixados por uma rede de cliente e comparados byte a byte com os arquivos do commit C7.4 autorizado `ccc5a31c3da7c1c93570df0337e553e5a06404ac`.

A sonda externa é `scripts/run_phase7_c75_external_delivery_probe.ps1`. Ela compara cada URL pública com o mesmo arquivo em `raw.githubusercontent.com` pinado ao commit autorizado, evitando usar `main` móvel como referência.

Oito de oito assets devem retornar HTTP 200 e SHA-256 idêntico. Qualquer divergência continua bloqueando o fechamento.

## 6. Validadores formais

A evidência HostGator é validada por:

`scripts/validate_phase7_c75_remote_evidence.py`

Esse validador aceita `BLOCKED` apenas quando **todos** os `block_reasons` são diagnósticos `public_js_delivery_drift:*`. Qualquer outro motivo permanece fatal. Ele não transforma entrega não observável em entrega válida; apenas declara a camada de origem como validada e exige a segunda prova.

A evidência externa é validada por:

`scripts/validate_phase7_c75_external_delivery_evidence.py`

Ela exige:

- `status=PASS`;
- `production_mutated=false`;
- commit autorizado exato;
- conjunto exato dos oito JS;
- fonte esperada raw HTTP 200;
- URL pública HTTP 200;
- SHA esperado = SHA público para os 8/8 assets;
- nenhum `block_reason`.

## 7. Não-mutação

C7.5 não:

- reexecuta `run_phase7_c74_controlled_deploy.py`;
- altera arquivos em produção;
- limpa cache Cloudflare;
- limpa ETag/transient WordPress;
- cria ou remove diretórios;
- modifica dependências.

A sonda externa também é somente leitura. Problema encontrado significa `BLOCKED`, não reparo implícito.

## 8. Critério formal de fechamento da Fase 7

A Fase 7 só muda para `CONCLUÍDA` quando:

- C7.1–C7.4 permanecem concluídos;
- a evidência HostGator existente passa no validador de origem;
- a evidência externa prova os oito JavaScript públicos;
- os SHA-256 das duas evidências ficam registrados;
- `production_deployed=true` permanece verdadeiro;
- `production_mutated_by_c75=false`;
- nenhum bloqueio técnico do remake permanece aberto;
- Remake CI fica verde no mesmo head de fechamento.

Depois desse fechamento, mudanças futuras são manutenção evergreen ou nova evolução de produto; não reabrem o remake sem evidência objetiva.

## 9. Artefatos

- `state/phase7-c75-closure.json`;
- `docs/phase7-c75-initial-host-observation.json`;
- `scripts/run_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_remote_evidence.py`;
- `scripts/run_phase7_c75_external_delivery_probe.ps1`;
- `scripts/validate_phase7_c75_external_delivery_evidence.py`;
- `scripts/simulate_phase7_c75_postdeploy_validation.py`;
- `scripts/validate_phase7_c75_gate.py`;
- `tests/test_phase7_c75.py`;
- este documento.
