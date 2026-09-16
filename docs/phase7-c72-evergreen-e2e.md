# Fase 7 — C7.2 — E2E operacional, falhas e recuperação evergreen

**Status:** CONCLUÍDO

## Objetivo

C7.2 prova o caminho operacional entre publicação da release fiscal, `current.json`, cache/last-good do WordPress, REST canônica e H26–H29 antes de qualquer alteração no HostGator.

Este checkpoint não realiza deploy. `production_deployed=false` continua sendo uma fronteira obrigatória.

## Defeito operacional encontrado e corrigido

C6.1 já proibia relabelar uma release histórica quando uma sucessora fosse conhecida, porém esse conhecimento existia apenas dentro da requisição que havia lido o novo `current.json`.

A sequência problemática era:

1. WordPress possuía release A como `last_good`;
2. `current.json` anunciava release B;
3. o download/validação do artefato B falhava e A era corretamente bloqueada naquela requisição;
4. em uma requisição posterior, se `current.json` também estivesse indisponível, o processo já não lembrava que B havia sido observada;
5. A podia voltar a ser usada como `last_good`.

C7.2 fecha essa lacuna com `OPT_KNOWN_SUCCESSOR` (`sfa_fiscal_v12_known_successor`). O latch persiste `release_id`, SHA-256 do artefato anunciado e instante da observação. Ele não é cache e não é apagado pelo comando administrativo de refresh.

Enquanto o latch aponta para uma sucessora ainda não verificada:

- transient da predecessora não autoriza consumo;
- `304 Not Modified` não autoriza predecessora incompatível com o latch;
- indisponibilidade de `current.json` não autoriza predecessora;
- regressão do ponteiro para uma release anterior ao latch falha fechado;
- REST responde `503` quando não existe pacote atual validável;
- o latch só é resolvido por um pacote remoto integralmente validado correspondente à sucessora conhecida.

## Cenários executados contra os traits reais do bundle

O E2E usa `scripts/build_phase7_c71_bundle.py` e carrega, a partir dos bytes empacotados, `trait-sanida-fiscal-contract.php` e `trait-sanida-fiscal-network.php`. As respostas HTTP e primitivas do WordPress são simuladas; a lógica fiscal/operacional é a lógica real do plugin.

A transição usa duas releases imutáveis reais do repositório: a release atual e o `supersedes_release_id` declarado por ela. Nenhuma regra fiscal é inventada como fixture.

Cenários obrigatórios:

1. **cold start na predecessora** — `current.json` + artefato imutável são verificados; `last_good`, transient e ETag são preenchidos;
2. **transient válido** — segunda leitura não faz rede e é identificada como `origin=transient_cache`;
3. **indisponibilidade sem sucessora conhecida** — após expiração do transient, `last_good` validado pode ser usado com TTL de falha, sem relabeling;
4. **ETag/304** — `If-None-Match` é enviado e `304` reutiliza somente `last_good` permitido;
5. **sucessora anunciada + artefato indisponível** — latch persistente é criado, predecessora é bloqueada e REST retorna `503` inclusive em requisição subsequente;
6. **recuperação** — quando o mesmo `current.json` e o artefato correto voltam a responder, a sucessora é validada, passa a `last_good`, atualiza ETag e resolve o latch;
7. **falha posterior à recuperação** — o `last_good` agora é exatamente a sucessora validada;
8. **304 sem last-good** — falha fechado;
9. **last-good corrompido** — falha fechado.

## E2E até H26–H29

Após a recuperação do WordPress, a release retornada por esse fluxo é entregue aos runtimes JavaScript empacotados de H26, H27, H28 e H29. Os quatro devem executar e reportar exatamente o mesmo `release_id` da sucessora recuperada.

Assim, a prova não termina no cache WordPress: ela percorre publicação materializada → ponteiro → pacote WordPress → REST/release recuperada → `folha-core` → H26/H27/H28/H29.

## Operação evergreen

O workflow `.github/workflows/fiscal-release-v12.yml` permanece a fonte operacional de publicação fiscal e possui:

- execução agendada;
- `concurrency: sanida-dados-fiscais-writes-main`;
- gates semânticos e de publicação antes de promoção;
- revisão humana para mudanças estruturais;
- persistência de schema/evidência/release/estado;
- `git pull --rebase origin main` + `git push origin main` para materializar a release e o `current.json` no branch canônico.

O plugin consome por padrão o `current.json` e os artefatos imutáveis desse mesmo `main`. Mudança estrutural continua sem autopublicação; mudança validada e publicada chega ao consumidor sem edição manual de tabela ou fórmula.

O transient de sucesso permanece em 12 horas e a resposta REST usa `max-age=600, s-maxage=600`. Portanto o caminho é evergreen, mas não instantâneo: cache válido pode manter a release já validada até a expiração/refresh; depois disso o ponteiro é revalidado por ETag. C7.2 prova essa fronteira em vez de ocultá-la.

## Observabilidade adicionada

O shortcode administrativo de debug passa a expor `fiscais_known_successor`, além de `fiscais_origin`, `fiscais_release_id`, estado de cache e runtime. Um operador consegue distinguir:

- release remota recém-validada;
- transient;
- `last_good` por indisponibilidade;
- sucessora conhecida ainda não recuperada;
- indisponibilidade fail-closed.

## Artefatos permanentes

- `tests/php/phase7_c72_wordpress_operational.php`;
- `scripts/run_phase7_c72_operational_e2e.py`;
- `tests/test_phase7_c72.py`;
- `scripts/validate_phase7_c72_gate.py`;
- este documento;
- etapa permanente no `Remake CI`.

## Fronteira pós-C7.2

C7.2 não autoriza nem executa implantação no HostGator.

A próxima fronteira é **C7.3 — runbook, observabilidade e pré-flight de produção**: consolidar procedimento operacional, verificação das 11 dependências pré-existentes no ambiente real, plano de backup/rollback e critérios objetivos para então autorizar um deploy controlado em checkpoint posterior.
