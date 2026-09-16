# Sanida Dados Fiscais

Repositório canônico de dados e regras fiscais computáveis utilizados pelas ferramentas financeiras da Sanida.

> **Princípio central:** este projeto não deve apenas coletar números. Ele deve preservar, validar e publicar o significado jurídico-computacional desses números, sua vigência, sua origem oficial e as condições em que podem ser usados com segurança.

---

## 1. Objetivo

Transformar o `sanida-dados-fiscais` na fonte canônica, versionada e auditável para as regras e parâmetros consumidos pelas calculadoras financeiras da Sanida, em especial:

- **H26 — salário líquido CLT**
- **H27 — décimo terceiro**
- **H28 — férias CLT**
- **H29 — rescisão CLT**

A meta arquitetural é tornar essas ferramentas **evergreen**, com manutenção manual mínima e atualização automática sempre que a alteração normativa puder ser interpretada com segurança.

Isso **não** significa permitir que um scraper interprete autonomamente legislação nova. A automação deve ser máxima na detecção, coleta, validação e atualização de parâmetros; mudanças estruturais de regra devem ser detectadas automaticamente e bloqueadas para revisão humana.

---

## 2. Papel deste README

Este README é o **documento-mestre do projeto**. Ele deve permanecer sincronizado com objetivos, limites, arquitetura, fontes, contratos, invariantes, gates, riscos, decisões pendentes e estado de cada fase.

Mudanças relevantes de arquitetura ou metodologia não devem existir apenas no código.

---

## 3. Linha de base observada — 13/09/2026

Na abertura do remake, o repositório possuía essencialmente:

- `scraper.py` — coleta IRRF e INSS em páginas oficiais e incorpora taxas;
- `update_taxas.py` — coleta Selic no SGS/BCB e CDI em fonte Cetip/B3 via FTP;
- `dados_fiscais.json` — artefato agregado consumível pelo legado;
- `taxas_bacen.json` — artefato específico de taxas;
- `.github/workflows/main.yml` — atualização de `dados_fiscais.json`;
- `.github/workflows/taxas.yml` — atualização de `taxas_bacen.json`.

O `dados_fiscais.json` de baseline declarava `schema_version: 2.2.0`, ano 2026 e geração em 03/07/2026. Esse estado é histórico e não deve ser confundido com a arquitetura final.

---

## 4. Problema que estamos resolvendo

A arquitetura inicial capturava vários parâmetros corretos, mas não modelava suficientemente a **semântica jurídica das regras**. O exemplo central é a redução mensal do IRRF de 2026: não basta preservar coeficientes; o contrato também precisa declarar a grandeza jurídica sobre a qual a fórmula incide.

```text
rule_id = irrf.monthly.reduction
applies_to = rendimento tributável sujeito à incidência mensal
effective_from = 2026-01-01
```

A ausência dessa semântica permitia aplicar números corretos sobre a variável errada.

---

## 5. Princípios não negociáveis

### 5.1. Fonte oficial primeiro

Hierarquia geral:

1. API/dataset oficial estruturado;
2. ato normativo oficial;
3. página operacional oficial do órgão competente;
4. exemplo oficial de aplicação;
5. fonte oficial complementar para confirmação.

Fontes privadas podem auxiliar investigação, mas não substituem autoridade oficial quando existir fonte pública apropriada.

### 5.2. Scraper não é autoridade jurídica

Scrapers e parsers são sensores/coletadores. Eles podem detectar mudança, extrair parâmetros conhecidos, preservar snapshots e apontar divergências. Eles não devem inventar interpretação para regra nova, promover silenciosamente mudança estrutural, transformar ausência em zero, relabelar dado antigo com competência nova ou tratar acessibilidade da página como prova suficiente de validade jurídica.

### 5.3. Vigência é parte do dado

Todo parâmetro ou regra relevante declara, quando aplicável, início/fim de vigência, competência, publicação, coleta/verificação, fonte, versão do contrato e política de atualização. `generated_at` nunca equivale a vigência.

### 5.4. Falhar com segurança

O sistema distingue coleta sem mudança, coleta com mudança, fonte indisponível, parser incompatível, mudança estrutural, contrato inválido, last-good ainda utilizável e last-good preservado apenas para auditoria. Falha de coleta ou parser nunca produz um número aparentemente atual por conveniência.

### 5.5. Nunca relabelar dado antigo como novo

Sem contrato válido para nova vigência, o estado correto é indisponibilidade/revisão, não atualização fictícia.

### 5.6. Zero, ausência e não aplicabilidade são estados distintos

Valor zero real, valor ausente, não aplicável, não publicado, não encontrado, fonte indisponível, parser falhou, regra ainda não vigente e regra expirada não são equivalentes.

### 5.7. Dinheiro não deve depender de `float`

O motor fiscal usa `Decimal` e política explícita de arredondamento. Conversões para números JSON de compatibilidade só podem ocorrer em fronteiras identificadas e nunca se tornam nova autoridade fiscal.

### 5.8. Toda regra crítica precisa de teste de referência

Exemplos oficiais e regressões objetivas viram testes automatizados, complementados por invariantes e property-based testing.

---

## 6. Classes de atualização

- **Classe A — fonte oficial estruturada:** pode admitir automação quando schema, metadados, vigência e testes permanecem válidos.
- **Classe B — mudança paramétrica em regra conhecida:** pode admitir automação após validação forte e política de promoção.
- **Classe C — mudança estrutural de regra:** estado obrigatório `REVIEW_REQUIRED`.
- **Classe D — fonte indisponível ou inconclusiva:** last-good apenas dentro da política explícita e sem fingir nova observação.

---

## 7. Arquitetura vigente

```text
Fontes oficiais
      │
      ▼
Collectors / Sensors
      │
      ▼
Raw snapshots imutáveis
      │
      ▼
Parsers versionados
      │
      ▼
Normalized source candidates
      │
      ▼
Contrato/regras normalizadas
      │
      ▼
Semantic diff / change classifier
      │
      ▼
Validation + promotion gates
      │
      ▼
Versioned release
      │
      ▼
releases/fiscal-v1/current.json
      │
      ▼
WordPress/cache + /wp-json/sfa/v1/fiscal-release
      │
      ▼
folha-core
      │
      └── H26 / H27 / H28 / H29
```

`/wp-json/sfa/v1/folha` não é mais canal de dados fiscais: desde C6.7 é somente um tombstone `410 Gone`.

---

## 8. Separação de domínios

### 8.1. `payroll_fiscal`

Inclui INSS do empregado, IRRF mensal, desconto simplificado, dependentes, redução mensal, 13º, férias, terço constitucional, abono pecuniário e regras necessárias à rescisão dentro do escopo declarado.

### 8.2. `financial_reference`

Inclui Selic, CDI e eventuais indicadores financeiros futuros. Esses dados possuem ciclos, freshness, fallback e política de consumo próprios e não herdam automaticamente a semântica jurídica de folha.

---

## 9. Contrato Fiscal Canônico v1.1 / v1.2

A Fase 2 fechou o contrato v1.1 histórico com modelos Pydantic, JSON Schema, cobertura **32/32 regras**, **18 famílias tipadas de payload**, compatibilidade exata/fail-closed, identidade content-addressed e supersessão explícita.

A Fase 5 evoluiu aditivamente para **schema/API 1.2.0** para separar `GovernanceEvidenceObservation` da proveniência jurídico-fiscal oficial. Evidência interna das `technical_contract_rule` não pode ser confundida com autoridade legal.

Releases canônicas 32/32 são materializadas pelo assembler v1.2, validadas, publicadas de forma imutável em `releases/fiscal-v1/` e selecionadas por `current.json`. O schema v1.2 é gerado deterministicamente por `scripts/generate_contract_schema_v12.py`.

---

## 10. Fontes oficiais

### 10.1. Banco Central

- Selic: **BCB SGS 432**;
- CDI: **BCB SGS 12** na unidade diária de origem;
- annualização do CDI em 252 dias úteis apenas na fronteira de compatibilidade;
- B3 permanece referência metodológica/corroboração, não fallback automático.

### 10.2. Receita Federal — IRRF

Tabela, legislação, vigência e exemplos oficiais são reconciliados. HTML é sensor de estrutura conhecida, não contrato computacional autônomo.

### 10.3. INSS / MPS / eSocial

`INSS_TABLE_2026` é a entrada operacional registrada. Notícia anual pinned e `@@search` não são fallback automático.

---

## 11. Stack técnica

- Python 3.11+;
- `Decimal`;
- Pydantic v2;
- JSON Schema;
- pytest + Hypothesis;
- HTTPX com falhas tipadas;
- snapshots/candidatos content-addressed por SHA-256;
- estado operacional persistente.

---

## 12. Invariantes do sistema

1. dado histórico não recebe competência futura automaticamente;
2. ausência obrigatória não vira zero;
3. regra futura não é aplicada antes da vigência;
4. regra expirada não permanece ativa após substituição válida;
5. falha de fonte é distinta de coleta sem mudança;
6. mudança estrutural não é autopublicada;
7. artefato inválido não substitui last-good;
8. last-good só pode ser consumido quando a política permitir;
9. todo valor publicado possui proveniência rastreável;
10. todo cálculo crítico identifica versão de contrato/regras;
11. arredondamentos são determinísticos e testados;
12. exemplos oficiais são reproduzíveis;
13. `PARSER_INCOMPATIBLE` preserva evidência, mas não autoriza novo consumo;
14. `generated_at` não pode mascarar observação antiga;
15. H26–H29 não podem usar `dados_fiscais.json`, raw `main` ou `/sfa/v1/folha` como autoridade fiscal;
16. compatibilidade temporária precisa de consumidor identificado, finalidade restrita e prazo de retirada;
17. bundle de implantação só pode conter arquivos gerenciados conhecidos; dependências externas precisam ser declaradas e rollback precisa restaurar os bytes pré-deploy;
18. uma sucessora fiscal conhecida precisa permanecer conhecida entre requisições até que o próprio pacote da sucessora seja verificado; cache, 304 ou indisponibilidade posterior não podem ressuscitar silenciosamente a predecessora.

---

## 13. Snapshots e auditoria

```text
evidence/source-runtime-v1/
├── snapshots/
├── candidates/
└── state/

evidence/fiscal-authority-v1/   # snapshots oficiais usados pela release fiscal
evidence/governance-v1/         # manifests internos das technical_contract_rule
releases/fiscal-v1/              # releases imutáveis + current.json
```

Snapshots e candidatos são content-addressed; estado corrente fica materializado e estados anteriores permanecem no histórico Git. A política v1 não faz pruning automático.

---

## 14. Política de promoção

```text
DISCOVERED
  ↓
COLLECTED
  ↓
PARSED
  ↓
VALIDATED
  ↓
CLASSIFIED
  ↓
TESTED
  ↓
APPROVED_FOR_AUTO_PUBLISH ou REVIEW_REQUIRED
  ↓
PUBLISHED
```

A Fase 4 encerrou aquisição, parsing, evidência e fronteiras de compatibilidade. A Fase 5 encerrou diff semântico, aprovação e publicação. A Fase 6 encerrou a distribuição da release aos consumidores H26–H29 e retirou a autoridade fiscal legada do caminho de consumo.

---

## 15. Relação com as calculadoras H26–H29

A correção das quatro calculadoras está fechada sobre uma cadeia única. O consumidor não reconstrói, infere ou completa silenciosamente regra ausente.

- H26 usa o núcleo mensal canônico;
- H27 mantém 13º como assessment próprio, com adiantamento limitado ao contrato;
- H28 preserva período aquisitivo, direito/gozo/abono e incidências por componente;
- H29 permanece `partial_estimate`, com matriz eSocial `01/02/07/33` e fronteira de escopo explícita.

Toda execução crítica mantém `release_id`, `rule_id` e `rule_version` auditáveis.

---

## 16. Escopo inicial das calculadoras

### H26 — Salário líquido

INSS progressivo, IRRF mensal, desconto simplificado versus deduções, dependentes, redução mensal, ordem de cálculo, transições de faixas e arredondamento.

### H27 — 13º salário

Avos/regra dos 15 dias, admissão, referência remuneratória, primeira parcela no escopo suportado, parcela final, INSS/IRRF próprios e deduções confinadas à apuração.

### H28 — Férias

Período aquisitivo, direito/gozo, proporcionais, terço constitucional, abono como 1/3 do direito adquirido, incidências distintas e faltas somente nas faixas suportadas.

### H29 — Rescisão

Escopo deliberadamente parcial. Motivos `01`, `02`, `07` e `33`, saldo salarial e proporcionais conforme matriz. Aviso, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas permanecem fora da promessa automática.

---

## 17. Fases do remake

### Fase 0 — Baseline e governança

**Status: CONCLUÍDA**

README mestre, arquitetura inicial, princípios e invariantes congelados.

### Fase 1 — Inventário jurídico-fiscal

**Status: CONCLUÍDA**

Inventário, registro canônico de fontes e casos de referência formalizados. Artefatos históricos obrigatórios incluem:

- `docs/inventario-juridico-fiscal-v1.md`;
- `docs/rule-inventory-v1.json`;
- `docs/source-registry-v1.json`;
- `docs/phase1-closure.md`;
- `tests/reference_cases/phase1_reference_cases.json`.

### Fase 2 — Contrato Fiscal Canônico v1

**Status: CONCLUÍDA**

Cobertura 32/32, 18 famílias de payload, lifecycle, proveniência, SemVer, compatibilidade exata e gates negativos. O histórico v1.1 permanece preservado e a linha de publicação atual é v1.2.

### Fase 3 — Biblioteca fiscal e testes

**Status: CONCLUÍDA**

Engine determinístico em `Decimal`; regressões A01/A02; apurações mensal/13º/férias separadas; H29 limitado; memória auditável; property-based tests; gate `scripts/validate_phase3_gate.py` permanente.

### Fase 4 — Fontes e sensores

**Status: CONCLUÍDA**

Collector → snapshot → parser → candidato; RFB/INSS canônicos; Selic SGS 432; CDI SGS 12; evidência persistente; writers serializados; freshness específica; A01–A05 encerrados. Sob `PARSER_INCOMPATIBLE`, aplica-se `preserve_auditable_block_new_consumption`.

`dados_fiscais.json` permanece artefato de compatibilidade/evidência do produtor; desde a Fase 6 ele não é autoridade fiscal para H26–H29.

### Fase 5 — Diff semântico e gates de publicação

**Status: CONCLUÍDA**

Classificação semântica computada, promoção fail-closed, Contrato Fiscal v1.2, evidência oficial separada da governança interna, assembler 32/32, idempotência, release store imutável, bootstrap humano, sucessores com review-key vinculada e workflow `.github/workflows/fiscal-release-v12.yml`.

A ativação da primeira release v1.2 e sucessoras necessárias foi concluída posteriormente durante a Fase 6; a observação histórica de que isso estava pendente ao fechar a Fase 5 não representa o estado atual.

### Fase 6 — Migração dos consumidores

**Status: CONCLUÍDA**

A Fase 6 fechou os checkpoints:

- **C6.0a** — human-review gate determinístico;
- **C6.1** — WordPress/cache sobre manifest + release imutável;
- **C6.2** — `folha-core` como runtime compartilhado sem tabela fiscal duplicada;
- **C6.3** — H26 salário líquido CLT;
- **C6.4** — H27 décimo terceiro;
- **C6.5** — H28 férias CLT;
- **C6.6** — H29 rescisão CLT;
- **C6.7 — remoção controlada do legado** e fechamento formal.

C6.7 retirou as fórmulas fiscais duplicadas dos shortcodes antigos, aposentou o bootstrap fiscal legado e transformou `/sfa/v1/folha` em tombstone `410 Gone`. Naquele checkpoint, o único adaptador temporário preservado era `wordpress_table_shortcodes_v1`, exclusivamente para `ano_ref`, `inss_tabela` e `irrf_tabela`, com prazo `C7.1-before-production-deployment`. C7.1 posteriormente cumpriu esse prazo e retirou o adaptador do código executável.

H26–H29 consomem a mesma release canônica e não usam `dados_fiscais.json`, raw `main` ou `/sfa/v1/folha` como autoridade fiscal. Os gates C6.1–C6.7 permanecem no `Remake CI`.

C6.7 não afirma implantação no HostGator.

### Fase 7 — Fechamento e operação evergreen

**Status: EM ANDAMENTO**

Checkpoints concluídos:

- **C7.1 — bundle pré-deploy e rollback**: retirou `wordpress_table_shortcodes_v1`; promoveu o **plugin 2.7.0** com shortcodes informativos lendo diretamente a release; definiu 32 arquivos gerenciados e 11 dependências pré-existentes; constrói bundle determinístico; executa H26–H29 sobre os bytes empacotados; e prova apply/rollback exato em ambiente temporário sem mutar o HostGator.
- **C7.2 — E2E operacional, falhas e recuperação evergreen**: prova cold start, transient, ETag/304, indisponibilidade, `last_good`, sucessão real de release e recuperação até H26–H29. Corrige a lacuna pela qual uma sucessora conhecida podia ser esquecida entre requisições: `OPT_KNOWN_SUCCESSOR` passa a manter um latch persistente até a sucessora correspondente ser integralmente verificada. O fluxo fail-closed retorna `503` enquanto a sucessora conhecida não puder ser validada. `production_deployed=false` permanece obrigatório.
- **C7.3 — runbook, observabilidade e pré-flight de produção**: formalizou health operacional, pré-flight read-only, backup/rollback e criação segura de diretórios gerenciados. A inspeção real do HostGator fechou em `PASS/GO` com 32/32 destinos, 11/11 dependências, quatro diretórios planejados e zero bloqueios. A evidência remota foi preservada por SHA-256 e aprovada pelo validador C7.3. Nenhuma mutação ou implantação foi executada.

Checkpoint em andamento:

- **C7.4 — autorização e implantação controlada**: autorização single-use `c74-20260916-a741aa78-843dca3e` registrada como `AUTHORIZED_READY_TO_DEPLOY`, vinculada ao SHA do bundle, à evidência remota C7.3 e à release `a741aa…`. O executor exige pré-flight fresco sem drift, snapshot exato antes da primeira escrita, journal fora dos roots, aplicação atômica, health pós-write e rollback automático/manual. Neste estado de repositório, `deployment_authorized=true` e `production_deployed=false`.

Objetivos restantes:

- executar a única implantação autorizada no HostGator;
- preservar e registrar a evidência `APPLIED_HEALTHY` do journal;
- fechar formalmente C7.4 e os critérios objetivos do remake.

---

## 18. Decisões consolidadas

1. `sanida-dados-fiscais` é a fonte canônica das regras/dados fiscais usados pelas calculadoras.
2. O README é o centro documental do projeto.
3. A arquitetura deve prevenir obsolescência, não apenas corrigir 2026.
4. Scraping HTML é sensor/parser, não autoridade jurídica autônoma.
5. APIs oficiais estruturadas são preferidas quando disponíveis.
6. Mudança paramétrica e estrutural possuem políticas diferentes.
7. Mudança estrutural exige revisão humana.
8. Fallback estático que possa fingir atualidade é proibido.
9. Regras monetárias usam `Decimal` e arredondamento explícito.
10. H26–H29 são testadas individualmente mesmo quando compartilham motor.
11. `payroll_fiscal` e `financial_reference` possuem contratos e políticas independentes.
12. A cadeia é auditável: fonte → snapshot → parser → candidato → contrato/testes → release → consumidor.
13. Mensal, férias e 13º são apurações explícitas; `termination` é contexto de origem.
14. Redutor IR usa o rendimento tributável pertinente, não base pós-deduções por conveniência.
15. H27 não usa `total13 * 0.5` como regra universal.
16. Férias separam direito, gozo, abono, natureza e componentes tributários.
17. H29 permanece `partial_estimate` com motivos `01/02/07/33`.
18. H29 não usa divisor 30 universal para saldo de salário.
19. Inventário e registro da Fase 1 só reabrem por evidência concreta.
20. Toda PR do remake usa `Remake CI`; CI de validação é read-only.
21. `schema_version`, `contract_api_version` e `rule_version` usam compatibilidade exata testada.
22. `release_id` é content-addressed do payload imutável.
23. Release publicada é imutável; supersessão é declarada pela sucessora.
24. Campos narrativos não decidem operação fiscal.
25. Limiar proporcional de férias é parte explícita do contrato.
26. H29 cruza matriz geral com regras específicas e falha fechado em divergência.
27. Saldo salarial usa dias civis reais do mês no escopo padrão.
28. Memória comum é envelope auditável, não fusão de regras.
29. Fases só fecham com código, testes, gate e documentação verdes no mesmo head.
30. Fases encerradas não reabrem por redesign oportunista.
31. Coleta e interpretação são etapas distintas; snapshot nasce antes do parser.
32. Estado operacional preserva last-good sem torná-lo automaticamente consumível.
33. `INSS_TABLE_2026` usa somente a fonte registrada; notícia/search não são fallback.
34. `dados_fiscais.json` é compatibilidade do produtor, não autoridade dos consumidores H26–H29.
35. Evidência de produção não depende do filesystem efêmero do runner.
36. `financial_reference` não herda vigência jurídica de `payroll_fiscal`.
37. Selic usa SGS 432; CDI usa SGS 12 e annualização explícita no bridge.
38. FTP B3/Cetip não é fallback automático.
39. Falha financeira não autoriza fallback estático nem refresh fictício de timestamp.
40. `scraper.py` só consome `taxas_bacen.json` local evidence-gated.
41. Freshness é específica por série; data futura é rejeitada.
42. Writers de `main` são serializados.
43. Gates de Fases 3–6 são permanentes e cumulativos.
44. `PARSER_INCOMPATIBLE` preserva auditoria e bloqueia novo consumo.
45. `change_class` é calculado e relativo à transição.
46. Mudança numérica só é paramétrica se forma e semântica permanecerem iguais.
47. Contrato v1.2 separa autoridade jurídico-fiscal de governança técnica interna.
48. Release fiscal exige conjunto exato 32/32.
49. Bootstrap inicial é humano; schedule não pode bootstrapar.
50. Autopromoção depende de evidência hashada e parser versionado.
51. Fonte estrutural pode ser coletada automaticamente, mas mudança estrutural não é interpretada nem publicada automaticamente.
52. Sem delta, o resultado é `NO_PUBLISH_REQUIRED`, sem churn de release/timestamp.
53. A única API fiscal de dados para consumidores após C6.7 é `/wp-json/sfa/v1/fiscal-release`; `/wp-json/sfa/v1/folha` responde 410.
54. Shortcodes legados de cálculo não executam mais fórmulas; são pontes para H26/H27.
55. Adaptador temporário só pode permanecer com consumidor nomeado e prazo; `wordpress_table_shortcodes_v1` expirou e foi retirado em C7.1 antes de qualquer deploy.
56. A Fase 6 fecha migração de consumidores; deployment e operação evergreen pertencem à Fase 7.
57. O bundle de implantação distingue arquivos gerenciados de dependências pré-existentes; nenhum arquivo externo é inventado, e rollback restaura os bytes capturados antes da aplicação sem tocar em arquivos não gerenciados.
58. Conhecimento de sucessora fiscal é estado de segurança durável, não cache: depois que `current.json` anuncia uma sucessora, a predecessora fica bloqueada entre requisições até que o pacote da sucessora seja verificado ou o estado seja resolvido de forma explícita.
59. O refresh administrativo pode limpar transient e ETag, mas não pode apagar `OPT_KNOWN_SUCCESSOR`.
60. A cadeia evergreen é deliberadamente cacheada: transient válido evita rede; após expiração/refresh, `current.json` é revalidado por ETag e uma release nova só substitui `last_good` após validação integral.
61. Diretório gerenciado ausente não precisa ser criado manualmente antes do deploy: o pré-flight pode aprová-lo somente quando a cadeia for seguramente criável e registrar o plano em `planned_directory_creations`.
62. Rollback de diretório criado pelo deploy usa apenas `rmdir` quando vazio; deleção recursiva é proibida e conteúdo não gerenciado sempre prevalece sobre a limpeza automática.
63. `technical_go_no_go=GO` em C7.3 comprova prontidão técnica do ambiente, mas não autoriza nem executa implantação; autorização de deploy pertence a checkpoint explícito posterior.
64. A autorização C7.4 é single-use e vinculada ao SHA-256 do bundle, SHA-256 da evidência C7.3 e release id; qualquer drift antes da primeira escrita bloqueia a execução e exige nova decisão explícita.

---

## 19. Questões em aberto

As **Fases 0–6 estão formalmente concluídas**, C7.1–C7.3 estão concluídos e C7.4 está autorizado/em andamento. Restam para a Fase 7:

- executar a implantação controlada autorizada no HostGator;
- registrar o journal de produção e os health checks pós-write;
- fechar formalmente C7.4 e o remake após evidência de produção.

Se a Fase 7 revelar lacuna semântica objetiva, a correção volta explicitamente à camada responsável; não será escondida em collector, parser ou consumidor.

---

## 20. Próxima etapa

Executar a implantação single-use de **C7.4 — autorização e implantação controlada** usando `scripts/run_phase7_c74_controlled_deploy.py` e authorization id `c74-20260916-a741aa78-843dca3e`.

A execução deve reconstruir exatamente o bundle autorizado, revalidar a evidência C7.3, fazer pré-flight fresco sem drift, capturar snapshot/backup fora dos roots, aplicar somente os 32 destinos declarados e validar imediatamente `/fiscal-health`, `/fiscal-release`, `/folha` e H26–H29. Neste ponto `deployment_authorized=true` e `production_deployed=false`; somente uma execução `APPLIED_HEALTHY` pode promover produção a implantada.

---

## 21. Convenção de status

Cada fase usa apenas:

- `PENDENTE`
- `EM ANDAMENTO`
- `BLOQUEADA`
- `EM REVISÃO`
- `CONCLUÍDA`

Uma fase só é `CONCLUÍDA` quando seus critérios objetivos e gates correspondentes estão atendidos.

---

## 22. Changelog do README

### 2026-09-16 — C7.4 — autorização e implantação controlada iniciada

- recebida autorização explícita para iniciar C7.4 e executar a implantação controlada;
- autorização single-use `c74-20260916-a741aa78-843dca3e` vinculada ao bundle SHA-256 `843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1`, evidência C7.3 SHA-256 `e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7` e release `a741aa…`;
- criado executor transacional com pré-flight fresco e detecção de drift antes da primeira escrita;
- snapshot exato e journal single-use passam a ser obrigatórios fora dos roots de produção;
- aplicação usa escrita atômica e ordem controlada, deixando o arquivo principal do plugin por último;
- health pós-write exige `healthy`, release correta, `/folha` 410 e H26–H29 HTTP 200;
- qualquer falha de apply/health dispara rollback automático; rollback manual por journal cobre interrupção residual;
- simulação e gate C7.4 passam ao `Remake CI`;
- estado inicial do checkpoint: `AUTHORIZED_READY_TO_DEPLOY`, `deployment_authorized=true`, `production_deployed=false`.

### 2026-09-16 — C7.3 — runbook, observabilidade e pré-flight de produção

- criado pré-flight read-only fail-closed para os roots reais do HostGator;
- criados critérios objetivos de `GO/NO_GO`, health endpoint, runbook, backup e rollback;
- primeira inspeção real revelou parents ainda ausentes em `parts/` e `includes/`, sem falta de dependências;
- política foi corrigida para planejar diretórios seguramente criáveis em vez de exigir criação manual anterior;
- criado journal `planned_directory_creations` e simulação de rollback por `rmdir` somente quando vazio, com deleção recursiva proibida;
- novo pré-flight real fechou em `PASS/GO`: 32/32 destinos, 11/11 dependências, quatro diretórios planejados, zero `block_reasons`;
- evidência remota preservada por SHA-256 `e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7`;
- manifesto vinculado preservado por SHA-256 `843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1`;
- validador formal C7.3 schema 1.1.0 confirmou `PASS/GO`, `production_mutated=false` e `deployment_authorized=false`;
- C7.3 promovido a `CONCLUÍDO` sem qualquer deploy; próxima fronteira: **C7.4 — autorização e implantação controlada**.

### 2026-09-16 — C7.2 — E2E operacional, falhas e recuperação evergreen

- criado E2E operacional contra os traits WordPress reais reconstruídos no bundle C7.1;
- transição usa a release atual e seu `supersedes_release_id` real, sem fabricar regra fiscal;
- provados cold start, transient, `last_good`, ETag/304, indisponibilidade, sucessão e recuperação;
- detectada e corrigida a perda de memória de sucessora conhecida entre requisições;
- criado `OPT_KNOWN_SUCCESSOR` como latch persistente de segurança;
- falha do artefato da sucessora passa a manter a predecessora bloqueada inclusive em requisições posteriores e REST responde `503`;
- recuperação válida promove exatamente a sucessora, atualiza `last_good`/ETag e resolve o latch;
- release recuperada é executada por H26–H29 a partir dos bytes do bundle;
- debug administrativo passa a expor `fiscais_known_successor`;
- evidência operacional JSON e gate C7.2 passam a integrar o `Remake CI`;
- nenhum deploy foi realizado; `production_deployed=false`;
- próxima fronteira: **C7.3 — runbook, observabilidade e pré-flight de produção**.

### 2026-09-16 — C7.1 — bundle pré-deploy e rollback

- `wordpress_table_shortcodes_v1` retirado antes do deployment;
- plugin promovido para **2.7.0** e shortcodes `ano_ref`, `inss_tabela` e `irrf_tabela` passaram a ler a release canônica diretamente;
- criado `docs/phase7-c71-deployment-manifest-v1.json` com 32 arquivos gerenciados e 11 dependências pré-existentes;
- criado builder determinístico `scripts/build_phase7_c71_bundle.py`;
- criado simulador de apply/rollback `scripts/simulate_phase7_c71_deployment.py`;
- criado gate `scripts/validate_phase7_c71_gate.py` com E2E H26–H29 sobre os bytes empacotados;
- rollback exato, preservação de dependências e arquivos não gerenciados tornam-se requisitos automatizados;
- bundle passa a ser produzido no `Remake CI` como artefato temporário;
- nenhum arquivo foi implantado no HostGator neste checkpoint;
- Fase 7 promovida a `EM ANDAMENTO`; próxima fronteira: **C7.2**.

### 2026-09-16 — fechamento formal da Fase 6 / C6.7

- bootstrap humano e sucessoras v1.2 já publicados e integrados aos consumidores;
- WordPress/cache, `folha-core` e H26–H29 concluídos em C6.1–C6.6;
- `/sfa/v1/folha` aposentado como fonte de dados e transformado em tombstone 410;
- `sfa_bootstrap_folha` deixou de transportar números fiscais;
- calculadoras fiscais antigas embutidas no plugin foram removidas e os shortcodes viraram pontes para H26/H27;
- `dados_fiscais.json` permanece somente na fronteira de compatibilidade/evidência do produtor, sem autoridade sobre H26–H29;
- adaptador `wordpress_table_shortcodes_v1` restrito a três shortcodes informativos e com retirada obrigatória em `C7.1-before-production-deployment`;
- criado `docs/phase6-c67-closure.md` e `scripts/validate_phase6_c67_gate.py`;
- C6.7 integrado permanentemente ao `Remake CI`;
- Fase 6 promovida a `CONCLUÍDA` após gate verde no mesmo head;
- próxima etapa: **Fase 7 — Fechamento e operação evergreen**.

### 2026-09-15 — checkpoints C6.3–C6.6

- H26 migrou para `folha-core` e preservou A01;
- H27 ganhou assessment próprio, rounding técnico e branches fail-closed;
- H28 fechou direito/gozo/abono, incidências separadas e período aquisitivo;
- H29 fechou matriz eSocial `01/02/07/33`, saldo por dias civis e `partial_estimate`.

### 2026-09-14 — fechamento formal da Fase 5 / Contrato Fiscal v1.2

- semantic diff computado e fail-closed consolidado;
- política de autopromoção separada de revisão estrutural;
- publicação exige inventário exato 32/32;
- Contrato Fiscal evoluído para schema/API 1.2.0;
- evidência interna hash-addressed separada de autoridade governamental;
- release store content-addressed e `current.json` atômico;
- bootstrap definido como exclusivamente humano;
- workflow permanente `fiscal-release-v12.yml` e gate formal da Fase 5 criados.

### 2026-09-13 — fechamento formal da Fase 4

- A05 resolvido com `preserve_auditable_block_new_consumption`;
- last-good sob `PARSER_INCOMPATIBLE` permanece auditável, mas não alimenta artefato novo;
- gates de fundação, fronteira de produção e fechamento formal tornaram-se permanentes.

### 2026-09-13 — Fases 1–3

- registro canônico de fontes e inventário machine-readable congelados;
- Contrato Fiscal fechado com cobertura 32/32 e 18 famílias;
- biblioteca determinística em `Decimal`, memória auditável, regressões A01/A02, H27/H28/H29 e property-based tests consolidados;
- `Remake CI` instituído como gate do remake.

### 2026-09-13 — criação

- criado o documento-mestre;
- registrada a linha de base;
- formalizados princípios, arquitetura e fases do remake.
