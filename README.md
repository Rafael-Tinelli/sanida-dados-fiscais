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

Este README é o **documento-mestre do projeto**.

Ele deve registrar e permanecer sincronizado com:

- objetivos e limites do sistema;
- arquitetura vigente e arquitetura-alvo;
- decisões metodológicas;
- fontes oficiais;
- contratos de dados e regras;
- invariantes de segurança;
- fases de implementação;
- critérios de promoção de releases;
- riscos conhecidos;
- decisões ainda pendentes;
- estado de cada etapa.

Mudanças relevantes de arquitetura ou metodologia não devem existir apenas no código: devem ser refletidas aqui.

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

A arquitetura inicial conseguia capturar vários parâmetros corretos, mas não modelava suficientemente a **semântica jurídica das regras**.

O exemplo central é a redução mensal do IRRF de 2026. Não basta preservar coeficientes numéricos; o contrato deve preservar também a grandeza jurídica sobre a qual a fórmula incide.

```text
rule_id = irrf.monthly.reduction
applies_to = rendimento tributável sujeito à incidência mensal
effective_from = 2026-01-01
```

A ausência dessa semântica permitia que números corretos fossem aplicados sobre a variável errada.

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

Scrapers e parsers são sensores/coletadores. Eles podem detectar mudança, extrair parâmetros conhecidos, preservar snapshots e apontar divergências.

Eles não devem:

- inventar interpretação para regra nova;
- promover silenciosamente mudança estrutural;
- transformar ausência em zero;
- relabelar dado antigo com competência nova;
- tratar acessibilidade da página como prova suficiente de validade jurídica.

### 5.3. Vigência é parte do dado

Todo parâmetro ou regra relevante deve declarar, quando aplicável, início/fim de vigência, competência, data de publicação, data de coleta/verificação, fonte normativa, fonte operacional e versão do contrato.

`generated_at` nunca deve ser confundido com vigência.

### 5.4. Falhar com segurança

O sistema distingue coleta sem mudança, coleta com mudança, fonte indisponível, parser incompatível, mudança estrutural, contrato inválido, last-good ainda utilizável e last-good apenas preservado para auditoria.

Falha de coleta ou parser nunca deve produzir um número aparentemente atual por conveniência.

### 5.5. Nunca relabelar dado antigo como novo

Sem contrato válido para nova vigência, o estado correto é indisponibilidade/revisão, não atualização fictícia.

### 5.6. Zero, ausência e não aplicabilidade são estados distintos

Valor zero real, valor ausente, não aplicável, não publicado, não encontrado, fonte indisponível, parser falhou, regra ainda não vigente e regra expirada não são equivalentes.

### 5.7. Dinheiro não deve depender de `float`

O motor fiscal usa `Decimal` e política explícita de arredondamento. Conversões para números JSON legados só podem ocorrer em fronteiras de compatibilidade identificadas.

### 5.8. Toda regra crítica precisa de teste de referência

Exemplos oficiais e regressões objetivas devem virar testes automatizados, complementados por invariantes e property-based testing.

---

## 6. Classes de atualização

### Classe A — fonte oficial estruturada

Pode admitir automação quando schema, metadados, vigência, faixas e testes permanecem válidos.

### Classe B — mudança paramétrica em regra conhecida

Ex.: novas faixas mantendo a mesma estrutura jurídica. Pode admitir automação apenas após validação forte e política da Fase 5.

### Classe C — mudança estrutural de regra

Ex.: nova hipótese, nova base de incidência, exceção, ordem de cálculo, regra removida ou novo regime. Estado obrigatório: `REVIEW_REQUIRED`.

### Classe D — fonte indisponível ou inconclusiva

Pode preservar last-good apenas dentro da política explícita de validade e sem fingir nova observação.

---

## 7. Arquitetura-alvo

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
Consumer contract
      │
      ├── WordPress/plugin/cache
      ├── folha-core
      └── H26 / H27 / H28 / H29
```

---

## 8. Separação de domínios

### 8.1. `payroll_fiscal`

Inclui INSS do empregado, IRRF mensal, desconto simplificado, dependentes, redução mensal, 13º, férias, terço constitucional, abono pecuniário e regras necessárias à rescisão dentro do escopo declarado.

### 8.2. `financial_reference`

Inclui Selic, CDI e eventuais indicadores financeiros futuros. Esses dados possuem ciclos, freshness, fallback e política de consumo próprios e não herdam automaticamente a semântica jurídica de folha.

---

## 9. Contrato Fiscal Canônico v1.1

A **Fase 2 está concluída**. O contrato possui:

- modelos Pydantic em `sanida_fiscal/types_v1.py` e `sanida_fiscal/contract_v1.py`;
- JSON Schema público em `contracts/fiscal-contract-v1.schema.json`;
- cobertura de **32/32 regras** do inventário;
- **18 famílias tipadas de payload**;
- compatibilidade exata/fail-closed;
- `release_id` content-addressed para releases validadas/publicadas;
- releases publicadas imutáveis com supersessão explícita.

Cada regra expressa, conforme aplicável, `rule_id`, versão, domínio, consumidores/contextos, target semântico, predicados, dependências, ordem de cálculo, competência, vigência, arredondamento, payload, proveniência, qualidade, classe de mudança e política de atualização.

O contrato v1.1 incorporou explicitamente o limiar de 15 dias da aquisição proporcional de férias depois que a Fase 3 revelou essa lacuna computacional. Leitores 1.0 não aceitam silenciosamente 1.1.

---

## 10. Fontes oficiais

### 10.1. Banco Central

O domínio `financial_reference` usa registro próprio em `docs/financial-source-registry-v1.json`.

- Selic: **BCB SGS 432**;
- CDI: **BCB SGS 12** na unidade diária de origem;
- annualização do CDI em 252 dias úteis apenas na fronteira de compatibilidade;
- B3 permanece referência metodológica/corroboração do benchmark DI, não fallback automático.

### 10.2. Receita Federal — IRRF

Tabela, legislação, vigência e exemplos oficiais devem ser reconciliados. HTML é sensor de estrutura conhecida, não contrato computacional autônomo.

### 10.3. INSS / MPS / eSocial

`INSS_TABLE_2026` é a entrada operacional registrada. Notícia anual pinned e `@@search` não são fallback automático.

---

## 11. Stack técnica

### Núcleo

- Python 3.11+;
- `Decimal`;
- Pydantic v2;
- JSON Schema.

### Testes

- pytest;
- Hypothesis;
- casos oficiais;
- fixtures/snapshots reproduzíveis.

### Fontes

- HTTPX com timeouts e falhas tipadas;
- snapshots e candidatos content-addressed por SHA-256;
- estado operacional persistente.

### Princípio de dependência

Bibliotecas só entram no caminho crítico quando melhoram auditabilidade, segurança ou manutenção objetivamente.

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
13. `PARSER_INCOMPATIBLE` preserva evidência, mas não autoriza consumo novo;
14. `generated_at` não pode mascarar observação antiga.

---

## 13. Snapshots e auditoria

A evidência deve permitir responder qual conteúdo oficial foi observado, quando, em qual recurso, com qual hash, parser, candidato, contrato, testes, release e consumidor.

Na Fase 4, a persistência de produção foi fixada em:

```text
evidence/source-runtime-v1/
├── snapshots/
├── candidates/
└── state/
```

Snapshots e candidatos são content-addressed; estado corrente fica materializado e estados anteriores permanecem no histórico Git. A política v1 não faz pruning automático.

---

## 14. Política de promoção

Nenhum novo contrato/release é promovido apenas porque um scraper terminou sem exceção.

Lifecycle conceitual:

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

A Fase 4 encerra aquisição, parsing, evidência e fronteiras de compatibilidade. A classificação semântica e a promoção pertencem à Fase 5.

---

## 15. Relação com as calculadoras H26–H29

O repositório é a origem canônica; as calculadoras são consumidores.

```text
sanida-dados-fiscais
        ↓
contrato/release validada
        ↓
camada WordPress/cache
        ↓
folha-core
        ↓
H26 / H27 / H28 / H29
```

O consumidor não deve reconstruir, inferir ou completar silenciosamente regra ausente. A correção das calculadoras só estará encerrada quando produtor e consumidor compartilharem o mesmo contrato semântico.

Outras páginas e matérias podem futuramente consumir valores/metadados do repositório, mas não definem o escopo da migração atual das calculadoras.

---

## 16. Escopo inicial das calculadoras

### H26 — Salário líquido

- INSS progressivo;
- IRRF mensal;
- desconto simplificado versus deduções;
- dependentes;
- redução mensal vigente;
- ordem correta de cálculo;
- transições de faixas;
- arredondamento.

### H27 — 13º salário

- avos e regra dos 15 dias;
- ano de admissão;
- referência remuneratória;
- primeira parcela/adiantamento dentro do escopo suportado;
- parcela final;
- INSS e IRRF próprios do 13º;
- deduções por apuração.

### H28 — Férias

- período aquisitivo;
- direito/gozo;
- férias integrais e proporcionais;
- terço constitucional;
- abono pecuniário como 1/3 do direito adquirido;
- incidências distintas do principal e do terço sobre abono;
- faltas somente quando efetivamente suportadas.

### H29 — Rescisão

Escopo v1 deliberadamente parcial. Suporta inicialmente os motivos eSocial `01`, `02`, `07` e `33`, com saldo salarial e proporcionais conforme a matriz fechada. Aviso, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas permanecem fora do total.

---

## 17. Fases do remake

### Fase 0 — Baseline e governança

**Status: CONCLUÍDA**

README mestre, arquitetura inicial, princípios e invariantes congelados.

### Fase 1 — Inventário jurídico-fiscal

**Status: CONCLUÍDA**

Artefatos principais:

- `docs/inventario-juridico-fiscal-v1.md`;
- `docs/rule-inventory-v1.json`;
- `docs/source-registry-v1.json`;
- `docs/phase1-closure.md`;
- `tests/reference_cases/phase1_reference_cases.json`.

Nenhuma variável relevante de H26–H29 ficou sem origem/semântica definida no inventário.

### Fase 2 — Contrato Fiscal Canônico v1

**Status: CONCLUÍDA**

Fechamento com cobertura 32/32, 18 famílias de payload, lifecycle, proveniência, versionamento, compatibilidade exata e gates negativos.

### Fase 3 — Biblioteca fiscal e testes

**Status: CONCLUÍDA**

Estado final inclui:

- engine determinístico em `Decimal`;
- cinco casos oficiais RFB 2026;
- regressão A01: renda tributável `6000.00`, base IR `5350.40`, redutor aplicado sobre `6000.00`, IRRF final `382.88`;
- mensal, 13º e férias como apurações distintas;
- 13º com avos, regra dos 15 dias, adiantamento simples suportado e branches não modeladas fail-closed;
- férias ancoradas no período aquisitivo, A02 `01/09/2025 → 31/03/2026 = 7/12`;
- abono com principal e terço constitucional separados nas incidências;
- H29 limitado à matriz eSocial `01/02/07/33`;
- saldo salarial por dias civis reais do mês no escopo padrão;
- memória auditável comum sem fusão semântica;
- fechamento histórico com **150 testes verdes** e `scripts/validate_phase3_gate.py` permanente.

### Fase 4 — Fontes e sensores

**Status: CONCLUÍDA**

A Fase 4 fechou:

- separação `source registry → collector → raw snapshot → parser → normalized candidate`;
- distinção `SOURCE_UNAVAILABLE` × `PARSER_INCOMPATIBLE`;
- parsers canônicos para RFB e INSS;
- eliminação do discovery/pinned do INSS;
- bridge explícito para `dados_fiscais.json 2.2.0`;
- persistência durável de snapshots/candidatos/estado em `evidence/source-runtime-v1`;
- Selic BCB SGS 432 e CDI BCB SGS 12;
- remoção do FTP Cetip/B3 e de fallbacks financeiros estáticos;
- `taxas_bacen.json 1.4.0` como artefato financeiro de compatibilidade evidence-gated;
- freshness específica: Selic persistente até mudança; CDI diário com máximo de 7 dias corridos; datas futuras rejeitadas;
- writers de `main` serializados pelo mesmo concurrency group;
- cadeia de evidência composta RFB + INSS + Selic + CDI;
- auditoria A01–A05 concluída.

#### A05 — decisão final

Política:

```text
preserve_auditable_block_new_consumption
```

Se a tentativa corrente entra em `PARSER_INCOMPATIBLE`, o last-good permanece preservado e verificável para auditoria, mas fica em quarentena: não pode formar novo `taxas_bacen.json`, não pode entrar em novo `dados_fiscais.json`, não renova `generated_at` e não volta a ser consumível até que uma coleta corrente produza novamente `PARSED` com evidência válida.

Artefatos de fechamento:

- `docs/phase4-sources-sensors-v1.md`;
- `docs/phase4-final-boundary-audit-v1.json`;
- `docs/phase4-financial-reference-policy-v1.json`;
- `docs/phase4-production-persistence-v1.json`;
- `docs/phase4-closure-gate.md`;
- `scripts/validate_phase4_foundation.py`;
- `scripts/validate_phase4_preclosure_gate.py`;
- `scripts/validate_phase4_closure_gate.py`.

No head técnico de fechamento, o Remake CI run `34797878486` passou com **210 testes**, Phase 3 closure PASS, Phase 4 foundation PASS, production boundary PASS com A05 corrigido e formal closure gate PASS.

### Fase 5 — Diff semântico e gates de publicação

**Status: PENDENTE**

Objetivos:

- classificar refresh sem mudança, mudança paramétrica, vigência e mudança estrutural;
- bloquear promoção insegura;
- definir critérios de confirmação aplicáveis;
- publicar somente releases validadas;
- tornar o estado operacional observável sem confundir coleta com aprovação.

### Fase 6 — Migração dos consumidores

**Status: PENDENTE**

Objetivos:

- migrar plugin/cache;
- migrar `folha-core`;
- corrigir H26–H29 contra o contrato/release canônico;
- remover duplicação de regra fiscal nos consumidores.

### Fase 7 — Fechamento e operação evergreen

**Status: PENDENTE**

Objetivos:

- testes ponta a ponta;
- simulação de falhas e mudanças;
- validação da atualização automática;
- documentação operacional;
- critérios objetivos de encerramento.

---

## 18. Decisões consolidadas

1. `sanida-dados-fiscais` é a fonte canônica das regras/dados fiscais usados pelas calculadoras.
2. O README é o centro documental do projeto.
3. A solução não é um patch para 2026: a arquitetura deve prevenir obsolescência futura.
4. Scraping HTML é sensor/parser, não autoridade jurídica autônoma.
5. APIs oficiais estruturadas são preferidas quando disponíveis.
6. Mudança paramétrica e estrutural possuem políticas diferentes.
7. Mudança estrutural exige revisão humana.
8. Fallback estático que possa fingir atualidade é proibido.
9. Regras monetárias usam `Decimal` e rounding explícito.
10. H26–H29 são testadas individualmente mesmo quando compartilham motor.
11. `payroll_fiscal` e `financial_reference` possuem contratos e políticas de validade independentes.
12. A cadeia deve ser auditável retroativamente: fonte → snapshot → parser → candidato → contrato/testes → release → consumidor.
13. Mensal, férias e 13º são apurações explícitas; `termination` é contexto de origem, não quarto tipo de IRRF.
14. Redutor IR 2026 usa o rendimento tributável pertinente, nunca base pós-deduções por conveniência.
15. H27 não usa `total13 * 0.5` como regra universal; branches não suportadas falham fechadas.
16. Férias separam direito, gozo, abono, natureza e componentes tributários.
17. H29 v1 permanece `partial_estimate` com motivos eSocial `01/02/07/33`.
18. H29 padrão não usa divisor 30 universal para saldo de salário.
19. Inventário e registro de fontes da Fase 1 só são reabertos por evidência concreta.
20. Toda PR do remake usa `Remake CI`; o CI de validação é read-only.
21. `schema_version`, `contract_api_version` e `rule_version` seguem SemVer com compatibilidade exata testada.
22. `release_id` é content-addressed do payload imutável.
23. Release publicada é imutável; supersessão é declarada pela sucessora.
24. Campos narrativos não decidem operação fiscal.
25. Limiar proporcional de férias é parte explícita do contrato v1.1.
26. H29 cruza a matriz geral com regras específicas; divergência/motivo não suportado é erro.
27. Saldo salarial usa dias civis reais do mês no escopo padrão.
28. Memória comum é envelope auditável, não fusão de regras.
29. Fase 3 só foi encerrada com contrato, suíte, gate e documentação verdes no mesmo head.
30. Fase 3 não é reaberta por redesign oportunista.
31. Na Fase 4, coleta e interpretação são etapas distintas e o snapshot nasce antes do parser.
32. Estado operacional preserva last-good sem transformá-lo automaticamente em autorização de uso.
33. `INSS_TABLE_2026` usa somente a fonte registrada; notícia/search não são fallback.
34. `dados_fiscais.json` é artefato temporário de compatibilidade e nova escrita exige candidatos atuais válidos.
35. Evidência de produção não depende do filesystem efêmero do runner.
36. `financial_reference` não herda vigência jurídica de `payroll_fiscal`.
37. Selic usa SGS 432; CDI usa SGS 12 e annualização explícita no bridge.
38. FTP B3/Cetip não é input/fallback automático após a migração.
39. Falha financeira não autoriza fallback estático nem refresh de timestamp.
40. `scraper.py` só consome `taxas_bacen.json` local 1.4.0 com evidência válida.
41. Freshness é específica por série; data futura é rejeitada.
42. `main.yml` e `taxas.yml` são writers serializados do mesmo `main`/runtime.
43. Fundação, fronteira de produção e fechamento formal da Fase 4 possuem gates separados e permanentes.
44. Sob `PARSER_INCOMPATIBLE`, o last-good financeiro é **preservado para auditoria e bloqueado para novo consumo** (`preserve_auditable_block_new_consumption`).
45. O fechamento da Fase 4 não antecipa semantic diff/promoção da Fase 5 nem migração de consumidores da Fase 6.

---

## 19. Questões em aberto

As Fases 0–4 estão formalmente concluídas. Questões restantes pertencem às fases posteriores:

- critérios exatos de confirmação de mudanças paramétricas — Fase 5;
- semantic diff e classificação de mudanças — Fase 5;
- gates de promoção/publicação canônica — Fase 5;
- distribuição para WordPress/SFA e migração dos consumidores — Fase 6;
- testes ponta a ponta e operação evergreen — Fase 7.

Se uma fase posterior revelar lacuna semântica objetiva, a correção deve voltar explicitamente ao contrato/camada responsável; não será escondida em collector, parser ou consumidor.

---

## 20. Próxima etapa

Iniciar a **Fase 5 — Diff semântico e gates de publicação**.

A Fase 5 deve consumir os candidatos e evidências produzidos pela Fase 4 para distinguir, de forma executável, pelo menos:

- `SOURCE_REFRESH_NO_CHANGE`;
- `PARAMETER_CHANGE`;
- `EFFECTIVE_DATE_CHANGE`;
- `STRUCTURAL_CHANGE`;
- `RULE_ADDED`;
- `RULE_REMOVED`;
- estados operacionais que não autorizam promoção.

Ela deve definir quando uma mudança pode ser promovida automaticamente e quando exige `REVIEW_REQUIRED`, sem migrar ainda WordPress/`folha-core`/H26–H29.

### Ativação da Fase 4 após merge

O merge da Fase 4 ativa a infraestrutura de fontes/sensores, não as calculadoras.

A ordem segura é:

1. `taxas.yml` produzir primeiro um `taxas_bacen.json 1.4.0` evidence-gated;
2. depois `main.yml` poderá produzir novo `dados_fiscais.json` com a cadeia das quatro fontes comprovada;
3. Fase 5 passa a governar semantic diff/promoção;
4. WordPress, `folha-core` e H26–H29 permanecem congelados até a Fase 6.

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

### 2026-09-13 — fechamento formal da Fase 4

- A05 resolvido com política `preserve_auditable_block_new_consumption`;
- last-good sob `PARSER_INCOMPATIBLE` permanece auditável, mas não pode alimentar artefato novo;
- `sanida_fiscal/financial_evidence_v1.py` separa verificação de consumo de verificação audit-only;
- adicionado teste de regressão A05;
- auditoria final registra A01–A05 como `CORRECTED` e `closure_authorized=true`;
- criado `scripts/validate_phase4_closure_gate.py` e integrado ao `Remake CI`;
- criado `docs/phase4-closure-gate.md`;
- Remake CI run `34797878486`: **210 passed**, Fase 3 PASS, Fase 4 foundation PASS, production boundary PASS e formal closure PASS;
- Fase 4 promovida a `CONCLUÍDA`;
- próxima etapa: **Fase 5 — Diff semântico e gates de publicação**.

### 2026-09-13 — correções A01–A04 da auditoria de fronteira

- `scraper.py` passou a exigir `taxas_bacen.json` local 1.4.0, proveniência SGS 432/12 e evidência persistida;
- gate de `dados_fiscais.json` passou a verificar RFB, INSS, Selic e CDI;
- freshness específica instituída para Selic/CDI;
- writers alinhados ao mesmo concurrency group e fila;
- criada auditoria machine-readable e pre-closure gate;
- checkpoint chegou a **209 testes verdes**, ainda com A05 aberto naquele momento.

### 2026-09-13 — migração de Selic e CDI na Fase 4

- criado registro próprio de `financial_reference`;
- Selic migrou para SGS 432 e CDI para SGS 12;
- removidos FTP Cetip/B3 e fallbacks financeiros estáticos;
- criado `taxas_bacen.json 1.4.0` como compatibilidade evidence-gated;
- checkpoint chegou a **200 testes verdes**.

### 2026-09-13 — persistência real de evidências

- definido `evidence/source-runtime-v1` como backend Git-tracked;
- snapshots/candidatos content-addressed por SHA-256;
- estado operacional separa tentativa corrente e last-good;
- `main.yml` só stageia artefato comprovado;
- checkpoint chegou a **187 testes verdes**.

### 2026-09-13 — fronteira `scraper.py → dados_fiscais.json`

- catálogo único de RFB/INSS;
- parsers/discovery legados removidos de `scraper.py`;
- bridge explícito para compatibilidade 2.2.0;
- fallback fiscal estático removido;
- checkpoint chegou a **180 testes verdes**.

### 2026-09-13 — INSS canônico

- divergência `INSS_TABLE_2026` × notícia anual encerrada;
- notícia e `@@search` proibidos como fallback automático;
- parser canônico de quatro faixas criado;
- checkpoint chegou a **172 testes verdes**.

### 2026-09-13 — primeiro pipeline real RFB

- `RFB_IRRF_TABLE_2026` materializado em collector → snapshot → parser → candidato;
- estado operacional persistente e validadores HTTP adicionados;
- mudança de parser força refetch;
- checkpoint chegou a **166 testes verdes**.

### 2026-09-13 — início da Fase 4

- superfície legada de coleta inventariada;
- separação collector/snapshot/parser/candidato criada;
- `SOURCE_UNAVAILABLE` e `PARSER_INCOMPATIBLE` separados;
- primeiro gate de fundação criado;
- checkpoint inicial com **160 testes verdes**.

### 2026-09-13 — fechamento formal da Fase 3

- biblioteca determinística, memória auditável e invariantes encerradas;
- Fase 3 fechou com **150 testes verdes** e gate permanente;
- Fase 4 passou a ser a próxima etapa.

### 2026-09-13 — checkpoints da Fase 3

- implementadas apurações separadas mensal/13º/férias;
- 13º ganhou núcleo próprio;
- contrato evoluiu para v1.1 para explicitar férias proporcionais;
- H29 ganhou matriz limitada e fail-closed;
- memória comum auditável e property-based tests foram consolidados.

### 2026-09-13 — fechamento da Fase 2

- Contrato Fiscal Canônico fechado com SemVer, compatibilidade exata, identidade content-addressed, imutabilidade/supersessão e auditoria semântica;
- cobertura **32/32** e **18 famílias**;
- suíte específica fechada em **52 testes verdes**.

### 2026-09-13 — fechamento da Fase 1

- registro canônico de fontes e inventário machine-readable congelados;
- casos de referência ampliados;
- A01, A02, abono e escopo H29 formalizados;
- `Remake CI` instituído.

### 2026-09-13 — criação

- criado o documento-mestre;
- registrada a linha de base;
- formalizados princípios, arquitetura e fases do remake.
