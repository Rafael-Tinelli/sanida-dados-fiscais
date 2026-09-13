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

Ele deve ser atualizado conforme o trabalho avança e registrar:

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

## 3. Estado inicial observado — 13/09/2026

Na linha de base deste remake, o repositório possui essencialmente:

- `scraper.py` — coleta IRRF e INSS em páginas oficiais e incorpora taxas;
- `update_taxas.py` — coleta Selic no SGS/BCB e CDI em fonte Cetip/B3 via FTP;
- `dados_fiscais.json` — artefato agregado atualmente consumível;
- `taxas_bacen.json` — artefato específico de taxas;
- `.github/workflows/main.yml` — atualização de `dados_fiscais.json`;
- `.github/workflows/taxas.yml` — atualização de `taxas_bacen.json`;
- `requirements.txt` — dependências atuais reduzidas a Requests e BeautifulSoup.

O `dados_fiscais.json` observado na abertura desta fase declara:

- `schema_version: 2.2.0`;
- `ano: 2026`;
- `generated_at_utc: 2026-07-03T11:41:54Z`;
- IRRF originado da Receita Federal;
- INSS originado de página oficial do INSS;
- Selic originada do SGS/BCB;
- CDI originado de arquivo FTP Cetip/B3.

Este estado é tratado como **baseline**, não como arquitetura final.

---

## 4. Problema que estamos resolvendo

A arquitetura atual consegue capturar vários parâmetros corretos, mas não modela suficientemente a **semântica jurídica das regras**.

Exemplo: a redução mensal do IRRF de 2026 pode ser representada numericamente por limites e coeficientes, mas isso é insuficiente se o contrato não disser **sobre qual grandeza jurídica a fórmula deve ser aplicada**.

Não basta preservar:

```text
A = 978,62
B = 0,133145
```

O contrato precisa preservar também algo semanticamente equivalente a:

```text
rule_id = irrf.monthly.reduction
applies_to = rendimento tributável sujeito à incidência mensal
effective_from = 2026-01-01
```

A ausência dessa semântica permite que números corretos sejam aplicados de maneira juridicamente incorreta.

---

## 5. Princípios não negociáveis

### 5.1. Fonte oficial primeiro

Dados e regras devem ser derivados prioritariamente de fontes oficiais.

Hierarquia geral:

1. API/dataset oficial estruturado;
2. ato normativo oficial;
3. página operacional oficial do órgão competente;
4. exemplo oficial de aplicação;
5. fonte oficial complementar para confirmação.

Fontes privadas podem ser usadas para investigação ou diagnóstico, mas não devem se tornar autoridade canônica para uma regra fiscal quando houver fonte oficial apropriada.

### 5.2. Scraper não é autoridade jurídica

Scrapers e parsers são sensores/coletadores.

Eles podem:

- detectar mudança;
- extrair parâmetros conhecidos;
- preservar snapshots;
- apontar divergências;
- alimentar validações.

Eles não devem:

- inventar interpretação para regra nova;
- promover silenciosamente uma mudança estrutural;
- transformar ausência de dado em zero;
- relabelar dado antigo com competência nova;
- considerar página acessível como prova suficiente de validade jurídica.

### 5.3. Vigência é parte do dado

Todo parâmetro ou regra relevante deve declarar, quando aplicável:

- início de vigência;
- fim de vigência;
- competência;
- data de publicação;
- data de coleta/verificação;
- fonte normativa;
- fonte operacional;
- versão do contrato.

`generated_at` nunca deve ser confundido com vigência da regra.

### 5.4. Falhar com segurança

Quando uma fonte falhar, o sistema deve distinguir claramente:

- fonte consultada com sucesso e sem alteração;
- fonte consultada com sucesso e com alteração;
- fonte indisponível;
- parser incompatível com novo formato;
- mudança estrutural detectada;
- contrato inválido;
- `last-good` ainda juridicamente vigente;
- `last-good` expirado ou de vigência incerta.

Uma falha de coleta nunca deve produzir um número aparentemente válido por conveniência.

### 5.5. Nunca relabelar dado antigo como novo

É proibido usar parâmetros históricos e atribuir a eles automaticamente o ano ou competência corrente.

Se não houver contrato válido para a nova vigência, o estado correto é de indisponibilidade/revisão, não de atualização fictícia.

### 5.6. Zero, ausência e não aplicabilidade são estados distintos

O contrato deve distinguir pelo menos:

- valor zero real;
- valor ausente;
- não aplicável;
- não publicado;
- não encontrado;
- fonte indisponível;
- parser falhou;
- regra ainda não vigente;
- regra expirada.

### 5.7. Dinheiro não deve depender de `float`

O novo motor fiscal deve usar representação decimal apropriada para valores monetários e regras de arredondamento explicitamente documentadas.

### 5.8. Toda regra crítica precisa de teste de referência

Sempre que um órgão oficial publicar exemplos de aplicação, esses exemplos devem ser transformados em testes automatizados.

Além dos exemplos pontuais, o sistema deve possuir invariantes e testes de propriedades.

---

## 6. Classes de atualização

Toda alteração detectada deve ser classificada antes da publicação.

### Classe A — fonte oficial estruturada

Exemplo: API SGS do Banco Central.

Pode admitir promoção automática quando:

- schema esperado continua válido;
- metadados são coerentes;
- controles de faixa e consistência passam;
- a vigência/competência é interpretada corretamente;
- testes aplicáveis passam.

### Classe B — mudança paramétrica em regra conhecida

Exemplos:

- novas faixas de INSS mantendo a mesma estrutura jurídica;
- alteração de dedução por dependente;
- atualização de limites de tabela;
- alteração de alíquotas mantendo o mesmo modelo.

Pode admitir automação após validação forte, preferencialmente com confirmação oficial independente e regressão completa.

### Classe C — mudança estrutural de regra

Exemplos:

- nova hipótese de redução;
- mudança de base jurídica de incidência;
- nova exceção;
- nova ordem de cálculo;
- regra retirada;
- novo regime de tributação.

Estado obrigatório:

```text
REVIEW_REQUIRED
```

Não deve ser promovida automaticamente.

### Classe D — fonte indisponível ou inconclusiva

O sistema pode conservar um `last-good` somente se:

- a vigência desse contrato ainda for válida;
- não houver evidência de substituição;
- o estado de saúde indicar explicitamente que a nova consulta falhou.

Nunca deve criar fallback fiscal fictício.

---

## 7. Arquitetura-alvo

```text
Fontes oficiais
      │
      ├── APIs/datasets estruturados
      ├── atos normativos
      ├── páginas operacionais
      └── exemplos oficiais
      │
      ▼
Collectors / Sensors
      │
      ▼
Raw snapshots imutáveis
      │
      ▼
Parsers
      │
      ▼
Normalized legal/fiscal contracts
      │
      ├── semântica da regra
      ├── parâmetros
      ├── vigência
      ├── proveniência
      └── estado de qualidade
      │
      ▼
Semantic diff / change classifier
      │
      ├── SOURCE_REFRESH_NO_CHANGE
      ├── PARAMETER_CHANGE
      ├── EFFECTIVE_DATE_CHANGE
      ├── STRUCTURAL_CHANGE
      ├── RULE_ADDED
      ├── RULE_REMOVED
      ├── SOURCE_UNAVAILABLE
      └── PARSER_INCOMPATIBLE
      │
      ▼
Validation gates
      │
      ├── schema
      ├── casos oficiais
      ├── regressão
      ├── invariantes
      └── property-based testing
      │
      ▼
Versioned release
      │
      ▼
Consumer contract
      │
      ├── WordPress/plugin
      ├── folha-core
      └── H26 / H27 / H28 / H29
```

---

## 8. Separação de domínios

O projeto deve distinguir dois domínios com ciclos de vida diferentes.

### 8.1. Contratos jurídico-fiscais de folha

Incluem, conforme o escopo final:

- INSS do empregado;
- IRRF mensal;
- desconto simplificado mensal;
- dedução por dependente;
- redução mensal do IRRF;
- 13º salário;
- férias;
- terço constitucional;
- abono pecuniário;
- regras necessárias ao cálculo de rescisão dentro do escopo declarado pela ferramenta.

### 8.2. Dados financeiros/de referência

Incluem, por exemplo:

- Selic;
- CDI;
- outros indicadores monetários futuros.

Esses dados podem permanecer no mesmo repositório, mas não devem compartilhar automaticamente o mesmo contrato de vigência, fallback ou validação jurídica das regras trabalhistas/fiscais.

---

## 9. Contrato Fiscal Canônico v1

A **Fase 2 está concluída**. O Contrato Fiscal Canônico v1 foi fechado como entrada formal da biblioteca fiscal da Fase 3. A especificação consolidada está em `docs/phase2-contract-v1.md` e o handoff em `docs/phase2-to-phase3-handoff.md`.

O contrato possui duas representações sincronizadas:

- modelos Pydantic v2 em `sanida_fiscal/types_v1.py` e `sanida_fiscal/contract_v1.py`;
- JSON Schema público gerado deterministicamente em `contracts/fiscal-contract-v1.schema.json`.

Cada regra é representada por `FiscalRuleV1` e expressa, conforme aplicável:

- `rule_id` e `rule_version`;
- domínio;
- consumidores e contextos explícitos de apuração;
- target semântico (`applies_to`);
- predicados de aplicabilidade;
- dependências e ordem de cálculo;
- política de competência e janela de vigência;
- política de arredondamento;
- payload tipado;
- proveniência;
- estado de qualidade;
- classe da mudança;
- política de atualização.

O checkpoint de cobertura integral está documentado em `docs/contract-coverage-v1.json` e `docs/phase2-schema-coverage.md`. As **32 regras** do inventário da Fase 1 estão mapeadas para **18 famílias tipadas de payload**, e o exemplo `CANDIDATE` materializa ao menos uma regra de cada família. O CI exige cobertura exata 32/32 e igualdade entre as famílias usadas pelo mapa e a união admitida pelo schema. Nova família de payload é mudança estrutural do schema.

Um exemplo `CANDIDATE` vive em `contracts/examples/fiscal-contract-v1.example.json`. Ele **não é release de produção**: releases `VALIDATED`/`PUBLISHED` exigem evidência oficial disponível com hash de snapshot por regra. Snapshots reais pertencem à camada de fontes da Fase 4.

O v1 congela SemVer para `schema_version`, `contract_api_version` e `rule_version`, mas adota compatibilidade **exata** e fail-closed até teste explícito. `release_id` não é SemVer: releases validadas/publicadas usam `fiscal-v1-sha256-<hash do payload imutável>`. Releases publicadas são imutáveis; uma sucessora declara `supersedes_release_id` sem reescrever a predecessora.

Os gates atuais já rejeitam, entre outras situações:

- regra fora da vigência ou seleção ambígua;
- sobreposição do mesmo `rule_id` no mesmo contexto;
- dependência inexistente;
- regra estrutural com autopublicação;
- regra estrutural validada sem revisão humana;
- `last-good` expirado, não validado ou com sucessora conhecida;
- tentativa de relabelar dado histórico como corrente;
- regressão do target semântico de A01;
- fusão indevida entre principal e terço do abono;
- expansão silenciosa do escopo de H29.

A auditoria final da Fase 2 fechou os pontos de inferência remanescentes: método de tabela progressiva, unidades, fórmula de redução, competência `rule_specific`, predicados, rounding stages, policies, componentes de fórmula/incidência, sistema de códigos, escopo H29 e prorrateio estão tipados ou cruzados contra o inventário. Campos narrativos não decidem o cálculo.

---

## 10. Fontes oficiais — estratégia inicial

### 10.1. Banco Central

Quando houver API/dataset oficial estruturado, deve-se preferi-lo ao scraping.

Na Fase 4, o domínio `financial_reference` passou a ter registro operacional próprio em `docs/financial-source-registry-v1.json`, separado do registro jurídico-fiscal de folha.

A Selic usa a série oficial **SGS 432** do BCB. O CDI deixou de depender do FTP Cetip/B3 no caminho automático: a entrada operacional canônica passou a ser a série **SGS 12** do BCB, preservada como taxa diária decimal; a anualização em 252 dias úteis ocorre somente na fronteira de compatibilidade de `taxas_bacen.json`. A B3 permanece referência de metodologia/corroboração do benchmark DI, não fallback automático.

### 10.2. Receita Federal — IRRF

Não presumir que uma página HTML seja contrato computacional.

O sistema deve combinar, quando disponível:

- tabela oficial;
- legislação associada;
- vigência;
- exemplos oficiais de cálculo;
- validações semânticas próprias.

A extração HTML deve funcionar como sensor/parser de uma estrutura conhecida, não como intérprete autônomo de legislação nova.

### 10.3. INSS / MPS / eSocial

A tabela operacional deve ser reconciliada com a norma correspondente e, quando útil, com publicações oficiais independentes do ecossistema previdenciário/trabalhista.

Mudanças apenas numéricas podem ser automatizáveis; mudança semântica deve exigir revisão.

---

## 11. Stack técnica planejada

A adoção será incremental, evitando refatoração big-bang.

### Núcleo

- Python 3.11+;
- `Decimal` para dinheiro e coeficientes monetários;
- Pydantic v2 para contratos tipados e validação forte;
- JSON Schema como contrato público independente da implementação Python.

### Testes

- pytest;
- Hypothesis para property-based testing;
- casos oficiais convertidos em testes de referência;
- fixtures/snapshots reproduzíveis de fontes externas.

### Mudanças e proveniência

- DeepDiff ou mecanismo equivalente para diff semântico;
- hashes de snapshots;
- classificação explícita de mudanças.

### HTTP e resiliência

Em implementação na Fase 4:

- HTTPX com timeouts explícitos já integra a nova camada de fontes;
- retry é seletivo e tipado por classe de falha no primeiro checkpoint;
- VCR.py ou mecanismo equivalente poderá ser adicionado quando parsers reais de fontes externas forem materializados.

### Princípio de dependência

Uma biblioteca só deve entrar no caminho crítico se melhorar auditabilidade, segurança ou manutenção de forma objetiva.

Não será adotada uma rules engine genérica apenas para abstrair regras fiscais simples e auditáveis em funções puras.

---

## 12. Invariantes mínimas do sistema

As seguintes condições deverão se tornar testes obrigatórios:

1. dado histórico nunca recebe automaticamente competência futura;
2. ausência de parâmetro obrigatório nunca vira zero por conveniência;
3. regra futura não é aplicada antes da vigência;
4. regra expirada não permanece ativa após substituição válida;
5. falha de fonte não é indistinguível de coleta bem-sucedida sem mudança;
6. mudança estrutural não é autopublicada;
7. artefato inválido não substitui `last-good`;
8. `last-good` só pode ser usado enquanto sua vigência permitir;
9. todo valor publicado deve ter proveniência rastreável;
10. todo cálculo crítico deve conseguir identificar a versão do contrato utilizado;
11. arredondamentos fiscais devem ser determinísticos e testados;
12. exemplos oficiais devem ser reproduzíveis pela implementação correspondente.

---

## 13. Estratégia de snapshots e auditoria

O projeto deverá preservar evidência suficiente para responder posteriormente:

- qual conteúdo oficial foi observado;
- quando foi observado;
- de qual URL/recurso veio;
- qual hash possuía;
- qual parser o interpretou;
- qual contrato resultou;
- quais testes passaram;
- qual release foi promovida;
- quais consumidores receberam essa release.

Na Fase 4, a identidade mínima do snapshot bruto passa a ser `source_id + sha256(raw_bytes)`, com caminho content-addressed e verificação de integridade na leitura. No quinto checkpoint, o backend de produção foi fechado para o ambiente real do GitHub Actions: `main.yml` usa `evidence/source-runtime-v1`, rastreado no Git, com snapshots e candidatos normalizados imutáveis por SHA-256 e estado operacional materializado por fonte. A política v1 não faz pruning automático; o histórico Git preserva versões anteriores do estado.

---

## 14. Política de promoção

Nenhum novo contrato fiscal deve ser promovido apenas porque o scraper terminou sem exceção.

A promoção deverá depender de gates explícitos.

Estado conceitual desejado:

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
APPROVED_FOR_AUTO_PUBLISH
  ou
REVIEW_REQUIRED
  ↓
PUBLISHED
```

A implementação definitiva desse lifecycle será feita em fase posterior.

---

## 15. Relação com as calculadoras H26–H29

Este repositório será a origem canônica; as calculadoras serão consumidores.

O consumidor não deve reconstruir, inferir ou completar silenciosamente uma regra ausente no contrato.

Fluxo desejado:

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

A correção final das calculadoras só deve ser considerada encerrada quando produtor e consumidor compartilham o mesmo contrato semântico.

---

## 16. Escopo inicial das calculadoras

### H26 — Salário líquido

Deverá cobrir pelo menos:

- INSS progressivo;
- IRRF mensal;
- desconto simplificado versus deduções aplicáveis;
- dependentes;
- redução mensal vigente;
- ordem correta de cálculo;
- limites e transições de faixas;
- arredondamento.

### H27 — 13º salário

Deverá distinguir a apuração própria do 13º da folha mensal e mapear:

- avos;
- regra dos 15 dias;
- ano de admissão;
- primeira parcela;
- segunda/parcela final;
- INSS específico do 13º;
- IRRF específico do 13º;
- deduções aplicáveis;
- datas/regras de pagamento quando pertinentes ao escopo.

### H28 — Férias

Deverá distinguir:

- período aquisitivo;
- período concessivo/gozo, quando pertinente ao escopo;
- férias integrais e proporcionais;
- terço constitucional;
- abono pecuniário;
- limites legais do abono;
- incidências fiscais/previdenciárias;
- impacto de faltas injustificadas somente se efetivamente suportado pela ferramenta.

### H29 — Rescisão

Deverá calcular apenas aquilo que estiver expressamente dentro do escopo declarado, sem criar aparência de cobertura universal de todas as modalidades, verbas e exceções trabalhistas.

O inventário deverá partir da implementação real da ferramenta antes de definir regras adicionais.

---

## 17. Fases do remake

### Fase 0 — Baseline e governança

**Status: CONCLUÍDA**

Objetivos:

- criar README mestre;
- registrar arquitetura atual;
- congelar princípios e invariantes;
- não alterar ainda a produção sem contrato e testes.

Critério de conclusão:

- README inicial criado e coerente com o repositório real.

### Fase 1 — Inventário jurídico-fiscal

**Status: CONCLUÍDA**

Objetivos:

- mapear todas as regras necessárias a H26–H29;
- identificar fontes normativas e operacionais;
- classificar cada regra como parâmetro, regra parametrizável ou regra estrutural;
- documentar vigência, dependências e exemplos oficiais.

Critério de conclusão:

- nenhuma variável relevante das quatro calculadoras sem origem e semântica definidas.

Artefatos de fechamento:

- `docs/inventario-juridico-fiscal-v1.md`;
- `docs/rule-inventory-v1.json`;
- `docs/source-registry-v1.json`;
- `docs/phase1-closure.md`;
- `tests/reference_cases/phase1_reference_cases.json`.

### Fase 2 — Contrato Fiscal Canônico v1

**Status: CONCLUÍDA**

Objetivos:

- definir modelos Pydantic;
- definir JSON Schema;
- definir estados de qualidade e mudança;
- definir política de `last-good`;
- definir proveniência.

Primeiro corte executável:

- `sanida_fiscal/types_v1.py`;
- `sanida_fiscal/contract_v1.py`;
- `contracts/fiscal-contract-v1.schema.json`;
- `contracts/examples/fiscal-contract-v1.example.json`;
- `scripts/generate_contract_schema.py`;
- `scripts/validate_contract_v1.py`;
- `tests/test_contract_v1.py`;
- `docs/phase2-contract-v1.md`;
- `requirements-contract.txt`;
- `requirements-dev.txt`.

Estado atual do CI:

- geração/validação determinística do schema: ativa;
- cobertura do `rule-inventory-v1.json`: **32/32**;
- famílias tipadas de payload: **18/18 materializadas no CANDIDATE**;
- validação cruzada com source registry, rule inventory, coverage map e reference cases: ativa;
- proveniência, competência e lifecycle de release possuem gates negativos;
- suíte final do contrato: **52 testes verdes**;
- versionamento, compatibilidade exata, identidade content-addressed e imutabilidade/supersessão possuem gates executáveis;
- auditoria final de inferência semântica concluída.

Artefatos adicionais do checkpoint:

- `docs/contract-coverage-v1.json`;
- `docs/phase2-schema-coverage.md`;
- `docs/phase2-to-phase3-handoff.md`.

### Fase 3 — Biblioteca fiscal e testes

**Status: CONCLUÍDA**

Objetivos:

- implementar funções fiscais puras;
- usar `Decimal`;
- converter exemplos oficiais em testes;
- criar property-based tests e invariantes.

Primeira rodada executável:

- `sanida_fiscal/money.py` — entrada decimal estrita e quantização por `RoundingPolicy`;
- `sanida_fiscal/engine_v1.py` — tabelas progressivas, redutor afim e memória de IRRF;
- `tests/test_engine_v1.py` — casos RFB, regressão A01 e property-based tests;
- `docs/phase3-library-v1.md` — escopo, checkpoints e handoff da biblioteca fiscal.

Estado final da Fase 3:

- cinco casos oficiais RFB de 2026 executados contra o engine;
- A01 reproduz `382.88` para renda tributável de `6000.00`;
- executor marginal com teto implementado para a família usada pelo INSS;
- `float`/`bool` rejeitados no caminho fiscal;
- Hypothesis integrado ao CI;
- identidade explícita de apuração (`monthly`, `thirteenth`, `vacation`) separada do contexto de origem;
- `termination` tratado como origem que pode conter apurações mensal e de 13º distintas, nunca como quarto tipo de IRRF;
- previdência, dependentes e pensão vinculados a uma única apuração e impedidos de vazar entre contextos;
- bundles de regras selecionados pelo tipo de rendimento, inclusive dentro de H29;
- 13º com avos, fronteira de 15 dias, referência anual/rescisória, bruto proporcional e adiantamento fixo simples;
- remuneração variável pré-calculada aceita apenas como entrada externa explicitamente marcada;
- INSS do 13º e IRRF exclusivo do 13º possuem memórias próprias, inclusive dentro de `termination`;
- branches especiais de adiantamento ainda não modeladas falham fechadas em vez de usar `total13 * 0.5`;
- o gate da Fase 2 detectou que A02 precisava transportar para a máquina o limiar de 15 dias da fração proporcional de férias; o contrato foi corrigido explicitamente para `schema_version`/`contract_api_version` **1.1.0**, mantendo compatibilidade exata/fail-closed;
- período aquisitivo e avos proporcionais de férias agora são ancorados no aniversário do vínculo, sem reset em 1º de janeiro;
- regressão A02 reproduz `01/09/2025 → 31/03/2026 = 7/12`;
- faixas de direito por faltas e abono de 1/3 do **direito adquirido** estão executáveis;
- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;
- H29 ganhou núcleo limitado próprio, com matriz eSocial `01/02/07/33`, sem expansão para motivos não suportados;
- motivo `01` mantém saldo salarial e bloqueia 13º/férias proporcionais; `02/07/33` habilitam ambos;
- saldo salarial usa `salário-base mensal normalizado × dias considerados / dias civis do mês`, sem divisor 30 universal;
- 13º rescisório continua no calendário anual enquanto férias proporcionais continuam no período aquisitivo — no caso `01/09/2025 → 31/03/2026`, isso produz 3/12 de 13º e 7/12 de férias;
- H29 continua declarando `partial_estimate`, com aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas fora do total;
- `sanida_fiscal/memory_v1.py` introduz um envelope comum de memória auditável sem fundir semânticas fiscais: IRRF, 13º, férias e H29 continuam preservando identidade/contexto e podem ser compostos como memórias-filhas;
- valores monetários da memória comum partem de `Decimal` e são serializados como texto canônico, sem reintroduzir `float` no caminho fiscal;
- property-based tests foram ampliados para fronteiras do redutor de 2026, denominadores civis do saldo salarial, monotonicidade de 13º/férias, matriz H29 fechada e separação tributária do abono;
- `scripts/validate_phase3_gate.py` tornou o gate de fechamento executável e permanente no `Remake CI`;
- revisão formal dos sete critérios do gate concluída sem aresta objetiva bloqueante;
- suíte integral do fechamento: **150 testes verdes**;
- `docs/phase3-closure-gate.md` registra a decisão formal de encerramento.

Critério de conclusão atendido:

- validação do repositório, contrato, suíte integral e gate executável verdes no mesmo head de fechamento;
- README e documentação da fase sincronizados;
- ausência de helper/workflow temporário residual;
- ausência de mudança de produção/consumidor misturada à biblioteca.

### Fase 4 — Fontes e sensores

**Status: EM ANDAMENTO**

Objetivos:

- separar collectors de parsers;
- priorizar APIs oficiais estruturadas;
- preservar snapshots necessários;
- melhorar retry/timeouts;
- implementar detecção de alterações e falhas.

Primeiro checkpoint executável:

- `docs/phase4-collection-surface-v1.json` inventaria os caminhos legados de IRRF, INSS, Selic, CDI e o artefato interno de taxas;
- `sanida_fiscal/sources_v1.py` separa `SourceSpec`, `HttpCollectorV1`, `RawSnapshot`, `SnapshotStore`, parser e `NormalizedSourceCandidate`;
- snapshots são content-addressed por SHA-256 e verificados na leitura;
- `SOURCE_UNAVAILABLE` fica distinto de `PARSER_INCOMPATIBLE`;
- timeout, erro de rede, 4xx e falha transitória 429/5xx possuem estados operacionais explícitos;
- `scripts/validate_phase4_foundation.py` torna a fundação verificável no `Remake CI`;
- suíte integral chega a **160 testes verdes** no primeiro checkpoint;
- o legado de produção permanece intacto neste checkpoint.

Segundo checkpoint executável:

- `sanida_fiscal/rfb_irrf_v1.py` materializa o primeiro parser específico no novo pipeline, para `RFB_IRRF_TABLE_2026`;
- `sanida_fiscal/source_runtime_v1.py` persiste estado operacional entre execuções, sem confundir last-good operacional com vigência jurídica;
- `ETag` e `Last-Modified` passam a ser preservados e reutilizados em consultas condicionais quando o parser permanece compatível;
- mudança de `parser_id`/`parser_version` força refetch completo antes de aceitar o novo parser;
- igualdade de snapshot bruto e fingerprint de candidato é observável sem antecipar semantic diff da Fase 5;
- `scripts/run_source_pipeline_v1.py` executa o pipeline real RFB sem publicar ou alterar artefatos de produção;
- fixture oficial mínima e testes de estado elevam a suíte integral para **166 testes verdes**.

Terceiro checkpoint executável:

- `docs/phase4-inss-source-resolution-v1.json` fecha formalmente a divergência `INSS_TABLE_2026` × notícia anual pinned/descoberta;
- a URL registrada em `INSS_TABLE_2026` passa a ser a única entrada operacional automática do novo pipeline de INSS;
- notícia anual e `@@search` ficam proibidos como fallback automático: indisponibilidade da URL canônica produz `SOURCE_UNAVAILABLE`;
- `sanida_fiscal/inss_employee_v1.py` implementa `inss_employee_table_v1@1.0.0` para a tabela oficial de empregado, doméstico e trabalhador avulso;
- o candidato normalizado preserva quatro faixas progressivas, teto `8475.55`, vigência 2026, referência à Portaria Interministerial MPS/MF nº 13/2026 e separação do 13º;
- notícia anual, mesmo contendo valores, não satisfaz o contrato estrutural do parser canônico;
- o runner manual passa a aceitar `--source-id INSS_TABLE_2026` sem chamar discovery legado;
- o gate da Fase 4 cruza política INSS, source registry, fixture e superfície de coleta;
- suíte integral chega a **172 testes verdes**.

Quarto checkpoint executável:

- `sanida_fiscal/source_catalog_v1.py` centraliza os bindings RFB + INSS usados pelo runner e pelo produtor legado;
- `scraper.py` deixa de conter fetch/parser próprio de RFB/INSS, pinned URL e discovery `@@search`;
- `sanida_fiscal/legacy_artifact_v1.py` cria a única fronteira explícita `normalized candidate → dados_fiscais.json 2.2.0`;
- `dados_fiscais.json` fica formalmente classificado como artefato de compatibilidade, não como release do Contrato Fiscal Canônico;
- uma nova escrita exige candidatos `PARSED` da execução corrente, mesmo `reference_year` e igualdade com o ano UTC corrente;
- `SourcePipelineState`/304 isolado não pode gerar nova publicação de compatibilidade;
- o antigo fallback fiscal estático de `scraper.py` foi removido; sem candidato atual, somente um last-good válido do mesmo ano pode permanecer inalterado;
- candidato/artefato de ano anterior nunca é relabelado como corrente;
- proveniência do artefato legado passa a carregar hashes de snapshot/candidato e versão do parser;
- `requirements.txt` passa a instalar o runtime da Fase 4;
- suíte integral chega a **180 testes verdes** e o gate registra `legacy artifact bridge prepared`.

Quinto checkpoint executável:

- `docs/phase4-production-persistence-v1.json` define o backend real de persistência usado pelo workflow de produção;
- `evidence/source-runtime-v1` passa a ser o runtime rastreado em Git para snapshots, candidatos normalizados e estado operacional;
- `CandidateStore` persiste o payload normalizado canônico sob o mesmo `candidate_sha256` exposto na proveniência;
- `SourcePipelineState` v1.1 preserva timestamp, caminho/hash do snapshot last-good, identidade do parser bem-sucedido e caminho/hash do candidato;
- falha corrente não apaga os ponteiros last-good necessários para comprovar um artefato preservado;
- `scripts/validate_production_evidence_v1.py` verifica que hashes do artefato resolvem para bytes realmente persistidos e confinados ao runtime root;
- `main.yml` usa `SFA_SOURCE_RUNTIME_ROOT=evidence/source-runtime-v1`, nunca commitando um `dados_fiscais.json` que falhe no gate de evidência;
- snapshots/estado de uma tentativa falha podem ser persistidos sem publicar o artefato, e o workflow encerra com erro depois dessa persistência;
- retenção v1 mantém todos os conteúdos únicos, sem pruning automático; estado corrente fica materializado e estados anteriores permanecem no histórico Git;
- suíte integral chega a **187 testes verdes** e o gate registra `production evidence persistence anchored`.


Sexto checkpoint executável:

- criado `docs/financial-source-registry-v1.json`, mantendo `financial_reference` separado do registro jurídico-fiscal de folha;
- Selic migra para `BCB_SELIC_META_SGS_432`, via JSON bruto do SGS 432 + parser decimal `bcb_selic_meta_sgs432_v1@1.0.0`;
- CDI migra para `BCB_CDI_DAILY_SGS_12`, via JSON bruto do SGS 12 + parser decimal `bcb_cdi_daily_sgs12_v1@1.0.0`;
- o parser do CDI preserva a unidade de origem `% ao dia útil`; a taxa anual legada é derivada somente no bridge por `((1 + taxa_dia/100)^252 - 1) × 100`, com `Decimal` e `ROUND_HALF_UP` a duas casas;
- a fixture `0.051660% a.d.` reproduz `13.90% a.a.`, coerente com o valor legado esperado;
- `update_taxas.py` deixa de conter FTP Cetip/B3, discovery por arquivo e `FALLBACK_SELIC`/`FALLBACK_CDI`;
- falha de qualquer fonte não reescreve `taxas_bacen.json`, não atualiza `generated_at_utc` e não relabela last-good como observação corrente;
- `sanida_fiscal/financial_artifact_v1.py` cria a fronteira explícita `normalized financial candidates → taxas_bacen.json 1.4.0`;
- `sanida_fiscal/financial_evidence_v1.py` e `scripts/validate_financial_evidence_v1.py` comprovam snapshot, candidato, parser, timestamp e estado antes do commit;
- `taxas.yml` passa a usar `evidence/source-runtime-v1` e a mesma transação fail-closed de artefato + evidência usada pelo workflow principal;
- B3 permanece referência de metodologia/corroboração do DI, mas o FTP legado deixa de ser input/fallback automático;
- suíte integral chega a **200 testes verdes** e o gate registra `Selic SGS 432 + CDI SGS 12 migrated; static financial fallback removed`.

### Fase 5 — Diff semântico e gates de publicação

**Status: PENDENTE**

Objetivos:

- classificar mudança paramétrica versus estrutural;
- bloquear promoção insegura;
- publicar somente releases validadas;
- tornar estado operacional observável.

### Fase 6 — Migração dos consumidores

**Status: PENDENTE**

Objetivos:

- migrar plugin/cache;
- migrar `folha-core`;
- corrigir H26–H29 contra o contrato canônico;
- remover duplicação de regra fiscal nos consumidores.

### Fase 7 — Fechamento e operação evergreen

**Status: PENDENTE**

Objetivos:

- testes ponta a ponta;
- validação de atualização automática;
- simulação de falha de fonte;
- simulação de mudança paramétrica;
- simulação de mudança estrutural;
- documentação operacional;
- critérios objetivos de encerramento.

---

## 18. Decisões já tomadas

1. O `sanida-dados-fiscais` será a fonte canônica das regras/dados fiscais usados pelas calculadoras.
2. O README será o centro documental do projeto.
3. Não será feita apenas uma correção pontual para 2026; a arquitetura deve prevenir obsolescência futura.
4. Scraping de HTML será tratado como sensor/parser, não como autoridade jurídica autônoma.
5. APIs oficiais estruturadas serão preferidas quando disponíveis.
6. Mudança paramétrica e mudança estrutural terão políticas diferentes.
7. Mudança estrutural exigirá revisão humana.
8. Fallback estático que possa fingir atualidade será removido da arquitetura final.
9. Regras monetárias usarão representação decimal e política explícita de arredondamento.
10. H26–H29 serão auditadas e testadas individualmente, mesmo quando compartilham motor comum.
11. O repositório pode conter dados fiscais e indicadores financeiros, mas os domínios terão contratos e políticas de validade independentes.
12. O novo sistema deverá ser auditável retroativamente: fonte observada → parser → contrato → testes → release → consumidor.
13. Mensal, férias e 13º serão contextos explícitos de apuração do IR; uma função genérica não poderá apagar diferenças semânticas entre eles.
14. O redutor de IR de 2026 usará o rendimento tributável pertinente como variável de entrada, nunca a base pós-deduções por conveniência.
15. H27 não tratará `total13 * 0.5` como regra universal da primeira parcela, e remuneração variável terá contrato próprio.
16. Férias separarão direito, gozo, abono, natureza gozada/indenizada e os componentes tributários do principal e do terço do abono.
17. H29 v1 permanecerá uma estimativa parcial e suportará inicialmente apenas os motivos eSocial `01`, `02`, `07` e `33`; demais motivos serão explicitamente não suportados.
18. Para o escopo padrão de H29 v1, não haverá divisor 30 universal de saldo de salário; exceções exigirão override tipado e proveniência.
19. O registro de fontes e o inventário de regras da Fase 1 são entradas formais da Fase 2 e não devem ser reabertos sem evidência oficial nova ou contradição objetiva.
20. Toda PR do remake terá validação automática em `Remake CI`; o CI é read-only e não executa publicação de dados.
21. `schema_version`, `contract_api_version` e `rule_version` seguem SemVer, mas o v1 exige compatibilidade exata e testada antes de aceitar qualquer nova versão.
22. `release_id` é identidade content-addressed do payload fiscal imutável, não número de versão.
23. Release `PUBLISHED` é imutável; supersessão é declarada pela sucessora em `supersedes_release_id`, sem mutar a predecessora.
24. Campos narrativos existem para auditoria humana, mas o engine não pode depender deles para decidir operação fiscal.
25. O limiar proporcional de férias não pode existir como constante jurídica escondida no engine: `vacation.acquisition_period` v1.1 declara `proportional_qualifying_days=15` e o método de aquisição proporcional; leitores 1.0.0 não aceitam silenciosamente o contrato 1.1.0.
26. H29 deve executar a matriz eSocial `01/02/07/33` como escopo fechado e cruzar a matriz geral com as regras específicas de 13º e férias proporcionais; divergência ou motivo fora do conjunto suportado é erro, não aproximação.
27. O saldo salarial de H29 v1 não usa divisor 30 universal. O denominador é o número de dias civis do mês de desligamento e o numerador é fornecido explicitamente como dias considerados até o desligamento; regimes fora de mensalista/quinzenalista normalizado ficam fora do escopo.
28. A memória comum de cálculo é um envelope de representação/auditoria, não uma rules engine nem uma fusão semântica. Bases e identidades de mensal, 13º, férias e rescisão permanecem separadas; composições usam memórias-filhas.
29. O fechamento da Fase 3 exige, no mesmo head, validação do repositório, Contrato Fiscal Canônico, suíte integral, `scripts/validate_phase3_gate.py`, documentação sincronizada e ausência de helpers/workflows temporários.
30. A Fase 3 foi formalmente encerrada depois de revisão dos sete critérios do gate; qualquer reabertura da biblioteca deverá decorrer de defeito objetivo ou exigência explícita de uma fase posterior, não de redesign oportunista.
31. Na Fase 4, coleta e interpretação são etapas distintas: bytes brutos são preservados e identificados por SHA-256 antes de qualquer parser; indisponibilidade da fonte e incompatibilidade do parser nunca são o mesmo estado.
32. Estado operacional de fonte preserva a última observação bem-sucedida, validadores HTTP e falhas correntes sem transformar last-good operacional em autorização jurídica de uso; mudança de versão do parser invalida o atalho condicional e exige refetch.
33. Para `INSS_TABLE_2026`, o novo pipeline usa exclusivamente a URL canônica registrada; notícia anual pinned e `@@search` não são fallback automático e falha da fonte registrada permanece `SOURCE_UNAVAILABLE`.
34. `dados_fiscais.json` permanece temporariamente como artefato de compatibilidade 2.2.0: só pode ser reescrito a partir de candidatos RFB/INSS `PARSED` da execução corrente e do mesmo ano UTC; estado operacional isolado, fallback estático ou dado de ano anterior não autorizam nova escrita.
35. O runtime de fontes do workflow `main.yml` não pode depender do filesystem efêmero do runner: snapshots, candidatos normalizados e estado operacional de RFB/INSS são persistidos em `evidence/source-runtime-v1`; um artefato não verificado nunca é commitado, mas evidência operacional de uma execução falha pode ser preservada antes de o workflow sinalizar erro.
36. `financial_reference` possui registro, parser e política de falha próprios; não herda vigência jurídica de `payroll_fiscal`.
37. Selic de compatibilidade usa BCB SGS 432 como entrada operacional estruturada; CDI usa BCB SGS 12 na unidade diária de origem e só é anualizado no bridge legado em base de 252 dias úteis.
38. FTP B3/Cetip não é fallback automático nem input de produção após a migração; B3 permanece autoridade/metodologia do benchmark DI e fonte de corroboração humana.
39. Falha financeira nunca autoriza fallback estático nem refresh de timestamp: `taxas_bacen.json` só é reescrito com dois candidatos atuais `PARSED` e evidência persistida validada.

---

## 19. Questões em aberto

As Fases 1, 2 e 3 estão formalmente concluídas. Se a implementação posterior revelar uma lacuna semântica objetiva, o processo exige emenda explícita e versionada do contrato em vez de hardcode no engine. A primeira ocorrência foi A02, corrigida na Fase 3 como Contrato v1.1. As questões abertas restantes pertencem às fases posteriores:

- critérios de confirmação multi-fonte para mudanças paramétricas — Fase 4/5;
- revisão final da fronteira entre `taxas.yml`, `main.yml`, `taxas_bacen.json` e `dados_fiscais.json` — fechamento da Fase 4;
- mecanismo de semantic diff e promoção — Fase 5;
- distribuição para WordPress/SFA e migração dos consumidores — Fase 6.

---

## 20. Próxima etapa

Continuar a **Fase 4 — Fontes e sensores** com os quatro coletores externos já migrados na branch: RFB, INSS, Selic/SGS 432 e CDI/SGS 12.

Próximo checkpoint: revisar a **fronteira final de produção** entre `taxas.yml`, `main.yml`, `taxas_bacen.json` e `dados_fiscais.json`, confirmar que nenhuma rota legada de coleta/fallback permanece alcançável e executar o gate formal de fechamento da Fase 4. Semantic diff, promoção e publicação continuam reservados à Fase 5.

Qualquer necessidade de reinterpretar regra jurídica ou alterar a biblioteca da Fase 3 deve voltar explicitamente ao contrato/inventário com evidência concreta, não ser resolvida silenciosamente dentro de collector ou parser.

A ativação em `main`/produção continua congelada: nesta branch `scraper.py`, `update_taxas.py`, `main.yml` e `taxas.yml` já estão preparados para os novos pipelines e persistência de evidências, mas os conteúdos publicados atuais de `dados_fiscais.json` e `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram ativados/migrados pelo merge.

---

## 21. Convenção de status

Para manter este README útil ao longo do projeto, cada fase deverá usar apenas um destes estados:

- `PENDENTE`
- `EM ANDAMENTO`
- `BLOQUEADA`
- `EM REVISÃO`
- `CONCLUÍDA`

Uma fase só deve ser marcada como `CONCLUÍDA` quando seus critérios de conclusão estiverem objetivamente atendidos.

---

## 22. Changelog do README

### 2026-09-13 — migração de Selic e CDI na Fase 4

- criado registro próprio de fontes do domínio `financial_reference`;
- Selic passa a usar BCB SGS 432 em pipeline `collector → snapshot → parser decimal → candidato`;
- CDI passa a usar BCB SGS 12 como taxa diária, com annualização explícita somente no bridge legado em base 252;
- removidos FTP Cetip/B3 e fallbacks estáticos do caminho automático de `update_taxas.py`;
- criado `taxas_bacen.json` 1.4.0 como artefato de compatibilidade com proveniência completa;
- `taxas.yml` passa a persistir snapshots/candidatos/estado no mesmo backend Git-tracked e a exigir gate de evidência antes do commit;
- falha financeira preserva o artefato anterior byte a byte e persiste apenas evidência operacional quando aplicável;
- B3 permanece referência metodológica do benchmark DI, sem funcionar como fallback automático;
- suíte integral chega a **200 testes verdes**.

### 2026-09-13 — persistência real de evidências no workflow de produção

- definido `evidence/source-runtime-v1` como backend Git-tracked para o runner efêmero do GitHub Actions;
- snapshots brutos e candidatos normalizados passam a ter arquivos content-addressed preservados por SHA-256;
- `SourcePipelineState` v1.1 separa a tentativa corrente do conjunto last-good auditável;
- criado gate que resolve a proveniência de `dados_fiscais.json` contra snapshot, candidato e estado persistidos;
- `main.yml` só stageia o artefato quando esse gate passa;
- falha operacional pode persistir evidência/estado sem publicar dados e ainda termina o workflow em erro;
- política v1 retém todos os conteúdos únicos, sem pruning automático;
- suíte integral chega a **187 testes verdes**.

### 2026-09-13 — fronteira `scraper.py` → `dados_fiscais.json` na Fase 4

- criado catálogo único de pipelines para RFB e INSS;
- removidos de `scraper.py` os parsers de folha, pinned URL e discovery do INSS;
- criado bridge explícito para manter `dados_fiscais.json` 2.2.0 apenas como artefato de compatibilidade;
- nova escrita passa a exigir candidatos atuais `PARSED` e competência anual coerente;
- removido o fallback fiscal estático e bloqueada relabelagem de ano anterior;
- proveniência legada passa a incluir hashes de snapshot/candidato e parser id/versão;
- runtime de produção passa a instalar Pydantic/HTTPX/BeautifulSoup necessários ao novo caminho;
- suíte integral chega a **180 testes verdes**.

### 2026-09-13 — INSS canônico sem discovery na Fase 4

- encerrada formalmente a divergência entre `INSS_TABLE_2026` e a notícia anual pinned/descoberta do legado;
- criada política machine-readable que proíbe notícia e `@@search` como fallback automático;
- indisponibilidade da URL registrada passa a permanecer `SOURCE_UNAVAILABLE`, sem troca oportunística de fonte;
- criado `inss_employee_table_v1@1.0.0` para quatro faixas, teto, vigência, referência normativa e separação do 13º;
- runner de fontes passa a aceitar `INSS_TABLE_2026` diretamente do source registry;
- gate da Fase 4 ancora RFB + INSS e marca a divergência de fonte como resolvida;
- suíte integral chega a **172 testes verdes**.

### 2026-09-13 — primeiro pipeline real RFB na Fase 4

- materializado `RFB_IRRF_TABLE_2026` em `collector → snapshot → parser → candidato normalizado`;
- parser RFB versionado normaliza tabela mensal, dependente, desconto simplificado e redutor 2026 sem publicar contrato;
- estado operacional persistente passa a carregar `ETag`, `Last-Modified`, snapshot/candidato last-good e falhas correntes;
- 304 e respostas 200 idênticas são distinguíveis sem antecipar classificação semântica;
- mudança de versão do parser força coleta completa;
- criado runner real sem publicação e runtime local ignorado pelo Git;
- suíte integral chega a **166 testes verdes**.

### 2026-09-13 — início da Fase 4

- Fase 4 marcada como `EM ANDAMENTO`;
- inventariada a superfície legada de coleta em `docs/phase4-collection-surface-v1.json`;
- criada a separação operacional `collector → raw snapshot → parser → normalized source candidate`;
- snapshots passam a ter identidade content-addressed por SHA-256 e verificação de integridade;
- `SOURCE_UNAVAILABLE` e `PARSER_INCOMPATIBLE` passam a ser estados distintos;
- HTTPX entra apenas na nova camada de fontes, sem migrar ainda os scripts de produção;
- criado `scripts/validate_phase4_foundation.py` como gate permanente do primeiro checkpoint;
- suíte integral chega a **160 testes verdes**.

### 2026-09-13 — fechamento formal da Fase 3

- revisados formalmente os sete critérios de promoção definidos em `docs/phase3-closure-gate.md`;
- validação do repositório, Contrato Fiscal Canônico v1.1, suíte integral e gate de fechamento permaneceram verdes;
- suíte de fechamento permaneceu em **150 testes verdes**;
- confirmada a higiene do branch: apenas `main.yml`, `taxas.yml` e `remake-ci.yml` permanecem como workflows;
- confirmado pelo diff do PR que `scraper.py`, `update_taxas.py`, `dados_fiscais.json` e `taxas_bacen.json` não foram alterados pela Fase 3 em relação à base do PR;
- README, `docs/phase3-library-v1.md` e `docs/phase3-closure-gate.md` sincronizados para `CONCLUÍDA`;
- Fase 4 — Fontes e sensores passa a ser a próxima etapa; produção e consumidores continuam congelados.

### 2026-09-13 — checkpoint de memória comum, invariantes e gate da Fase 3

- criado `sanida_fiscal/memory_v1.py` como envelope determinístico de memória auditável, preservando separação entre contextos e bases fiscais;
- fatos da memória passam a carregar papel, unidade e `rule_ids`, com valores monetários originados em `Decimal` e serializados como texto canônico;
- H29 passa a expor memória composta por filhos separados para saldo salarial, 13º proporcional e férias proporcionais, sem fabricar filhos inelegíveis no motivo `01`;
- `tests/test_memory_v1.py` valida reconciliação A01, serialização determinística, unicidade de fatos e separação das memórias;
- `tests/test_phase3_invariants.py` amplia property-based tests para fronteiras legais e monetárias;
- criado `scripts/validate_phase3_gate.py` e integrado ao `Remake CI`;
- criado `docs/phase3-closure-gate.md` com critérios objetivos para a promoção formal da Fase 3;
- run `34775296512` fecha o checkpoint com validação do repositório, Contrato v1.1, **150 testes** e gate da Fase 3 em `PASS`.

### 2026-09-13 — checkpoint H29 limitado na Fase 3

- criado `sanida_fiscal/termination_v1.py` com escopo fail-closed para mensalista/quinzenalista normalizado, contrato por prazo indeterminado e motivos eSocial `01/02/07/33`;
- materializada no `CANDIDATE` a regra já inventariada `termination.vacation_proportional`, elevando o exemplo para 21 regras sem criar nova família de payload;
- matriz geral e regras específicas de 13º/férias proporcionais passam a ser cruzadas em runtime;
- motivo `01` bloqueia proporcionais; `02/07/33` habilitam 13º e férias proporcionais;
- saldo salarial usa os dias civis reais do mês e reproduz R$ 3.100 / 31 × 10 = R$ 1.000;
- no caso A02, H29 preserva simultaneamente 3/12 de 13º no ano civil e 7/12 de férias no período aquisitivo;
- resultado continua `partial_estimate` e não incorpora aviso, FGTS, seguro-desemprego ou demais verbas excluídas;
- suíte completa chega a **130 testes verdes**.

### 2026-09-13 — checkpoint de férias/A02 na Fase 3

- identificado pelo próprio gate da Fase 2 que o contrato não transportava o limiar de 15 dias das férias proporcionais;
- Contrato Fiscal Canônico v1 recebeu emenda aditiva e compatibilidade exata em `schema_version`/`contract_api_version` 1.1.0;
- `vacation.acquisition_period` v1.1 explicita método proporcional e `proportional_qualifying_days=15`;
- A02 passou a ser executável: `01/09/2025 → 31/03/2026 = 7/12`, sem reset em janeiro;
- direito por faltas e abono de 1/3 do entitlement passaram a funções puras;
- principal do abono e terço constitucional permanecem separados nas incidências e bases;
- suíte completa chega a **114 testes verdes**.

### 2026-09-13 — checkpoint de 13º salário na Fase 3

- implementados avos e fronteira legal de 15 dias;
- referências remuneratórias anual e rescisória tornadas executáveis;
- bruto proporcional exige rounding explícito;
- adiantamento fixo simples reproduz o caso oficial de R$ 4.000 → R$ 2.000;
- admissão no ano/remuneração variável no adiantamento permanecem fail-closed;
- INSS e IRRF do 13º passam a ter memórias próprias e isoladas da folha mensal;
- `thirteenth.irrf.reduction.2026` é selecionado explicitamente;
- suíte completa chega a 89 testes verdes.

### 2026-09-13 — checkpoint de apurações separadas na Fase 3

- `monthly`, `thirteenth` e `vacation` passam a ter identidade explícita de apuração;
- `termination` permanece contexto de origem, com mensal e 13º isolados entre si;
- deduções legais são vinculadas à apuração e não podem vazar entre contextos;
- seleção de regras e redutor é feita pelo tipo de rendimento;
- suíte completa chega a 73 testes verdes.

### 2026-09-13 — início da Fase 3

- aberta a biblioteca fiscal determinística sobre o Contrato v1;
- primitives de `Decimal`, tabela progressiva e redutor afim implementados;
- cinco casos oficiais RFB e regressão A01 tornados executáveis;
- Hypothesis integrado; primeiro checkpoint com 63 testes verdes.

### 2026-09-13 — fechamento da Fase 2

- Fase 2 marcada como `CONCLUÍDA`;
- congelado SemVer para `schema_version`, `contract_api_version` e `rule_version`;
- congelada compatibilidade fail-closed/exata do v1;
- `release_id` definido como identidade content-addressed do payload imutável;
- release publicada tornada imutável e supersessão movida para `supersedes_release_id` da sucessora;
- auditoria final de inferência tipou método de tabela, unidades, competência específica, policies, fórmulas, incidências, códigos, escopo e prorrateio;
- divergência de `applies_to` em `termination.reason_scope` detectada pelo novo gate e alinhada ao inventário da Fase 1;
- suíte específica do contrato fechada com **52 testes verdes**;
- criado `docs/phase2-to-phase3-handoff.md`;
- próxima etapa alterada para **Fase 3 — Biblioteca fiscal e testes**.

### 2026-09-13 — checkpoint 32/32 da Fase 2

- percorrido integralmente o `rule-inventory-v1.json`;
- mapeadas **32/32 regras** para famílias expressáveis pelo Contrato v1;
- ampliado o schema para **18 famílias de payload**;
- criado `docs/contract-coverage-v1.json` como gate machine-readable de cobertura;
- criado `docs/phase2-schema-coverage.md` como documentação humana do checkpoint;
- proveniência endurecida com método de obtenção, consistência parser/versão e regras de snapshot;
- competência endurecida com overrides tipados por contexto;
- lifecycle de release endurecido para `DRAFT`, `CANDIDATE`, `VALIDATED`, `PUBLISHED`, `SUPERSEDED` e `BLOCKED`;
- ampliada a suíte negativa; checkpoint validado com **43 testes verdes**;
- próxima etapa reduzida à rodada final de versionamento, compatibilidade, imutabilidade/supersessão e handoff para a Fase 3.

### 2026-09-13 — início da Fase 2

- Fase 2 marcada como `EM ANDAMENTO`;
- criada branch `refactor/fiscal-contract-v1` a partir do merge da Fase 1;
- materializados modelos Pydantic v2 e JSON Schema determinístico do Contrato Fiscal Canônico v1;
- criado exemplo `CANDIDATE` sem fingir release de produção;
- implementadas invariantes de vigência, qualidade, proveniência, mudança estrutural e `last-good`;
- preservadas como gates executáveis A01, separação do abono e escopo H29;
- CI ampliado para validar schema, contrato e suíte de testes;
- documentação detalhada da fase criada em `docs/phase2-contract-v1.md`.

### 2026-09-13 — fechamento da Fase 1

- Fases 0 e 1 marcadas como concluídas;
- criado registro canônico de fontes oficiais (`source-registry-v1.json`);
- congelado inventário machine-readable de regras (`rule-inventory-v1.json`);
- criado `phase1-closure.md` como handoff formal para o Contrato Fiscal Canônico v1;
- ampliados os casos de referência para 13º, férias, abono e modalidades de desligamento;
- formalizado novo P0 em H28: o principal do abono e o terço constitucional incidente sobre ele possuem tratamento de IR distinto;
- fechado o escopo inicial de H29 para motivos eSocial `01`, `02`, `07` e `33`, mantendo resultado explicitamente parcial;
- criada trilha automática `Remake CI` para toda PR destinada a `main`;
- próxima etapa alterada para Fase 2 — Contrato Fiscal Canônico v1.

### 2026-09-13 — criação

- criado o documento-mestre do remake;
- registrada a linha de base observada;
- formalizados princípios de segurança;
- definida a arquitetura-alvo;
- definida a política conceitual de atualização;
- estabelecidas as fases do projeto;
- registrado que a próxima etapa é o inventário jurídico-fiscal.
