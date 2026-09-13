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

A **Fase 2 está em andamento** e o primeiro corte executável do Contrato Fiscal Canônico v1 já foi materializado. A especificação detalhada da fase está em `docs/phase2-contract-v1.md`.

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

O schema v1 ainda está sob validação da Fase 2; alterações estruturais continuam permitidas nesta branch até o fechamento formal da fase, sempre acompanhadas de testes e atualização deste README.

---

## 10. Fontes oficiais — estratégia inicial

### 10.1. Banco Central

Quando houver API/dataset oficial estruturado, deve-se preferi-lo ao scraping.

A Selic já é obtida via SGS/BCB no código atual.

A estratégia para CDI será revisada para verificar se o consumo pode ser centralizado em recurso oficial estruturado, reduzindo dependências de FTP/formato legado.

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

Planejado:

- HTTPX ou cliente HTTP equivalente com timeouts explícitos;
- Tenacity para retry/backoff seletivo;
- VCR.py ou mecanismo equivalente para reproduzir respostas oficiais em testes.

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

A arquitetura exata de retenção será definida durante a implementação para evitar crescimento desnecessário do repositório.

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

**Status: EM ANDAMENTO**

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
- suíte de contrato no checkpoint de cobertura: **43 testes verdes** em `Remake CI`.

Artefatos adicionais do checkpoint:

- `docs/contract-coverage-v1.json`;
- `docs/phase2-schema-coverage.md`.

### Fase 3 — Biblioteca fiscal e testes

**Status: PENDENTE**

Objetivos:

- implementar funções fiscais puras;
- usar `Decimal`;
- converter exemplos oficiais em testes;
- criar property-based tests e invariantes.

### Fase 4 — Fontes e sensores

**Status: PENDENTE**

Objetivos:

- separar collectors de parsers;
- priorizar APIs oficiais estruturadas;
- preservar snapshots necessários;
- melhorar retry/timeouts;
- implementar detecção de alterações e falhas.

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

---

## 19. Questões em aberto

Após o fechamento da Fase 1, as questões remanescentes são de **design de software e operação**, não lacunas jurídicas P0 do inventário. A Fase 2 já fechou a expressividade do schema para as 32 regras, com 18 famílias tipadas, e endureceu proveniência, competência e lifecycle. Permanecem em aberto:

- política definitiva de versionamento entre `schema_version`, `contract_api_version`, `rule_version` e `release_id`;
- compatibilidade backward/forward e critérios objetivos para major/minor/patch;
- imutabilidade, supersessão e organização física das releases canônicas;
- revisão final de campos que ainda poderiam exigir inferência do consumidor;
- política exata de retenção de snapshots na futura camada de fontes;
- critérios de confirmação multi-fonte para mudanças paramétricas;
- mecanismo de semantic diff — Fase 5;
- mecanismo de distribuição para WordPress/SFA — Fase 6;
- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência.

---

## 20. Próxima etapa

Continuar a **Fase 2 — Contrato Fiscal Canônico v1** pela rodada final de estabilidade do contrato.

A cobertura de expressividade já está fechada: **32/32 regras**, **18 famílias tipadas**, proveniência/competência/lifecycle endurecidos e testes negativos ativos. As próximas entregas dentro da própria Fase 2 são:

- congelar a política de versionamento (`schema_version`, `contract_api_version`, `rule_version`, `release_id`);
- definir regras de compatibilidade backward/forward;
- fechar imutabilidade e supersessão de releases;
- revisar se resta qualquer campo semântico que force inferência do consumidor;
- documentar o handoff formal para a Fase 3.

O pipeline de produção continua congelado: `scraper.py`, `update_taxas.py`, `dados_fiscais.json`, `taxas_bacen.json` e os consumidores não serão migrados antes do fechamento do Contrato v1.

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
