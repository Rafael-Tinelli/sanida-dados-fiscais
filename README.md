Sanida Dados Fiscais

Repositório canônico de dados e regras fiscais computáveis utilizados pelas ferramentas financeiras da Sanida.

Princípio central: este projeto não deve apenas coletar números. Ele deve preservar, validar e publicar o significado jurídico-computacional desses números, sua vigência, sua origem oficial e as condições em que podem ser usados com segurança.

1. Objetivo

Transformar o sanida-dados-fiscais na fonte canônica, versionada e auditável para as regras e parâmetros consumidos pelas calculadoras financeiras da Sanida, em especial:

H26 — salário líquido CLT

H27 — décimo terceiro

H28 — férias CLT

H29 — rescisão CLT

A meta arquitetural é tornar essas ferramentas evergreen, com manutenção manual mínima e atualização automática sempre que a alteração normativa puder ser interpretada com segurança.

Isso não significa permitir que um scraper interprete autonomamente legislação nova. A automação deve ser máxima na detecção, coleta, validação e atualização de parâmetros; mudanças estruturais de regra devem ser detectadas automaticamente e bloqueadas para revisão humana.

2. Papel deste README

Este README é o documento-mestre do projeto.

Ele deve ser atualizado conforme o trabalho avança e registrar:

objetivos e limites do sistema;

arquitetura vigente e arquitetura-alvo;

decisões metodológicas;

fontes oficiais;

contratos de dados e regras;

invariantes de segurança;

fases de implementação;

critérios de promoção de releases;

riscos conhecidos;

decisões ainda pendentes;

estado de cada etapa.

Mudanças relevantes de arquitetura ou metodologia não devem existir apenas no código: devem ser refletidas aqui.

3. Estado inicial observado — 13/09/2026

Na linha de base deste remake, o repositório possui essencialmente:

scraper.py — coleta IRRF e INSS em páginas oficiais e incorpora taxas;

update_taxas.py — coleta Selic no SGS/BCB e CDI em fonte Cetip/B3 via FTP;

dados_fiscais.json — artefato agregado atualmente consumível;

taxas_bacen.json — artefato específico de taxas;

.github/workflows/main.yml — atualização de dados_fiscais.json;

.github/workflows/taxas.yml — atualização de taxas_bacen.json;

requirements.txt — dependências atuais reduzidas a Requests e BeautifulSoup.

O dados_fiscais.json observado na abertura desta fase declara:

schema_version: 2.2.0;

ano: 2026;

generated_at_utc: 2026-07-03T11:41:54Z;

IRRF originado da Receita Federal;

INSS originado de página oficial do INSS;

Selic originada do SGS/BCB;

CDI originado de arquivo FTP Cetip/B3.

Este estado é tratado como baseline, não como arquitetura final.

4. Problema que estamos resolvendo

A arquitetura atual consegue capturar vários parâmetros corretos, mas não modela suficientemente a semântica jurídica das regras.

Exemplo: a redução mensal do IRRF de 2026 pode ser representada numericamente por limites e coeficientes, mas isso é insuficiente se o contrato não disser sobre qual grandeza jurídica a fórmula deve ser aplicada.

Não basta preservar:

A = 978,62
B = 0,133145

O contrato precisa preservar também algo semanticamente equivalente a:

rule_id = irrf.monthly.reduction
applies_to = rendimento tributável sujeito à incidência mensal
effective_from = 2026-01-01

A ausência dessa semântica permite que números corretos sejam aplicados de maneira juridicamente incorreta.

5. Princípios não negociáveis

5.1. Fonte oficial primeiro

Dados e regras devem ser derivados prioritariamente de fontes oficiais.

Hierarquia geral:

API/dataset oficial estruturado;

ato normativo oficial;

página operacional oficial do órgão competente;

exemplo oficial de aplicação;

fonte oficial complementar para confirmação.

Fontes privadas podem ser usadas para investigação ou diagnóstico, mas não devem se tornar autoridade canônica para uma regra fiscal quando houver fonte oficial apropriada.

5.2. Scraper não é autoridade jurídica

Scrapers e parsers são sensores/coletadores.

Eles podem:

detectar mudança;

extrair parâmetros conhecidos;

preservar snapshots;

apontar divergências;

alimentar validações.

Eles não devem:

inventar interpretação para regra nova;

promover silenciosamente uma mudança estrutural;

transformar ausência de dado em zero;

relabelar dado antigo com competência nova;

considerar página acessível como prova suficiente de validade jurídica.

5.3. Vigência é parte do dado

Todo parâmetro ou regra relevante deve declarar, quando aplicável:

início de vigência;

fim de vigência;

competência;

data de publicação;

data de coleta/verificação;

fonte normativa;

fonte operacional;

versão do contrato.

generated_at nunca deve ser confundido com vigência da regra.

5.4. Falhar com segurança

Quando uma fonte falhar, o sistema deve distinguir claramente:

fonte consultada com sucesso e sem alteração;

fonte consultada com sucesso e com alteração;

fonte indisponível;

parser incompatível com novo formato;

mudança estrutural detectada;

contrato inválido;

last-good ainda juridicamente vigente;

last-good expirado ou de vigência incerta.

Uma falha de coleta nunca deve produzir um número aparentemente válido por conveniência.

5.5. Nunca relabelar dado antigo como novo

É proibido usar parâmetros históricos e atribuir a eles automaticamente o ano ou competência corrente.

Se não houver contrato válido para a nova vigência, o estado correto é de indisponibilidade/revisão, não de atualização fictícia.

5.6. Zero, ausência e não aplicabilidade são estados distintos

O contrato deve distinguir pelo menos:

valor zero real;

valor ausente;

não aplicável;

não publicado;

não encontrado;

fonte indisponível;

parser falhou;

regra ainda não vigente;

regra expirada.

5.7. Dinheiro não deve depender de float

O novo motor fiscal deve usar representação decimal apropriada para valores monetários e regras de arredondamento explicitamente documentadas.

5.8. Toda regra crítica precisa de teste de referência

Sempre que um órgão oficial publicar exemplos de aplicação, esses exemplos devem ser transformados em testes automatizados.

Além dos exemplos pontuais, o sistema deve possuir invariantes e testes de propriedades.

6. Classes de atualização

Toda alteração detectada deve ser classificada antes da publicação.

Classe A — fonte oficial estruturada

Exemplo: API SGS do Banco Central.

Pode admitir promoção automática quando:

schema esperado continua válido;

metadados são coerentes;

controles de faixa e consistência passam;

a vigência/competência é interpretada corretamente;

testes aplicáveis passam.

Classe B — mudança paramétrica em regra conhecida

Exemplos:

novas faixas de INSS mantendo a mesma estrutura jurídica;

alteração de dedução por dependente;

atualização de limites de tabela;

alteração de alíquotas mantendo o mesmo modelo.

Pode admitir automação após validação forte, preferencialmente com confirmação oficial independente e regressão completa.

Classe C — mudança estrutural de regra

Exemplos:

nova hipótese de redução;

mudança de base jurídica de incidência;

nova exceção;

nova ordem de cálculo;

regra retirada;

novo regime de tributação.

Estado obrigatório:

REVIEW_REQUIRED

Não deve ser promovida automaticamente.

Classe D — fonte indisponível ou inconclusiva

O sistema pode conservar um last-good somente se:

a vigência desse contrato ainda for válida;

não houver evidência de substituição;

o estado de saúde indicar explicitamente que a nova consulta falhou.

Nunca deve criar fallback fiscal fictício.

7. Arquitetura-alvo

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

8. Separação de domínios

O projeto deve distinguir dois domínios com ciclos de vida diferentes.

8.1. Contratos jurídico-fiscais de folha

Incluem, conforme o escopo final:

INSS do empregado;

IRRF mensal;

desconto simplificado mensal;

dedução por dependente;

redução mensal do IRRF;

13º salário;

férias;

terço constitucional;

abono pecuniário;

regras necessárias ao cálculo de rescisão dentro do escopo declarado pela ferramenta.

8.2. Dados financeiros/de referência

Incluem, por exemplo:

Selic;

CDI;

outros indicadores monetários futuros.

Esses dados podem permanecer no mesmo repositório, mas não devem compartilhar automaticamente o mesmo contrato de vigência, fallback ou validação jurídica das regras trabalhistas/fiscais.

9. Contrato Fiscal Canônico v1

A primeira grande entrega deste remake será a especificação e implementação do Contrato Fiscal Canônico v1.

Cada regra deverá ser capaz de expressar, conforme aplicável:

rule_id estável;

versão do schema;

jurisdição;

domínio;

descrição técnica;

variável/objeto jurídico ao qual se aplica;

dependências;

ordem de cálculo;

parâmetros;

unidade;

regras de arredondamento;

vigência inicial;

vigência final;

competência;

fonte normativa;

fonte operacional;

exemplos oficiais associados;

método de extração;

hash/snapshot da fonte;

data da última verificação;

estado de qualidade;

política de atualização;

classificação da última mudança.

O schema definitivo ainda será desenhado. Esta seção descreve requisitos, não uma estrutura já congelada.

10. Fontes oficiais — estratégia inicial

10.1. Banco Central

Quando houver API/dataset oficial estruturado, deve-se preferi-lo ao scraping.

A Selic já é obtida via SGS/BCB no código atual.

A estratégia para CDI será revisada para verificar se o consumo pode ser centralizado em recurso oficial estruturado, reduzindo dependências de FTP/formato legado.

10.2. Receita Federal — IRRF

Não presumir que uma página HTML seja contrato computacional.

O sistema deve combinar, quando disponível:

tabela oficial;

legislação associada;

vigência;

exemplos oficiais de cálculo;

validações semânticas próprias.

A extração HTML deve funcionar como sensor/parser de uma estrutura conhecida, não como intérprete autônomo de legislação nova.

10.3. INSS / MPS / eSocial

A tabela operacional deve ser reconciliada com a norma correspondente e, quando útil, com publicações oficiais independentes do ecossistema previdenciário/trabalhista.

Mudanças apenas numéricas podem ser automatizáveis; mudança semântica deve exigir revisão.

11. Stack técnica planejada

A adoção será incremental, evitando refatoração big-bang.

Núcleo

Python 3.11+;

Decimal para dinheiro e coeficientes monetários;

Pydantic v2 para contratos tipados e validação forte;

JSON Schema como contrato público independente da implementação Python.

Testes

pytest;

Hypothesis para property-based testing;

casos oficiais convertidos em testes de referência;

fixtures/snapshots reproduzíveis de fontes externas.

Mudanças e proveniência

DeepDiff ou mecanismo equivalente para diff semântico;

hashes de snapshots;

classificação explícita de mudanças.

HTTP e resiliência

Planejado:

HTTPX ou cliente HTTP equivalente com timeouts explícitos;

Tenacity para retry/backoff seletivo;

VCR.py ou mecanismo equivalente para reproduzir respostas oficiais em testes.

Princípio de dependência

Uma biblioteca só deve entrar no caminho crítico se melhorar auditabilidade, segurança ou manutenção de forma objetiva.

Não será adotada uma rules engine genérica apenas para abstrair regras fiscais simples e auditáveis em funções puras.

12. Invariantes mínimas do sistema

As seguintes condições deverão se tornar testes obrigatórios:

dado histórico nunca recebe automaticamente competência futura;

ausência de parâmetro obrigatório nunca vira zero por conveniência;

regra futura não é aplicada antes da vigência;

regra expirada não permanece ativa após substituição válida;

falha de fonte não é indistinguível de coleta bem-sucedida sem mudança;

mudança estrutural não é autopublicada;

artefato inválido não substitui last-good;

last-good só pode ser usado enquanto sua vigência permitir;

todo valor publicado deve ter proveniência rastreável;

todo cálculo crítico deve conseguir identificar a versão do contrato utilizado;

arredondamentos fiscais devem ser determinísticos e testados;

exemplos oficiais devem ser reproduzíveis pela implementação correspondente.

13. Estratégia de snapshots e auditoria

O projeto deverá preservar evidência suficiente para responder posteriormente:

qual conteúdo oficial foi observado;

quando foi observado;

de qual URL/recurso veio;

qual hash possuía;

qual parser o interpretou;

qual contrato resultou;

quais testes passaram;

qual release foi promovida;

quais consumidores receberam essa release.

A arquitetura exata de retenção será definida durante a implementação para evitar crescimento desnecessário do repositório.

14. Política de promoção

Nenhum novo contrato fiscal deve ser promovido apenas porque o scraper terminou sem exceção.

A promoção deverá depender de gates explícitos.

Estado conceitual desejado:

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

A implementação definitiva desse lifecycle será feita em fase posterior.

15. Relação com as calculadoras H26–H29

Este repositório será a origem canônica; as calculadoras serão consumidores.

O consumidor não deve reconstruir, inferir ou completar silenciosamente uma regra ausente no contrato.

Fluxo desejado:

sanida-dados-fiscais
        ↓
contrato/release validada
        ↓
camada WordPress/cache
        ↓
folha-core
        ↓
H26 / H27 / H28 / H29

A correção final das calculadoras só deve ser considerada encerrada quando produtor e consumidor compartilham o mesmo contrato semântico.

16. Escopo inicial das calculadoras

H26 — Salário líquido

Deverá cobrir pelo menos:

INSS progressivo;

IRRF mensal;

desconto simplificado versus deduções aplicáveis;

dependentes;

redução mensal vigente;

ordem correta de cálculo;

limites e transições de faixas;

arredondamento.

H27 — 13º salário

Deverá distinguir a apuração própria do 13º da folha mensal e mapear:

avos;

regra dos 15 dias;

ano de admissão;

primeira parcela;

segunda/parcela final;

INSS específico do 13º;

IRRF específico do 13º;

deduções aplicáveis;

datas/regras de pagamento quando pertinentes ao escopo.

H28 — Férias

Deverá distinguir:

período aquisitivo;

período concessivo/gozo, quando pertinente ao escopo;

férias integrais e proporcionais;

terço constitucional;

abono pecuniário;

limites legais do abono;

incidências fiscais/previdenciárias;

impacto de faltas injustificadas somente se efetivamente suportado pela ferramenta.

H29 — Rescisão

Deverá calcular apenas aquilo que estiver expressamente dentro do escopo declarado, sem criar aparência de cobertura universal de todas as modalidades, verbas e exceções trabalhistas.

O inventário deverá partir da implementação real da ferramenta antes de definir regras adicionais.

17. Fases do remake

Fase 0 — Baseline e governança

Status: EM ANDAMENTO

Objetivos:

criar README mestre;

registrar arquitetura atual;

congelar princípios e invariantes;

não alterar ainda a produção sem contrato e testes.

Critério de conclusão:

README inicial criado e coerente com o repositório real.

Fase 1 — Inventário jurídico-fiscal

Status: PENDENTE

Objetivos:

mapear todas as regras necessárias a H26–H29;

identificar fontes normativas e operacionais;

classificar cada regra como parâmetro, regra parametrizável ou regra estrutural;

documentar vigência, dependências e exemplos oficiais.

Critério de conclusão:

nenhuma variável relevante das quatro calculadoras sem origem e semântica definidas.

Fase 2 — Contrato Fiscal Canônico v1

Status: PENDENTE

Objetivos:

definir modelos Pydantic;

definir JSON Schema;

definir estados de qualidade e mudança;

definir política de last-good;

definir proveniência.

Fase 3 — Biblioteca fiscal e testes

Status: PENDENTE

Objetivos:

implementar funções fiscais puras;

usar Decimal;

converter exemplos oficiais em testes;

criar property-based tests e invariantes.

Fase 4 — Fontes e sensores

Status: PENDENTE

Objetivos:

separar collectors de parsers;

priorizar APIs oficiais estruturadas;

preservar snapshots necessários;

melhorar retry/timeouts;

implementar detecção de alterações e falhas.

Fase 5 — Diff semântico e gates de publicação

Status: PENDENTE

Objetivos:

classificar mudança paramétrica versus estrutural;

bloquear promoção insegura;

publicar somente releases validadas;

tornar estado operacional observável.

Fase 6 — Migração dos consumidores

Status: PENDENTE

Objetivos:

migrar plugin/cache;

migrar folha-core;

corrigir H26–H29 contra o contrato canônico;

remover duplicação de regra fiscal nos consumidores.

Fase 7 — Fechamento e operação evergreen

Status: PENDENTE

Objetivos:

testes ponta a ponta;

validação de atualização automática;

simulação de falha de fonte;

simulação de mudança paramétrica;

simulação de mudança estrutural;

documentação operacional;

critérios objetivos de encerramento.

18. Decisões já tomadas

O sanida-dados-fiscais será a fonte canônica das regras/dados fiscais usados pelas calculadoras.

O README será o centro documental do projeto.

Não será feita apenas uma correção pontual para 2026; a arquitetura deve prevenir obsolescência futura.

Scraping de HTML será tratado como sensor/parser, não como autoridade jurídica autônoma.

APIs oficiais estruturadas serão preferidas quando disponíveis.

Mudança paramétrica e mudança estrutural terão políticas diferentes.

Mudança estrutural exigirá revisão humana.

Fallback estático que possa fingir atualidade será removido da arquitetura final.

Regras monetárias usarão representação decimal e política explícita de arredondamento.

H26–H29 serão auditadas e testadas individualmente, mesmo quando compartilham motor comum.

O repositório pode conter dados fiscais e indicadores financeiros, mas os domínios terão contratos e políticas de validade independentes.

O novo sistema deverá ser auditável retroativamente: fonte observada → parser → contrato → testes → release → consumidor.

19. Questões em aberto

Estas decisões ainda precisam ser fechadas durante as fases seguintes:

schema exato do Contrato Fiscal Canônico v1;

conjunto completo de regras necessárias às quatro calculadoras;

política exata de retenção de snapshots;

critérios de confirmação multi-fonte para mudanças paramétricas;

política de versionamento de releases e schemas;

mecanismo de distribuição para WordPress;

tempo máximo de uso de last-good por domínio/regra;

estratégia final para CDI;

escopo jurídico exato de H29;

granularidade dos contratos: por regra, por competência, por domínio ou combinação dessas abordagens.

20. Próxima etapa

Executar a Fase 1 — Inventário jurídico-fiscal antes de alterar o pipeline principal.

A primeira saída dessa fase deve ser uma matriz das regras necessárias a H26, H27, H28 e H29 contendo, para cada item:

regra/conceito;

ferramenta que consome;

significado jurídico;

fonte normativa;

fonte operacional;

parâmetros;

variável de aplicação;

vigência;

ordem/dependências;

classificação de automação;

exemplos oficiais disponíveis;

riscos/exceções;

status de implementação atual.

A matriz será a especificação jurídica que antecede a especificação de software.

21. Convenção de status

Para manter este README útil ao longo do projeto, cada fase deverá usar apenas um destes estados:

PENDENTE

EM ANDAMENTO

BLOQUEADA

EM REVISÃO

CONCLUÍDA

Uma fase só deve ser marcada como CONCLUÍDA quando seus critérios de conclusão estiverem objetivamente atendidos.

22. Changelog do README

2026-09-13 — criação

criado o documento-mestre do remake;

registrada a linha de base observada;

formalizados princípios de segurança;

definida a arquitetura-alvo;

definida a política conceitual de atualização;

estabelecidas as fases do projeto;

registrado que a próxima etapa é o inventário jurídico-fiscal.
