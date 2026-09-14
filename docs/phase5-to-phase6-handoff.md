# Handoff formal — Fase 5 → Fase 6

**Data:** 2026-09-14  
**Origem:** Fase 5 — Diff semântico e gates de publicação  
**Destino:** Fase 6 — Migração dos consumidores  
**Status da Fase 5:** CONCLUÍDA  
**Contrato de publicação:** Fiscal Contract **v1.2.0**  
**Escopo da Fase 6:** consumidores das calculadoras H26–H29

---

## 1. Estado recebido pela Fase 6

A Fase 6 recebe um produtor canônico já fechado nas camadas de inventário, contrato, engine, fontes/sensores, diff semântico e publicação.

A cadeia disponível é:

```text
fontes oficiais / governança interna
        ↓
snapshots hash-addressed
        ↓
parsers conhecidos (somente onde autorizados)
        ↓
assembler fiscal 32/32
        ↓
semantic diff + promotion gates
        ↓
release fiscal v1.2 imutável
        ↓
releases/fiscal-v1/current.json
        ↓
Fase 6 — consumidores
```

O Remake CI run **#304 / 34802354014** validou no mesmo head técnico:

- **237 testes**;
- JSON Schema v1.2 gerado deterministicamente;
- Phase 3 closure gate: PASS;
- Phase 4 foundation, boundary e formal closure: PASS;
- Phase 5 semantic-diff foundation: PASS;
- Phase 5 publication foundation: PASS;
- Phase 5 formal closure gate: PASS.

A Fase 6 não deve reabrir decisões das Fases 1–5 sem defeito objetivo e reproduzível.

---

## 2. Pré-condição operacional antes de migrar qualquer consumidor

O merge da Fase 5 instala o mecanismo de publicação, mas **não cria automaticamente a primeira release fiscal v1.2**.

Antes de alterar WordPress, `folha-core` ou H26–H29, deve existir uma primeira release `PUBLISHED` 32/32 produzida pelo workflow:

```text
.github/workflows/fiscal-release-v12.yml
```

O bootstrap é deliberadamente manual e exige `approval_reference` explícita. Execução agendada não pode criar a primeira release.

A entrada da Fase 6 só fica operacionalmente liberada quando forem verificados:

1. `releases/fiscal-v1/current.json` existente e íntegro;
2. artefato apontado por `current.json` existente e com SHA-256 correspondente;
3. contrato `schema_version=1.2.0` e `contract_api_version=1.2.0`;
4. status `PUBLISHED`;
5. cobertura exata das **32 regras**;
6. `release_id` content-addressed válido;
7. lifecycle com `approval_mode=human_reviewed` no bootstrap;
8. `assert_consumable()` equivalente passando antes de qualquer exposição ao frontend.

Nenhum consumidor deve ser migrado para `CANDIDATE`, para o exemplo histórico v1.1 ou diretamente para `dados_fiscais.json` como substituto do contrato canônico.

---

## 3. Objetivo da Fase 6

Migrar a cadeia de consumo das calculadoras para a release fiscal canônica sem duplicar autoridade fiscal nos consumidores.

Sequência alvo:

```text
releases/fiscal-v1/current.json
        ↓
release imutável v1.2
        ↓
WordPress / sanida-fiscais-auto / cache / REST
        ↓
folha-core
        ↓
H26 / H27 / H28 / H29
```

O consumidor pode executar cálculo e apresentação, mas não pode reconstruir fonte, inventar regra ausente, alterar vigência, completar valor faltante ou reinterpretar mudança estrutural.

---

## 4. Ordem obrigatória de implementação

### C6.0 — ativação da primeira release v1.2

Antes do frontend:

- fazer merge da Fase 5;
- executar bootstrap manual do workflow v1.2 com referência humana verificável;
- validar `current.json`, artefato, hash, 32/32 e lifecycle;
- congelar o `release_id` inicial como baseline dos testes da migração.

### C6.1 — contrato de consumo WordPress / `sanida-fiscais-auto`

Migrar primeiro a fronteira de rede/cache.

O plugin deve:

- buscar `current.json`;
- resolver apenas o artefato imutável apontado pelo manifest;
- verificar SHA-256 e identidade da release antes de aceitar bytes;
- exigir compatibilidade exata de schema/API suportada;
- rejeitar `CANDIDATE`, release inválida, hash divergente ou regra obrigatória ausente;
- armazenar em cache a release íntegra e o respectivo `release_id`;
- expor metadados mínimos de release ao `folha-core` para auditabilidade;
- não usar `raw.githubusercontent.com/.../main/dados_fiscais.json` como fallback fiscal canônico;
- não transformar falha de atualização em timestamp de atualidade fictício.

Last-good só pode ser reutilizado segundo política explícita de validade/compatibilidade; falha de rede não autoriza reinterpretar release antiga como nova.

### C6.2 — `folha-core`

Depois de estabilizada a fronteira WordPress:

- substituir tabelas/parâmetros fiscais hardcoded pelo contrato v1.2;
- centralizar seleção por vigência, contexto e assessment identity;
- preservar dinheiro em representação decimal segura na fronteira de cálculo;
- preservar `release_id`, `rule_id` e `rule_version` na memória de cálculo quando aplicável;
- falhar fechado quando faltar regra/semântica suportada;
- remover lógica paralela que contradiga o contrato.

A migração não deve criar uma segunda interpretação jurídica em JavaScript. Fórmulas executadas no browser devem obedecer diretamente aos payloads/semânticas já fechados pelo contrato.

### C6.3 — H26 — salário líquido CLT

Critérios mínimos:

- INSS progressivo vindo do contrato;
- IRRF mensal com tabela, dependentes e desconto simplificado do contrato;
- A01 preservado: redutor de 2026 recebe o rendimento tributável sujeito à incidência mensal, **não** a base pós-deduções;
- regressão de referência de `6000.00` preservando base `5350.40` e IRRF final `382.88`;
- resultado identifica a release fiscal efetivamente usada.

### C6.4 — H27 — décimo terceiro

Critérios mínimos:

- avos e limiar de 15 dias conforme contrato;
- assessment de 13º separado da remuneração mensal;
- deduções confinadas à própria apuração;
- eliminar `total13 * 0.5` como regra universal de primeira parcela;
- branches não modeladas de admissão/remuneração variável devem falhar fechadas ou ser explicitamente limitadas na UX.

### C6.5 — H28 — férias CLT

Critérios mínimos:

- período aquisitivo ancorado no vínculo, inclusive cruzando ano civil;
- regressão `01/09/2025 → 31/03/2026 = 7/12`;
- direito por faltas conforme regra contratual suportada;
- abono = 1/3 do direito adquirido, sem arredondamento arbitrário de “dias” que altere a regra;
- principal do abono e terço constitucional permanecem componentes tributários separados;
- férias gozadas mantêm assessment próprio de IRRF;
- tratamento indenizatório não herda incidência de férias gozadas.

### C6.6 — H29 — rescisão CLT

Critérios mínimos:

- UI/modelo exige motivo eSocial suportado `01`, `02`, `07` ou `33`;
- motivo `01` preserva saldo salarial e bloqueia 13º/férias proporcionais;
- `02/07/33` habilitam proporcionais segundo a matriz fechada;
- saldo de salário usa dias civis reais do mês no escopo padrão; divisor 30 não é default universal;
- 13º permanece calendar-year based;
- férias permanecem acquisition-period based;
- resultado continua explicitamente `partial_estimate`;
- aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas permanecem fora do total até extensão formal de contrato.

### C6.7 — remoção controlada do legado

Somente depois de H26–H29 passarem pelos testes da nova cadeia:

- remover fallback fiscal antigo do plugin/REST;
- remover tabelas e constantes fiscais duplicadas do `folha-core` e dos JS das calculadoras;
- manter adaptadores temporários apenas quando houver consumidor identificado e prazo de retirada;
- provar por busca/teste que H26–H29 não voltam a consumir `dados_fiscais.json` como autoridade canônica.

---

## 5. Defeitos legados que a Fase 6 deve eliminar

O estado de frontend recebido antes da migração contém, entre outros, estes desvios já conhecidos:

- `folha-core.js`: redutor IR aplicado sobre base pós-deduções em vez do rendimento-alvo da regra;
- `folha-core.js`: cálculo de férias proporcionais perde meses do período aquisitivo quando cruza ano civil;
- `ferias-clt.js`: cálculo de abono por `Math.round(dias / 3)` conflita com a semântica do direito adquirido e com a UX fixa de “10 dias”;
- `decimo-terceiro.js`: primeira parcela modelada genericamente como `total13 * 0.5`;
- H29: ausência do motivo eSocial como input, divisor 30 oferecido como padrão comercial e proporcionais aplicados sem a matriz `01/02/07/33`;
- `sanida-fiscais-auto`: consumo fiscal legado por `dados_fiscais.json` / raw `main`, em vez de manifest + release imutável v1.2.

Esses pontos devem ser corrigidos pela migração ao contrato, e não por novos hardcodes paralelos.

---

## 6. Testes exigidos na Fase 6

A Fase 6 deve criar gates próprios, sem apagar os gates anteriores. No mínimo:

- manifest válido → release válida → cache → REST → `folha-core`;
- SHA divergente bloqueia consumo;
- schema/API incompatível bloqueia consumo;
- release incompleta bloqueia consumo;
- indisponibilidade de rede não fabrica release atual;
- troca de `current.json` para sucessora válida invalida/renova cache de modo determinístico;
- mesma release não gera churn de cache;
- H26 reproduz A01;
- H27 cobre admissão, 15 dias e branch não suportada;
- H28 cobre aquisição cross-year, abono e incidências separadas;
- H29 cobre os quatro motivos eSocial suportados e a natureza parcial do resultado;
- cada cálculo consegue informar o `release_id` usado.

A Fase 6 pode usar testes de paridade entre engine Python e implementação consumidora para os vetores fechados, desde que a implementação do consumidor não passe a ser nova autoridade normativa.

---

## 7. Critérios de saída da Fase 6

A Fase 6 só pode ser considerada concluída quando:

1. existe release fiscal v1.2 `PUBLISHED` 32/32 em produção;
2. `sanida-fiscais-auto` consome manifest + release imutável e valida identidade/integridade;
3. `folha-core` deixou de carregar regra fiscal conflitante/duplicada;
4. H26, H27, H28 e H29 utilizam a mesma release canônica;
5. os defeitos A01/A02 e os desvios H27/H28/H29 conhecidos estão cobertos por regressão;
6. falhas de release/cache são fail-closed;
7. `release_id` e versões de regra ficam auditáveis no caminho do cálculo;
8. gates permanentes da Fase 6 passam junto com os gates das Fases 3–5;
9. documentação e README são sincronizados no mesmo head;
10. somente então o projeto é entregue à **Fase 7 — fechamento E2E e operação evergreen**.

---

## 8. Fora do escopo da Fase 6

Não fazem parte desta fase, salvo defeito diretamente necessário à migração das calculadoras:

- matérias do blog;
- Hub Finanças;
- consórcios;
- crédito;
- previdência;
- redesign editorial/SEO;
- novos produtos ou novas regras trabalhistas fora do inventário fechado;
- expansão de H29 além do escopo `partial_estimate` atual;
- reinterpretação jurídico-fiscal das Fases 1–3 sem evidência nova.

Outras páginas podem futuramente consumir a release, mas são consumidores posteriores e não devem ampliar este checkpoint.

---

## 9. Primeiro checkpoint recomendado da Fase 6

O primeiro checkpoint deve ser estreito e verificável:

```text
C6.0 bootstrap v1.2 publicado e validado
        +
C6.1 consumer adapter do sanida-fiscais-auto
        +
cache/REST com release_id e integridade
        +
testes fail-closed
```

Só depois disso deve começar a migração de `folha-core` e das quatro calculadoras.

Esse sequenciamento evita corrigir novamente o frontend contra um contrato ainda não materializado em produção.
