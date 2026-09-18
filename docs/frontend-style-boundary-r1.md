# Frontend Style Boundary R1

**Status:** AUDIT_OPEN  
**Branch:** `refactor/frontend-css-boundary-r1`  
**Baseline de produção:** `427d93e4cfcddb848e59dcad89f207ed66a754b8`

## 1. Objetivo

Separar a responsabilidade visual e de UX das calculadoras H25–H29 da responsabilidade jurídico-fiscal deste repositório, sem reabrir fórmulas, regras, releases ou contratos já encerrados.

A revisão parte de um problema concreto: o frontend público está funcional e integrado ao backend, mas a camada de estilo evoluiu de forma fragmentada. O objetivo desta fase não é aplicar patches cosméticos isolados; é construir uma arquitetura visual coerente e preparar sua extração para um repositório frontend próprio.

## 2. Fronteira de escopo

### Dentro desta fase

- inventário do CSS efetivamente consumido por H25–H29;
- estilos inline e blocos `<style>` nos templates;
- design tokens;
- tipografia, espaçamento, grid, cards, botões, formulários e resultados;
- responsive behavior;
- foco, contraste, estados de erro/sucesso, motion e acessibilidade visual;
- consistência visual entre H25, H26, H27, H28 e H29;
- testes de browser/computed style;
- preparação da extração do frontend para repositório próprio.

### Fora desta fase

- regras fiscais;
- fórmulas de INSS/IRRF/13º/férias/rescisão;
- Fiscal Contract 1.2;
- releases fiscais;
- collectors/sensors;
- Python fiscal;
- mudança do escopo funcional de H26–H29.

## 3. Estado atual observado

O repositório controla hoje as páginas e grande parte do comportamento público das calculadoras em `consumers/frontend/`.

Ao mesmo tempo:

1. H25/H26 usam folhas externas compartilhadas e classes globais do site;
2. H27 usa uma folha visual própria distinta;
3. H28 e H29 carregam `calculadoras-ui.css`, mas complementam a interface com grandes blocos `<style>` dentro de `01-head-hero.php`;
4. há atributos `style="..."` de apresentação dentro de templates;
5. já existem declarações `!important` para vencer a cascade;
6. os três stylesheets públicos principais aparecem no deployment V2 como dependências preexistentes, não como `managed_files`;
7. os testes de frontend atuais verificam principalmente DOM, shell, semântica, SEO, runtime e contratos; não existe ainda um contrato de estilo suficiente para detectar regressões visuais importantes.

Um inventário executável foi adicionado em:

```text
scripts/audit_frontend_style_boundary.py
```

Ele reporta:

- arquivos PHP analisados;
- blocos `<style>`;
- atributos `style`;
- uso de `!important`;
- referências a stylesheets;
- CSS existentes sob `consumers/frontend`;
- folhas referenciadas sem arquivo-fonte correspondente sob essa árvore.

## 4. Achados arquiteturais

### CSS-01 — ownership misturado

O repositório fiscal passou a decidir simultaneamente regras computacionais e apresentação pública. Isso aumenta o blast radius de mudanças puramente visuais e torna o ciclo de estilo dependente de gates concebidos para integridade fiscal.

### CSS-02 — design system incompleto

`calculadoras-ui.css` funciona como camada-base, mas não cobre de forma suficiente todos os componentes usados pela família. Cada página completa a linguagem visual de maneira própria.

### CSS-03 — três dialetos visuais

Na prática existem pelo menos três abordagens concorrentes:

- `cp-*` compartilhado;
- sistema visual próprio do H27;
- extensões `vf-*`/`rc-*` de H28/H29.

### CSS-04 — cascade por sobrescrita

Blocos inline, seletores prefixados por `#calc-page` e `!important` indicam que a composição visual depende de especificidade crescente em vez de componentes previsíveis.

### CSS-05 — tokens semânticos inconsistentes

Há variáveis de cor com papéis diferentes entre as páginas. A arquitetura futura precisa distinguir claramente tokens de marca, tokens semânticos e tokens de componente.

### CSS-06 — dívida de apresentação dentro do markup

Backgrounds, visibilidade e outros detalhes de apresentação ainda aparecem em atributos `style="..."`. Essa dívida deve ser eliminada gradualmente, sem quebrar o runtime que hoje usa `style.display` em alguns estados dinâmicos.

### CSS-07 — acessibilidade visual sem contrato sistêmico

Estados de foco, contraste, reduced motion, touch targets e responsive behavior não são validados como uma política única da família.

### CSS-08 — CI não representa qualidade visual

O browser smoke prova execução e contratos funcionais, mas ainda não prova que a página respeita a hierarquia visual, a largura dos grids, a visibilidade dos controles, os estados de foco ou os breakpoints esperados.

## 5. Arquitetura-alvo

### 5.1. `sanida-dados-fiscais`

Deve permanecer responsável por:

- regras e parâmetros fiscais;
- contratos e schemas;
- releases;
- endpoints/caches fiscais;
- engines/browser runtime que implementem lógica fiscal compartilhada;
- testes de paridade e fail-closed;
- contrato de integração consumido pelo frontend.

Não deve ser o proprietário definitivo de decisões como cor, espaçamento, radius, sombra, hero, grid, breakpoint ou composição editorial da página.

### 5.2. futuro repositório frontend

Deve ser responsável por:

- H25–H29 como páginas públicas;
- templates PHP e composição de shell;
- CSS e design tokens;
- componentes visuais;
- adaptadores DOM/UI;
- UX copy estrutural;
- acessibilidade de interface;
- browser smoke visual/computed style;
- deployment dos arquivos públicos do frontend.

Nome de trabalho sugerido:

```text
sanida-financas-frontend
```

O nome não é contrato; o boundary é.

### 5.3. contrato entre os repositórios

O frontend não deve conhecer detalhes internos do pipeline fiscal. Ele deve consumir uma fronteira estável e versionada:

```text
frontend
   │
   ├── endpoint/release fiscal publicado
   └── runtime fiscal versionado
           │
           └── contrato 1.2 + fail-closed
```

Uma alteração visual não deve exigir mudança em regra fiscal. Uma alteração fiscal não deve exigir reescrever markup ou CSS, salvo quando o contrato público realmente mudar.

## 6. Design system alvo

A família H25–H29 deve compartilhar uma gramática visual única.

### Tokens

- cores de marca;
- cores semânticas: text, muted, success, warning, danger, info;
- backgrounds/surfaces;
- border;
- radius;
- shadows;
- spacing scale;
- type scale;
- container widths;
- breakpoints;
- sticky offsets.

### Componentes comuns

- page shell;
- hero;
- section header;
- cards;
- form fields;
- checkbox/radio/select;
- primary/secondary/ghost button;
- alert;
- result summary;
- KPI;
- calculation rows;
- details/progressive disclosure;
- scope/limitation callout;
- FAQ;
- related calculators;
- empty/loading/error/success states.

### Extensões específicas

Cada calculadora pode manter classes próprias apenas para diferenças reais do produto:

- H26: `sl-*`;
- H27: `d13-*`;
- H28: `vf-*`;
- H29: `rc-*`.

Essas extensões não devem reimplementar cards, inputs, buttons, grids ou KPIs já existentes no sistema comum.

## 7. Sequência de migração

### M0 — inventário e freeze de dívida

- mapear estilos atuais;
- impedir novos blocos `<style>`, `style="..."` e `!important` fora da allowlist temporária;
- não alterar produção.

### M1 — design system canônico

- definir tokens e componentes comuns;
- eliminar colisões de nomes/semântica;
- criar testes de computed style em Chrome;
- manter DOM e contratos fiscais estáveis.

### M2 — migrar página por página

Ordem proposta:

1. H25, por ser estruturalmente simples;
2. H26, por possuir maior mistura de camadas;
3. H28;
4. H29;
5. H27, preservando cuidadosamente seu frontend mais elaborado e o território orgânico já trabalhado.

Cada migração deve reduzir dívida; nunca acrescentar uma quarta linguagem visual.

### M3 — externalização

- criar repositório frontend próprio;
- mover templates, CSS, componentes e UI adapters;
- manter no repositório fiscal somente a fronteira computacional;
- preservar URLs públicas e paths de assets;
- introduzir integração cross-repo versionada.

### M4 — deployment independente

- frontend passa a publicar suas páginas/assets;
- fiscal continua publicando release/runtime fiscal;
- integração é validada antes de produção;
- rollback visual não reverte dados/regras fiscais e vice-versa.

## 8. Guardrail inicial

O teste:

```text
tests/test_frontend_style_boundary_r1.py
```

não declara que a dívida atual é aceitável. Ele apenas impede que ela se espalhe enquanto a migração estiver em andamento.

O objetivo final é reduzir as allowlists a zero para:

- blocos `<style>` embutidos;
- atributos inline usados apenas para apresentação;
- `!important` usado para vencer conflitos de cascade.

Exceções dinâmicas estritamente necessárias devem ser justificadas por comportamento, não por conveniência de CSS.

## 9. Critério de sucesso

A fase só pode ser considerada fechada quando:

1. H25–H29 parecem uma família única;
2. os componentes comuns têm uma única implementação visual;
3. cada calculadora mantém apenas extensões específicas reais;
4. contraste/foco/responsividade possuem testes objetivos;
5. não há novos blocos inline ou escalada de especificidade;
6. o frontend pode ser extraído sem levar junto regras, releases ou engines fiscais;
7. o backend fiscal pode evoluir sem possuir decisões puramente visuais;
8. deployment e rollback das duas camadas podem ser desacoplados com segurança.
