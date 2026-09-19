# Dados Oficiais BR

Código versionado do plugin WordPress **Dados Oficiais BR**.

## Estado canônico

- versão: **1.4.8**
- produção: `/home1/sanid210/public_html/blog/wp-content/plugins/dados-oficiais-br/dados-oficiais-br.php`
- fonte versionada: `integrations/wordpress/dados-oficiais-br/dados-oficiais-br.php`
- produção v1.4.8 foi validada em 2026-09-19 com PHP lint, harness sintético e WP-CLI.

Este diretório é deliberadamente separado de `consumers/wordpress/sanida-fiscais-auto.php`.
O DOBR não integra o contrato `br.sanida.fiscal` nem as releases imutáveis de `releases/fiscal-v1`.
Ele é uma integração WordPress complementar que coleta/exibe dados oficiais fora desse contrato.

## Responsabilidades

### Salário mínimo

Shortcodes:

- `[sm_valor]`
- `[sm_valor_raw]`
- `[sm_vigencia]`
- `[sm_dia]`
- `[sm_hora]`
- `[sm_serie_json]`
- `[sm_debug]`

A fonte operacional é a página estável do INSS:

`https://www.gov.br/inss/pt-br/direitos-e-deveres/inscricao-e-contribuicao/tabela-de-contribuicao-mensal`

O parser:

1. exige marcador do ano corrente;
2. extrai o teto da primeira faixa de empregado;
3. exige o mesmo piso nas linhas de 5% e 11% da seção de contribuinte individual/facultativo;
4. falha fechado se a estrutura ou os valores divergirem.

O cache também é year-aware: um transient do ano anterior não atravessa a virada anual.

### PIS / abono salarial

`[pis_valor meses="N"]` usa o salário mínimo efetivo do ano corrente e arredondamento para a unidade inteira superior.

### Seguro-desemprego

`[sd_min_parcela]` expõe o salário mínimo efetivo como referência mínima. Mudanças legais nessa semântica exigem revisão explícita.

### Desemprego / PNAD Contínua

Shortcodes:

- `[desemprego_serie_json]`
- `[desemprego_valor]`
- `[desemprego_debug]`

Fonte: IpeaData, série `PNADC12_TDESOC12`.

O `last_good` é aceito somente dentro de uma janela de frescor. O padrão é **60 dias** e pode ser alterado por:

`DOBR_DESEMPREGO_LAST_GOOD_MAX_AGE`

Depois da janela, o plugin falha fechado em vez de publicar indefinidamente um valor antigo.

## Configurações

Todas são opcionais:

- `DOBR_SM_AUTO_ENABLED`
- `DOBR_SM_CACHE_TTL_SUCCESS`
- `DOBR_SM_CACHE_TTL_FALLBACK`
- `DOBR_SM_SSLVERIFY`
- `DOBR_SM_AUTO_SYNC_SETTINGS`
- `DOBR_SM_SEED_VALOR`
- `DOBR_SM_SEED_VIGENCIA`
- `DOBR_DESEMPREGO_CACHE_TTL_SUCCESS`
- `DOBR_DESEMPREGO_CACHE_TTL_FALLBACK`
- `DOBR_DESEMPREGO_LAST_GOOD_MAX_AGE`
- `DOBR_DESEMPREGO_SSLVERIFY`
- `DOBR_DEBUG_LOG`

Seed/manual são fallback opt-in; não existe default operacional anual.

## Implantação

Este diretório não implica deploy automático.

Antes de substituir o arquivo de produção:

1. criar backup do arquivo ativo;
2. rodar `php -l`;
3. confirmar os testes deste repositório;
4. substituir de forma atômica;
5. limpar apenas os transients do DOBR;
6. validar os shortcodes reais via WP-CLI.

Não editar a cópia de produção como nova fonte de verdade sem posteriormente reconciliar o Git.
