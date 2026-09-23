# Dados Oficiais BR

Código versionado do plugin WordPress **Dados Oficiais BR**.

## Estado canônico

- versão: **1.4.10**
- produção: `/home1/sanid210/public_html/blog/wp-content/plugins/dados-oficiais-br/dados-oficiais-br.php`
- fonte versionada: `integrations/wordpress/dados-oficiais-br/dados-oficiais-br.php`
- produção atualmente implantada permanece na versão anterior até o deploy controlado; a versão 1.4.10 consolida o contrato evergreen do seguro-desemprego com seleção por vigência oficial e só deve ser promovida após CI e validação.

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

Shortcodes:

- `[sd_min_parcela]` — piso vinculado ao salário mínimo efetivo;
- `[sd_parametros_json]` — contrato público JSON da tabela vigente;
- `[sd_debug]` — diagnóstico restrito ao administrador.

A tabela do trabalhador formal é descoberta automaticamente em fontes oficiais do MTE/FAT. A coleta:

1. procura primeiro o slug estável observado do Portal FAT;
2. descobre candidatos nas páginas oficiais de notícias/pesquisa do FAT;
3. usa a página operacional do MTE apenas quando ela contém a competência esperada;
4. exige marcador inequívoco do ano de referência;
5. extrai piso, limites das duas primeiras faixas, percentuais, parcela-base, teto e vigência;
6. valida continuidade das faixas, referência da segunda faixa, limiar do teto e consistência da parcela-base;
7. cruza o piso com o salário mínimo corrente quando ambos estão disponíveis;
8. avalia em paralelo a competência do ano corrente e a imediatamente anterior;
9. escolhe a tabela mais nova cuja própria data oficial de vigência já tenha começado;
10. não presume uma data fixa de troca anual;
11. aceita `last_good` apenas por janela operacional curta e sem relabelar a competência;
12. falha fechado se já houver evidência oficial da nova competência, mas sua estrutura não puder ser validada.

A troca anual é dirigida pela data de vigência publicada na própria fonte. Se uma nova tabela for publicada com vigência futura, a anterior permanece ativa até essa data; quando a nova vigência começa, ela passa a ser selecionada automaticamente. Não existe no código a suposição de que a troca sempre ocorra em 11 de janeiro.

O parser aceita data de vigência em qualquer mês e taxas expressas como fator decimal ou percentual. Nenhum valor anual da tabela do seguro-desemprego fica hardcoded no plugin.

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
- `DOBR_SD_PARAMS_CACHE_TTL_SUCCESS`
- `DOBR_SD_PARAMS_CACHE_TTL_FALLBACK`
- `DOBR_SD_PARAMS_LAST_GOOD_MAX_AGE`
- `DOBR_SD_PARAMS_SSLVERIFY`
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
