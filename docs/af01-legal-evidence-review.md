# AF01 — revisão jurídica e contrato de evidência

**Situação:** interpretação oficial documentada; publicação permanece condicionada a evidência oficial imutável e semanticamente identificada, revisão da rubrica e autorização específica da release sucessora. O PDF original continua sendo a via preferencial; como o endpoint legado atualmente pode entregar apenas a SPA do portal Normas, o registro oficial da API da Receita é uma via alternativa aceitável somente quando provar de forma fail-closed a identidade exata da SCI Cosit nº 8/2015 e tiver os bytes brutos preservados com SHA-256.

**Fonte primária:** Receita Federal, SCI Cosit nº 8, datada de 12 de junho de 2015 (publicação no sistema em 6 de julho de 2015): https://normas.receita.fazenda.gov.br/sijut2consulta/anexoOutros.action?idArquivoBinario=36769.

**Corroboração posterior:** SC Cosit nº 209/2021 reproduz a orientação da SCI nº 8 para o IR. Parecer PGFN SEI nº 784/2026, disponível no portal da Receita, distingue a modulação do Tema 985 do STF (contribuição patronal) da discussão da contribuição do empregado. Não utilizar o Tema 985 como dispensa previdenciária do segurado.

**Decisão semântica aplicável ao H28:**
- Principal de abono pecuniário do art. 143: fora das bases CP e IRRF.
- Terço constitucional integral das férias gozadas durante o contrato: CP e IRRF, incluindo a fração atribuível aos dias convertidos em abono; contabilizar **uma vez**.
- Terço de férias indenizadas na rescisão: fora de CP e IRRF no cenário coberto pela SCI.
- Não confundir fração do terço constitucional obrigatório com adicional extraordinário concedido pelo empregador.
- Apuração isolada do INSS de férias não substitui o fechamento por competência da folha.

**Reconciliação eSocial (05/10/2026):** a Tabela 03 vigente do eSocial S-1.3/NT 07/2026 classifica a natureza 1017 como terço constitucional de férias e a natureza 1023 como abono pecuniário, cuja descrição agrega o adicional constitucional. O leiaute S-1010, porém, trata `natRubr` e `codIncCP` como campos distintos. Portanto, a natureza agregada 1023 é classificação operacional e não autoridade material para afastar a incidência definida pela SCI Cosit nº 8/2015. O motor deve continuar separando internamente o principal do abono (CP/IR não) da fração do terço constitucional legal (CP/IR sim), sem duplicar o terço. Se uma integração de folha não conseguir representar essa separação de modo verificável, deve bloquear a adaptação em vez de sobrescrever a regra fiscal.

**Critérios de homologação material:** há duas vias oficiais, ambas fail-closed e com snapshot imutável dos bytes originais + SHA-256:

1. **PDF oficial (preferencial):** PDF verdadeiro obtido da Receita, com identidade SCI nº 8/2015 e teses materiais de CP/IR verificadas no texto extraído.
2. **API oficial Normas (fallback atual):** resposta JSON bruta da Receita em `/api/indexacao/ato/pesquisar`, exigindo exatamente um registro compatível com `idAto=65843`, número `8`, ano `2015`, tipo `SCI / Solução de Consulta Interna`, órgão Cosit, datas `12/06/2015` e `06/07/2015`, além dos marcadores materiais da ementa sobre terço constitucional, conversão em abono, férias indenizadas e incidência de IR.

A resposta HTML genérica/SPA do portal, texto indexado de buscador, captura visual ou resultado sem identidade estrutural exata **não** satisfazem a etapa. A via API não reduz a exigência de proveniência: ela troca um binário legado indisponível pelo registro estruturado da interface oficial atualmente servida pela Receita, preservado byte a byte.

**Fluxo:** `python scripts/prepare_af01_official_evidence.py --output /tmp/af01-official-evidence` após instalar `pypdf`. O script tenta primeiro o PDF oficial; se o endpoint legado não entregar PDF válido, consulta a API oficial e só aceita o registro exato descrito acima. HTTP fora do padrão, JSON inválido, identidade ambígua/divergente ou ementa sem os marcadores obrigatórios bloqueiam a captura. Nem o script nem este parecer aprovam publicação, review_key antigo ou implantação.
