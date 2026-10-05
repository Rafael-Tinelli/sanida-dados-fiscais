# AF01 — revisão jurídica e contrato de evidência

**Situação:** interpretação oficial documentada; publicação e homologação criptográfica condicionadas à captura do PDF original, revisão da rubrica e autorização específica da release sucessora.

**Fonte primária:** Receita Federal, SCI Cosit nº 8, datada de 12 de junho de 2015 (publicação no sistema em 6 de julho de 2015): https://normas.receita.fazenda.gov.br/sijut2consulta/anexoOutros.action?idArquivoBinario=36769.

**Corroboração posterior:** SC Cosit nº 209/2021 reproduz a orientação da SCI nº 8 para o IR. Parecer PGFN SEI nº 784/2026, disponível no portal da Receita, distingue a modulação do Tema 985 do STF (contribuição patronal) da discussão da contribuição do empregado. Não utilizar o Tema 985 como dispensa previdenciária do segurado.

**Decisão semântica aplicável ao H28:**
- Principal de abono pecuniário do art. 143: fora das bases CP e IRRF.
- Terço constitucional integral das férias gozadas durante o contrato: CP e IRRF, incluindo a fração atribuível aos dias convertidos em abono; contabilizar **uma vez**.
- Terço de férias indenizadas na rescisão: fora de CP e IRRF no cenário coberto pela SCI.
- Não confundir fração do terço constitucional obrigatório com adicional extraordinário concedido pelo empregador.
- Apuração isolada do INSS de férias não substitui o fechamento por competência da folha.

**Reconciliação eSocial (05/10/2026):** a Tabela 03 vigente do eSocial S-1.3/NT 07/2026 classifica a natureza 1017 como terço constitucional de férias e a natureza 1023 como abono pecuniário, cuja descrição agrega o adicional constitucional. O leiaute S-1010, porém, trata `natRubr` e `codIncCP` como campos distintos. Portanto, a natureza agregada 1023 é classificação operacional e não autoridade material para afastar a incidência definida pela SCI Cosit nº 8/2015. O motor deve continuar separando internamente o principal do abono (CP/IR não) da fração do terço constitucional legal (CP/IR sim), sem duplicar o terço. Se uma integração de folha não conseguir representar essa separação de modo verificável, deve bloquear a adaptação em vez de sobrescrever a regra fiscal.

**Critérios de homologação material:** o arquivo obtido deve ser PDF verdadeiro a partir da URL oficial, conter o cabeçalho SCI nº 8/2015 e as teses de CP e IR na íntegra, ter hash SHA-256 dos bytes originais e snapshot imutável verificável. Texto indexado de buscador ou resposta HTML sem PDF não satisfazem esta etapa.

**Fluxo:** `python scripts/prepare_af01_official_evidence.py --output /tmp/af01-official-evidence` após instalar `pypdf`. O script bloqueia HTTP fora do padrão, PDF falso, conteúdo divergente ou documento sem extração textual. Nem o script nem este parecer aprovam publicação, Issue #85 ou implantação.
