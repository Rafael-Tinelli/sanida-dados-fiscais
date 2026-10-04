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

**Ponto de revisão obrigatória antes de produção:** conferir classificação e incidência de eventos eSocial vigentes, especialmente rubricas 1017 e 1023, contra o entendimento da SCI. Uma rubrica agregada não justifica incluir o principal isento nem duplicar a parcela do terço. Qualquer divergência normativa atual deve bloquear promoção automática.

**Critérios de homologação material:** o arquivo obtido deve ser PDF verdadeiro a partir da URL oficial, conter o cabeçalho SCI nº 8/2015 e as teses de CP e IR na íntegra, ter hash SHA-256 dos bytes originais e snapshot imutável verificável. Texto indexado de buscador ou resposta HTML sem PDF não satisfazem esta etapa.

**Fluxo:** `python scripts/prepare_af01_official_evidence.py --output /tmp/af01-official-evidence` após instalar `pypdf`. O script bloqueia HTTP fora do padrão, PDF falso, conteúdo divergente ou documento sem extração textual. Nem o script nem este parecer aprovam publicação, Issue #85 ou implantação.
