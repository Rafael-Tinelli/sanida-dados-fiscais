<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';
$page_title = 'Calculadora de Salário Líquido: Calcule INSS e IRRF | Sanida';
$page_desc  = 'Calcule salário líquido CLT de forma simples. Simule salário bruto para líquido, desconto de INSS, IRRF, dependentes, pensão e outros descontos mensais.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/salario-liquido-clt/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';
include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadoras-ui.css?v=<?php echo time(); ?>">
<link rel="stylesheet" href="/financas/calculadoras/assets/salario-liquido-clt.css?v=<?php echo time(); ?>">
<script src="/financas/calculadoras/assets/folha-core.js" defer></script>
<script src="/financas/calculadoras/assets/salario-liquido.js" defer></script>
<script type="application/ld+json">
{"@context":"https://schema.org","@graph":[{"@type":"BreadcrumbList","itemListElement":[{"@type":"ListItem","position":1,"name":"Sanida","item":"https://sanida.com.br/"},{"@type":"ListItem","position":2,"name":"Finanças","item":"https://sanida.com.br/financas/"},{"@type":"ListItem","position":3,"name":"Calculadoras","item":"https://sanida.com.br/financas/calculadoras/"},{"@type":"ListItem","position":4,"name":"Salário Líquido CLT","item":"https://sanida.com.br/financas/calculadoras/salario-liquido-clt/"}]},{"@type":"WebPage","name":"Calculadora de Salário Líquido CLT","description":"Calcule salário líquido CLT com INSS, IRRF, dependentes, pensão e outros descontos.","url":"https://sanida.com.br/financas/calculadoras/salario-liquido-clt/","mainEntity":{"@type":"SoftwareApplication","name":"Calculadora de Salário Líquido CLT","applicationCategory":"FinanceApplication","operatingSystem":"Any"}},{"@type":"FAQPage","mainEntity":[{"@type":"Question","name":"Como calcular salário líquido?","acceptedAnswer":{"@type":"Answer","text":"Para calcular salário líquido, parte-se do salário bruto e subtraem-se descontos como INSS, IRRF, pensão alimentícia e outros descontos informados. O resultado é uma estimativa do valor que pode cair na conta."}},{"@type":"Question","name":"Qual a diferença entre salário bruto e salário líquido?","acceptedAnswer":{"@type":"Answer","text":"Salário bruto é o valor antes dos descontos. Salário líquido é o valor estimado depois de descontos como INSS, IRRF, pensão e outros abatimentos."}},{"@type":"Question","name":"A calculadora substitui o holerite?","acceptedAnswer":{"@type":"Answer","text":"Não. A calculadora é educativa e serve para conferência inicial. O holerite oficial, a folha de pagamento, o RH ou o contador devem prevalecer em situações específicas."}}]}]}
</script>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>
<main id="calc-page">
  <section id="sessao1" class="sessao1 sl-hero">
    <div class="padding-gerais centralizador corrigir-zindex-textos-sessao1">
      <p class="cp-section-kicker">Calculadora trabalhista educativa</p>
      <h1 class="half-start my-lg">Calculadora de Salário Líquido CLT</h1>
      <p class="cp-sub mb-md">Calcule salário líquido a partir do salário bruto, com estimativa de desconto de INSS, IRRF, dependentes, pensão alimentícia e outros descontos mensais.</p>
      <div class="cp-note"><strong>Estimativa educativa:</strong> a ferramenta ajuda a conferir o contracheque e entender o caminho do salário bruto para líquido, mas valores finais podem variar por benefícios, sindicato, adicionais, faltas, adiantamentos e regras internas de folha.</div>
      <div class="cp-actions sl-hero-actions">
        <a class="cp-btn cp-btn--primary" href="#calc-salario-liquido">Calcular salário líquido</a>
        <a class="cp-btn cp-btn--ghost" href="#guia-salario-liquido">Entender descontos</a>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy sl-calc-section" id="calc-salario-liquido">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Ferramenta principal</p>
        <h2>Calculadora salário líquido: estime quanto cai na conta</h2>
        <p>Preencha os campos principais para transformar salário bruto em salário líquido. A ferramenta organiza INSS, IRRF, pensão e descontos informados sem misturar esta página com férias, 13º ou rescisão.</p>
      </div>

      <div class="cp-grid cp-grid--main sl-tool-grid">
        <div class="cp-card cp-card--form sl-form-card">
          <div data-alert class="cp-alert"></div>
          <form novalidate>
            <div class="cp-card__header">
              <h3>Dados da simulação do salário</h3>
              <p>Informe a remuneração mensal e os descontos conhecidos. Use a calculadora para estimar o salário líquido CLT e comparar com o holerite.</p>
            </div>

            <div class="cp-fields sl-fields">
              <div class="cp-field sl-field-main">
                <label for="slc-salario">Salário bruto mensal</label>
                <input id="slc-salario" name="salario" inputmode="decimal" placeholder="Ex.: 4500,00" autocomplete="off">
                <small>Valor antes de INSS, IRRF e demais descontos.</small>
              </div>

              <div class="cp-field sl-field-main">
                <label for="slc-variaveis">Média de variáveis habituais</label>
                <input id="slc-variaveis" name="variaveis" inputmode="decimal" placeholder="Ex.: 350,00" autocomplete="off">
                <small>Horas extras, adicional noturno, comissões ou valores habituais.</small>
              </div>

              <div class="cp-field sl-field-small">
                <label for="slc-deps">Dependentes</label>
                <input id="slc-deps" name="dependentes" inputmode="numeric" placeholder="0" autocomplete="off">
                <small>Usado na estimativa de IRRF.</small>
              </div>

              <div class="cp-field sl-field-small">
                <label for="slc-pensao">Pensão alimentícia</label>
                <input id="slc-pensao" name="pensao" inputmode="decimal" placeholder="0,00" autocomplete="off">
                <small>Reduz a base do IR quando aplicável e sai do líquido.</small>
              </div>

              <div class="cp-field sl-field-small">
                <label for="slc-outros">Outros descontos</label>
                <input id="slc-outros" name="outros" inputmode="decimal" placeholder="0,00" autocomplete="off">
                <small>Vale, adiantamento, coparticipação ou outros descontos.</small>
              </div>
            </div>

            <div class="cp-actions">
              <button class="cp-btn cp-btn--primary" type="submit">Calcular salário líquido</button>
              <a class="cp-btn cp-btn--ghost" href="#guia-salario-liquido">Entender critérios</a>
            </div>
          </form>
        </div>

        <aside class="cp-card cp-card--result cp-result sl-result-card" data-result>
          <div class="cp-card__header">
            <h3>Resultado da calculadora de salário líquido</h3>
            <p>Veja o valor bruto, os principais descontos e o salário líquido estimado para conferência inicial.</p>
          </div>

          <div class="sl-liquid-panel">
            <strong>Salário líquido estimado</strong>
            <span data-kpi="liquido">—</span>
          </div>

          <div class="cp-kpis sl-kpis">
            <div class="cp-kpi"><strong>Remuneração bruta</strong><span data-kpi="bruto">—</span></div>
            <div class="cp-kpi"><strong>Descontos estimados</strong><span data-kpi="descontos">—</span></div>
          </div>

          <details class="sl-details" open>
            <summary>Ver descontos e dados fiscais</summary>
            <div class="cp-rows">
              <div class="cp-row"><span>INSS</span><strong class="cp-neg" data-row="inss">—</strong></div>
              <div class="cp-row"><span>IRRF</span><strong class="cp-neg" data-row="irrf">—</strong></div>
              <div class="cp-row"><span>Pensão</span><strong class="cp-neg" data-row="pensao">—</strong></div>
              <div class="cp-row"><span>Outros descontos</span><strong class="cp-neg" data-row="outros">—</strong></div>
              <div class="cp-row"><span>Base usada no IR</span><strong data-row="base-ir">—</strong></div>
              <div class="cp-row"><span>IRRF antes da redução</span><strong data-row="ir-antes-reducao">—</strong></div>
              <div class="cp-row"><span>Rendimento testado no redutor</span><strong data-row="renda-redutor">—</strong></div>
              <div class="cp-row"><span>Modo de cálculo do IR</span><strong data-row="modo">—</strong></div>
              <div class="cp-row"><span>Redução aplicada</span><strong class="cp-pos" data-row="reducao">—</strong></div>
              <div class="cp-row"><span>Teto da base do INSS</span><strong data-row="aliquotas">—</strong></div>
              <div class="cp-row"><span>Ano fiscal</span><strong data-row="ano">—</strong></div>
              <div class="cp-row"><span>Data de referência</span><strong data-row="referencia">—</strong></div>
              <div class="cp-row"><span>Release fiscal usada</span><code class="sl-release-id" data-row="release-id">—</code></div>
            </div>
          </details>

          <p class="cp-disclaimer">Estimativa educativa. Compare com o holerite oficial em casos com benefícios, descontos internos, faltas, adicionais, variáveis específicas ou regras particulares.</p>
        </aside>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy sl-after-calc" id="guia-salario-liquido" style="background-color: var(--color-background4);">
    <div class="cp-center cp-grid">
      <div class="cp-card">
        <h2 class="mb-md">Como calcular salário líquido?</h2>
        <p>O salário líquido é o valor que sobra depois dos descontos aplicados sobre a remuneração bruta. Em uma folha CLT, os descontos mais comuns são INSS, IRRF, pensão alimentícia e abatimentos informados pela empresa ou pelo próprio trabalhador.</p>
        <p>Por isso, uma boa calculadora de salário líquido não deve olhar apenas para o salário-base. Ela precisa considerar variáveis habituais, dependentes, pensão e outros descontos para chegar a uma estimativa mais próxima do valor que pode cair na conta.</p>
        <div class="cp-links">
          <a href="#calc-salario-liquido">Usar calculadora</a>
          <a href="/financas/calculadoras/decimo-terceiro/">Calcular 13º</a>
          <a href="/financas/calculadoras/ferias-clt/">Calcular férias</a>
        </div>
      </div>
      <div class="cp-card">
        <h2 class="mb-md">O que entra no cálculo?</h2>
        <ul class="cp-boxlist">
          <li>Salário bruto mensal e média de variáveis habituais.</li>
          <li>Desconto de INSS por faixas progressivas.</li>
          <li>IRRF com dependentes, pensão e redução aplicável.</li>
          <li>Outros descontos informados, como adiantamentos, vale ou coparticipação.</li>
        </ul>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy sl-examples-section">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Exemplos rápidos</p>
        <h2>Salário bruto para líquido: dúvidas comuns</h2>
        <p>Os exemplos abaixo ajudam a entender por que duas pessoas com o mesmo salário bruto podem ter salários líquidos diferentes.</p>
      </div>
      <div class="cp-grid cp-grid--cards sl-info-grid">
        <div class="cp-card">
          <h3>Salário de R$ 3.000 líquido</h3>
          <p>O valor líquido depende de INSS, eventual IRRF, dependentes e descontos internos. Use a ferramenta para simular com seus dados reais.</p>
        </div>
        <div class="cp-card">
          <h3>Salário de R$ 5.000 líquido</h3>
          <p>Quanto maior a remuneração, maior tende a ser o peso de INSS e IRRF. Variáveis habituais também podem alterar a base de cálculo.</p>
        </div>
        <div class="cp-card">
          <h3>Salário bruto ou líquido?</h3>
          <p>Salário bruto é o valor antes dos descontos. Salário líquido é o valor estimado depois de INSS, IRRF, pensão e outros abatimentos.</p>
        </div>
        <div class="cp-card">
          <h3>Desconto de INSS e IRRF</h3>
          <p>INSS e IRRF seguem lógicas diferentes. O INSS incide primeiro; depois, o IRRF considera base própria, dependentes e deduções aplicáveis.</p>
        </div>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy" style="background-color: var(--color-background4);">
    <div class="cp-center cp-grid">
      <div class="cp-card">
        <h2 class="mb-md">O que esta calculadora não substitui</h2>
        <ul class="cp-boxlist">
          <li>Não substitui holerite, folha oficial, RH ou contador.</li>
          <li>Não calcula férias, décimo terceiro ou rescisão nesta página.</li>
          <li>Não considera regras internas específicas de sindicatos, empresas ou benefícios.</li>
          <li>Não interpreta contratos, convenções coletivas ou situações litigiosas.</li>
        </ul>
      </div>
      <div class="cp-card">
        <h2 class="mb-md">Ferramentas relacionadas</h2>
        <div class="cp-links">
          <a href="/financas/calculadoras/decimo-terceiro/">Calculadora de décimo terceiro</a>
          <a href="/financas/calculadoras/ferias-clt/">Calculadora de férias CLT</a>
          <a href="/financas/calculadoras/rescisao-clt/">Calculadora de rescisão CLT</a>
          <a href="/financas/calculadoras/">Central de calculadoras</a>
        </div>
        <p class="cp-disclaimer">A separação entre as ferramentas evita confundir salário mensal, férias, 13º e encerramento de contrato.</p>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy sl-faq-section">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Perguntas frequentes</p>
        <h2>FAQ sobre salário líquido CLT</h2>
      </div>
      <div class="cp-faq">
        <details>
          <summary>Como calcular salário líquido?</summary>
          <p>Some o salário bruto e as variáveis habituais, depois desconte INSS, IRRF, pensão alimentícia e outros descontos informados. O resultado é uma estimativa do valor líquido.</p>
        </details>
        <details>
          <summary>Qual a diferença entre salário bruto e salário líquido?</summary>
          <p>Salário bruto é o valor antes dos descontos. Salário líquido é o valor que sobra depois de descontos como INSS, IRRF, pensão e abatimentos internos.</p>
        </details>
        <details>
          <summary>O FGTS é descontado do salário?</summary>
          <p>Não. O FGTS não deve ser descontado do salário do empregado. Ele é uma obrigação do empregador. Esta calculadora foca no salário líquido recebido pelo trabalhador.</p>
        </details>
        <details>
          <summary>Por que meu salário líquido pode ser diferente do resultado?</summary>
          <p>Porque a folha pode incluir benefícios, faltas, descontos sindicais, coparticipação, adiantamento, horas extras, adicionais ou regras internas que não aparecem na simulação.</p>
        </details>
        <details>
          <summary>A calculadora usa tabela de INSS e IRRF?</summary>
          <p>Sim. A ferramenta consome a release fiscal publicada e validada pelo sistema, seleciona as regras pela data de referência e exibe a identificação da release usada no cálculo. Ainda assim, o resultado deve ser usado como conferência educativa.</p>
        </details>
      </div>
    </div>
  </section>

  <section id="sessao4"><?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/section-produtos.php'; ?></section>
  <section id="cta-final" class="padding-gerais centralizador"><?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/cta-final.php'; ?></section>
  <section id="sessao5"><?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/section-formulario.php'; ?></section>
</main>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/footer.php'; ?>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/btn-whatsapp.php'; ?>
</body>
</html>
