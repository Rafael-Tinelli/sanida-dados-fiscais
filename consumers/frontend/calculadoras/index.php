<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';
$page_title = 'Calculadoras Trabalhistas CLT: Salário, 13º, Férias e Rescisão | Sanida';
$page_desc  = 'Escolha a calculadora trabalhista da Sanida para estimar salário líquido, décimo terceiro, férias CLT ou uma rescisão parcial, com escopo e limites claros.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';
include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadoras-ui.css?v=20260916-f06f10">
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Sanida", "item": "https://sanida.com.br/"},
        {"@type": "ListItem", "position": 2, "name": "Finanças", "item": "https://sanida.com.br/financas/"},
        {"@type": "ListItem", "position": 3, "name": "Calculadoras", "item": "https://sanida.com.br/financas/calculadoras/"}
      ]
    },
    {
      "@type": "CollectionPage",
      "name": "Calculadoras Trabalhistas CLT da Sanida",
      "url": "https://sanida.com.br/financas/calculadoras/",
      "description": "Central de acesso às calculadoras de salário líquido, décimo terceiro, férias CLT e rescisão parcial da Sanida.",
      "mainEntity": {
        "@type": "ItemList",
        "itemListElement": [
          {"@type": "ListItem", "position": 1, "url": "https://sanida.com.br/financas/calculadoras/salario-liquido-clt/", "name": "Calculadora de Salário Líquido CLT"},
          {"@type": "ListItem", "position": 2, "url": "https://sanida.com.br/financas/calculadoras/decimo-terceiro/", "name": "Calculadora de Décimo Terceiro"},
          {"@type": "ListItem", "position": 3, "url": "https://sanida.com.br/financas/calculadoras/ferias-clt/", "name": "Calculadora de Férias CLT"},
          {"@type": "ListItem", "position": 4, "url": "https://sanida.com.br/financas/calculadoras/rescisao-clt/", "name": "Calculadora de Rescisão CLT"}
        ]
      }
    }
  ]
}
</script>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>

<main id="calc-page">
  <section id="sessao1" class="sessao1">
    <div class="padding-gerais centralizador corrigir-zindex-textos-sessao1">
      <p class="cp-section-kicker">Ferramentas trabalhistas educativas</p>
      <h1 class="half-start my-lg">Calculadoras Trabalhistas CLT</h1>
      <p class="cp-sub mb-md">Escolha a ferramenta que corresponde ao recebimento que você quer estimar. Cada calculadora tem escopo próprio, usa as regras fiscais publicadas para sua data de referência e mostra os limites da simulação.</p>
      <div class="cp-note"><strong>Central de navegação:</strong> esta página não faz cálculos. Ela direciona para quatro ferramentas diferentes para evitar misturar salário mensal, décimo terceiro, férias e encerramento de contrato.</div>
      <div class="cp-actions">
        <a class="cp-btn cp-btn--primary" href="#escolha-calculadora">Escolher calculadora</a>
        <a class="cp-btn cp-btn--ghost" href="#como-escolher">Como escolher</a>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy" id="escolha-calculadora">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Escolha pelo tipo de recebimento</p>
        <h2>Qual cálculo você precisa fazer?</h2>
        <p>Abra a ferramenta correspondente ao seu caso. Os cards abaixo resumem o que cada calculadora realmente entrega hoje.</p>
      </div>

      <div class="cp-grid cp-grid--cards">
        <article class="cp-card">
          <p class="cp-section-kicker">Salário mensal</p>
          <h3>Calculadora de Salário Líquido CLT</h3>
          <p>Transforme remuneração bruta em uma estimativa de salário líquido com INSS, IRRF, dependentes, pensão alimentícia e outros descontos informados.</p>
          <ul class="cp-boxlist">
            <li>Salário bruto para líquido.</li>
            <li>INSS e IRRF do salário mensal.</li>
            <li>Dependentes, pensão e outros descontos.</li>
          </ul>
          <div class="cp-links"><a href="/financas/calculadoras/salario-liquido-clt/">Calcular salário líquido</a></div>
        </article>

        <article class="cp-card">
          <p class="cp-section-kicker">13º salário</p>
          <h3>Calculadora de Décimo Terceiro</h3>
          <p>Estime o 13º total, a primeira parcela, a segunda parcela e o valor proporcional conforme os meses válidos do ano.</p>
          <ul class="cp-boxlist">
            <li>Primeira e segunda parcela.</li>
            <li>Décimo terceiro proporcional e avos.</li>
            <li>Projeção de INSS e IRRF na quitação.</li>
          </ul>
          <div class="cp-links"><a href="/financas/calculadoras/decimo-terceiro/">Calcular décimo terceiro</a></div>
        </article>

        <article class="cp-card">
          <p class="cp-section-kicker">Férias</p>
          <h3>Calculadora de Férias CLT</h3>
          <p>Estime férias com 1/3 constitucional, dias de direito, efeito de faltas e, quando escolhido, abono pecuniário de 1/3 do período.</p>
          <ul class="cp-boxlist">
            <li>Valor bruto e líquido estimado das férias.</li>
            <li>1/3 constitucional e descontos fiscais.</li>
            <li>Abono pecuniário, inclusive a situação comum de vender 10 dias quando o direito é de 30.</li>
          </ul>
          <div class="cp-links"><a href="/financas/calculadoras/ferias-clt/">Calcular férias</a></div>
        </article>

        <article class="cp-card">
          <p class="cp-section-kicker">Encerramento do contrato</p>
          <h3>Calculadora de Rescisão CLT</h3>
          <p>Faça uma <strong>estimativa parcial</strong> de verbas implementadas para demissão sem justa causa, pedido de demissão, acordo do art. 484-A ou justa causa.</p>
          <ul class="cp-boxlist">
            <li>Saldo salarial dentro do período informado.</li>
            <li>13º proporcional e férias proporcionais dentro do modelo atual.</li>
            <li><strong>Não calcula FGTS, multa de 40%, aviso prévio, seguro-desemprego nem o total completo do TRCT.</strong></li>
          </ul>
          <div class="cp-links"><a href="/financas/calculadoras/rescisao-clt/">Estimar rescisão</a></div>
        </article>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy" id="como-escolher" style="background-color: var(--color-background4);">
    <div class="cp-center cp-grid">
      <div class="cp-card">
        <h2 class="mb-md">Como escolher a calculadora</h2>
        <ul class="cp-boxlist">
          <li><strong>Quero conferir quanto pode cair na conta todo mês:</strong> use salário líquido.</li>
          <li><strong>Quero estimar o 13º:</strong> use décimo terceiro.</li>
          <li><strong>Quero calcular férias e eventual abono:</strong> use férias CLT.</li>
          <li><strong>Quero estimar verbas no fim do contrato:</strong> use rescisão CLT e observe que o resultado é parcial.</li>
        </ul>
      </div>
      <div class="cp-card">
        <h2 class="mb-md">O que as quatro ferramentas têm em comum</h2>
        <ul class="cp-boxlist">
          <li>São simulações educativas, não substitutos de holerite, TRCT, RH, contador ou análise jurídica.</li>
          <li>Consomem regras fiscais publicadas e validadas pelo sistema da Sanida quando o cálculo exige INSS ou IRRF.</li>
          <li>Expõem premissas, memória do resultado e limites para facilitar conferência.</li>
          <li>Não usam a central para duplicar o conteúdo detalhado de cada ferramenta.</li>
        </ul>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Leitura responsável</p>
        <h2>Use o resultado como conferência, não como documento oficial</h2>
        <p>Folhas reais podem conter benefícios, adicionais, faltas, adiantamentos, convenções coletivas, regras internas e outras particularidades. Quando houver divergência relevante, compare a memória da calculadora com o documento oficial do pagamento e com a situação concreta.</p>
      </div>
      <div class="cp-links">
        <a href="/financas/">Voltar para Finanças</a>
        <a href="/financas/calculadoras/salario-liquido-clt/">Salário líquido</a>
        <a href="/financas/calculadoras/decimo-terceiro/">Décimo terceiro</a>
        <a href="/financas/calculadoras/ferias-clt/">Férias CLT</a>
        <a href="/financas/calculadoras/rescisao-clt/">Rescisão CLT</a>
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
