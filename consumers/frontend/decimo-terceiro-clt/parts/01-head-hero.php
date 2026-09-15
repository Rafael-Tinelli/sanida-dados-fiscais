<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';
$page_title = 'Calculadora de Décimo Terceiro Salário: 1ª, 2ª Parcela e Proporcional | Sanida';
$page_desc  = 'Calcule o décimo terceiro salário com 1ª parcela, 2ª parcela, proporcional, INSS e IRRF. Simulação educativa para estimar o valor do 13º salário.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/decimo-terceiro/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';
include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadora-decimo-terceiro-ui.css?v=2.1.0">
<script src="/financas/calculadoras/assets/folha-core.js" defer></script>
<script src="/financas/calculadoras/assets/folha-thirteenth.js" defer></script>
<script src="/financas/calculadoras/assets/decimo-terceiro.js" defer></script>

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Sanida", "item": "https://sanida.com.br/"},
        {"@type": "ListItem", "position": 2, "name": "Negócios e Finanças", "item": "https://sanida.com.br/financas/"},
        {"@type": "ListItem", "position": 3, "name": "Calculadoras", "item": "https://sanida.com.br/financas/calculadoras/"},
        {"@type": "ListItem", "position": 4, "name": "Décimo Terceiro", "item": "https://sanida.com.br/financas/calculadoras/decimo-terceiro/"}
      ]
    },
    {
      "@type": "WebPage",
      "name": "Calculadora de Décimo Terceiro Salário",
      "url": "https://sanida.com.br/financas/calculadoras/decimo-terceiro/",
      "description": "Calcule o décimo terceiro salário com 1ª parcela, 2ª parcela, proporcional, INSS e IRRF. Simulação educativa para estimar o valor do 13º salário.",
      "mainEntity": {
        "@type": "SoftwareApplication",
        "name": "Calculadora de Décimo Terceiro Salário",
        "applicationCategory": "FinanceApplication",
        "operatingSystem": "Any"
      }
    },
    {
      "@type": "FAQPage",
      "mainEntity": [
        {
          "@type": "Question",
          "name": "Como calcular o décimo terceiro salário?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "O cálculo do décimo terceiro salário parte da remuneração considerada para o benefício e aplica 1/12 por mês trabalhado. Se o ano foi completo, o valor tende a equivaler a uma remuneração mensal integral. Se não foi, entra o cálculo proporcional."
          }
        },
        {
          "@type": "Question",
          "name": "O que entra na base de cálculo do décimo terceiro salário?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "A base de cálculo do décimo terceiro salário costuma considerar a remuneração utilizada para o benefício, incluindo salário-base e, quando aplicável, médias de verbas variáveis como comissão, horas extras e adicionais habituais."
          }
        },
        {
          "@type": "Question",
          "name": "Como funciona a primeira parcela do décimo terceiro salário?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "A primeira parcela do décimo terceiro salário funciona como adiantamento. Em regra, ela não sofre desconto de INSS nem de IRRF no momento do pagamento."
          }
        },
        {
          "@type": "Question",
          "name": "Por que a segunda parcela do décimo terceiro salário costuma ser menor?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "A segunda parcela do décimo terceiro salário costuma ser menor porque nela entram os descontos calculados sobre o 13º total, além da dedução do valor já pago como adiantamento."
          }
        },
        {
          "@type": "Question",
          "name": "Como calcular o décimo terceiro salário proporcional?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "O décimo terceiro salário proporcional considera 1/12 por mês válido. Em geral, o mês conta quando houve pelo menos 15 dias trabalhados."
          }
        },
        {
          "@type": "Question",
          "name": "Comissão e hora extra entram no décimo terceiro salário?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Comissão, hora extra e outras verbas variáveis podem entrar no décimo terceiro salário por meio de médias, conforme a realidade remuneratória do trabalhador."
          }
        },
        {
          "@type": "Question",
          "name": "Quem trabalhou poucos meses recebe décimo terceiro salário?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Quem trabalhou parte do ano pode receber décimo terceiro salário proporcional, desde que os meses considerados cumpram a regra mínima para contagem do avo."
          }
        },
        {
          "@type": "Question",
          "name": "O décimo terceiro salário tem INSS e IRRF?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "O décimo terceiro salário pode sofrer incidência de INSS e IRRF na quitação final. A primeira parcela, em regra, não recebe esses descontos no ato do pagamento."
          }
        }
      ]
    }
  ]
}
</script>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>

<main id="calc-page">
  <section id="sessao1" class="cp-hero-section">
    <div class="padding-gerais">
      <div class="cp-center cp-hero">
        <div class="cp-hero__content">
          <p class="cp-eyebrow">Simulação educativa e conferência rápida</p>

          <h1 class="cp-hero__title">Calculadora de Décimo Terceiro Salário</h1>

          <p class="cp-hero__lead">
            Calcule o décimo terceiro salário com estimativa de <strong>1ª parcela</strong>,
            <strong>2ª parcela</strong>, valor <strong>proporcional</strong> e projeção com
            <strong>INSS</strong> e <strong>IRRF</strong>.
          </p>

          <div class="cp-hero__highlights">
            <div class="cp-hero__highlight">
              <strong>1ª parcela</strong>
              <span>adiantamento do 13º salário</span>
            </div>
            <div class="cp-hero__highlight">
              <strong>2ª parcela</strong>
              <span>quitação com projeção de descontos</span>
            </div>
            <div class="cp-hero__highlight">
              <strong>Proporcional</strong>
              <span>estimativa por meses trabalhados</span>
            </div>
          </div>

          <div class="cp-hero__actions">
            <a class="cp-btn cp-btn--primary" href="#calc-decimo-terceiro">Calcular décimo terceiro salário</a>
            <a class="cp-btn cp-btn--ghost" href="#guia-decimo-terceiro">Entender o cálculo</a>
          </div>

          <nav class="cp-hero__mini-nav" aria-label="Atalhos desta página">
            <a href="#calc-decimo-terceiro">Calculadora</a>
            <a href="#primeira-parcela-decimo-terceiro">1ª parcela</a>
            <a href="#segunda-parcela-decimo-terceiro">2ª parcela</a>
            <a href="#decimo-terceiro-proporcional">Proporcional</a>
            <a href="#faq-decimo-terceiro">FAQ</a>
          </nav>

          <div class="cp-note cp-note--hero">
            <strong>Lógica da simulação:</strong> a primeira parcela é tratada como adiantamento.
            A segunda parcela considera os descontos sobre o 13º total antes de abater o valor já antecipado.
          </div>
        </div>

        <aside class="cp-hero__preview" aria-label="Prévia da calculadora de décimo terceiro salário">
          <div class="cp-preview-card">
            <div class="cp-preview-card__top">
              <span class="cp-preview-badge">Visão rápida</span>
              <h2>Décimo terceiro salário</h2>
            </div>

            <div class="cp-preview-list">
              <div class="cp-preview-row">
                <span>Salário-base</span>
                <strong>R$ 4.500,00</strong>
              </div>
              <div class="cp-preview-row">
                <span>Média de variáveis</span>
                <strong>R$ 350,00</strong>
              </div>
              <div class="cp-preview-row">
                <span>Meses trabalhados</span>
                <strong>12/12</strong>
              </div>
              <div class="cp-preview-row">
                <span>1ª parcela</span>
                <strong>Regra própria</strong>
              </div>
              <div class="cp-preview-row">
                <span>2ª parcela</span>
                <strong>Com descontos</strong>
              </div>
            </div>

            <p class="cp-preview-foot">
              A calculadora logo abaixo organiza o valor total, o adiantamento e a projeção da quitação final.
            </p>
          </div>
        </aside>
      </div>
    </div>
  </section>

