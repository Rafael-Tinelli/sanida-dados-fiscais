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
        {"@type":"Question","name":"Como calcular o décimo terceiro salário?","acceptedAnswer":{"@type":"Answer","text":"O cálculo parte da remuneração considerada para o benefício e aplica 1/12 por mês válido. Se o ano foi completo, o valor tende a equivaler a uma remuneração mensal integral; caso contrário, entra a proporcionalidade."}},
        {"@type":"Question","name":"Como funciona a primeira parcela do décimo terceiro?","acceptedAnswer":{"@type":"Answer","text":"A primeira parcela funciona como adiantamento. No ramo padrão suportado, a referência é o salário do mês anterior ao adiantamento; situações especiais podem exigir o valor efetivamente pago."}},
        {"@type":"Question","name":"Por que a segunda parcela do décimo terceiro costuma ser menor?","acceptedAnswer":{"@type":"Answer","text":"Porque a quitação considera os descontos incidentes sobre o 13º e depois abate o adiantamento já pago. A ferramenta mostra esses componentes separadamente."}},
        {"@type":"Question","name":"Como calcular o décimo terceiro proporcional?","acceptedAnswer":{"@type":"Answer","text":"O décimo terceiro proporcional considera os avos válidos no ano de referência. Em geral, um mês entra na contagem quando houve pelo menos 15 dias de serviço."}},
        {"@type":"Question","name":"Comissão e hora extra entram no décimo terceiro?","acceptedAnswer":{"@type":"Answer","text":"Verbas variáveis podem influenciar a remuneração de referência por meio de médias. Esta calculadora recebe a média já apurada, mas não calcula a média normativa por conta própria."}},
        {"@type":"Question","name":"O décimo terceiro tem INSS e IRRF?","acceptedAnswer":{"@type":"Answer","text":"A quitação do décimo terceiro pode envolver INSS e IRRF em apurações próprias. A ferramenta mostra os descontos quando a estimativa líquida está ativada."}}
      ]
    }
  ]
}
</script>

<style>
#calc-page .d13-result-summary{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:1rem 0}
#calc-page .d13-result-card{padding:1rem;border:1px solid rgba(15,23,42,.10);border-radius:16px;background:linear-gradient(180deg,#f8fafc,#fff)}
#calc-page .d13-result-card small{display:block;min-height:2.4em;color:var(--cp-muted,#7b8493);font-size:.78rem;line-height:1.3}
#calc-page .d13-result-card strong{display:block;margin-top:.35rem;color:var(--cp-ink-strong,#181819);font-size:1.2rem;line-height:1.2}
#calc-page .d13-result-card--main{border-color:rgba(22,101,52,.16);background:linear-gradient(180deg,rgba(22,101,52,.06),rgba(22,101,52,.02))}
#calc-page .d13-result-card--main strong{color:#166534}
#calc-page .d13-state{margin:.85rem 0;padding:.85rem 1rem;border-left:4px solid #3498db;border-radius:0 13px 13px 0;background:rgba(52,152,219,.06)}
#calc-page .d13-state span{display:block;color:var(--cp-muted,#7b8493);font-size:.8rem;font-weight:650;text-transform:uppercase;letter-spacing:.04em}
#calc-page .d13-state strong{display:block;margin-top:.2rem;color:var(--cp-ink-strong,#181819)}
#calc-page .d13-detail{margin-top:.75rem;border-top:1px solid rgba(15,23,42,.09)}
#calc-page .d13-detail summary{padding:.9rem 0;cursor:pointer;color:#2472ac;font-weight:700}
#calc-page .d13-warning{padding:.85rem 1rem;margin-top:.85rem;border-left:4px solid #f59e0b;border-radius:0 13px 13px 0;background:rgba(245,158,11,.07);color:var(--cp-muted,#7b8493);line-height:1.55}
@media(max-width:760px){#calc-page .d13-result-summary{grid-template-columns:1fr}}
</style>
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
            <div class="cp-hero__highlight"><strong>1ª parcela</strong><span>adiantamento do 13º salário</span></div>
            <div class="cp-hero__highlight"><strong>2ª parcela</strong><span>quitação com projeção de descontos</span></div>
            <div class="cp-hero__highlight"><strong>Proporcional</strong><span>estimativa por meses trabalhados</span></div>
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
            <div class="cp-preview-card__top"><span class="cp-preview-badge">Visão rápida</span><h2>Décimo terceiro salário</h2></div>
            <div class="cp-preview-list">
              <div class="cp-preview-row"><span>Salário-base</span><strong>R$ 4.500,00</strong></div>
              <div class="cp-preview-row"><span>Média de variáveis</span><strong>R$ 350,00</strong></div>
              <div class="cp-preview-row"><span>Meses trabalhados</span><strong>12/12</strong></div>
              <div class="cp-preview-row"><span>1ª parcela</span><strong>Regra própria</strong></div>
              <div class="cp-preview-row"><span>2ª parcela</span><strong>Com descontos</strong></div>
            </div>
            <p class="cp-preview-foot">A calculadora logo abaixo organiza o valor total, o adiantamento e a projeção da quitação final.</p>
          </div>
        </aside>
      </div>
    </div>
  </section>
