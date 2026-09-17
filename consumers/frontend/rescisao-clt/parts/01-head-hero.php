<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';

$page_title = 'Calculadora de Rescisão CLT: Saldo, 13º e Férias | Sanida';
$page_desc  = 'Faça uma estimativa parcial da rescisão CLT para sem justa causa, pedido de demissão, acordo ou justa causa. Veja saldo, 13º, férias e limites.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/rescisao-clt/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';

include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadoras-ui.css?v=20260917-h29-r1">

<script src="/financas/calculadoras/assets/folha-core.js" defer></script>
<script src="/financas/calculadoras/assets/folha-termination.js" defer></script>
<script src="/financas/calculadoras/assets/rescisao-clt.js" defer></script>

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Sanida", "item": "https://sanida.com.br/"},
        {"@type": "ListItem", "position": 2, "name": "Finanças", "item": "https://sanida.com.br/financas/"},
        {"@type": "ListItem", "position": 3, "name": "Calculadoras", "item": "https://sanida.com.br/financas/calculadoras/"},
        {"@type": "ListItem", "position": 4, "name": "Rescisão CLT", "item": "https://sanida.com.br/financas/calculadoras/rescisao-clt/"}
      ]
    },
    {
      "@type": "WebPage",
      "name": "Calculadora de Rescisão CLT",
      "url": "https://sanida.com.br/financas/calculadoras/rescisao-clt/",
      "description": "Estimativa parcial de rescisão CLT para os motivos de desligamento suportados, com saldo de salário, 13º proporcional, férias proporcionais e limites explícitos.",
      "mainEntity": {
        "@type": "SoftwareApplication",
        "name": "Calculadora de Rescisão CLT",
        "applicationCategory": "FinanceApplication",
        "operatingSystem": "Any"
      }
    },
    {
      "@type": "FAQPage",
      "mainEntity": [
        {
          "@type": "Question",
          "name": "A calculadora mostra o total da minha rescisão?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Não. A ferramenta oferece uma estimativa parcial das parcelas que o modelo atual consegue calcular com segurança e mostra separadamente o que permanece fora do cálculo automático."
          }
        },
        {
          "@type": "Question",
          "name": "A calculadora funciona para pedido de demissão?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Sim. O pedido de demissão é um dos quatro motivos atualmente suportados, dentro do escopo parcial declarado pela ferramenta."
          }
        },
        {
          "@type": "Question",
          "name": "A calculadora funciona para demissão sem justa causa?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Sim. A demissão sem justa causa está entre os motivos suportados para saldo de salário, 13º proporcional e férias proporcionais dentro do escopo atual."
          }
        },
        {
          "@type": "Question",
          "name": "A calculadora considera rescisão por acordo?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Sim. O acordo do artigo 484-A está entre os motivos suportados pelo modelo atual, respeitando os limites divulgados pela ferramenta."
          }
        },
        {
          "@type": "Question",
          "name": "FGTS, multa de 40% e aviso prévio entram no cálculo?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Não nesta versão. Aviso prévio, multa e saque do FGTS e outras parcelas permanecem fora do cálculo automático."
          }
        }
      ]
    }
  ]
}
</script>

<style>
#calc-page{
  --rc-line:rgba(15,23,42,.10);
  --rc-soft:#f7f9fb;
  --rc-blue:#2472ac;
  --rc-green:#166534;
  --rc-warn:#8a5200;
  --rc-danger:#991b1b;
}

#calc-page .rc-hero-grid{
  max-width:1120px;
  margin:auto;
}

#calc-page .rc-hero-points{
  display:grid;
  grid-template-columns:repeat(3,minmax(0,1fr));
  gap:12px;
  max-width:900px;
  margin:1.7rem auto 0;
  text-align:left;
}

#calc-page .rc-hero-point{
  padding:1rem 1.05rem;
  border:1px solid var(--cp-border,var(--rc-line));
  border-radius:18px;
  background:rgba(255,255,255,.84);
  box-shadow:var(--cp-shadow-soft,0 8px 20px rgba(2,6,23,.04));
}

#calc-page .rc-hero-point strong{
  display:block;
  margin-bottom:.25rem;
  color:var(--cp-ink-strong,#181819);
  font-size:.98rem;
}

#calc-page .rc-hero-point span{
  display:block;
  color:var(--cp-muted,#7b8493);
  font-size:.88rem;
  line-height:1.45;
}

#calc-page .rc-calc-section{
  padding-top:clamp(2.8rem,5vw,4.4rem);
  padding-bottom:clamp(3rem,5vw,4.6rem);
}

#calc-page .rc-tool-grid{
  display:grid;
  grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr);
  gap:20px;
  align-items:start;
}

#calc-page .rc-form-card,
#calc-page .rc-result-card{
  padding:clamp(1.1rem,2.5vw,1.55rem)!important;
}

#calc-page .rc-card-head{
  margin-bottom:1.15rem;
}

#calc-page .rc-card-head h3{
  margin:0 0 .45rem;
  color:var(--cp-ink-strong,#181819);
  font-size:clamp(1.3rem,2vw,1.6rem);
  line-height:1.2;
}

#calc-page .rc-card-head p{
  margin:0;
  color:var(--cp-muted,#7b8493);
  line-height:1.6;
}

#calc-page .rc-fields{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:14px;
}

#calc-page .rc-field{
  min-width:0;
}

#calc-page .rc-field--full{
  grid-column:1/-1;
}

#calc-page .rc-field label{
  display:block;
  margin-bottom:.4rem;
  color:var(--cp-ink-strong,#181819);
  font-weight:650;
  line-height:1.35;
}

#calc-page .rc-field input,
#calc-page .rc-field select{
  width:100%;
  min-height:50px;
  padding:.78rem .88rem;
  border:1px solid rgba(15,23,42,.16);
  border-radius:14px;
  background:#fff;
  color:var(--cp-ink-strong,#181819);
  font:inherit;
}

#calc-page .rc-field input:focus,
#calc-page .rc-field select:focus{
  outline:none;
  border-color:rgba(52,152,219,.58);
  box-shadow:0 0 0 4px rgba(52,152,219,.10);
}

#calc-page .rc-field small{
  display:block;
  margin-top:.38rem;
  color:var(--cp-muted,#7b8493);
  font-size:.84rem;
  line-height:1.45;
}

#calc-page .rc-contract-note{
  margin:.95rem 0 0;
  padding:.85rem 1rem;
  border:1px solid var(--rc-line);
  border-radius:14px;
  background:var(--rc-soft);
  color:var(--cp-muted,#7b8493);
  font-size:.9rem;
  line-height:1.55;
}

#calc-page .rc-advanced{
  margin-top:1rem;
  border:1px solid var(--rc-line);
  border-radius:15px;
  background:#fff;
  overflow:hidden;
}

#calc-page .rc-advanced summary{
  padding:.9rem 1rem;
  cursor:pointer;
  color:var(--cp-ink-strong,#181819);
  font-weight:650;
}

#calc-page .rc-advanced-body{
  padding:0 1rem 1rem;
}

#calc-page .rc-actions{
  display:flex;
  gap:10px;
  flex-wrap:wrap;
  margin-top:1.1rem;
}

#calc-page .rc-alert{
  display:none;
  margin-top:1rem;
  padding:.9rem 1rem;
  border:1px solid rgba(239,68,68,.20);
  border-radius:14px;
  background:rgba(239,68,68,.07);
  color:var(--rc-danger);
  line-height:1.5;
}

#calc-page .rc-result-card{
  position:sticky;
  top:92px;
}

#calc-page .rc-result-badge{
  display:inline-flex;
  padding:.34rem .62rem;
  margin-bottom:.7rem;
  border-radius:999px;
  background:rgba(245,158,11,.10);
  color:var(--rc-warn);
  font-size:.78rem;
  font-weight:750;
}

#calc-page .rc-kpis{
  display:grid;
  grid-template-columns:repeat(3,minmax(0,1fr));
  gap:10px;
  margin:1rem 0;
}

#calc-page .rc-kpi{
  padding:.9rem;
  border:1px solid var(--rc-line);
  border-radius:15px;
  background:linear-gradient(180deg,#f8fafc,#fff);
}

#calc-page .rc-kpi small{
  display:block;
  min-height:2.5em;
  color:var(--cp-muted,#7b8493);
  font-size:.78rem;
  line-height:1.3;
}

#calc-page .rc-kpi strong{
  display:block;
  margin-top:.35rem;
  color:var(--cp-ink-strong,#181819);
  font-size:1.12rem;
  line-height:1.25;
}

#calc-page .rc-partial-warning{
  padding:.9rem 1rem;
  margin:1rem 0;
  border-left:4px solid #f59e0b;
  border-radius:0 12px 12px 0;
  background:rgba(245,158,11,.07);
  color:var(--cp-muted,#7b8493);
  line-height:1.55;
}

#calc-page .rc-detail{
  margin-top:.8rem;
  border-top:1px solid var(--rc-line);
}

#calc-page .rc-detail summary{
  padding:.9rem 0;
  cursor:pointer;
  color:var(--rc-blue);
  font-weight:700;
}

#calc-page .rc-rows{
  border-top:1px dashed var(--rc-line);
}

#calc-page .rc-row{
  display:grid;
  grid-template-columns:minmax(0,1fr) minmax(120px,auto);
  gap:1rem;
  align-items:center;
  padding:.58rem 0;
  border-bottom:1px dashed var(--rc-line);
}

#calc-page .rc-row span{
  color:var(--cp-muted,#7b8493);
  font-size:.9rem;
}

#calc-page .rc-row strong{
  color:var(--cp-ink-strong,#181819);
  text-align:right;
  font-size:.92rem;
  overflow-wrap:anywhere;
}

#calc-page .rc-scope-grid{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:12px;
  margin-top:1rem;
}

#calc-page .rc-scope-box{
  padding:1rem;
  border:1px solid var(--rc-line);
  border-radius:15px;
  background:#fff;
}

#calc-page .rc-scope-box--in{
  border-color:rgba(22,101,52,.18);
  background:rgba(22,101,52,.035);
}

#calc-page .rc-scope-box--out{
  border-color:rgba(153,27,27,.15);
  background:rgba(153,27,27,.025);
}

#calc-page .rc-scope-box h4{
  margin:0 0 .55rem;
  color:var(--cp-ink-strong,#181819);
  font-size:1rem;
}

#calc-page .rc-scope-box ul{
  margin:0;
  padding-left:1.1rem;
  color:var(--cp-muted,#7b8493);
  font-size:.88rem;
  line-height:1.55;
}

#calc-page .rc-guide-grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:16px;
}

#calc-page .rc-guide-card h3{
  margin:.1rem 0 .45rem;
  color:var(--cp-ink-strong,#181819);
  font-size:1.1rem;
}

#calc-page .rc-guide-card p{
  margin:0;
  color:var(--cp-muted,#7b8493);
  line-height:1.65;
}

#calc-page .rc-inline-links{
  display:flex;
  flex-wrap:wrap;
  gap:8px;
  margin-top:1rem;
}

#calc-page .rc-inline-links a{
  display:inline-flex;
  padding:.48rem .72rem;
  border:1px solid rgba(52,152,219,.18);
  border-radius:999px;
  color:var(--rc-blue);
  background:#fff;
  text-decoration:none;
  font-size:.9rem;
  font-weight:650;
}

#calc-page .rc-inline-links a:hover{
  text-decoration:underline;
  text-underline-offset:2px;
}

@media(max-width:980px){
  #calc-page .rc-tool-grid{grid-template-columns:1fr;}
  #calc-page .rc-result-card{position:static;}
}

@media(max-width:760px){
  #calc-page .rc-hero-points,
  #calc-page .rc-kpis,
  #calc-page .rc-scope-grid,
  #calc-page .rc-guide-grid{grid-template-columns:1fr;}
  #calc-page .rc-fields{grid-template-columns:1fr;}
  #calc-page .rc-field--full{grid-column:auto;}
  #calc-page .rc-row{grid-template-columns:1fr;gap:.2rem;}
  #calc-page .rc-row strong{text-align:left;}
}
</style>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>

<main id="calc-page">
  <section id="sessao1" class="sessao1">
    <div class="padding-gerais centralizador corrigir-zindex-textos-sessao1 rc-hero-grid">
      <p class="cp-section-kicker">Calculadora trabalhista educativa</p>
      <h1 class="half-start my-lg">Calculadora de Rescisão CLT</h1>
      <p class="cp-sub mb-md">
        Faça uma <strong>estimativa parcial</strong> das parcelas que esta versão consegue calcular com segurança em casos de
        <strong>demissão sem justa causa</strong>, <strong>pedido de demissão</strong>, <strong>acordo</strong> ou <strong>justa causa</strong>.
      </p>

      <div class="rc-hero-points" aria-label="O que a calculadora oferece">
        <div class="rc-hero-point">
          <strong>Saldo de salário</strong>
          <span>Considera os dias civis efetivamente computados no mês do desligamento.</span>
        </div>
        <div class="rc-hero-point">
          <strong>13º proporcional</strong>
          <span>Quando previsto para o motivo selecionado, com contagem própria de avos.</span>
        </div>
        <div class="rc-hero-point">
          <strong>Férias proporcionais</strong>
          <span>Mostra os avos no período aquisitivo, sem inventar parcelas fora do modelo.</span>
        </div>
      </div>

      <div class="cp-actions">
        <a class="cp-btn cp-btn--primary" href="#calc-rescisao-clt">Calcular rescisão</a>
        <a class="cp-btn cp-btn--ghost" href="#como-calculamos-rescisao">Entender o cálculo</a>
      </div>

      <div class="cp-note">
        <strong>Esta não é uma calculadora de “total da rescisão”.</strong>
        Aviso prévio, multa e saque do FGTS, seguro-desemprego e outras verbas permanecem fora do cálculo automático desta versão.
      </div>
    </div>
  </section>
