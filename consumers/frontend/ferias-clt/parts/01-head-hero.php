<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';

$page_title = 'Calculadora de Férias CLT: Valor, 1/3 e Abono | Sanida';
$page_desc  = 'Calcule férias CLT, 1/3 constitucional, dias de descanso e venda de 1/3 das férias, com INSS e IRRF conforme a vigência fiscal informada.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/ferias-clt/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';

include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadoras-ui.css?v=20260917-h28-r1">

<script src="/financas/calculadoras/assets/folha-core.js" defer></script>
<script src="/financas/calculadoras/assets/folha-vacation.js" defer></script>
<script src="/financas/calculadoras/assets/ferias-clt.js" defer></script>

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        {"@type":"ListItem","position":1,"name":"Sanida","item":"https://sanida.com.br/"},
        {"@type":"ListItem","position":2,"name":"Finanças","item":"https://sanida.com.br/financas/"},
        {"@type":"ListItem","position":3,"name":"Calculadoras","item":"https://sanida.com.br/financas/calculadoras/"},
        {"@type":"ListItem","position":4,"name":"Férias CLT","item":"https://sanida.com.br/financas/calculadoras/ferias-clt/"}
      ]
    },
    {
      "@type":"WebPage",
      "name":"Calculadora de Férias CLT",
      "url":"https://sanida.com.br/financas/calculadoras/ferias-clt/",
      "description":"Calcule férias CLT, 1/3 constitucional, dias de descanso e venda de 1/3 das férias, com INSS e IRRF conforme a vigência fiscal informada.",
      "mainEntity":{
        "@type":"SoftwareApplication",
        "name":"Calculadora de Férias CLT",
        "applicationCategory":"FinanceApplication",
        "operatingSystem":"Any"
      }
    },
    {
      "@type":"FAQPage",
      "mainEntity":[
        {
          "@type":"Question",
          "name":"Como calcular férias CLT?",
          "acceptedAnswer":{"@type":"Answer","text":"A ferramenta parte da base integral de férias já apurada, considera o direito em dias conforme faltas injustificadas, calcula o terço constitucional e aplica as incidências fiscais dos componentes suportados."}
        },
        {
          "@type":"Question",
          "name":"Se eu vender 10 dias de férias, sempre descanso 20?",
          "acceptedAnswer":{"@type":"Answer","text":"Somente quando o direito adquirido é de 30 dias. O abono pecuniário corresponde a um terço do período de férias a que a pessoa efetivamente tem direito."}
        },
        {
          "@type":"Question",
          "name":"O abono pecuniário entra no INSS e no IRRF?",
          "acceptedAnswer":{"@type":"Answer","text":"A calculadora separa o principal do abono e o terço constitucional sobre o abono porque os componentes podem ter tratamentos fiscais diferentes conforme a regra vigente."}
        },
        {
          "@type":"Question",
          "name":"A calculadora faz média de horas extras e comissões?",
          "acceptedAnswer":{"@type":"Answer","text":"Não. A base integral de férias deve ser informada já apurada. A ferramenta não inventa médias de verbas variáveis sem regra executável para o caso concreto."}
        }
      ]
    }
  ]
}
</script>

<style>
#calc-page{
  --vf-line:rgba(15,23,42,.10);
  --vf-soft:#f7f9fb;
  --vf-blue:#2472ac;
  --vf-green:#166534;
  --vf-warn:#8a5200;
  --vf-danger:#991b1b;
}
#calc-page .vf-hero-points{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;max-width:900px;margin:1.7rem auto 0;text-align:left}
#calc-page .vf-hero-point{padding:1rem 1.05rem;border:1px solid var(--vf-line);border-radius:18px;background:rgba(255,255,255,.84);box-shadow:0 8px 20px rgba(2,6,23,.04)}
#calc-page .vf-hero-point strong{display:block;margin-bottom:.25rem;color:var(--cp-ink-strong,#181819);font-size:.98rem}
#calc-page .vf-hero-point span{display:block;color:var(--cp-muted,#7b8493);font-size:.88rem;line-height:1.45}
#calc-page .vf-tool-grid{display:grid;grid-template-columns:minmax(0,1.03fr) minmax(360px,.97fr);gap:20px;align-items:start}
#calc-page .vf-card{padding:clamp(1.1rem,2.5vw,1.55rem)!important}
#calc-page .vf-card-head{margin-bottom:1.1rem}
#calc-page .vf-card-head h3{margin:0 0 .45rem;color:var(--cp-ink-strong,#181819);font-size:clamp(1.3rem,2vw,1.6rem);line-height:1.2}
#calc-page .vf-card-head p{margin:0;color:var(--cp-muted,#7b8493);line-height:1.6}
#calc-page .vf-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
#calc-page .vf-field--full{grid-column:1/-1}
#calc-page .vf-field label{display:block;margin-bottom:.4rem;color:var(--cp-ink-strong,#181819);font-weight:650;line-height:1.35}
#calc-page .vf-field input{width:100%;min-height:50px;padding:.78rem .88rem;border:1px solid rgba(15,23,42,.16);border-radius:14px;background:#fff;color:var(--cp-ink-strong,#181819);font:inherit}
#calc-page .vf-field input:focus{outline:none;border-color:rgba(52,152,219,.58);box-shadow:0 0 0 4px rgba(52,152,219,.10)}
#calc-page .vf-field small{display:block;margin-top:.38rem;color:var(--cp-muted,#7b8493);font-size:.84rem;line-height:1.45}
#calc-page .vf-check{display:flex;align-items:flex-start;gap:.7rem;padding:.9rem 1rem;border:1px solid rgba(52,152,219,.16);border-radius:15px;background:rgba(52,152,219,.04);font-weight:600}
#calc-page .vf-check input{width:auto;min-height:auto;margin-top:.25rem}
#calc-page .vf-alert{display:none;margin-top:1rem;padding:.9rem 1rem;border:1px solid rgba(239,68,68,.20);border-radius:14px;background:rgba(239,68,68,.07);color:var(--vf-danger);line-height:1.5}
#calc-page .vf-result-card{position:sticky;top:92px}
#calc-page .vf-liquid{padding:1.05rem 1.1rem;margin:.8rem 0 1rem;border:1px solid rgba(22,101,52,.15);border-radius:16px;background:linear-gradient(180deg,rgba(22,101,52,.06),rgba(22,101,52,.025))}
#calc-page .vf-liquid span{display:block;color:var(--cp-muted,#7b8493);font-size:.82rem;font-weight:650}
#calc-page .vf-liquid strong{display:block;margin-top:.25rem;color:var(--vf-green);font-size:clamp(1.65rem,3vw,2.15rem);line-height:1.1}
#calc-page .vf-kpis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:1rem 0}
#calc-page .vf-kpi{padding:.9rem;border:1px solid var(--vf-line);border-radius:15px;background:linear-gradient(180deg,#f8fafc,#fff)}
#calc-page .vf-kpi small{display:block;color:var(--cp-muted,#7b8493);font-size:.78rem;line-height:1.3}
#calc-page .vf-kpi strong{display:block;margin-top:.35rem;color:var(--cp-ink-strong,#181819);font-size:1.08rem;line-height:1.25}
#calc-page .vf-detail{margin-top:.8rem;border-top:1px solid var(--vf-line)}
#calc-page .vf-detail summary{padding:.9rem 0;cursor:pointer;color:var(--vf-blue);font-weight:700}
#calc-page .vf-rows{border-top:1px dashed var(--vf-line)}
#calc-page .vf-row{display:grid;grid-template-columns:minmax(0,1fr) minmax(120px,auto);gap:1rem;align-items:center;padding:.58rem 0;border-bottom:1px dashed var(--vf-line)}
#calc-page .vf-row span{color:var(--cp-muted,#7b8493);font-size:.9rem}
#calc-page .vf-row strong{color:var(--cp-ink-strong,#181819);text-align:right;font-size:.92rem;overflow-wrap:anywhere}
#calc-page .vf-guide-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
#calc-page .vf-guide-card h3{margin:.1rem 0 .45rem;color:var(--cp-ink-strong,#181819);font-size:1.1rem}
#calc-page .vf-guide-card p{margin:0;color:var(--cp-muted,#7b8493);line-height:1.65}
#calc-page .vf-scope{padding:1rem 1.1rem;border-left:4px solid #f59e0b;border-radius:0 14px 14px 0;background:rgba(245,158,11,.07);color:var(--cp-muted,#7b8493);line-height:1.6}
@media(max-width:980px){#calc-page .vf-tool-grid{grid-template-columns:1fr}#calc-page .vf-result-card{position:static}}
@media(max-width:760px){#calc-page .vf-hero-points,#calc-page .vf-kpis,#calc-page .vf-guide-grid,#calc-page .vf-fields{grid-template-columns:1fr}#calc-page .vf-field--full{grid-column:auto}#calc-page .vf-row{grid-template-columns:1fr;gap:.2rem}#calc-page .vf-row strong{text-align:left}}
</style>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>

<main id="calc-page">
  <section id="sessao1" class="sessao1">
    <div class="padding-gerais centralizador corrigir-zindex-textos-sessao1">
      <p class="cp-section-kicker">Calculadora trabalhista educativa</p>
      <h1 class="half-start my-lg">Calculadora de Férias CLT</h1>
      <p class="cp-sub mb-md">
        Estime o valor das férias, o <strong>1/3 constitucional</strong>, os dias de descanso e, se quiser,
        o <strong>abono pecuniário</strong> — a conhecida venda de 1/3 das férias.
      </p>

      <div class="vf-hero-points" aria-label="O que a calculadora mostra">
        <div class="vf-hero-point"><strong>Direito em dias</strong><span>O período de férias é definido primeiro conforme as faltas injustificadas informadas.</span></div>
        <div class="vf-hero-point"><strong>1/3 e valor líquido</strong><span>Principal, terço constitucional, INSS e IRRF ficam separados na memória de cálculo.</span></div>
        <div class="vf-hero-point"><strong>Venda de 1/3</strong><span>Quando o direito é de 30 dias, a opção corresponde aos conhecidos 10 dias de abono.</span></div>
      </div>

      <div class="cp-actions">
        <a class="cp-btn cp-btn--primary" href="#calc-ferias-clt">Calcular férias</a>
        <a class="cp-btn cp-btn--ghost" href="#abono-pecuniario">Entender a venda de férias</a>
      </div>

      <div class="cp-note">
        <strong>Base usada pela ferramenta:</strong> informe a <em>base integral das férias</em> já apurada para o período adquirido, sem o terço constitucional. Médias de horas extras, comissões e outros componentes variáveis não são inventadas por esta calculadora.
      </div>
    </div>
  </section>
