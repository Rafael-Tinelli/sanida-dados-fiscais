<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Calculadora de rescisão CLT: estimativa parcial por motivo eSocial</title>
  <meta name="description" content="Estime o subconjunto suportado da rescisão CLT: saldo salarial, 13º proporcional e avos de férias proporcionais, com motivo eSocial e limites explícitos.">
  <style>
    :root{--ink:#18212b;--muted:#5d6875;--line:#dde3e8;--soft:#f5f7f9;--brand:#174f7b;--warn:#8a5200;--danger:#8c2f2f}
    *{box-sizing:border-box}body{margin:0;font:16px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:var(--ink);background:#fff}
    .wrap{width:min(1080px,calc(100% - 32px));margin:auto}.hero{padding:48px 0 28px}.eyebrow{font-size:.82rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--brand)}
    h1{font-size:clamp(2rem,5vw,3.3rem);line-height:1.06;margin:.35rem 0 1rem}.lead{max-width:790px;color:var(--muted);font-size:1.08rem}.note,.scope-note{padding:14px 16px;border:1px solid var(--line);border-radius:12px;background:var(--soft);margin-top:20px}.scope-note{border-left:4px solid var(--warn)}
    .calc{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:24px;margin:20px 0 44px}.panel{border:1px solid var(--line);border-radius:16px;padding:22px;background:#fff}.panel h2{margin-top:0}
    label{display:block;font-weight:650;margin:14px 0 6px}input,select{width:100%;padding:11px 12px;border:1px solid #bac3cc;border-radius:9px;font:inherit;background:#fff}.is-muted{opacity:.6}
    button{margin-top:20px;border:0;border-radius:9px;padding:12px 18px;background:var(--brand);color:#fff;font:inherit;font-weight:700;cursor:pointer}button:disabled{opacity:.55}.alert{display:none;margin-top:16px;padding:12px 14px;border-radius:9px;background:#fff4e5;color:#754400}
    .kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.kpi{padding:14px;border-radius:12px;background:var(--soft)}.kpi small{display:block;color:var(--muted)}.kpi strong{display:block;font-size:1.2rem;margin-top:4px}.rows{margin-top:16px;border-top:1px solid var(--line)}.row{display:flex;justify-content:space-between;gap:20px;padding:9px 0;border-bottom:1px solid var(--line)}.row span:first-child{color:var(--muted)}.audit{font-size:.82rem;overflow-wrap:anywhere;color:var(--muted)}
    .scope-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:22px}.scope-box{padding:16px;border:1px solid var(--line);border-radius:12px}.scope-box h3{margin-top:0}.scope-box ul{padding-left:20px}.scope-box--excluded{border-color:#e4caca}
    article{max-width:820px;margin:0 auto 48px}article h2{margin-top:2rem}article h3{margin-top:1.5rem}.faq details{border-top:1px solid var(--line);padding:14px 0}.faq summary{font-weight:700;cursor:pointer}.warning{border-left:4px solid var(--danger);padding:12px 16px;background:#fff7f7}
    @media(max-width:780px){.calc,.scope-grid{grid-template-columns:1fr}.kpis{grid-template-columns:1fr}.row{align-items:flex-start}.hero{padding-top:30px}}
  </style>
</head>
<body>
<main id="calc-rescisao-clt">
  <header class="hero wrap">
    <div class="eyebrow">Calculadora trabalhista · H29</div>
    <h1>Rescisão CLT: estime apenas as verbas que este modelo realmente cobre</h1>
    <p class="lead">A calculadora usa o motivo eSocial para decidir quais parcelas proporcionais entram no escopo, mantém o 13º no ano civil e as férias no período aquisitivo do vínculo.</p>
    <div class="scope-note"><strong>Esta não é uma calculadora de “total da rescisão”.</strong> Aviso prévio, multa e saque do FGTS, seguro-desemprego e outras verbas permanecem fora do cálculo automático desta versão.</div>
  </header>
