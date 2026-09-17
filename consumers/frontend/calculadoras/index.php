<?php
$config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';
$page_title = 'Calculadoras Online: Trabalhistas e Financeiras | Sanida';
$page_desc  = 'Use as calculadoras online da Sanida para estimar salário líquido CLT, décimo terceiro, férias e rescisão com dados fiscais atualizados.';
$canonical  = 'https://sanida.com.br/financas/calculadoras/';
$page_image = 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png';
include $_SERVER['DOCUMENT_ROOT'] . '/PHP/head-global.php';
$css_path = $_SERVER['DOCUMENT_ROOT'] . '/financas/calculadoras/assets/calculadoras-ui.css';
$css_ver  = file_exists($css_path) ? filemtime($css_path) : '1.0.0';
?>
<link rel="stylesheet" href="/financas/calculadoras/assets/calculadoras-ui.css?v=<?php echo $css_ver; ?>">

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "@id": "https://sanida.com.br/financas/calculadoras/#breadcrumb",
      "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Início", "item": "https://sanida.com.br/"},
        {"@type": "ListItem", "position": 2, "name": "Finanças", "item": "https://sanida.com.br/financas/"},
        {"@type": "ListItem", "position": 3, "name": "Calculadoras", "item": "https://sanida.com.br/financas/calculadoras/"}
      ]
    },
    {
      "@type": "CollectionPage",
      "@id": "https://sanida.com.br/financas/calculadoras/#webpage",
      "url": "https://sanida.com.br/financas/calculadoras/",
      "name": "Calculadoras Online: Trabalhistas e Financeiras | Sanida",
      "description": "Central de calculadoras online da Sanida para estimativas trabalhistas e financeiras: salário líquido CLT, décimo terceiro, férias e rescisão.",
      "isPartOf": {"@type": "WebSite", "name": "Sanida", "url": "https://sanida.com.br/"},
      "breadcrumb": {"@id": "https://sanida.com.br/financas/calculadoras/#breadcrumb"},
      "mainEntity": {"@id": "https://sanida.com.br/financas/calculadoras/#itemlist"}
    },
    {
      "@type": "ItemList",
      "@id": "https://sanida.com.br/financas/calculadoras/#itemlist",
      "name": "Calculadoras trabalhistas e financeiras da Sanida",
      "itemListElement": [
        {"@type": "ListItem", "position": 1, "url": "https://sanida.com.br/financas/calculadoras/salario-liquido-clt/", "name": "Calculadora de salário líquido CLT"},
        {"@type": "ListItem", "position": 2, "url": "https://sanida.com.br/financas/calculadoras/decimo-terceiro/", "name": "Calculadora de décimo terceiro"},
        {"@type": "ListItem", "position": 3, "url": "https://sanida.com.br/financas/calculadoras/ferias-clt/", "name": "Calculadora de férias CLT"},
        {"@type": "ListItem", "position": 4, "url": "https://sanida.com.br/financas/calculadoras/rescisao-clt/", "name": "Calculadora de rescisão CLT"}
      ]
    },
    {
      "@type": "FAQPage",
      "@id": "https://sanida.com.br/financas/calculadoras/#faq",
      "mainEntity": [
        {"@type": "Question", "name": "As calculadoras da Sanida substituem o RH, contador ou advogado?", "acceptedAnswer": {"@type": "Answer", "text": "Não. As calculadoras oferecem estimativas educativas com base nas informações preenchidas pelo usuário e nas tabelas fiscais disponíveis. Casos com regras específicas devem ser conferidos com RH, contador ou profissional jurídico."}},
        {"@type": "Question", "name": "Qual calculadora usar para saber quanto recebo por mês?", "acceptedAnswer": {"@type": "Answer", "text": "Para estimar o valor mensal depois de INSS, IRRF, dependentes e descontos, use a calculadora de salário líquido CLT."}},
        {"@type": "Question", "name": "Qual calculadora usar para primeira e segunda parcela do 13º?", "acceptedAnswer": {"@type": "Answer", "text": "Para estimar décimo terceiro total, primeira parcela, segunda parcela, descontos e proporcionalidade, use a calculadora de décimo terceiro."}},
        {"@type": "Question", "name": "As calculadoras usam dados atualizados?", "acceptedAnswer": {"@type": "Answer", "text": "As ferramentas foram estruturadas para consumir dados fiscais atualizados pelo sistema da Sanida. Ainda assim, os resultados devem ser tratados como estimativas, pois a folha de pagamento pode ter regras específicas."}}
      ]
    }
  ]
}
</script>
</head>
<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . '/PHP/header-menu.php'; ?>
<main id="calc-page">
  <section id="sessao1" class="sessao1">
    <div class="padding-gerais centralizador corrigir-zindex-textos-sessao1" style="text-align:center;">
      <span class="cp-eyebrow">Ferramentas financeiras da Sanida</span>
      <h1 class="half-start my-lg">Calculadoras online para trabalhadores CLT e planejamento financeiro</h1>
      <p class="cp-sub mb-md">Acesse calculadoras trabalhistas e financeiras para estimar salário líquido CLT, décimo terceiro, férias e rescisão. As ferramentas ajudam a transformar dúvidas comuns em números mais claros, sem substituir a conferência de RH, contador ou especialista.</p>
      <div class="cp-hero-actions">
        <a class="cp-btn cp-btn--primary" href="#calculadoras-disponiveis">Ver calculadoras</a>
        <a class="cp-btn cp-btn--soft" href="#qual-calculadora-usar">Escolher a ferramenta certa</a>
      </div>
      <div class="cp-note"><strong>Importante:</strong> os resultados são estimativas educativas. Valores finais podem variar por folha de pagamento, acordos coletivos, adicionais, descontos, regras do empregador e documentos oficiais.</div>
    </div>
  </section>

  <section id="calculadoras-disponiveis" class="padding-gerais py-xy">
    <div class="cp-section-title">
      <h2>Calculadoras trabalhistas e financeiras disponíveis</h2>
      <p>Escolha a calculadora conforme a dúvida do momento. A central organiza as ferramentas, mas cada página filha concentra seu próprio cálculo para evitar confusão entre salário mensal, 13º, férias e desligamento.</p>
    </div>

    <div class="cp-center cp-grid cp-grid--cards">
      <a class="cp-card cp-card--link cp-tool-card" href="/financas/calculadoras/salario-liquido-clt/">
        <span class="cp-card-kicker">Salário mensal</span>
        <h2 class="cp-card-title">Calculadora de salário líquido CLT</h2>
        <p class="cp-card-desc">Converta salário bruto em salário líquido e veja a estimativa de INSS, IRRF, dependentes, pensão e outros descontos informados.</p>
        <div class="cp-card-tags"><span>salário bruto → líquido</span><span>INSS</span><span>IRRF</span><span>dependentes</span></div>
      </a>

      <a class="cp-card cp-card--link cp-tool-card" href="/financas/calculadoras/decimo-terceiro/">
        <span class="cp-card-kicker">Gratificação anual</span>
        <h2 class="cp-card-title">Calculadora de décimo terceiro</h2>
        <p class="cp-card-desc">Calcule uma estimativa do 13º salário, primeira parcela, segunda parcela, descontos e proporcionalidade.</p>
        <div class="cp-card-tags"><span>13º salário</span><span>1ª parcela</span><span>2ª parcela</span><span>proporcional</span></div>
      </a>

      <a class="cp-card cp-card--link cp-tool-card" href="/financas/calculadoras/ferias-clt/">
        <span class="cp-card-kicker">Descanso remunerado</span>
        <h2 class="cp-card-title">Calculadora de férias CLT</h2>
        <p class="cp-card-desc">Simule férias, terço constitucional, abono pecuniário, dias de gozo e estimativa líquida conforme os dados preenchidos.</p>
        <div class="cp-card-tags"><span>férias</span><span>1/3 constitucional</span><span>abono</span><span>CLT</span></div>
      </a>

      <a class="cp-card cp-card--link cp-tool-card" href="/financas/calculadoras/rescisao-clt/">
        <span class="cp-card-kicker">Encerramento do contrato</span>
        <h2 class="cp-card-title">Calculadora de rescisão CLT</h2>
        <p class="cp-card-desc">Veja uma estimativa parcial das verbas suportadas: saldo de salário, férias e 13º proporcional nos cenários de desligamento implementados. FGTS, aviso prévio e seguro-desemprego ficam fora do cálculo automático.</p>
        <div class="cp-card-tags"><span>estimativa parcial</span><span>saldo</span><span>13º proporcional</span><span>férias</span></div>
      </a>
    </div>
  </section>

  <section id="qual-calculadora-usar" class="padding-gerais py-xy">
    <div class="cp-section-title">
      <h2>Qual calculadora usar?</h2>
      <p>Nem toda dúvida trabalhista pede a mesma ferramenta. Use este guia rápido para entrar diretamente na página certa.</p>
    </div>

    <div class="cp-center cp-use-grid">
      <div class="cp-use-item">
        <h3>Quero saber quanto recebo por mês</h3>
        <p>Use a <a href="/financas/calculadoras/salario-liquido-clt/">calculadora de salário líquido CLT</a> para estimar descontos e salário final.</p>
      </div>
      <div class="cp-use-item">
        <h3>Quero calcular o 13º salário</h3>
        <p>Use a <a href="/financas/calculadoras/decimo-terceiro/">calculadora de décimo terceiro</a> para estimar valor total, parcelas e proporcionalidade.</p>
      </div>
      <div class="cp-use-item">
        <h3>Vou sair de férias</h3>
        <p>Use a <a href="/financas/calculadoras/ferias-clt/">calculadora de férias CLT</a> para simular férias, terço constitucional e venda de dias, quando aplicável.</p>
      </div>
      <div class="cp-use-item">
        <h3>Fui desligado ou pedi demissão</h3>
        <p>Use a <a href="/financas/calculadoras/rescisao-clt/">calculadora de rescisão CLT</a> para estimar as verbas suportadas nesta versão. FGTS, aviso prévio e seguro-desemprego não entram no cálculo automático.</p>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy">
    <div class="cp-center cp-link-panel">
      <h2>Calculadoras para organizar decisões financeiras</h2>
      <p>As calculadoras online ajudam a entender números imediatos, mas a decisão financeira vem depois: quitar dívidas, montar reserva, proteger renda, planejar crédito, consórcio ou previdência. Por isso, esta central faz parte do hub de finanças da Sanida.</p>
      <div class="cp-pill-row">
        <a href="/financas/">Hub de finanças</a>
        <a href="/financas/credito/">Crédito</a>
        <a href="/financas/consorcio/">Consórcio</a>
        <a href="/financas/previdencia/">Previdência</a>
        <a href="/financas/protecao/">Proteção financeira</a>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy">
    <div class="cp-section-title">
      <h2>Como tratamos os cálculos</h2>
      <p>O objetivo das ferramentas é entregar clareza, velocidade e uma base confiável para comparação. Mesmo assim, nenhum simulador online substitui documentos oficiais de folha ou análise individual.</p>
    </div>
    <div class="cp-center cp-trust-grid">
      <div class="cp-trust-item">
        <strong>Estimativas educativas</strong>
        <p>Os resultados ajudam a entender cenários prováveis, mas podem variar conforme regras internas, sindicatos, benefícios e descontos específicos.</p>
      </div>
      <div class="cp-trust-item">
        <strong>Dados fiscais atualizados</strong>
        <p>As calculadoras foram estruturadas para usar dados fiscais mantidos pela Sanida, reduzindo dependência de valores fixos no conteúdo da página.</p>
      </div>
      <div class="cp-trust-item">
        <strong>Separação por finalidade</strong>
        <p>Cada calculadora tem território próprio: salário mensal, décimo terceiro, férias ou rescisão. Isso melhora a navegação e evita cálculos misturados.</p>
      </div>
    </div>
  </section>

  <section class="padding-gerais py-xy">
    <div class="cp-section-title">
      <h2>Perguntas frequentes sobre as calculadoras online</h2>
      <p>Antes de usar as ferramentas, veja os principais cuidados para interpretar os resultados.</p>
    </div>
    <div class="cp-faq">
      <details>
        <summary>As calculadoras da Sanida substituem o RH, contador ou advogado?</summary>
        <p>Não. Elas oferecem estimativas educativas com base nos dados informados. Em caso de rescisão, férias com regras específicas, adicionais, afastamentos ou divergência com a empresa, o ideal é conferir com RH, contador ou profissional jurídico.</p>
      </details>
      <details>
        <summary>Os valores das calculadoras são exatos?</summary>
        <p>As ferramentas buscam boa precisão, mas o resultado final pode mudar por benefícios, descontos internos, acordos coletivos, rubricas de folha, datas, adicionais e informações que o usuário não tenha informado.</p>
      </details>
      <details>
        <summary>Qual calculadora usar para saber quanto vou receber no mês?</summary>
        <p>Para salário mensal, use a calculadora de salário líquido CLT. Ela é a ferramenta correta para estimar salário depois de INSS, IRRF, dependentes, pensão e descontos informados.</p>
      </details>
      <details>
        <summary>Qual calculadora usar para décimo terceiro?</summary>
        <p>Use a calculadora de décimo terceiro. Ela foi criada para estimar 13º salário total, primeira parcela, segunda parcela, descontos e cálculo proporcional.</p>
      </details>
      <details>
        <summary>Por que as calculadoras fazem parte do hub de finanças?</summary>
        <p>Porque os números trabalhistas influenciam decisões financeiras. Saber quanto pode entrar de salário, férias, 13º ou rescisão ajuda a organizar dívidas, proteção financeira, previdência, crédito e consórcio com mais responsabilidade.</p>
      </details>
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
