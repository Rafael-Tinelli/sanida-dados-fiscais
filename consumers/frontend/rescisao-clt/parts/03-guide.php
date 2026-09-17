<section class="padding-gerais py-xy" id="como-calculamos-rescisao" style="background-color:var(--color-background4);">
  <div class="cp-center">
    <div class="cp-section-head">
      <p class="cp-section-kicker">Como a ferramenta trabalha</p>
      <h2>Como funciona o cálculo de rescisão nesta página</h2>
      <p>
        A calculadora não tenta reproduzir todo o TRCT. Ela calcula apenas as parcelas que o contrato atual consegue determinar de forma explícita e auditável.
      </p>
    </div>

    <div class="rc-guide-grid">
      <article class="cp-card rc-guide-card" id="rescisao-sem-justa-causa">
        <h3>Demissão sem justa causa</h3>
        <p>No cenário suportado, a ferramenta pode calcular saldo de salário, 13º proporcional e férias proporcionais. FGTS, multa rescisória, aviso prévio e outras verbas continuam fora desta versão.</p>
      </article>

      <article class="cp-card rc-guide-card" id="pedido-de-demissao">
        <h3>Pedido de demissão</h3>
        <p>O pedido de demissão está entre os quatro motivos aceitos. A memória mantém separados o saldo de salário, os avos do 13º e os avos de férias aplicáveis ao modelo atual.</p>
      </article>

      <article class="cp-card rc-guide-card" id="rescisao-por-acordo">
        <h3>Rescisão por acordo</h3>
        <p>O acordo do art. 484-A é reconhecido pela matriz do H29. A calculadora, porém, continua parcial: ela não transforma automaticamente todos os efeitos possíveis do acordo em valor monetário.</p>
      </article>

      <article class="cp-card rc-guide-card" id="justa-causa">
        <h3>Demissão por justa causa</h3>
        <p>Neste modelo, o motivo correspondente mantém o saldo de salário no escopo e não calcula 13º proporcional nem férias proporcionais. A página mostra esse resultado sem criar parcelas que o contrato não autorizou.</p>
      </article>
    </div>
  </div>
</section>

<section class="padding-gerais py-xy">
  <div class="cp-center">
    <div class="cp-section-head">
      <p class="cp-section-kicker">Memória de cálculo</p>
      <h2>Por que 13º e férias podem ter quantidades diferentes de avos?</h2>
      <p>Porque as duas parcelas usam calendários diferentes. O 13º proporcional olha para o ano civil do desligamento; as férias proporcionais seguem o período aquisitivo iniciado na admissão.</p>
    </div>

    <div class="cp-grid cp-grid--cards">
      <article class="cp-card">
        <h3>Saldo de salário usa os dias reais do mês</h3>
        <p>O H29 não aplica um divisor 30 universal. O denominador corresponde à quantidade real de dias civis do mês do desligamento, enquanto o numerador representa os dias computados naquele mês.</p>
      </article>

      <article class="cp-card">
        <h3>13º proporcional usa o ano civil</h3>
        <p>A contagem dos avos do 13º ocorre dentro do ano da extinção e respeita o limiar contratual de dias para considerar o mês.</p>
      </article>

      <article class="cp-card">
        <h3>Férias seguem o período aquisitivo</h3>
        <p>A contagem das férias proporcionais permanece ancorada na data de admissão. Ela não reinicia automaticamente em janeiro.</p>
      </article>

      <article class="cp-card">
        <h3>Férias adquiridas ou vencidas não viram valor automaticamente</h3>
        <p>A ferramenta pode sinalizar a existência potencial dessa situação, mas não monetiza períodos adquiridos, vencidos, múltiplos períodos ou eventual dobra sem uma regra específica.</p>
      </article>
    </div>
  </div>
</section>

<section class="padding-gerais py-xy" id="limites-da-calculadora" style="background-color:var(--color-background4);">
  <div class="cp-center">
    <div class="cp-link-panel">
      <h2>O que esta calculadora de rescisão ainda não calcula</h2>
      <p>
        Aviso prévio ou desconto de aviso, multa e saque do FGTS, seguro-desemprego, indenizações de estabilidade,
        regras de contrato por prazo determinado, verbas específicas de negociação coletiva e componentes variáveis
        não explicitamente modelados permanecem fora da estimativa automática.
      </p>

      <div class="rc-inline-links">
        <a href="/financas/calculadoras/decimo-terceiro/">Calculadora de décimo terceiro</a>
        <a href="/financas/calculadoras/ferias-clt/">Calculadora de férias CLT</a>
        <a href="/financas/calculadoras/salario-liquido-clt/">Calculadora de salário líquido</a>
        <a href="/financas/calculadoras/">Ver todas as calculadoras</a>
      </div>
    </div>
  </div>
</section>
