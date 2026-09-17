<section class="padding-gerais py-xy" id="faq-ferias-clt">
  <div class="cp-center">
    <div class="cp-section-head">
      <p class="cp-section-kicker">Perguntas frequentes</p>
      <h2>FAQ sobre cálculo de férias</h2>
      <p>Respostas rápidas sobre valor das férias, 1/3 constitucional, faltas, venda de dias e incidências fiscais.</p>
    </div>

    <div class="cp-faq-list">
      <details class="cp-faq-item">
        <summary>Como calcular férias CLT?</summary>
        <div class="cp-faq__body"><p>A ferramenta parte da base integral de férias já apurada, considera o direito em dias conforme faltas injustificadas, calcula o 1/3 constitucional e aplica as incidências fiscais dos componentes suportados.</p></div>
      </details>

      <details class="cp-faq-item">
        <summary>Se eu vender 10 dias de férias, sempre descanso 20?</summary>
        <div class="cp-faq__body"><p>Somente quando o direito adquirido é de 30 dias. O abono pecuniário corresponde a 1/3 do período a que a pessoa efetivamente tem direito; por isso a ferramenta calcula primeiro o direito e só depois divide gozo e abono.</p></div>
      </details>

      <details class="cp-faq-item">
        <summary>O abono pecuniário paga INSS e IRRF?</summary>
        <div class="cp-faq__body"><p>A ferramenta não aplica uma resposta única ao bloco inteiro. O principal do abono e o terço constitucional sobre o abono são componentes distintos e seguem os perfis de incidência declarados pela release fiscal.</p></div>
      </details>

      <details class="cp-faq-item">
        <summary>O IRRF das férias é somado ao salário normal do mês?</summary>
        <div class="cp-faq__body"><p>Não nesta calculadora. O contrato fiscal usado por H28 exige uma avaliação separada do IRRF de férias. O salário ordinário do mês pertence a outra apuração.</p></div>
      </details>

      <details class="cp-faq-item">
        <summary>A calculadora faz média de comissão e hora extra?</summary>
        <div class="cp-faq__body"><p>Não. A base integral das férias é uma entrada externa já apurada. Essa fronteira evita que o navegador invente uma média sem regra contratual executável.</p></div>
      </details>

      <details class="cp-faq-item">
        <summary>Mais de 32 faltas é calculado automaticamente?</summary>
        <div class="cp-faq__body"><p>Não. Fora do intervalo modelado, a ferramenta falha fechado em vez de extrapolar uma regra que não está contratualmente definida.</p></div>
      </details>
    </div>

    <p class="cp-disclaimer">Estimativa informativa. A folha real pode envolver fatos e verbas que não estão no escopo automatizado desta versão.</p>
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
