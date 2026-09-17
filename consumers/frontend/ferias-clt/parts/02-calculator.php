<section class="padding-gerais py-xy" id="calc-ferias-clt" aria-label="Calculadora de férias CLT">
  <div class="cp-center">
    <div class="cp-section-head">
      <p class="cp-section-kicker">Ferramenta principal</p>
      <h2>Calcule férias, 1/3 constitucional e venda de dias</h2>
      <p>Preencha a base já apurada das férias e os dados do período. O resultado separa direito em dias, gozo, abono pecuniário, descontos e valor líquido.</p>
    </div>

    <div class="vf-tool-grid">
      <div class="cp-card cp-card--form vf-card">
        <div class="vf-card-head">
          <h3>Dados das férias</h3>
          <p>Use valores conhecidos. A ferramenta não reconstrói médias de comissão, horas extras ou adicionais a partir da folha mensal.</p>
        </div>

        <form novalidate>
          <div class="vf-fields">
            <div class="vf-field vf-field--full">
              <label for="base-ferias">Base integral das férias, sem 1/3</label>
              <input id="base-ferias" name="base_ferias" inputmode="decimal" autocomplete="off" placeholder="Ex.: 4000,00" required>
              <small>Use o principal já apurado para todo o período de férias a que você tem direito. A ferramenta não calcula médias de verbas variáveis.</small>
            </div>

            <div class="vf-field">
              <label for="faltas">Faltas injustificadas no período aquisitivo</label>
              <input id="faltas" name="faltas_injustificadas" type="number" min="0" step="1" value="0" required>
              <small>As faixas suportadas podem resultar em 30, 24, 18 ou 12 dias de direito.</small>
            </div>

            <div class="vf-field">
              <label for="data-pagamento">Data de pagamento das férias</label>
              <input id="data-pagamento" name="data_pagamento" type="date" required>
              <small>Obrigatória para definir a vigência fiscal. A calculadora não presume a data de hoje quando este campo fica vazio.</small>
            </div>

            <div class="vf-field vf-field--full">
              <label class="vf-check" for="vender-terco">
                <input id="vender-terco" name="vender_um_terco" type="checkbox">
                <span>Quero converter em dinheiro exatamente 1/3 do período de férias a que tenho direito</span>
              </label>
              <small>Com direito a 30 dias, isso corresponde à venda de 10 dias e ao gozo de 20 dias. Com direito menor, o abono acompanha 1/3 do período efetivamente adquirido.</small>
            </div>

            <div class="vf-field">
              <label for="dependentes">Dependentes para esta apuração de IRRF</label>
              <input id="dependentes" name="dependentes" type="number" min="0" step="1" value="0" required>
            </div>

            <div class="vf-field">
              <label for="pensao">Pensão dedutível nesta apuração</label>
              <input id="pensao" name="pensao" inputmode="decimal" autocomplete="off" value="0,00" required>
            </div>
          </div>

          <div class="cp-actions">
            <button class="cp-btn cp-btn--primary" type="submit">Calcular férias</button>
            <a class="cp-btn cp-btn--ghost" href="#como-calculamos-ferias">Entender o cálculo</a>
          </div>

          <div class="vf-alert" data-alert role="alert" aria-live="assertive" aria-atomic="true"></div>
        </form>
      </div>

      <aside class="cp-card cp-card--result vf-card vf-result-card" data-result role="status" aria-live="polite" aria-atomic="true" style="display:none">
        <div class="vf-card-head">
          <h3>Resultado da sua simulação de férias</h3>
          <p>Primeiro veja o direito em dias e o valor líquido. A composição fiscal completa fica disponível logo abaixo.</p>
        </div>

        <div class="vf-liquid">
          <span>Valor líquido estimado</span>
          <strong data-kpi="liquido">—</strong>
        </div>

        <div class="vf-kpis">
          <div class="vf-kpi"><small>Total bruto das férias</small><strong data-kpi="bruto">—</strong></div>
          <div class="vf-kpi"><small>Dias de gozo</small><strong data-kpi="dias">—</strong></div>
          <div class="vf-kpi"><small>Direito adquirido</small><strong data-row="direito">—</strong></div>
        </div>

        <details class="vf-detail" open>
          <summary>Como o período foi dividido</summary>
          <div class="vf-rows">
            <div class="vf-row"><span>Direito adquirido</span><strong data-row="direito">—</strong></div>
            <div class="vf-row"><span>Dias de gozo</span><strong data-row="gozo">—</strong></div>
            <div class="vf-row"><span>Dias convertidos em abono</span><strong data-row="abono-dias">—</strong></div>
            <div class="vf-row"><span>Principal das férias gozadas</span><strong data-row="principal-gozo">—</strong></div>
            <div class="vf-row"><span>1/3 sobre férias gozadas</span><strong data-row="terco-gozo">—</strong></div>
            <div class="vf-row"><span>Abono pecuniário — principal</span><strong data-row="abono-principal">—</strong></div>
            <div class="vf-row"><span>1/3 sobre o abono</span><strong data-row="abono-terco">—</strong></div>
          </div>
        </details>

        <details class="vf-detail">
          <summary>Ver descontos e memória fiscal</summary>
          <div class="vf-rows">
            <div class="vf-row"><span>Base previdenciária</span><strong data-row="base-inss">—</strong></div>
            <div class="vf-row"><span>INSS das férias</span><strong data-row="inss">—</strong></div>
            <div class="vf-row"><span>Rendimento tributável de férias</span><strong data-row="renda-ir">—</strong></div>
            <div class="vf-row"><span>Base do IRRF após dedução</span><strong data-row="base-ir">—</strong></div>
            <div class="vf-row"><span>Dedução escolhida no IRRF</span><strong data-row="modo-ir">—</strong></div>
            <div class="vf-row"><span>IRRF antes do redutor de 2026</span><strong data-row="ir-antes-reducao">—</strong></div>
            <div class="vf-row"><span>Rendimento usado pelo redutor</span><strong data-row="renda-redutor">—</strong></div>
            <div class="vf-row"><span>Redução do IR</span><strong data-row="reducao">—</strong></div>
            <div class="vf-row"><span>IRRF final</span><strong data-row="irrf">—</strong></div>
            <div class="vf-row"><span>Pensão informada</span><strong data-row="pensao">—</strong></div>
            <div class="vf-row"><span>Data fiscal usada</span><strong data-row="referencia">—</strong></div>
          </div>
        </details>

        <details class="vf-detail">
          <summary>Detalhes técnicos da apuração</summary>
          <div class="vf-rows">
            <div class="vf-row"><span>Release fiscal usada</span><strong data-row="release-id">—</strong></div>
          </div>
        </details>
      </aside>
    </div>
  </div>
</section>
