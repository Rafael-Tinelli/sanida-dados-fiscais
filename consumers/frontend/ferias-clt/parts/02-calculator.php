<section class="wrap calc" aria-label="Calculadora de férias CLT">
  <div class="panel">
    <h2>Dados das férias</h2>
    <form novalidate>
      <label for="base-ferias">Base integral das férias, sem 1/3</label>
      <input id="base-ferias" name="base_ferias" inputmode="decimal" autocomplete="off" placeholder="Ex.: 4000,00" required>
      <p><small>Use o principal já apurado para todo o período de férias a que você tem direito. A ferramenta não calcula médias de verbas variáveis.</small></p>

      <label for="faltas">Faltas injustificadas no período aquisitivo</label>
      <input id="faltas" name="faltas_injustificadas" type="number" min="0" step="1" value="0" required>

      <label for="data-pagamento">Data de pagamento das férias</label>
      <input id="data-pagamento" name="data_pagamento" type="date" required>
      <p><small>Obrigatória para definir a vigência fiscal. A calculadora não presume a data de hoje quando este campo fica vazio.</small></p>

      <label class="check" for="vender-terco">
        <input id="vender-terco" name="vender_um_terco" type="checkbox">
        <span>Converter em dinheiro exatamente 1/3 do período a que tenho direito</span>
      </label>

      <label for="dependentes">Dependentes para esta apuração de IRRF</label>
      <input id="dependentes" name="dependentes" type="number" min="0" step="1" value="0" required>

      <label for="pensao">Pensão dedutível nesta apuração</label>
      <input id="pensao" name="pensao" inputmode="decimal" autocomplete="off" value="0,00" required>

      <button type="submit">Calcular férias</button>
      <div class="alert" data-alert role="alert"></div>
    </form>
  </div>

  <div class="panel" data-result style="display:none">
    <h2>Resultado</h2>
    <div class="kpis">
      <div class="kpi"><small>Total bruto das férias</small><strong data-kpi="bruto">—</strong></div>
      <div class="kpi"><small>Valor líquido estimado</small><strong data-kpi="liquido">—</strong></div>
      <div class="kpi"><small>Dias de gozo</small><strong data-kpi="dias">—</strong></div>
    </div>

    <div class="rows">
      <div class="row"><span>Direito adquirido</span><strong data-row="direito">—</strong></div>
      <div class="row"><span>Dias de gozo</span><strong data-row="gozo">—</strong></div>
      <div class="row"><span>Dias convertidos em abono</span><strong data-row="abono-dias">—</strong></div>
      <div class="row"><span>Principal das férias gozadas</span><strong data-row="principal-gozo">—</strong></div>
      <div class="row"><span>1/3 sobre férias gozadas</span><strong data-row="terco-gozo">—</strong></div>
      <div class="row"><span>Abono pecuniário — principal</span><strong data-row="abono-principal">—</strong></div>
      <div class="row"><span>1/3 sobre o abono</span><strong data-row="abono-terco">—</strong></div>
      <div class="row"><span>Base previdenciária</span><strong data-row="base-inss">—</strong></div>
      <div class="row"><span>INSS das férias</span><strong data-row="inss">—</strong></div>
      <div class="row"><span>Rendimento tributável de férias</span><strong data-row="renda-ir">—</strong></div>
      <div class="row"><span>Base do IRRF após dedução</span><strong data-row="base-ir">—</strong></div>
      <div class="row"><span>Dedução escolhida no IRRF</span><strong data-row="modo-ir">—</strong></div>
      <div class="row"><span>IRRF antes do redutor de 2026</span><strong data-row="ir-antes-reducao">—</strong></div>
      <div class="row"><span>Rendimento usado pelo redutor</span><strong data-row="renda-redutor">—</strong></div>
      <div class="row"><span>Redução do IR</span><strong data-row="reducao">—</strong></div>
      <div class="row"><span>IRRF final</span><strong data-row="irrf">—</strong></div>
      <div class="row"><span>Pensão informada</span><strong data-row="pensao">—</strong></div>
      <div class="row"><span>Data fiscal usada</span><strong data-row="referencia">—</strong></div>
    </div>
    <p class="audit"><strong>Release fiscal usada:</strong> <span data-row="release-id">—</span></p>
  </div>
</section>
