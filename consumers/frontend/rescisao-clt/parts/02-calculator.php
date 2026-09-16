<section class="wrap calc" aria-label="Calculadora de rescisão CLT limitada">
  <div class="panel">
    <h2>Dados do desligamento</h2>
    <form novalidate>
      <label for="motivo-esocial">Motivo do desligamento (eSocial)</label>
      <select id="motivo-esocial" name="motivo_esocial" required>
        <option value="02">02 — Sem justa causa pelo empregador</option>
        <option value="07">07 — Pedido de demissão</option>
        <option value="33">33 — Acordo do art. 484-A</option>
        <option value="01">01 — Justa causa pelo empregador</option>
      </select>

      <label for="regime">Regime de remuneração</label>
      <select id="regime" name="regime_emprego" required>
        <option value="monthly">Mensalista</option>
        <option value="biweekly">Quinzenalista</option>
      </select>

      <label for="prazo">Prazo contratual</label>
      <select id="prazo" name="prazo_contrato" required>
        <option value="indefinite">Prazo indeterminado</option>
      </select>
      <p><small>Contrato a prazo determinado não é convertido silenciosamente para este modelo.</small></p>

      <label for="admissao">Data de admissão</label>
      <input id="admissao" name="data_admissao" type="date" required>

      <label for="desligamento">Data de desligamento considerada</label>
      <input id="desligamento" name="data_desligamento" type="date" required>
      <p><small>Não há projeção automática de aviso prévio nesta data.</small></p>

      <label for="salario-base">Salário-base mensal</label>
      <input id="salario-base" name="salario_base_mensal" inputmode="decimal" autocomplete="off" placeholder="Ex.: 3100,00" required>

      <label for="dias-computados">Dias computados no mês do desligamento</label>
      <input id="dias-computados" name="dias_computados" type="number" min="0" step="1" required>
      <p><small>O denominador é o número real de dias civis do mês; a ferramenta não usa divisor 30 universal.</small></p>

      <div data-conditional="thirteenth">
        <label for="remuneracao-desligamento">Remuneração do mês da extinção para referência do 13º</label>
        <input id="remuneracao-desligamento" name="remuneracao_mes_desligamento" inputmode="decimal" autocomplete="off" placeholder="Ex.: 3600,00" required>
        <p><small>Informe a remuneração fixa de referência já apurada. Médias e verbas variáveis rescisórias não são inventadas neste escopo.</small></p>
      </div>

      <button type="submit">Calcular estimativa parcial</button>
      <div class="alert" data-alert role="alert"></div>
    </form>
  </div>

  <div class="panel" data-result style="display:none">
    <h2>Resultado coberto pelo H29</h2>
    <div class="kpis">
      <div class="kpi"><small>Saldo salarial</small><strong data-kpi="saldo">—</strong></div>
      <div class="kpi"><small>13º proporcional</small><strong data-kpi="decimo">—</strong></div>
      <div class="kpi"><small>Férias proporcionais</small><strong data-kpi="ferias">—</strong></div>
    </div>

    <div class="rows">
      <div class="row"><span>Motivo</span><strong data-row="motivo">—</strong></div>
      <div class="row"><span>Memória do saldo</span><strong data-row="saldo-formula">—</strong></div>
      <div class="row"><span>Saldo salarial</span><strong data-row="saldo">—</strong></div>
      <div class="row"><span>13º — avos</span><strong data-row="13-avos">—</strong></div>
      <div class="row"><span>13º — remuneração de referência</span><strong data-row="13-referencia">—</strong></div>
      <div class="row"><span>13º — valor bruto proporcional</span><strong data-row="13-bruto">—</strong></div>
      <div class="row"><span>Período aquisitivo de férias</span><strong data-row="ferias-periodo">—</strong></div>
      <div class="row"><span>Férias proporcionais — avos</span><strong data-row="ferias-avos">—</strong></div>
      <div class="row"><span>Férias adquiridas/vencidas</span><strong data-row="ferias-adquiridas">—</strong></div>
      <div class="row"><span>Natureza do resultado</span><strong data-row="promessa">—</strong></div>
      <div class="row"><span>Data de referência</span><strong data-row="referencia">—</strong></div>
      <div class="row audit"><span>Release fiscal usada</span><strong data-row="release-id">—</strong></div>
    </div>

    <div class="scope-grid">
      <section class="scope-box">
        <h3>Incluído no modelo</h3>
        <ul data-list="incluidos"></ul>
      </section>
      <section class="scope-box scope-box--excluded">
        <h3>Fora do cálculo automático</h3>
        <ul data-list="excluidos"></ul>
      </section>
    </div>
    <p class="warning"><strong>Não some este resultado como se fosse o total final da rescisão.</strong> Alguns itens exibidos são apenas elegibilidade/avos e vários componentes rescisórios estão deliberadamente fora deste modelo.</p>
  </div>
</section>
