<section class="padding-gerais rc-calc-section" id="calc-rescisao-clt" aria-label="Calculadora de rescisão CLT">
  <div class="cp-center">
    <div class="cp-section-head">
      <p class="cp-section-kicker">Ferramenta principal</p>
      <h2>Estime as parcelas cobertas no seu desligamento</h2>
      <p>
        Comece pelo modo como o contrato terminou. A ferramenta usa os códigos técnicos necessários internamente,
        mas apresenta as opções na linguagem do trabalhador.
      </p>
    </div>

    <div class="rc-tool-grid">
      <div class="cp-card cp-card--form rc-form-card">
        <div class="rc-card-head">
          <h3>Dados para a estimativa</h3>
          <p>
            Informe somente dados conhecidos. Quando o cenário estiver fora do contrato suportado,
            a calculadora bloqueia a estimativa em vez de inventar uma aproximação.
          </p>
        </div>

        <form novalidate>
          <input type="hidden" name="prazo_contrato" value="indefinite">

          <div class="rc-fields">
            <div class="rc-field rc-field--full">
              <label for="motivo-esocial">Como terminou o contrato?</label>
              <select id="motivo-esocial" name="motivo_esocial" required>
                <option value="02">Fui demitido sem justa causa</option>
                <option value="07">Pedi demissão</option>
                <option value="33">Fiz acordo com a empresa</option>
                <option value="01">Fui demitido por justa causa</option>
              </select>
              <small>
                A ferramenta associa internamente a opção ao motivo eSocial correspondente: 02, 07, 33 ou 01.
              </small>
            </div>

            <div class="rc-field">
              <label for="regime">Como você recebe normalmente?</label>
              <select id="regime" name="regime_emprego" required>
                <option value="monthly">Salário mensal</option>
                <option value="biweekly">Pagamento quinzenal</option>
              </select>
            </div>

            <div class="rc-field">
              <label for="admissao">Data de admissão</label>
              <input id="admissao" name="data_admissao" type="date" required>
              <small>Usada também para localizar o período aquisitivo das férias.</small>
            </div>

            <div class="rc-field">
              <label for="desligamento">Data do desligamento considerada</label>
              <input id="desligamento" name="data_desligamento" type="date" required>
              <small>Esta versão não projeta automaticamente aviso prévio nessa data.</small>
            </div>

            <div class="rc-field">
              <label for="salario-base">Salário-base mensal</label>
              <input
                id="salario-base"
                name="salario_base_mensal"
                inputmode="decimal"
                autocomplete="off"
                placeholder="Ex.: 3100,00"
                required
              >
              <small>Este valor é indispensável para o saldo de salário e deve ser maior que zero.</small>
            </div>

            <div class="rc-field rc-field--full" data-conditional="thirteenth">
              <label for="remuneracao-desligamento">Remuneração de referência para o 13º</label>
              <input
                id="remuneracao-desligamento"
                name="remuneracao_mes_desligamento"
                inputmode="decimal"
                autocomplete="off"
                placeholder="Ex.: 3600,00"
                required
              >
              <small>
                Quando o motivo inclui 13º proporcional, informe uma remuneração fixa já apurada e maior que zero para o mês do desligamento.
                Médias e verbas variáveis não são estimadas automaticamente.
              </small>
            </div>
          </div>

          <p class="rc-contract-note">
            <strong>Escopo contratual:</strong> esta versão atende vínculo por prazo indeterminado.
            Contrato de experiência, outros contratos a prazo e modalidades não suportadas permanecem fora do cálculo.
          </p>

          <details class="rc-advanced">
            <summary>Ajustar dias computados no mês</summary>
            <div class="rc-advanced-body">
              <div class="rc-field">
                <label for="dias-computados">Dias computados até o desligamento</label>
                <input
                  id="dias-computados"
                  name="dias_computados"
                  type="number"
                  min="0"
                  step="1"
                  required
                >
                <small>
                  A ferramenta preenche este campo a partir das datas. Se admissão e desligamento ocorrerem no mesmo mês,
                  conta os dias inclusivos entre as duas datas. Ajuste o campo se houve férias, afastamentos ou outro fato
                  que reduza os dias efetivamente computados. O denominador continua sendo o número real de dias civis do mês.
                </small>
              </div>
            </div>
          </details>

          <div class="rc-actions">
            <button class="cp-btn cp-btn--primary" type="submit">Calcular estimativa parcial</button>
            <a class="cp-btn cp-btn--ghost" href="#limites-da-calculadora">Ver o que fica fora</a>
          </div>

          <div
            class="rc-alert"
            data-alert
            role="alert"
            aria-live="assertive"
            aria-atomic="true"
          ></div>
        </form>
      </div>

      <aside
        class="cp-card cp-card--result rc-result-card"
        data-result
        role="status"
        aria-live="polite"
        aria-atomic="true"
        style="display:none"
      >
        <span class="rc-result-badge">Estimativa parcial</span>

        <div class="rc-card-head">
          <h3>Resultado da sua simulação</h3>
          <p>
            O resumo abaixo separa o que foi efetivamente calculado do que ainda depende
            de outras regras, dados ou análise documental.
          </p>
        </div>

        <div class="rc-kpis">
          <div class="rc-kpi">
            <small>Saldo de salário</small>
            <strong data-kpi="saldo">—</strong>
          </div>
          <div class="rc-kpi">
            <small>13º proporcional</small>
            <strong data-kpi="decimo">—</strong>
          </div>
          <div class="rc-kpi">
            <small>Férias proporcionais</small>
            <strong data-kpi="ferias">—</strong>
          </div>
        </div>

        <div class="rc-partial-warning">
          <strong>Não some estes campos como se fossem o valor final da rescisão.</strong>
          Férias proporcionais são exibidas em avos nesta versão e outras verbas rescisórias
          permanecem fora do modelo automático.
        </div>

        <details class="rc-detail" open>
          <summary>Como chegamos a este resultado</summary>
          <div class="rc-rows">
            <div class="rc-row"><span>Forma de desligamento</span><strong data-row="motivo">—</strong></div>
            <div class="rc-row"><span>Memória do saldo de salário</span><strong data-row="saldo-formula">—</strong></div>
            <div class="rc-row"><span>Saldo de salário</span><strong data-row="saldo">—</strong></div>
            <div class="rc-row"><span>13º proporcional — avos</span><strong data-row="13-avos">—</strong></div>
            <div class="rc-row"><span>Remuneração usada no 13º</span><strong data-row="13-referencia">—</strong></div>
            <div class="rc-row"><span>13º proporcional — valor bruto</span><strong data-row="13-bruto">—</strong></div>
            <div class="rc-row"><span>Período aquisitivo das férias</span><strong data-row="ferias-periodo">—</strong></div>
            <div class="rc-row"><span>Férias proporcionais — avos</span><strong data-row="ferias-avos">—</strong></div>
            <div class="rc-row"><span>Férias adquiridas ou vencidas</span><strong data-row="ferias-adquiridas">—</strong></div>
          </div>
        </details>

        <div class="rc-scope-grid">
          <section class="rc-scope-box rc-scope-box--in">
            <h4>Incluído nesta estimativa</h4>
            <ul data-list="incluidos"></ul>
          </section>
          <section class="rc-scope-box rc-scope-box--out">
            <h4>Fora do cálculo automático</h4>
            <ul data-list="excluidos"></ul>
          </section>
        </div>

        <details class="rc-detail">
          <summary>Detalhes técnicos da apuração</summary>
          <div class="rc-rows">
            <div class="rc-row"><span>Natureza do resultado</span><strong data-row="promessa">—</strong></div>
            <div class="rc-row"><span>Data de referência</span><strong data-row="referencia">—</strong></div>
            <div class="rc-row"><span>Release fiscal utilizada</span><strong data-row="release-id">—</strong></div>
          </div>
        </details>
      </aside>
    </div>
  </div>
</section>
