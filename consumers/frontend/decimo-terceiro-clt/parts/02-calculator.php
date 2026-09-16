  <section class="padding-gerais cp-calc-section" id="calc-decimo-terceiro">
    <div class="cp-center">
      <div class="cp-section-head">
        <p class="cp-section-kicker">Ferramenta principal</p>
        <h2>Calculadora de décimo terceiro salário com 1ª parcela, 2ª parcela e proporcional</h2>
        <p>
          Preencha os campos para calcular décimo terceiro salário de forma rápida. A ferramenta estima o valor do 13º salário,
          a primeira parcela, a segunda parcela bruta e, quando a opção estiver ativa, uma projeção da segunda parcela líquida.
          Se o adiantamento e os descontos consumirem todo o saldo, a calculadora mostra valor a pagar igual a zero e separa a insuficiência para conferência, para não exibir uma “parcela negativa”.
        </p>
      </div>

      <div class="cp-grid cp-grid--main">
        <div class="cp-card cp-card--form">
          <div class="cp-card__header">
            <h3>Dados da simulação do décimo terceiro salário</h3>
            <p>
              Ideal para quem quer calcular décimo terceiro proporcional, calcular a primeira parcela do décimo terceiro
              e entender melhor o valor da segunda parcela.
            </p>
          </div>

          <div data-alert class="cp-alert"></div>

          <form novalidate>
            <div class="cp-fields">
              <div class="cp-field">
                <label for="d13-salario">Remuneração devida em dezembro</label>
                <input id="d13-salario" name="salario" inputmode="decimal" placeholder="Ex.: 4500,00">
                <small>Use a remuneração fixa devida em dezembro para o 13º anual.</small>
              </div>

              <div class="cp-field">
                <label for="d13-variaveis">Média de variáveis já apurada</label>
                <input id="d13-variaveis" name="variaveis" inputmode="decimal" placeholder="Ex.: 250,00">
                <small>Informe uma média externa já apurada de comissões, horas extras ou adicionais; a calculadora não inventa essa média.</small>
              </div>

              <div class="cp-field full">
                <span class="cp-label">Como informar os avos?</span>
                <label class="cp-check"><input type="radio" name="modo_avos" value="manual" checked> <span>Informar avos manualmente</span></label>
                <label class="cp-check"><input type="radio" name="modo_avos" value="datas"> <span>Calcular avos pelas datas de vínculo</span></label>
              </div>

              <div class="cp-field" data-manual-avos>
                <label for="d13-meses">Avos do 13º</label>
                <select id="d13-meses" name="meses">
                  <?php for($i=1;$i<=12;$i++): ?>
                    <option value="<?php echo $i; ?>" <?php echo $i===12 ? 'selected' : ''; ?>><?php echo $i; ?>/12</option>
                  <?php endfor; ?>
                </select>
                <small>Use quando você já souber quantos avos são devidos.</small>
              </div>

              <div class="cp-field" data-auto-avos hidden>
                <label for="d13-inicio">Data de admissão</label>
                <input id="d13-inicio" type="date" name="data_inicio">
                <small>O mês conta quando houver pelo menos 15 dias de serviço no ano de referência.</small>
              </div>

              <div class="cp-field" data-auto-avos hidden>
                <label for="d13-fim">Fim do período considerado</label>
                <input id="d13-fim" type="date" name="data_fim">
                <small>Para o 13º anual completo, use a data final do período considerado no ano.</small>
              </div>

              <div class="cp-field">
                <label for="d13-quitacao">Data de quitação / referência fiscal</label>
                <input id="d13-quitacao" type="date" name="data_quitacao" value="<?php echo date('Y-m-d'); ?>">
                <small>Define a vigência das regras fiscais usadas na simulação.</small>
              </div>

              <div class="cp-field">
                <label for="d13-adiantamento">1ª parcela já paga (opcional)</label>
                <input id="d13-adiantamento" name="adiantamento_pago" inputmode="decimal" placeholder="Ex.: 2000,00">
                <small>Se você já recebeu a 1ª parcela, informe o valor real, mesmo que ele seja maior que o 13º proporcional apurado depois. A diferença será tratada como insuficiência para conferência, não como parcela negativa.</small>
              </div>

              <div class="cp-field">
                <label for="d13-salario-anterior">Salário do mês anterior ao adiantamento</label>
                <input id="d13-salario-anterior" name="salario_mes_anterior" inputmode="decimal" placeholder="Ex.: 4000,00">
                <small>No ramo padrão, a 1ª parcela usa metade do salário recebido no mês anterior ao adiantamento — não metade do 13º total estimado.</small>
              </div>

              <div class="cp-field">
                <label for="d13-data-adiantamento">Data do adiantamento</label>
                <input id="d13-data-adiantamento" type="date" name="data_adiantamento">
                <small>Necessária apenas para calcular automaticamente a 1ª parcela padrão.</small>
              </div>

              <div class="cp-field full">
                <label class="cp-check"><input type="checkbox" name="admissao_ano"> <span>Houve admissão no ano de referência</span></label>
                <small>Se marcado no modo manual, a ferramenta não inventa a 1ª parcela: informe o valor efetivamente pago.</small>
              </div>

              <div class="cp-field">
                <label for="d13-deps">Dependentes</label>
                <input id="d13-deps" name="dependentes" inputmode="numeric" placeholder="0">
                <small>Usado somente na apuração exclusiva de IRRF do 13º.</small>
              </div>

              <div class="cp-field">
                <label for="d13-pensao">Pensão alimentícia vinculada ao 13º</label>
                <input id="d13-pensao" name="pensao" inputmode="decimal" placeholder="0,00">
                <small>Informe apenas a parcela dedutível vinculada a esta apuração do 13º.</small>
              </div>

              <div class="cp-field full">
                <label class="cp-check">
                  <input type="checkbox" name="liquido" checked>
                  <span>Estimar segunda parcela líquida com INSS e IRRF</span>
                </label>
                <small>INSS e IRRF são apurados sobre o 13º em avaliações separadas da folha mensal.</small>
              </div>
            </div>

            <div class="cp-actions">
              <button class="cp-btn cp-btn--primary" type="submit">Calcular 13º salário</button>
              <a class="cp-btn cp-btn--ghost" href="/financas/calculadoras/ferias-clt/">Ir para férias CLT</a>
            </div>
          </form>
        </div>

        <aside class="cp-card cp-card--result cp-result" data-result>
          <div class="cp-card__header">
            <h3>Resultado da calculadora de décimo terceiro salário</h3>
            <p>A memória abaixo separa avos, adiantamento, INSS, IRRF e eventual insuficiência, além de identificar a release fiscal usada.</p>
          </div>

          <div class="cp-kpis">
            <div class="cp-kpi"><strong>Base usada</strong><span data-kpi="base">—</span></div>
            <div class="cp-kpi"><strong>13º total</strong><span data-kpi="total13">—</span></div>
          </div>

          <div class="cp-rows">
            <div class="cp-row"><span>Avos considerados</span><strong data-row="avos">—</strong></div>
            <div class="cp-row"><span>1ª parcela</span><strong data-row="primeira">—</strong></div>
            <div class="cp-row"><span>Origem da 1ª parcela</span><strong data-row="status-adiantamento">—</strong></div>
            <div class="cp-row"><span>2ª parcela bruta a pagar</span><strong data-row="segunda-bruta">—</strong></div>
            <div class="cp-row"><span>INSS do 13º — apuração separada</span><strong class="cp-neg" data-row="inss13">—</strong></div>
            <div class="cp-row"><span>IRRF do 13º — apuração exclusiva</span><strong class="cp-neg" data-row="ir13">—</strong></div>
            <div class="cp-row"><span>2ª parcela líquida a pagar</span><strong class="cp-pos" data-row="segunda-liquida">—</strong></div>
            <div class="cp-row"><span>Situação da liquidação</span><strong data-row="status-liquidacao">—</strong></div>
            <div class="cp-row"><span>Saldo matemático antes do piso zero</span><strong data-row="saldo-antes-piso">—</strong></div>
            <div class="cp-row"><span>Insuficiência a conferir</span><strong class="cp-neg" data-row="insuficiencia">—</strong></div>
            <div class="cp-row"><span>Base do IRRF do 13º</span><strong data-row="base-ir">—</strong></div>
            <div class="cp-row"><span>IRRF antes do redutor</span><strong data-row="ir-antes-reducao">—</strong></div>
            <div class="cp-row"><span>Rendimento testado no redutor</span><strong data-row="renda-redutor">—</strong></div>
            <div class="cp-row"><span>Modo de dedução do IR</span><strong data-row="modo">—</strong></div>
            <div class="cp-row"><span>Redução aplicada</span><strong class="cp-pos" data-row="reducao">—</strong></div>
            <div class="cp-row"><span>Ano de referência</span><strong data-row="ano-fiscal">—</strong></div>
            <div class="cp-row"><span>Data fiscal usada</span><strong data-row="referencia">—</strong></div>
            <div class="cp-row"><span>Release fiscal usada</span><strong data-row="release-id">—</strong></div>
          </div>

          <p class="cp-disclaimer">
            Estimativa educativa para conferência. Quando o saldo matemático ficar negativo, a ferramenta mostra a 2ª parcela a pagar como R$ 0,00 e apresenta a insuficiência separadamente. Isso não determina por si só como eventual diferença será compensada ou tratada na folha. Casos de admissão no ano ou remuneração variável podem exigir que você informe a 1ª parcela efetivamente paga. A ferramenta falha fechado quando o contrato fiscal publicado não cobre o cenário automaticamente.
          </p>
        </aside>
      </div>
    </div>
  </section>

