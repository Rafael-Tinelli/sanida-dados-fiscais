<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Salary_Calc_Trait {

  public function sc_calc($atts=array()){
    $atts = shortcode_atts([
      'url_13'       => '',
      'url_ferias'   => '',
      'url_rescisao' => '',
    ], $atts, 'calc_salario_liquido');

    $d = $this->get_data();
    if ($this->fiscais_shortcodes_blocked($d)) {
      return $this->fiscais_unavailable_text('calc');
    }

    $safe_inss = is_array($d['inss'] ?? null) ? $d['inss'] : [];
    $safe_irrf = is_array($d['irrf']['tabela'] ?? null) ? $d['irrf']['tabela'] : [];
    $safe_simp = isset($d['irrf']['simplificado']) ? (float)$d['irrf']['simplificado'] : 0.0;
    $safe_dep  = isset($d['dep']) ? (float)$d['dep'] : 0.0;
    $safe_red  = isset($d['irrf']['reducao_mensal']) && is_array($d['irrf']['reducao_mensal']) ? $d['irrf']['reducao_mensal'] : null;

    list($u13, $uf, $ur) = $this->get_satellite_urls($atts);

    $js_data = wp_json_encode([
      'inss' => $safe_inss,
      'irrf' => [
        'tabela' => $safe_irrf,
        'simplificado' => $safe_simp,
        'reducao_mensal' => $safe_red
      ],
      'dep'  => $safe_dep
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT);

    $links_json = wp_json_encode(
      ['u13'=>$u13,'uf'=>$uf,'ur'=>$ur,'title'=> (defined('SFA_CTA_TITLE') ? (string) SFA_CTA_TITLE : 'Próximas simulações')],
      JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT
    );

    $id = 'sfa-calc-'.uniqid();

    ob_start(); ?>
    <style>
      .sfa-box{border:1px solid #e5e7eb;border-radius:12px;padding:20px;background:#f9fafb;max-width:760px;margin:20px auto;font-family:system-ui,-apple-system,sans-serif}
      .sfa-box *{box-sizing:border-box}
      .sfa-box h3{margin:0 0 15px;text-align:center;color:#111827;font-size:1.25rem}
      .sfa-grid{display:grid;gap:15px;grid-template-columns:1fr 1fr}
      .sfa-grid label{font-size:0.9em;font-weight:600;color:#374151;display:block;margin-bottom:6px}
      .sfa-grid input{width:100%;padding:12px;border:1px solid #d1d5db;border-radius:8px;background:#fff;font-size:1rem}
      .sfa-btn{margin-top:20px;width:100%;padding:14px;border-radius:8px;background:#22c55e;color:#fff;border:0;font-weight:700;cursor:pointer;font-size:1.1rem;transition:filter .2s}
      .sfa-btn:hover{filter:brightness(.95)}
      .sfa-res{margin-top:20px;border-top:1px dashed #cbd5e1;padding-top:15px;font-size:0.95rem;display:none}
      .sfa-res.active{display:block}
      .sfa-row{display:flex;justify-content:space-between;margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid #e5e7eb}
      .sfa-row.total{border-top:2px solid #e5e7eb;border-bottom:0;padding-top:12px;margin-bottom:0;margin-top:10px;align-items:center}
      .sfa-val{font-weight:bold;color:#1f2937}
      .sfa-neg{color:#dc2626}
      .sfa-good{color:#16a34a}
      .sfa-liq{color:#16a34a;font-size:1.5rem}

      .sfa-links{margin-top:14px;padding-top:12px;border-top:1px dashed #d1d5db}
      .sfa-links__title{font-size:.9rem;color:#374151;font-weight:900;margin:0 0 10px}
      .sfa-links__grid{display:grid;gap:10px;grid-template-columns:repeat(3,minmax(0,1fr))}
      .sfa-linkbtn{
        width:100%;min-height:56px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;
        padding:12px 10px;border-radius:12px;background:#111827;color:#fff;text-decoration:none;font-weight:900;font-size:.9rem;
        text-align:center;white-space:normal;line-height:1.1;transition:transform .15s ease, filter .15s ease;
      }
      .sfa-linkbtn:hover,
      .sfa-linkbtn:focus,
      .sfa-linkbtn:focus-visible{filter:brightness(.95);transform:translateY(-1px);color:#fff;text-decoration:none;}
      .sfa-linkbtn:visited{color:#fff}
      .sfa-linkbtn small{font-weight:800;font-size:.78rem;opacity:.9}
      .sfa-linkbtn--ghost{background:#e5e7eb;color:#111827}
      .sfa-linkbtn--ghost:visited{color:#111827}
      .sfa-linkbtn--ghost:hover,
      .sfa-linkbtn--ghost:focus,
      .sfa-linkbtn--ghost:focus-visible{background:#d1d5db;color:#111827;text-decoration:none;}

      @media (max-width:600px){
        .sfa-grid{grid-template-columns:1fr}
        .sfa-links__grid{grid-template-columns:1fr}
      }
    </style>

    <div class="sfa-box" id="<?php echo esc_attr($id); ?>">
      <h3>Calculadora de Salário Líquido (<?php echo esc_html((string)($d['ano'] ?? gmdate('Y'))); ?>)</h3>

      <div class="sfa-grid">
        <div>
          <label>Salário Bruto (R$)</label>
          <input type="text" inputmode="decimal" class="calc-bruto" placeholder="Ex: 3500,00">
        </div>
        <div>
          <label>Nº Dependentes</label>
          <input type="number" class="calc-dep" value="0" min="0">
        </div>
        <div>
          <label>Outros Descontos (R$)</label>
          <input type="text" inputmode="decimal" class="calc-outros" value="0" placeholder="0,00">
        </div>
      </div>

      <button type="button" class="sfa-btn" onclick="SFA_V2_Calc('<?php echo esc_js($id); ?>')">Calcular Líquido</button>
      <div class="sfa-res"></div>
    </div>

    <script>
      window['D_<?php echo esc_js($id); ?>'] = <?php echo $js_data; ?>;
      window['L_<?php echo esc_js($id); ?>'] = <?php echo $links_json; ?>;

      if (typeof SFA_V2_Calc === 'undefined') {
        window.SFA_V2_Calc = function(uid){
          try {
            var root = document.getElementById(uid);
            var dados = window['D_'+uid];
            var links = window['L_'+uid] || {};

            if(!dados || !dados.inss || dados.inss.length === 0){
              alert("Dados não carregados. Recarregue a página.");
              return;
            }

            var parseBR = function(v){
              if(!v) return 0;
              if(typeof v === 'number') return v;
              v = v.toString().replace('R$','').trim();
              if(v.indexOf('.') !== -1 && v.indexOf(',') !== -1){
                v = v.replace(/\./g, '').replace(',', '.');
              } else if(v.indexOf(',') !== -1){
                v = v.replace(',', '.');
              }
              return parseFloat(v) || 0;
            };

            var bruto  = parseBR(root.querySelector('.calc-bruto').value);
            var deps   = parseFloat(root.querySelector('.calc-dep').value) || 0;
            var outros = parseBR(root.querySelector('.calc-outros').value);

            if(bruto <= 0){ alert('Informe um salário bruto válido.'); return; }

            var inss = 0;
            var prev_lim = 0;
            for(var i=0; i < dados.inss.length; i++){
              var f = dados.inss[i];
              var base_faixa = Math.min(bruto, f.limite) - prev_lim;
              if(base_faixa > 0){ inss += base_faixa * f.aliquota; }
              prev_lim = f.limite;
            }

            var calcular_ir = function(base, tabela){
              if(base <= 0) return 0;
              for(var i=0; i < tabela.length; i++){
                var f = tabela[i];
                if(base <= f.limite){
                  return Math.max(0, (base * f.aliquota) - f.deducao);
                }
              }
              var ult = tabela[tabela.length-1];
              return Math.max(0, (base * ult.aliquota) - ult.deducao);
            };

            var aplicar_reducao = function(base, ir){
              var r = dados.irrf && dados.irrf.reducao_mensal ? dados.irrf.reducao_mensal : null;
              if(!r) return { ir: ir, reducao_aplicada: 0 };

              if(r.isenta_ate && base <= r.isenta_ate){
                return { ir: 0, reducao_aplicada: Math.max(0, ir) };
              }

              if(r.reduz_ate && r.isenta_ate && base > r.isenta_ate && base <= r.reduz_ate && typeof r.a === 'number' && typeof r.b === 'number'){
                var red = Math.max(0, (r.a - (r.b * base)));
                var aplicado = Math.min(Math.max(0, ir), red);
                return { ir: Math.max(0, ir - aplicado), reducao_aplicada: aplicado };
              }

              return { ir: ir, reducao_aplicada: 0 };
            };

            var base_legal = bruto - inss - (deps * dados.dep);
            var base_simp  = bruto - dados.irrf.simplificado;

            var ir_legal_pre = calcular_ir(base_legal, dados.irrf.tabela);
            var ir_simp_pre  = calcular_ir(base_simp,  dados.irrf.tabela);

            var legal = aplicar_reducao(base_legal, ir_legal_pre);
            var simp  = aplicar_reducao(base_simp,  ir_simp_pre);

            var irrf = (simp.ir < legal.ir) ? simp.ir : legal.ir;
            var reducao_escolhida = (simp.ir < legal.ir) ? simp.reducao_aplicada : legal.reducao_aplicada;

            var total_desc = inss + irrf + outros;
            var liquido = bruto - total_desc;

            var fmt = new Intl.NumberFormat('pt-BR', {style:'currency', currency:'BRL'});

            var html =
              '<div class="sfa-row"><span>Salário Bruto</span><span class="sfa-val">' + fmt.format(bruto) + '</span></div>' +
              '<div class="sfa-row"><span>INSS</span><span class="sfa-val sfa-neg">- ' + fmt.format(inss) + '</span></div>' +
              '<div class="sfa-row"><span>IRRF</span><span class="sfa-val sfa-neg">- ' + fmt.format(irrf) + '</span></div>';

            if(reducao_escolhida && reducao_escolhida > 0.005){
              html += '<div class="sfa-row"><span>Redução aplicada</span><span class="sfa-val sfa-good">' + fmt.format(reducao_escolhida) + '</span></div>';
            }

            if(outros > 0){
              html += '<div class="sfa-row"><span>Outros Descontos</span><span class="sfa-val sfa-neg">- ' + fmt.format(outros) + '</span></div>';
            }

            html +=
              '<div class="sfa-row total"><span>Salário Líquido</span><span class="sfa-val sfa-liq">' + fmt.format(liquido) + '</span></div>';

            var hasAny = (links.u13 || links.uf || links.ur);
            if(hasAny){
              var ctaTitle = (links.title || 'Próximas simulações');
              html += '<div class="sfa-links">' +
                '<div class="sfa-links__title">'+ctaTitle+'</div>' +
                '<div class="sfa-links__grid">' +
                  (links.u13 ? ('<a class="sfa-linkbtn" href="'+links.u13+'" target="_blank" rel="noopener noreferrer"><span>13º salário</span><small>estimativa</small></a>') : '') +
                  (links.uf  ? ('<a class="sfa-linkbtn sfa-linkbtn--ghost" href="'+links.uf+'" target="_blank" rel="noopener noreferrer"><span>Férias + 1/3</span><small>abono</small></a>') : '') +
                  (links.ur  ? ('<a class="sfa-linkbtn sfa-linkbtn--ghost" href="'+links.ur+'" target="_blank" rel="noopener noreferrer"><span>Rescisão CLT</span><small>cálculo</small></a>') : '') +
                '</div>' +
              '</div>';
            }

            var resBox = root.querySelector('.sfa-res');
            resBox.innerHTML = html;
            resBox.classList.add('active');

          } catch(e) {
            console.error(e);
            alert("Ocorreu um erro ao calcular. Verifique os valores digitados.");
          }
        }
      }

      var inputs = document.getElementById('<?php echo esc_js($id); ?>').querySelectorAll('input');
      for(var i=0; i<inputs.length; i++){
        inputs[i].addEventListener('keypress', function(e){
          if(e.key === 'Enter') SFA_V2_Calc('<?php echo esc_js($id); ?>');
        });
      }
    </script>
    <?php
    return ob_get_clean();
  }

}
