<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Shortcodes_Core_Trait {

  private function register_shortcodes($force = false){
    $map = [
      'ano_ref'              => 'sc_ano',
      'inss_tabela'          => 'sc_inss',
      'irrf_tabela'          => 'sc_irrf',
      'calc_salario_liquido' => 'sc_calc',
      'taxas_bacen'          => 'sc_bcb_box',
      'selic_atual'          => 'sc_selic',
      'cdi_atual'            => 'sc_cdi',
      'sfa_bootstrap_folha'  => 'sc_bootstrap_folha',
      'sanida_calculadora_13' => 'sc_calc13_assets',
      'sfa_calc13_assets'     => 'sc_calc13_assets',
      'fiscais_debug'        => 'sc_debug',
    ];

    foreach($map as $tag => $fn){
      if ($force) remove_shortcode($tag);
      add_shortcode($tag, [$this, $fn]);
    }
  }

  private function get_satellite_urls($atts = []){
    $u13 = $atts['url_13'] ?? '';
    $uf  = $atts['url_ferias'] ?? '';
    $ur  = $atts['url_rescisao'] ?? '';
    if ($u13 || $uf || $ur) {
      return [$u13 ? esc_url($u13) : '', $uf ? esc_url($uf) : '', $ur ? esc_url($ur) : ''];
    }
    return [
      esc_url(home_url('/financas/calculadoras/decimo-terceiro/')),
      esc_url(home_url('/financas/calculadoras/ferias-clt/')),
      esc_url(home_url('/financas/calculadoras/rescisao-clt/')),
    ];
  }

  public function sc_ano(){
    $d = $this->get_shortcode_display_data();
    if ($this->shortcode_display_blocked($d)) return $this->fiscais_unavailable_text('ano');
    return esc_html((string)($d['ano'] ?? ''));
  }

  public function sc_selic(){
    $d = $this->get_taxas_data();
    $v = isset($d['taxas']['selic']) ? (float)$d['taxas']['selic'] : 0;
    return esc_html(number_format($v, 2, ',', '.')) . '% a.a.';
  }

  public function sc_cdi(){
    $d = $this->get_taxas_data();
    $v = isset($d['taxas']['cdi']) ? (float)$d['taxas']['cdi'] : 0;
    return esc_html(number_format($v, 2, ',', '.')) . '% a.a.';
  }

  public function sc_bootstrap_folha($atts = []){
    $atts = shortcode_atts(['id' => 'sfa-folha-retired'], $atts, 'sfa_bootstrap_folha');
    $id = preg_replace('/[^A-Za-z0-9\-_:.]/', '', (string)$atts['id']);
    if ($id === '') $id = 'sfa-folha-retired';
    $payload = [
      'status' => 'retired',
      'retired_at' => 'C6.7',
      'replacement' => '/wp-json/sfa/v1/fiscal-release',
      'message' => 'Bootstrap fiscal legado removido; nenhuma regra fiscal é publicada por este shortcode.',
    ];
    $json = wp_json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT);
    return '<script type="application/json" id="'.esc_attr($id).'" data-sfa-retired="folha">'.esc_html($json).'</script>';
  }

  public function sc_inss(){
    $d = $this->get_shortcode_display_data();
    if ($this->shortcode_display_blocked($d)) return $this->fiscais_unavailable_text('inss');
    $inss = is_array($d['inss'] ?? null) ? $d['inss'] : [];
    ob_start(); ?>
      <div class="sfa-tablewrap" style="overflow-x:auto">
        <table class="sfa-table sfa-table--inss" role="table">
          <thead><tr><th>Salário (R$)</th><th>Alíquota</th></tr></thead>
          <tbody>
            <?php foreach($inss as $f):
              $lim = array_key_exists('limite', $f) ? $f['limite'] : null;
              $ali = (float)($f['aliquota'] ?? 0);
              $label = ($lim === null) ? 'Sem limite superior' : ('Até ' . number_format((float)$lim,2,',','.'));
            ?>
              <tr><td><?php echo esc_html($label); ?></td><td><?php echo esc_html(number_format($ali*100,1,',','.')) . '%'; ?></td></tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    <?php return ob_get_clean();
  }

  public function sc_irrf(){
    $d = $this->get_shortcode_display_data();
    if ($this->shortcode_display_blocked($d)) return $this->fiscais_unavailable_text('irrf');
    $tab  = is_array($d['irrf']['tabela'] ?? null) ? $d['irrf']['tabela'] : [];
    $simp = (float)($d['irrf']['simplificado'] ?? 0);
    ob_start(); ?>
      <p class="sfa-tablemeta" style="margin:.35rem 0 .6rem 0;opacity:.9"><strong>Desconto simplificado:</strong> R$ <?php echo esc_html(number_format($simp,2,',','.')); ?></p>
      <div class="sfa-tablewrap" style="overflow-x:auto">
        <table class="sfa-table sfa-table--irrf" role="table">
          <thead><tr><th>Base (R$)</th><th>Alíquota</th><th>Dedução</th></tr></thead>
          <tbody>
            <?php foreach($tab as $f):
              $lim = array_key_exists('limite', $f) ? $f['limite'] : null;
              $ali = (float)($f['aliquota'] ?? 0);
              $ded = (float)($f['deducao'] ?? 0);
              $label = ($lim === null) ? 'Acima da faixa anterior' : ('Até ' . number_format((float)$lim,2,',','.'));
            ?>
              <tr><td><?php echo esc_html($label); ?></td><td><?php echo esc_html(number_format($ali*100,1,',','.')) . '%'; ?></td><td><?php echo esc_html(number_format($ded,2,',','.')); ?></td></tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    <?php return ob_get_clean();
  }

  public function sc_bcb_box(){
    $d = $this->get_taxas_data();
    $t = $d['taxas'] ?? [];
    $sel = (float)($t['selic'] ?? 0);
    $cdi = (float)($t['cdi'] ?? 0);
    return "<div style='border:1px solid #ddd;padding:15px;border-radius:8px;display:flex;gap:20px;background:#fff'>"
      ."<div><strong>Selic</strong><br><span style='color:#007cba;font-size:1.4em'>".esc_html(number_format($sel,2,',','.'))."%</span></div>"
      ."<div><strong>CDI</strong><br><span style='color:#007cba;font-size:1.4em'>".esc_html(number_format($cdi,2,',','.'))."%</span></div>"
      ."</div>";
  }
}
