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
      'taxas_historicas_json' => 'sc_taxas_historicas_json',
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

  /*
   * C7.1: os shortcodes informativos leem diretamente a release canônica já
   * validada pelo plugin. Não há shape intermediário/adaptador de compatibilidade.
   */
  private function canonical_rules_for_shortcodes(){
    $package = $this->get_release_package();
    if (!$this->validate_release_package($package)) return null;
    $release = $package['release'] ?? null;
    if (!is_array($release)) return null;
    return $this->index_rules($release);
  }

  public function sc_ano(){
    $rules = $this->canonical_rules_for_shortcodes();
    if (!is_array($rules)) return $this->fiscais_unavailable_text('ano');

    $rule = $rules['irrf.monthly.progressive_table'] ?? null;
    $effective_from = is_array($rule) ? (string)($rule['vigency']['effective_from'] ?? '') : '';
    if (!preg_match('/^(\d{4})-/', $effective_from, $m)) return $this->fiscais_unavailable_text('ano');
    return esc_html($m[1]);
  }

  public function sc_selic(){
    $d = $this->get_taxas_data();
    if (!isset($d['taxas']['selic']) || !is_numeric($d['taxas']['selic'])) return $this->fiscais_unavailable_text('taxa');
    $v = (float)$d['taxas']['selic'];
    return esc_html(number_format($v, 2, ',', '.')) . '% a.a.';
  }

  public function sc_cdi(){
    $d = $this->get_taxas_data();
    if (!isset($d['taxas']['cdi']) || !is_numeric($d['taxas']['cdi'])) return $this->fiscais_unavailable_text('taxa');
    $v = (float)$d['taxas']['cdi'];
    return esc_html(number_format($v, 2, ',', '.')) . '% a.a.';
  }

  public function sc_taxas_historicas_json($atts = []){
    $atts = shortcode_atts([
      'meses' => '60',
      'incluir_mes_corrente' => '1',
    ], $atts, 'taxas_historicas_json');

    $months = max(1, min(self::TAXAS_SERIES_MONTHS, (int)$atts['meses']));
    $include = !in_array(
      strtolower(trim((string)$atts['incluir_mes_corrente'])),
      ['0', 'false', 'nao', 'não', 'no'],
      true
    );
    $view = $this->taxas_series_view($months, $include);
    $json = wp_json_encode(
      $view,
      JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
      | JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT
    );
    return is_string($json)
      ? $json
      : '{"schema_version":"1.0.0","available":false,"points":[]}';
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
    $rules = $this->canonical_rules_for_shortcodes();
    if (!is_array($rules)) return $this->fiscais_unavailable_text('inss');

    $rule = $rules['inss.employee.progressive_table'] ?? null;
    $brackets = is_array($rule) ? ($rule['payload']['brackets'] ?? null) : null;
    if (!is_array($brackets) || !$brackets) return $this->fiscais_unavailable_text('inss');

    foreach($brackets as $b){
      if (!is_array($b) || !array_key_exists('upper_bound', $b) || !isset($b['rate'])) {
        return $this->fiscais_unavailable_text('inss');
      }
    }

    ob_start(); ?>
      <div class="sfa-tablewrap" style="overflow-x:auto">
        <table class="sfa-table sfa-table--inss" role="table">
          <thead><tr><th>Salário (R$)</th><th>Alíquota</th></tr></thead>
          <tbody>
            <?php foreach($brackets as $b):
              $lim = $b['upper_bound'];
              $ali = (float)$b['rate'];
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
    $rules = $this->canonical_rules_for_shortcodes();
    if (!is_array($rules)) return $this->fiscais_unavailable_text('irrf');

    $table_rule = $rules['irrf.monthly.progressive_table'] ?? null;
    $simplified_rule = $rules['irrf.simplified_monthly_discount'] ?? null;
    $brackets = is_array($table_rule) ? ($table_rule['payload']['brackets'] ?? null) : null;
    $simplified_payload = is_array($simplified_rule) ? ($simplified_rule['payload'] ?? null) : null;
    if (!is_array($brackets) || !$brackets || !is_array($simplified_payload) || !array_key_exists('value', $simplified_payload)) {
      return $this->fiscais_unavailable_text('irrf');
    }

    foreach($brackets as $b){
      if (!is_array($b) || !array_key_exists('upper_bound', $b) || !isset($b['rate'], $b['deduction'])) {
        return $this->fiscais_unavailable_text('irrf');
      }
    }

    $simp = (float)$simplified_payload['value'];
    ob_start(); ?>
      <p class="sfa-tablemeta" style="margin:.35rem 0 .6rem 0;opacity:.9"><strong>Desconto simplificado:</strong> R$ <?php echo esc_html(number_format($simp,2,',','.')); ?></p>
      <div class="sfa-tablewrap" style="overflow-x:auto">
        <table class="sfa-table sfa-table--irrf" role="table">
          <thead><tr><th>Base (R$)</th><th>Alíquota</th><th>Dedução</th></tr></thead>
          <tbody>
            <?php foreach($brackets as $b):
              $lim = $b['upper_bound'];
              $ali = (float)$b['rate'];
              $ded = (float)$b['deduction'];
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
    if (!isset($t['selic'], $t['cdi']) || !is_numeric($t['selic']) || !is_numeric($t['cdi'])) {
      return $this->fiscais_unavailable_text('taxa');
    }
    $sel = (float)$t['selic'];
    $cdi = (float)$t['cdi'];
    return "<div style='border:1px solid #ddd;padding:15px;border-radius:8px;display:flex;gap:20px;background:#fff'>"
      ."<div><strong>Selic</strong><br><span style='color:#007cba;font-size:1.4em'>".esc_html(number_format($sel,2,',','.'))."%</span></div>"
      ."<div><strong>CDI</strong><br><span style='color:#007cba;font-size:1.4em'>".esc_html(number_format($cdi,2,',','.'))."%</span></div>"
      ."</div>";
  }
}
