<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Salary_Calc_Trait {

  public function sc_calc($atts=array()){
    $atts = shortcode_atts([
      'url' => '',
    ], $atts, 'calc_salario_liquido');

    $url = trim((string)($atts['url'] ?? ''));
    if ($url === '') $url = home_url('/financas/calculadoras/salario-liquido-clt/');
    $url = esc_url($url);

    return '<div class="sfa-legacy-calc-bridge" data-sfa-retired="calc_salario_liquido">'
      . '<p><strong>Calculadora de salário líquido atualizada.</strong> Este shortcode antigo não executa mais fórmulas fiscais.</p>'
      . '<p><a href="'.$url.'">Abrir a calculadora de salário líquido CLT</a></p>'
      . '</div>';
  }
}
