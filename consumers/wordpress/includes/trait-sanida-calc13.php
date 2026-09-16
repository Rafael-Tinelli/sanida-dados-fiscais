<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Calc13_Trait {

  public function sc_calc13_assets(){
    $url = esc_url(home_url('/financas/calculadoras/decimo-terceiro/'));
    return '<div class="sfa-legacy-calc-bridge" data-sfa-retired="calc13_assets">'
      . '<p><strong>Calculadora de 13º atualizada.</strong> O shortcode antigo não injeta mais fórmulas fiscais no navegador.</p>'
      . '<p><a href="'.$url.'">Abrir a calculadora de décimo terceiro</a></p>'
      . '</div>';
  }
}
