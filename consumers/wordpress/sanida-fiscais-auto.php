<?php
/**
 * Plugin Name: Sanida - Fiscais (Git Source + Calculadoras)
 * Description: Release fiscal canônica v1.2, referências financeiras atuais/históricas, cache/REST auditável e pontes para calculadoras.
 * Version:     2.8.0
 * Author:      Sanida
 */

if (!defined('ABSPATH')) exit;

require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-fiscal-network.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-fiscal-contract.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-taxas.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-shortcodes-core.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-calc13.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-salary-calc.php';
require_once plugin_dir_path(__FILE__) . 'includes/trait-sanida-admin-debug.php';

final class Sanida_Fiscais_Git {

  const VERSION = '2.8.0';

  const CURRENT_JSON_URL_DEFAULT = 'https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/releases/fiscal-v1/current.json';
  const RELEASE_BASE_URL_DEFAULT = 'https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/releases/fiscal-v1/';
  const TAXAS_JSON_URL_DEFAULT   = 'https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/taxas_bacen.json';
  const TAXAS_SERIES_JSON_URL_DEFAULT = 'https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/taxas_bacen_series.json';

  const CONTRACT_ID                    = 'br.sanida.fiscal';
  const SUPPORTED_SCHEMA_VERSION       = '1.2.0';
  const SUPPORTED_CONTRACT_API_VERSION = '1.2.0';
  const SUPPORTED_MANIFEST_VERSION     = '1.0.0';

  const T_CACHE              = 'sfa_fiscal_v12_cache';
  const OPT_LAST_GOOD        = 'sfa_fiscal_v12_last_good';
  const OPT_CURRENT_ETAG     = 'sfa_fiscal_v12_current_etag';
  const OPT_KNOWN_SUCCESSOR  = 'sfa_fiscal_v12_known_successor';

  const T_TAXAS_CACHE       = 'sfa_git_v2_taxas_cache';
  const OPT_TAXAS_LAST_GOOD = 'sfa_git_v2_taxas_last_good';
  const OPT_TAXAS_ETAG      = 'sfa_git_v2_taxas_etag';

  const T_TAXAS_SERIES_CACHE       = 'sfa_git_v2_taxas_series_cache';
  const OPT_TAXAS_SERIES_LAST_GOOD = 'sfa_git_v2_taxas_series_last_good';
  const OPT_TAXAS_SERIES_ETAG      = 'sfa_git_v2_taxas_series_etag';

  const TTL_SUCCESS   = 12 * HOUR_IN_SECONDS;
  const TTL_FAIL      = 15 * MINUTE_IN_SECONDS;

  const TTL_TAXAS_SUCCESS = 10 * MINUTE_IN_SECONDS;
  const TTL_TAXAS_FAIL    = 5 * MINUTE_IN_SECONDS;
  const TTL_TAXAS_SERIES_SUCCESS = 6 * HOUR_IN_SECONDS;
  const TTL_TAXAS_SERIES_FAIL    = 15 * MINUTE_IN_SECONDS;

  const CDI_MAX_OBSERVATION_AGE_DAYS = 7;
  const TAXAS_SERIES_SCHEMA_VERSION = '1.0.0';
  const TAXAS_SERIES_MONTHS = 120;

  use Sanida_Fiscais_Fiscal_Network_Trait;
  use Sanida_Fiscais_Fiscal_Contract_Trait;
  use Sanida_Fiscais_Taxas_Trait;
  use Sanida_Fiscais_Shortcodes_Core_Trait;
  use Sanida_Fiscais_Calc13_Trait;
  use Sanida_Fiscais_Salary_Calc_Trait;
  use Sanida_Fiscais_Admin_Debug_Trait;

  public function __construct(){
    $this->register_shortcodes();

    add_action('init', function(){
      $this->register_shortcodes(true);
    }, 10050);

    add_action('admin_bar_menu', [$this, 'admin_bar_clear'], 100);
    add_action('admin_init',     [$this, 'clear_cache']);
    add_action('rest_api_init',  [$this, 'register_rest_routes']);
    add_action('rest_api_init',  [$this, 'register_fiscal_health_route']);
    add_action('rest_api_init',  [$this, 'register_financial_series_route']);
  }

}

new Sanida_Fiscais_Git();