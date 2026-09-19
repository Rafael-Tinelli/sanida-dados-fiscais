<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Admin_Debug_Trait {

  public function admin_bar_clear($bar){
    if (!current_user_can('manage_options')) return;
    $url = wp_nonce_url(add_query_arg(['sfa_flush'=>'1']), 'sfa_flush');
    $bar->add_menu(['id'=>'sfa-flush','title'=>'SFA: Atualizar','href'=>$url]);
  }

  public function clear_cache(){
    if (!current_user_can('manage_options')) return;
    if (!isset($_GET['sfa_flush'])) return;
    if (!isset($_GET['_wpnonce']) || !wp_verify_nonce($_GET['_wpnonce'], 'sfa_flush')) return;

    delete_transient(self::T_CACHE);
    delete_option(self::OPT_CURRENT_ETAG);
    // C7.2: OPT_KNOWN_SUCCESSOR is a safety latch, not cache. Manual cache
    // refresh must not erase knowledge that a newer release was observed.
    delete_transient(self::T_TAXAS_CACHE);
    delete_option(self::OPT_TAXAS_ETAG);
    delete_transient(self::T_TAXAS_SERIES_CACHE);
    delete_option(self::OPT_TAXAS_SERIES_ETAG);

    wp_safe_redirect(remove_query_arg(['sfa_flush','_wpnonce']));
    exit;
  }

  public function register_fiscal_health_route(){
    register_rest_route('sfa/v1', '/fiscal-health', [
      'methods'  => 'GET',
      'callback' => [$this, 'rest_get_fiscal_health'],
      'permission_callback' => '__return_true',
    ]);
  }

  private function fiscal_health_snapshot($package = null){
    if ($package === null) $package = $this->get_release_package();

    $valid = $this->validate_release_package($package);
    $runtime = is_array($package['_runtime'] ?? null) ? $package['_runtime'] : [];
    $origin = isset($runtime['origin']) ? (string)$runtime['origin'] : 'unavailable';
    $known = $this->known_successor_state();
    $known_release_id = null;
    if (is_array($known) && empty($known['invalid']) && isset($known['release_id'])) {
      $known_release_id = (string)$known['release_id'];
    }

    if (!$valid) {
      $blocked = $known !== null || !empty($runtime['known_successor']);
      return [
        'status' => $blocked ? 'blocked_known_successor' : 'unavailable',
        'ready' => false,
        'release_id' => null,
        'origin' => $origin,
        'known_successor' => $blocked,
        'known_successor_release_id' => $known_release_id ?: ($runtime['known_successor_release_id'] ?? null),
        'contract_schema_version' => self::SUPPORTED_SCHEMA_VERSION,
        'contract_api_version' => self::SUPPORTED_CONTRACT_API_VERSION,
        'degradation_reason' => $blocked ? 'known_successor_not_yet_verified' : 'canonical_release_unavailable',
        'checked_at_utc' => $runtime['checked_at_utc'] ?? gmdate('c'),
      ];
    }

    $release = $package['release'];
    $cc = is_array($release['consumer_compatibility'] ?? null) ? $release['consumer_compatibility'] : [];
    $degraded = $origin === 'option_last_good';
    return [
      'status' => $degraded ? 'degraded_last_good' : 'healthy',
      'ready' => true,
      'release_id' => $release['release_id'] ?? null,
      'origin' => $origin,
      'known_successor' => false,
      'known_successor_release_id' => null,
      'contract_schema_version' => $release['schema_version'] ?? null,
      'contract_api_version' => $cc['contract_api_version'] ?? null,
      'degradation_reason' => $degraded ? 'canonical_source_unavailable_last_good_served' : null,
      'checked_at_utc' => $runtime['checked_at_utc'] ?? gmdate('c'),
    ];
  }

  public function rest_get_fiscal_health(WP_REST_Request $req){
    $health = $this->fiscal_health_snapshot();
    if (empty($health['ready'])) {
      return new WP_Error(
        'sfa_fiscal_health_unavailable',
        'Saúde fiscal indisponível para consumo.',
        [
          'status' => 503,
          'health' => $health,
        ]
      );
    }

    $res = rest_ensure_response($health);
    $res->header('Cache-Control', 'no-store');
    $res->header('X-Sanida-Fiscal-Status', (string)$health['status']);
    if (!empty($health['release_id'])) {
      $res->header('X-Sanida-Fiscal-Release', (string)$health['release_id']);
    }
    return $res;
  }

  public function sc_debug(){
    if (!current_user_can('manage_options')) return '';

    $package = $this->get_release_package();
    $release = is_array($package['release'] ?? null) ? $package['release'] : [];
    $cc = is_array($release['consumer_compatibility'] ?? null) ? $release['consumer_compatibility'] : [];
    $lifecycle = is_array($release['lifecycle'] ?? null) ? $release['lifecycle'] : [];
    $tx = $this->get_taxas_data();
    $series = $this->get_taxas_series_data();

    $out = [
      'plugin_version' => self::VERSION,
      'fiscais_current_url' => $this->current_json_url(),
      'fiscais_release_base_url' => $this->release_base_url(),
      'fiscais_cached_transient' => (bool) get_transient(self::T_CACHE),
      'fiscais_has_last_good' => (bool) get_option(self::OPT_LAST_GOOD),
      'fiscais_known_successor' => $this->known_successor_state(),
      'fiscais_health' => $this->fiscal_health_snapshot($package),
      'fiscais_release_valid' => $this->validate_release_package($package),
      'fiscais_origin' => $package['_runtime']['origin'] ?? null,
      'fiscais_release_id' => $release['release_id'] ?? null,
      'fiscais_contract_schema_version' => $release['schema_version'] ?? null,
      'fiscais_contract_api_version' => $cc['contract_api_version'] ?? null,
      'fiscais_approval_mode' => $lifecycle['approval_mode'] ?? null,
      'fiscais_runtime' => $package['_runtime'] ?? null,
      'presentation_shortcodes_mode' => 'direct_canonical_release',

      'taxas_json_url' => $this->taxas_json_url(),
      'taxas_cached_transient' => (bool) get_transient(self::T_TAXAS_CACHE),
      'taxas_has_last_good' => (bool) get_option(self::OPT_TAXAS_LAST_GOOD),
      'taxas_origin' => $tx['_runtime']['origin'] ?? null,
      'taxas' => $tx['taxas'] ?? null,
      'taxas_runtime' => $tx['_runtime'] ?? null,
      'taxas_meta' => $tx['meta'] ?? null,

      'taxas_series_json_url' => $this->taxas_series_json_url(),
      'taxas_series_cached_transient' => (bool) get_transient(self::T_TAXAS_SERIES_CACHE),
      'taxas_series_has_last_good' => (bool) get_option(self::OPT_TAXAS_SERIES_LAST_GOOD),
      'taxas_series_origin' => $series['_runtime']['origin'] ?? null,
      'taxas_series_available' => $this->validate_taxas_series_payload($series),
      'taxas_series_window' => $series['meta']['window'] ?? null,
      'taxas_series_latest' => !empty($series['points']) ? $series['points'][count($series['points']) - 1] : null,
      'taxas_series_runtime' => $series['_runtime'] ?? null,
    ];

    return '<pre style="white-space:pre-wrap">'.esc_html(print_r($out, true)).'</pre>';
  }
}
