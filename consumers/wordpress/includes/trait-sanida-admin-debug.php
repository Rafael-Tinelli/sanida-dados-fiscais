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
    delete_transient(self::T_TAXAS_CACHE);
    delete_option(self::OPT_TAXAS_ETAG);

    wp_safe_redirect(remove_query_arg(['sfa_flush','_wpnonce']));
    exit;
  }

  public function sc_debug(){
    if (!current_user_can('manage_options')) return '';

    $package = $this->get_release_package();
    $release = is_array($package['release'] ?? null) ? $package['release'] : [];
    $cc = is_array($release['consumer_compatibility'] ?? null) ? $release['consumer_compatibility'] : [];
    $lifecycle = is_array($release['lifecycle'] ?? null) ? $release['lifecycle'] : [];
    $tx = $this->get_taxas_data();

    $out = [
      'plugin_version' => self::VERSION,
      'fiscais_current_url' => $this->current_json_url(),
      'fiscais_release_base_url' => $this->release_base_url(),
      'fiscais_cached_transient' => (bool) get_transient(self::T_CACHE),
      'fiscais_has_last_good' => (bool) get_option(self::OPT_LAST_GOOD),
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
    ];

    return '<pre style="white-space:pre-wrap">'.esc_html(print_r($out, true)).'</pre>';
  }
}
