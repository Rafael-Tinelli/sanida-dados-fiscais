<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Taxas_Trait {

  private function taxas_json_url(){
    return defined('SFA_FISCAIS_TAXAS_JSON_URL')
      ? (string) SFA_FISCAIS_TAXAS_JSON_URL
      : self::TAXAS_JSON_URL_DEFAULT;
  }

  private function validate_taxas_payload($j){
    if (!is_array($j)) return false;
    if (!isset($j['taxas']) || !is_array($j['taxas'])) return false;
    if (!isset($j['taxas']['selic'], $j['taxas']['cdi'])) return false;
    if (!is_numeric($j['taxas']['selic']) || !is_numeric($j['taxas']['cdi'])) return false;
    $sel = (float) $j['taxas']['selic'];
    $cdi = (float) $j['taxas']['cdi'];
    return $sel >= 0 && $sel <= 60 && $cdi >= 0 && $cdi <= 60;
  }

  private function minimal_taxas_fallback(){
    return [
      'schema_version' => '1.3.0',
      'meta' => [
        'generated_at_utc' => gmdate('c'),
        'sources' => [],
        'errors' => ['fallback_minimal_taxas'],
        'warnings' => [],
      ],
      'taxas' => [
        'selic' => 15.00,
        'cdi'   => 14.90,
        'cdi_basis' => 'fallback',
      ],
    ];
  }

  private function get_taxas_data(){
    $d = get_transient(self::T_TAXAS_CACHE);
    if ($this->validate_taxas_payload($d)) {
      if (!isset($d['_runtime']) || !is_array($d['_runtime'])) {
        $d['_runtime'] = [
          'origin' => 'cached_taxas',
          'checked_at_utc' => gmdate('c'),
        ];
        set_transient(self::T_TAXAS_CACHE, $d, self::TTL_TAXAS_SUCCESS);
      }
      return $d;
    }
    if ($d !== false) {
      delete_transient(self::T_TAXAS_CACHE);
    }

    $url = $this->taxas_json_url();

    $headers = [
      'Accept' => 'application/json, text/plain, */*',
      'User-Agent' => 'SanidaFiscais/'.self::VERSION.' (+https://sanida.com.br)'
    ];

    $etag = get_option(self::OPT_TAXAS_ETAG);
    if ($etag) $headers['If-None-Match'] = $etag;

    $r = wp_remote_get($url, [
      'timeout'     => 20,
      'redirection' => 3,
      'sslverify'   => $this->sslverify(),
      'headers'     => $headers,
    ]);

    if (!is_wp_error($r)) {
      $code = (int) wp_remote_retrieve_response_code($r);

      if ($code === 304) {
        $lg = get_option(self::OPT_TAXAS_LAST_GOOD);
        if ($this->validate_taxas_payload($lg)) {
          $lg['_runtime'] = [
            'origin' => 'remote_304_last_good',
            'http_code' => 304,
            'checked_at_utc' => gmdate('c'),
          ];
          set_transient(self::T_TAXAS_CACHE, $lg, self::TTL_TAXAS_SUCCESS);
          return $lg;
        }
      }

      if ($code === 200) {
        $body = (string) wp_remote_retrieve_body($r);
        $json = json_decode($body, true);

        if ($this->validate_taxas_payload($json)) {
          $new_etag = wp_remote_retrieve_header($r, 'etag');
          if ($new_etag) update_option(self::OPT_TAXAS_ETAG, $new_etag, false);

          update_option(self::OPT_TAXAS_LAST_GOOD, $json, false);

          $json['_runtime'] = [
            'origin' => 'remote_200',
            'http_code' => 200,
            'checked_at_utc' => gmdate('c'),
          ];

          set_transient(self::T_TAXAS_CACHE, $json, self::TTL_TAXAS_SUCCESS);
          return $json;
        }
      }
    }

    $lg = get_option(self::OPT_TAXAS_LAST_GOOD);
    if ($this->validate_taxas_payload($lg)) {
      $lg['_runtime'] = [
        'origin' => 'option_last_good',
        'checked_at_utc' => gmdate('c'),
      ];
      set_transient(self::T_TAXAS_CACHE, $lg, self::TTL_TAXAS_FAIL);
      return $lg;
    }

    $min = $this->minimal_taxas_fallback();
    $min['_runtime'] = [
      'origin' => 'minimal_fallback',
      'checked_at_utc' => gmdate('c'),
    ];
    set_transient(self::T_TAXAS_CACHE, $min, self::TTL_TAXAS_FAIL);
    return $min;
  }

}
