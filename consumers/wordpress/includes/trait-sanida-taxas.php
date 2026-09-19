<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Taxas_Trait {

  private function taxas_json_url(){
    return defined('SFA_FISCAIS_TAXAS_JSON_URL')
      ? (string) SFA_FISCAIS_TAXAS_JSON_URL
      : self::TAXAS_JSON_URL_DEFAULT;
  }

  private function valid_taxas_iso_date($value){
    if (!is_string($value) || !preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $value, $m)) return false;
    return checkdate((int)$m[2], (int)$m[3], (int)$m[1]);
  }

  private function cdi_observation_is_fresh($source, $as_of = null){
    if (!is_array($source)) return false;
    if (($source['source_id'] ?? null) !== 'BCB_CDI_DAILY_SGS_12') return false;
    $observation_date = $source['source_observation_date'] ?? null;
    if (!$this->valid_taxas_iso_date($observation_date)) return false;

    $date = $as_of === null ? current_time('Y-m-d') : $as_of;
    if (!$this->valid_taxas_iso_date($date)) return false;
    if ($observation_date > $date) return false;

    $obs_ts = strtotime($observation_date.' 00:00:00 UTC');
    $date_ts = strtotime($date.' 00:00:00 UTC');
    if ($obs_ts === false || $date_ts === false) return false;
    $age_days = (int) floor(($date_ts - $obs_ts) / DAY_IN_SECONDS);

    $declared_max = $source['max_observation_age_calendar_days'] ?? null;
    if (!is_numeric($declared_max)) return false;
    $declared_max = (int)$declared_max;
    if ($declared_max < 0 || $declared_max > self::CDI_MAX_OBSERVATION_AGE_DAYS) return false;

    return $age_days <= $declared_max;
  }

  private function validate_taxas_payload($j){
    if (!is_array($j)) return false;
    if (($j['schema_version'] ?? null) !== '1.4.0') return false;
    if (!isset($j['taxas']) || !is_array($j['taxas'])) return false;
    if (!isset($j['taxas']['selic'], $j['taxas']['cdi'])) return false;
    if (!is_numeric($j['taxas']['selic']) || !is_numeric($j['taxas']['cdi'])) return false;
    if (($j['taxas']['cdi_basis'] ?? null) !== 'bcb_sgs_12_daily_compounded_252') return false;

    $sources = $j['meta']['sources'] ?? null;
    if (!is_array($sources)) return false;
    if (($sources['selic']['source_id'] ?? null) !== 'BCB_SELIC_META_SGS_432') return false;
    if (!$this->cdi_observation_is_fresh($sources['cdi'] ?? null)) return false;

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
        'errors' => ['taxas_unavailable_fail_closed'],
        'warnings' => [],
      ],
      'taxas' => [
        'selic' => null,
        'cdi'   => null,
        'cdi_basis' => 'unavailable',
      ],
      '_runtime' => [
        'origin' => 'unavailable_fail_closed',
        'unavailable' => true,
        'checked_at_utc' => gmdate('c'),
      ],
    ];
  }

  private function get_taxas_data(){
    $d = get_transient(self::T_TAXAS_CACHE);
    if (is_array($d) && !empty($d['_runtime']['unavailable'])) return $d;
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
    set_transient(self::T_TAXAS_CACHE, $min, self::TTL_TAXAS_FAIL);
    return $min;
  }

}
