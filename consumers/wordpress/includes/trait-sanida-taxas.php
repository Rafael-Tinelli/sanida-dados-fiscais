<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Taxas_Trait {

  private function taxas_json_url(){
    return defined('SFA_FISCAIS_TAXAS_JSON_URL')
      ? (string) SFA_FISCAIS_TAXAS_JSON_URL
      : self::TAXAS_JSON_URL_DEFAULT;
  }

  private function taxas_series_json_url(){
    return defined('SFA_FISCAIS_TAXAS_SERIES_JSON_URL')
      ? (string) SFA_FISCAIS_TAXAS_SERIES_JSON_URL
      : self::TAXAS_SERIES_JSON_URL_DEFAULT;
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

    $date = $as_of === null
      ? (function_exists('current_time') ? current_time('Y-m-d') : gmdate('Y-m-d'))
      : $as_of;
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

  private function valid_taxas_month($value){
    if (!is_string($value) || !preg_match('/^(\d{4})-(\d{2})$/', $value, $m)) return false;
    return checkdate((int)$m[2], 1, (int)$m[1]);
  }

  private function taxas_series_today(){
    $date = function_exists('current_time') ? current_time('Y-m-d') : gmdate('Y-m-d');
    return $this->valid_taxas_iso_date($date) ? $date : gmdate('Y-m-d');
  }

  private function taxas_series_cdi_is_fresh($observation_date, $as_of = null){
    if (!$this->valid_taxas_iso_date($observation_date)) return false;
    $date = $as_of === null ? $this->taxas_series_today() : $as_of;
    if (!$this->valid_taxas_iso_date($date)) return false;
    if ($observation_date > $date) return false;

    $obs_ts = strtotime($observation_date.' 00:00:00 UTC');
    $date_ts = strtotime($date.' 00:00:00 UTC');
    if ($obs_ts === false || $date_ts === false) return false;
    $age_days = (int) floor(($date_ts - $obs_ts) / DAY_IN_SECONDS);
    return $age_days >= 0 && $age_days <= self::CDI_MAX_OBSERVATION_AGE_DAYS;
  }

  private function validate_taxas_series_payload($j, $as_of = null){
    if (!is_array($j)) return false;
    if (($j['schema_version'] ?? null) !== self::TAXAS_SERIES_SCHEMA_VERSION) return false;

    $meta = $j['meta'] ?? null;
    if (!is_array($meta)) return false;
    if (($meta['timezone'] ?? null) !== 'America/Sao_Paulo') return false;

    $window = $meta['window'] ?? null;
    if (!is_array($window) || ($window['months'] ?? null) !== self::TAXAS_SERIES_MONTHS) return false;
    $start_date = $window['start_date'] ?? null;
    $end_date = $window['end_date'] ?? null;
    if (!$this->valid_taxas_iso_date($start_date) || !$this->valid_taxas_iso_date($end_date)) return false;
    if ($start_date > $end_date) return false;
    $today = $as_of === null ? $this->taxas_series_today() : $as_of;
    if (!$this->valid_taxas_iso_date($today) || $end_date > $today) return false;

    $sources = $meta['sources'] ?? null;
    if (!is_array($sources)) return false;
    if (($sources['selic']['source_id'] ?? null) !== 'BCB_SELIC_META_SGS_432') return false;
    if (($sources['cdi']['source_id'] ?? null) !== 'BCB_CDI_DAILY_SGS_12') return false;

    $methodology = $meta['methodology'] ?? null;
    if (!is_array($methodology)) return false;
    foreach (['selic_monthly','cdi_monthly_level','cdi_monthly_return','current_month'] as $key) {
      if (!isset($methodology[$key]) || !is_string($methodology[$key]) || trim($methodology[$key]) === '') return false;
    }

    $points = $j['points'] ?? null;
    if (!is_array($points) || count($points) !== self::TAXAS_SERIES_MONTHS) return false;

    $expected = null;
    foreach ($points as $index => $point) {
      if (!is_array($point)) return false;
      $month = $point['month'] ?? null;
      if (!$this->valid_taxas_month($month)) return false;

      if ($expected === null) {
        try {
          $expected = new DateTimeImmutable($month.'-01', new DateTimeZone('UTC'));
        } catch (Exception $e) {
          return false;
        }
      }
      if ($expected->format('Y-m') !== $month) return false;
      $expected = $expected->modify('+1 month');

      if (!is_bool($point['month_complete'] ?? null)) return false;
      if (!is_numeric($point['selic_annual_rate_pct'] ?? null)) return false;
      if (!is_numeric($point['cdi_daily_rate_pct'] ?? null)) return false;
      if (!is_numeric($point['cdi_annualized_rate_pct'] ?? null)) return false;
      if (!is_numeric($point['cdi_month_return_pct'] ?? null)) return false;
      if (!is_int($point['cdi_observation_count'] ?? null) || $point['cdi_observation_count'] < 1) return false;

      $selic = (float)$point['selic_annual_rate_pct'];
      $cdi_daily = (float)$point['cdi_daily_rate_pct'];
      $cdi_annual = (float)$point['cdi_annualized_rate_pct'];
      $cdi_month = (float)$point['cdi_month_return_pct'];
      if ($selic < 0 || $selic > 60) return false;
      if ($cdi_daily < 0 || $cdi_daily > 10) return false;
      if ($cdi_annual < 0 || $cdi_annual > 60) return false;
      if ($cdi_month < 0 || $cdi_month > 30) return false;

      foreach (['selic_observation_date','cdi_observation_date'] as $date_key) {
        $obs = $point[$date_key] ?? null;
        if (!$this->valid_taxas_iso_date($obs)) return false;
        if (substr($obs, 0, 7) !== $month) return false;
        if ($obs > $end_date) return false;
      }
    }

    $first = $points[0];
    $last = $points[count($points)-1];
    if (substr($start_date, 0, 7) !== ($first['month'] ?? null)) return false;
    if (substr($end_date, 0, 7) !== ($last['month'] ?? null)) return false;
    if (!$this->taxas_series_cdi_is_fresh($last['cdi_observation_date'] ?? null, $today)) return false;

    return true;
  }

  private function minimal_taxas_series_fallback(){
    return [
      'schema_version' => self::TAXAS_SERIES_SCHEMA_VERSION,
      'available' => false,
      'points' => [],
      '_runtime' => [
        'origin' => 'unavailable_fail_closed',
        'unavailable' => true,
        'checked_at_utc' => gmdate('c'),
      ],
    ];
  }

  private function get_taxas_series_data(){
    $d = get_transient(self::T_TAXAS_SERIES_CACHE);
    if (is_array($d) && !empty($d['_runtime']['unavailable'])) return $d;
    if ($this->validate_taxas_series_payload($d)) {
      if (!isset($d['_runtime']) || !is_array($d['_runtime'])) {
        $d['_runtime'] = [
          'origin' => 'cached_taxas_series',
          'checked_at_utc' => gmdate('c'),
        ];
        set_transient(self::T_TAXAS_SERIES_CACHE, $d, self::TTL_TAXAS_SERIES_SUCCESS);
      }
      return $d;
    }
    if ($d !== false) delete_transient(self::T_TAXAS_SERIES_CACHE);

    $headers = [
      'Accept' => 'application/json, text/plain, */*',
      'User-Agent' => 'SanidaFiscais/'.self::VERSION.' (+https://sanida.com.br)',
    ];
    $etag = get_option(self::OPT_TAXAS_SERIES_ETAG);
    if ($etag) $headers['If-None-Match'] = $etag;

    $r = wp_remote_get($this->taxas_series_json_url(), [
      'timeout' => 25,
      'redirection' => 3,
      'sslverify' => $this->sslverify(),
      'headers' => $headers,
    ]);

    if (!is_wp_error($r)) {
      $code = (int)wp_remote_retrieve_response_code($r);

      if ($code === 304) {
        $lg = get_option(self::OPT_TAXAS_SERIES_LAST_GOOD);
        if ($this->validate_taxas_series_payload($lg)) {
          $lg['_runtime'] = [
            'origin' => 'remote_304_last_good',
            'http_code' => 304,
            'checked_at_utc' => gmdate('c'),
          ];
          set_transient(self::T_TAXAS_SERIES_CACHE, $lg, self::TTL_TAXAS_SERIES_SUCCESS);
          return $lg;
        }
      }

      if ($code === 200) {
        $body = (string)wp_remote_retrieve_body($r);
        $json = json_decode($body, true);
        if ($this->validate_taxas_series_payload($json)) {
          $new_etag = wp_remote_retrieve_header($r, 'etag');
          if ($new_etag) update_option(self::OPT_TAXAS_SERIES_ETAG, $new_etag, false);
          update_option(self::OPT_TAXAS_SERIES_LAST_GOOD, $json, false);

          $json['_runtime'] = [
            'origin' => 'remote_200',
            'http_code' => 200,
            'checked_at_utc' => gmdate('c'),
          ];
          set_transient(self::T_TAXAS_SERIES_CACHE, $json, self::TTL_TAXAS_SERIES_SUCCESS);
          return $json;
        }
      }
    }

    $lg = get_option(self::OPT_TAXAS_SERIES_LAST_GOOD);
    if ($this->validate_taxas_series_payload($lg)) {
      $lg['_runtime'] = [
        'origin' => 'option_last_good',
        'checked_at_utc' => gmdate('c'),
      ];
      set_transient(self::T_TAXAS_SERIES_CACHE, $lg, self::TTL_TAXAS_SERIES_FAIL);
      return $lg;
    }

    $min = $this->minimal_taxas_series_fallback();
    set_transient(self::T_TAXAS_SERIES_CACHE, $min, self::TTL_TAXAS_SERIES_FAIL);
    return $min;
  }

  private function taxas_series_view($months = 60, $include_current_month = true){
    $months = max(1, min(self::TAXAS_SERIES_MONTHS, (int)$months));
    $data = $this->get_taxas_series_data();
    if (!$this->validate_taxas_series_payload($data)) {
      return [
        'schema_version' => self::TAXAS_SERIES_SCHEMA_VERSION,
        'available' => false,
        'points' => [],
      ];
    }

    $points = $data['points'];
    if (!$include_current_month) {
      $points = array_values(array_filter($points, function($point){
        return is_array($point) && !empty($point['month_complete']);
      }));
    }
    $points = array_slice($points, -$months);
    $first = $points ? ($points[0]['month'] ?? null) : null;
    $last = $points ? ($points[count($points)-1]['month'] ?? null) : null;

    return [
      'schema_version' => self::TAXAS_SERIES_SCHEMA_VERSION,
      'available' => true,
      'generated_at_utc' => $data['meta']['generated_at_utc'] ?? null,
      'timezone' => $data['meta']['timezone'] ?? 'America/Sao_Paulo',
      'window' => [
        'requested_months' => $months,
        'returned_months' => count($points),
        'include_current_month' => (bool)$include_current_month,
        'first_month' => $first,
        'last_month' => $last,
      ],
      'methodology' => $data['meta']['methodology'] ?? [],
      'points' => $points,
    ];
  }

  public function register_financial_series_route(){
    register_rest_route('sfa/v1', '/taxas-historicas', [
      'methods' => 'GET',
      'callback' => [$this, 'rest_get_taxas_series'],
      'permission_callback' => '__return_true',
      'args' => [
        'meses' => [
          'default' => 60,
          'sanitize_callback' => 'absint',
        ],
        'incluir_mes_corrente' => [
          'default' => 1,
          'sanitize_callback' => 'absint',
        ],
      ],
    ]);
  }

  public function rest_get_taxas_series(WP_REST_Request $req){
    $months = (int)$req->get_param('meses');
    $include = (int)$req->get_param('incluir_mes_corrente') !== 0;
    $view = $this->taxas_series_view($months, $include);
    if (empty($view['available'])) {
      return new WP_Error(
        'sfa_financial_series_unavailable',
        'Série histórica financeira indisponível para consumo.',
        ['status' => 503]
      );
    }

    $res = rest_ensure_response($view);
    $res->header('Cache-Control', 'public, max-age=21600, s-maxage=21600');
    $res->header('X-Sanida-Financial-Series', self::TAXAS_SERIES_SCHEMA_VERSION);
    return $res;
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
