<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Fiscal_Network_Trait {

  public function register_rest_routes(){
    register_rest_route('sfa/v1', '/folha', [
      'methods'  => 'GET',
      'callback' => [$this, 'rest_get_folha'],
      'permission_callback' => '__return_true',
    ]);

    register_rest_route('sfa/v1', '/fiscal-release', [
      'methods'  => 'GET',
      'callback' => [$this, 'rest_get_fiscal_release'],
      'permission_callback' => '__return_true',
    ]);
  }

  public function rest_get_folha(WP_REST_Request $req){
    return new WP_Error(
      'sfa_legacy_folha_retired',
      'Endpoint legado removido em C6.7. Consuma a release fiscal canônica.',
      [
        'status' => 410,
        'replacement' => '/wp-json/sfa/v1/fiscal-release',
      ]
    );
  }

  public function rest_get_fiscal_release(WP_REST_Request $req){
    $package = $this->get_release_package();
    if (!$this->validate_release_package($package)) {
      return new WP_Error(
        'sfa_fiscal_release_unavailable',
        'Release fiscal canônica indisponível.',
        ['status' => 503]
      );
    }

    $release = $package['release'];
    $res = rest_ensure_response($release);
    $res->header('Cache-Control', 'public, max-age=600, s-maxage=600');
    $res->header('X-Sanida-Fiscal-Release', (string)$release['release_id']);
    return $res;
  }

  private function current_json_url(){
    return defined('SFA_FISCAL_CURRENT_URL')
      ? (string) SFA_FISCAL_CURRENT_URL
      : self::CURRENT_JSON_URL_DEFAULT;
  }

  private function release_base_url(){
    $url = defined('SFA_FISCAL_RELEASE_BASE_URL')
      ? (string) SFA_FISCAL_RELEASE_BASE_URL
      : self::RELEASE_BASE_URL_DEFAULT;
    return rtrim($url, '/') . '/';
  }

  private function sslverify(){
    return defined('SFA_FISCAIS_SSLVERIFY') ? (bool) SFA_FISCAIS_SSLVERIFY : true;
  }

  private function body_sample($body, $max = 220){
    $body = trim((string) $body);
    if ($body === '') return null;
    return function_exists('mb_substr') ? mb_substr($body, 0, $max) : substr($body, 0, $max);
  }

  private function http_headers(){
    return [
      'Accept' => 'application/json, text/plain, */*',
      'User-Agent' => 'SanidaFiscais/'.self::VERSION.' (+https://sanida.com.br)'
    ];
  }

  private function fetch_json_document($url, $headers = []){
    $r = wp_remote_get($url, [
      'timeout'     => 20,
      'redirection' => 3,
      'sslverify'   => $this->sslverify(),
      'headers'     => array_merge($this->http_headers(), is_array($headers) ? $headers : []),
    ]);

    if (is_wp_error($r)) {
      return [
        'ok' => false,
        'code' => null,
        'body' => null,
        'json' => null,
        'etag' => null,
        'error' => $r->get_error_message(),
      ];
    }

    $code = (int) wp_remote_retrieve_response_code($r);
    $body = (string) wp_remote_retrieve_body($r);
    $json = ($code === 200) ? json_decode($body, true) : null;
    $etag = wp_remote_retrieve_header($r, 'etag');

    return [
      'ok' => ($code >= 200 && $code < 300),
      'code' => $code,
      'body' => $body,
      'json' => is_array($json) ? $json : null,
      'etag' => $etag ? (string)$etag : null,
      'error' => null,
    ];
  }

  private function release_runtime($origin, $extra = []){
    return array_merge([
      'origin' => (string)$origin,
      'checked_at_utc' => gmdate('c'),
      'canonical_source' => 'fiscal_contract_v1_2_release',
    ], is_array($extra) ? $extra : []);
  }

  private function fiscais_unavailable_text($kind){
    if ($kind === 'ano') return '—';
    if ($kind === 'inss') return '<em>Tabela do INSS temporariamente indisponível.</em>';
    if ($kind === 'irrf') return '<em>Tabela do IRRF temporariamente indisponível.</em>';
    return '<em>Dados fiscais temporariamente indisponíveis.</em>';
  }

  private function with_package_runtime($package, $origin, $extra = []){
    if (!is_array($package)) $package = [];
    $package['_runtime'] = $this->release_runtime($origin, $extra);
    return $package;
  }

  private function get_release_package(){
    $cached = get_transient(self::T_CACHE);
    if ($this->validate_release_package($cached)) {
      if (!isset($cached['_runtime']) || !is_array($cached['_runtime'])) {
        $cached = $this->with_package_runtime($cached, 'transient_cache');
      }
      return $cached;
    }
    if ($cached !== false) delete_transient(self::T_CACHE);

    $current_url = $this->current_json_url();
    $etag = get_option(self::OPT_CURRENT_ETAG);
    $current_headers = [];
    if ($etag) $current_headers['If-None-Match'] = $etag;

    $current = $this->fetch_json_document($current_url, $current_headers);
    $error = $current['error'];
    $body_sample = $this->body_sample($current['body'] ?? '');
    $known_successor = false;

    if (($current['code'] ?? null) === 304) {
      $last_good = get_option(self::OPT_LAST_GOOD);
      if ($this->package_allows_last_good($last_good)) {
        $last_good = $this->with_package_runtime($last_good, 'current_304_last_good', [
          'current_url' => $current_url,
          'http_code' => 304,
          'etag_sent' => true,
          'release_id' => $last_good['release']['release_id'],
        ]);
        set_transient(self::T_CACHE, $last_good, self::TTL_SUCCESS);
        return $last_good;
      }
      $error = 'current_304_without_valid_last_good';
    }

    if (($current['code'] ?? null) === 200 && $this->validate_manifest($current['json'])) {
      $manifest = $current['json'];
      $last_good = get_option(self::OPT_LAST_GOOD);
      if ($this->validate_release_package($last_good)) {
        $last_good_id = $last_good['release']['release_id'] ?? null;
        $known_successor = is_string($last_good_id) && $last_good_id !== $manifest['release_id'];
      }

      if ($this->validate_release_package($last_good)
          && ($last_good['manifest']['release_id'] ?? null) === $manifest['release_id']
          && ($last_good['manifest']['artifact_sha256'] ?? null) === $manifest['artifact_sha256']) {
        if (!empty($current['etag'])) update_option(self::OPT_CURRENT_ETAG, $current['etag'], false);
        $last_good = $this->with_package_runtime($last_good, 'current_200_same_release_last_good', [
          'current_url' => $current_url,
          'http_code' => 200,
          'release_id' => $manifest['release_id'],
          'artifact_sha256' => $manifest['artifact_sha256'],
        ]);
        set_transient(self::T_CACHE, $last_good, self::TTL_SUCCESS);
        return $last_good;
      }

      $artifact_url = $this->release_base_url() . ltrim($manifest['artifact'], '/');
      $artifact = $this->fetch_json_document($artifact_url);
      if (($artifact['code'] ?? null) === 200 && is_string($artifact['body'])) {
        $actual_sha = hash('sha256', $artifact['body']);
        if (hash_equals($manifest['artifact_sha256'], $actual_sha)
            && $this->validate_release($artifact['json'], $manifest, $artifact['body'])) {
          $package = [
            'manifest' => $manifest,
            'release' => $artifact['json'],
            'artifact_body' => $artifact['body'],
          ];
          $package = $this->with_package_runtime($package, 'remote_verified_release', [
            'current_url' => $current_url,
            'artifact_url' => $artifact_url,
            'http_code' => 200,
            'release_id' => $manifest['release_id'],
            'artifact_sha256' => $actual_sha,
          ]);
          if (!empty($current['etag'])) update_option(self::OPT_CURRENT_ETAG, $current['etag'], false);
          update_option(self::OPT_LAST_GOOD, $package, false);
          set_transient(self::T_CACHE, $package, self::TTL_SUCCESS);
          return $package;
        }
        $error = hash_equals($manifest['artifact_sha256'], $actual_sha)
          ? 'release_incompativel_ou_invalida'
          : 'artifact_sha256_divergente';
        $body_sample = $this->body_sample($artifact['body']);
      } else {
        $error = $artifact['error'] ?: ('artifact_http_'.(string)($artifact['code'] ?? 'unknown'));
        $body_sample = $this->body_sample($artifact['body'] ?? '');
      }
    } elseif (($current['code'] ?? null) === 200) {
      $error = 'manifest_invalido_ou_incompativel';
    } elseif (!$error && ($current['code'] ?? null) !== 304) {
      $error = 'current_http_'.(string)($current['code'] ?? 'unknown');
    }

    $last_good = get_option(self::OPT_LAST_GOOD);
    if ($this->package_allows_last_good($last_good, $known_successor)) {
      $last_good = $this->with_package_runtime($last_good, 'option_last_good', [
        'current_url' => $current_url,
        'http_code' => $current['code'] ?? null,
        'last_fetch_error' => $error,
        'body_sample' => $body_sample,
        'release_id' => $last_good['release']['release_id'],
        'historical_release_relabelled_as_current' => false,
        'known_successor' => false,
      ]);
      set_transient(self::T_CACHE, $last_good, self::TTL_FAIL);
      return $last_good;
    }

    return [
      'manifest' => null,
      'release' => null,
      '_runtime' => $this->release_runtime('unavailable', [
        'current_url' => $current_url,
        'http_code' => $current['code'] ?? null,
        'last_fetch_error' => $error,
        'body_sample' => $body_sample,
        'known_successor' => (bool)$known_successor,
      ]),
    ];
  }

  private function index_rules($release){
    $out = [];
    foreach (($release['rules'] ?? []) as $rule) {
      if (is_array($rule) && isset($rule['rule_id'])) $out[(string)$rule['rule_id']] = $rule;
    }
    return $out;
  }

}
