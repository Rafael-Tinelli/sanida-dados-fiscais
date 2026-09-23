<?php
/**
 * Plugin Name: Dados Oficiais BR
 * Description: Shortcodes de dados oficiais (salário mínimo, PIS etc.), série histórica automática e suporte a meta description dos plugins de SEO. Inclui série dinâmica da taxa de desemprego (PNAD Contínua via IpeaData) com fallback, cache inteligente e diagnóstico; e automação assistida do salário mínimo com cron, fallback e debug.
 * Version: 1.4.10
 * Author: Sanida
 */

if (!defined('ABSPATH')) exit;

final class DOBR_Plugin {
  const OPT = 'dobr_settings';
  const OPT_HIST = 'dobr_history';

  // Desemprego (PNAD/IpeaData)
  const DESEMPREGO_CACHE_KEY = 'dobr_desemprego_payload';
  const DESEMPREGO_CACHE_KEY_JSON_COMPAT = 'dobr_desemprego_json'; // legado/compat
  const DESEMPREGO_LAST_GOOD_OPTION = 'dobr_desemprego_last_good';

  // Salário mínimo (auto híbrido)
  const SM_CACHE_KEY = 'dobr_sm_payload';
  const SM_LAST_GOOD_OPTION = 'dobr_sm_last_good';
  const CRON_HOOK_SM_REFRESH = 'dobr_cron_refresh_sm';

  // Seguro-desemprego (tabela anual do trabalhador formal)
  const SD_PARAMS_CACHE_KEY = 'dobr_sd_params_payload';
  const SD_PARAMS_LAST_GOOD_OPTION = 'dobr_sd_params_last_good';
  const CRON_HOOK_SD_PARAMS_REFRESH = 'dobr_cron_refresh_sd_params';

  public function __construct() {
    // Shortcodes existentes (SM)
    add_shortcode('sm_valor',         [$this,'sc_sm_valor']);
    add_shortcode('sm_valor_raw',     [$this,'sc_sm_valor_raw']);
    add_shortcode('sm_vigencia',      [$this,'sc_sm_vigencia']);
    add_shortcode('sm_dia',           [$this,'sc_sm_dia']);
    add_shortcode('sm_hora',          [$this,'sc_sm_hora']);
    add_shortcode('pis_valor',        [$this,'sc_pis_valor']);
    add_shortcode('sd_min_parcela',   [$this,'sc_sd_min_parcela']);
    add_shortcode('sd_parametros_json', [$this,'sc_sd_parametros_json']);
    add_shortcode('sm_serie_json',    [$this,'sc_sm_serie_json']);

    // Desemprego PNAD Contínua (IpeaData)
    add_shortcode('desemprego_serie_json', [$this,'sc_desemprego_serie_json']);
    add_shortcode('desemprego_valor',      [$this,'sc_desemprego_valor']);

    // Debugs (admin only no retorno)
    add_shortcode('desemprego_debug', [$this, 'sc_desemprego_debug']);
    add_shortcode('sm_debug',         [$this, 'sc_sm_debug']);
    add_shortcode('sd_debug',         [$this, 'sc_sd_debug']);

    // Admin
    if (is_admin()) {
      add_action('admin_menu', [$this,'admin_menu']);
      add_action('admin_init', [$this,'admin_init']);
      add_action('update_option_' . self::OPT, [$this,'on_update_settings'], 10, 3);
      add_action('admin_init', [$this, 'admin_handle_clear_cache']);
      add_action('admin_notices', [$this, 'admin_notices']);
    }

    // Admin bar
    add_action('admin_bar_menu', [$this, 'admin_bar_menu'], 100);

    // Cron (salário mínimo)
    add_action('init', [$this, 'ensure_cron_events']);
    add_action(self::CRON_HOOK_SM_REFRESH, [$this, 'cron_refresh_sm']);
    add_action(self::CRON_HOOK_SD_PARAMS_REFRESH, [$this, 'cron_refresh_sd_params']);
  }

  /* ===== Config helpers ===== */
  private function cfg(string $name, $default) {
    return defined($name) ? constant($name) : $default;
  }

  // Desemprego
  private function desemprego_ttl_success(): int {
    $v = (int) $this->cfg('DOBR_DESEMPREGO_CACHE_TTL_SUCCESS', 6 * HOUR_IN_SECONDS);
    return max(60, $v);
  }
  private function desemprego_ttl_fallback(): int {
    $v = (int) $this->cfg('DOBR_DESEMPREGO_CACHE_TTL_FALLBACK', 10 * MINUTE_IN_SECONDS);
    return max(60, $v);
  }
  private function desemprego_last_good_max_age(): int {
    $v = (int) $this->cfg('DOBR_DESEMPREGO_LAST_GOOD_MAX_AGE', 60 * DAY_IN_SECONDS);
    return max(DAY_IN_SECONDS, $v);
  }
  private function desemprego_last_good_is_fresh($lg): bool {
    if (!is_array($lg) || !isset($lg['ts']) || !is_numeric($lg['ts'])) return false;
    $age = time() - (int)$lg['ts'];
    return $age >= 0 && $age <= $this->desemprego_last_good_max_age();
  }
  private function desemprego_sslverify(): bool {
    return (bool) $this->cfg('DOBR_DESEMPREGO_SSLVERIFY', true);
  }

  // Salário mínimo
  private function sm_auto_enabled(): bool {
    return (bool) $this->cfg('DOBR_SM_AUTO_ENABLED', true);
  }
  private function sm_ttl_success(): int {
    $v = (int) $this->cfg('DOBR_SM_CACHE_TTL_SUCCESS', 24 * HOUR_IN_SECONDS);
    return max(60, $v);
  }
  private function sm_ttl_fallback(): int {
    $v = (int) $this->cfg('DOBR_SM_CACHE_TTL_FALLBACK', 6 * HOUR_IN_SECONDS);
    return max(60, $v);
  }
  private function sm_sslverify(): bool {
    return (bool) $this->cfg('DOBR_SM_SSLVERIFY', true);
  }
  private function sm_auto_sync_settings(): bool {
    return (bool) $this->cfg('DOBR_SM_AUTO_SYNC_SETTINGS', false);
  }
  private function sm_seed_valor(): float {
    return (float) $this->cfg('DOBR_SM_SEED_VALOR', 0.0);
  }
  private function sm_seed_vigencia(): string {
    $v = trim((string) $this->cfg('DOBR_SM_SEED_VIGENCIA', ''));
    return preg_match('/^\d{4}-\d{2}-\d{2}$/', $v) ? $v : '';
  }

  // Seguro-desemprego
  private function sd_params_ttl_success(): int {
    $v = (int) $this->cfg('DOBR_SD_PARAMS_CACHE_TTL_SUCCESS', 24 * HOUR_IN_SECONDS);
    return max(300, $v);
  }

  private function sd_params_ttl_fallback(): int {
    $v = (int) $this->cfg('DOBR_SD_PARAMS_CACHE_TTL_FALLBACK', 30 * MINUTE_IN_SECONDS);
    return max(300, $v);
  }

  private function sd_params_last_good_max_age(): int {
    // Fallback operacional curto. Se as fontes oficiais ficarem indisponíveis
    // por período prolongado, o contrato falha fechado em vez de eternizar a tabela.
    $v = (int) $this->cfg('DOBR_SD_PARAMS_LAST_GOOD_MAX_AGE', 60 * DAY_IN_SECONDS);
    return max(7 * DAY_IN_SECONDS, $v);
  }

  private function sd_params_sslverify(): bool {
    return (bool) $this->cfg('DOBR_SD_PARAMS_SSLVERIFY', true);
  }

  private function sd_reference_year_candidates(): array {
    $year = (int) current_time('Y');
    return [$year, $year - 1];
  }

  // Geral
  private function debug_log_enabled(): bool {
    return (bool) $this->cfg('DOBR_DEBUG_LOG', false);
  }

  /* ===== Utils ===== */
  private function baseline_history(): array {
    // Base inicial; histórico pode ser enriquecido pelo admin e filtros.
    return ['2021'=>1100.00,'2022'=>1212.00,'2023'=>1320.00,'2024'=>1412.00,'2025'=>1518.00];
  }

  private function get_settings(): array {
    $opt = get_option(self::OPT) ?: [];
    // Sem default anual: configuração manual é apenas fallback opt-in.
    $defaults = ['valor'=>0.0, 'vigencia'=>''];
    return array_merge($defaults, array_intersect_key($opt, $defaults));
  }

  private function get_manual_sm_config(): array {
    $s = $this->get_settings();
    $valor = isset($s['valor']) ? (float)$s['valor'] : 0.0;
    $vig = isset($s['vigencia']) ? trim((string)$s['vigencia']) : '';
    if ($vig !== '' && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $vig)) {
      $t = strtotime($vig);
      $vig = $t ? date('Y-m-d', $t) : '';
    }
    return [
      'valor' => $valor,
      'vigencia' => $vig,
      'year' => (int) substr($vig, 0, 4),
    ];
  }

  private function money_format($v, $format='br') {
    $v = (float)$v;
    return (strtolower($format)==='raw') ? number_format($v, 2, '.', '') : 'R$ ' . number_format($v, 2, ',', '.');
  }

  private function normalize_currency_input($str): float {
    $raw = preg_replace('/[^\d,\.]/','', (string)$str);
    if (strpos($raw, ',') !== false && strpos($raw, '.') !== false) {
      $raw = str_replace('.', '', $raw);
      $raw = str_replace(',', '.', $raw);
    } else {
      $raw = str_replace(',', '.', $raw);
    }
    return (float)$raw;
  }

  private function body_sample($body, int $max = 220) {
    $body = trim((string) $body);
    if ($body === '') return null;
    return function_exists('mb_substr') ? mb_substr($body, 0, $max) : substr($body, 0, $max);
  }

  private function log(string $message, array $context = []): void {
    if (!$this->debug_log_enabled()) return;
    if (defined('WP_DEBUG') && !WP_DEBUG) return;
    $line = '[DOBR] ' . $message;
    if ($context) {
      $json = wp_json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
      if ($json) $line .= ' | ' . $json;
    }
    error_log($line);
  }

  private function can_request_host(string $host): bool {
    if (!defined('WP_HTTP_BLOCK_EXTERNAL') || !WP_HTTP_BLOCK_EXTERNAL) return true;
    if (!defined('WP_ACCESSIBLE_HOSTS') || !WP_ACCESSIBLE_HOSTS) return false;

    $allowed_raw = explode(',', (string) WP_ACCESSIBLE_HOSTS);
    foreach ($allowed_raw as $pattern) {
      $pattern = trim($pattern);
      if ($pattern === '') continue;
      $regex = preg_quote($pattern, '/');
      $regex = str_replace('\*', '.*', $regex);
      if (preg_match('/^' . $regex . '$/i', $host)) return true;
    }
    return false;
  }

  private function get_history(): array {
    $h = get_option(self::OPT_HIST, []);
    if (!is_array($h)) $h = [];
    $baseline = apply_filters('dobr_baseline_history', $this->baseline_history());
    foreach ($baseline as $y => $v) {
      if (!isset($h[$y])) $h[$y] = (float)$v;
    }

    $norm = [];
    foreach ($h as $y => $v) $norm[(string)(int)$y] = (float)$v;
    ksort($norm, SORT_NUMERIC);
    update_option(self::OPT_HIST, $norm);
    return $norm;
  }

  private function set_history(array $h): void {
    ksort($h, SORT_NUMERIC);
    update_option(self::OPT_HIST, $h);
  }

  private function get_sm_year_map(): array {
    // Mapa consolidado: histórico + manual (ano da vigência manual)
    $hist = $this->get_history();
    $manual = $this->get_manual_sm_config();

    $map = [];
    foreach ($hist as $y => $v) {
      $yi = (int)$y;
      if ($yi > 0 && is_numeric($v)) $map[(string)$yi] = (float)$v;
    }

    if ($manual['year'] > 0 && $manual['valor'] > 0) {
      $map[(string)$manual['year']] = (float)$manual['valor'];
    }

    ksort($map, SORT_NUMERIC);
    return $map;
  }

  /* ===== Cron ===== */

  public function ensure_cron_events(): void {
    // Agenda atualização diária do salário mínimo (tentativa assistida)
    if (!wp_next_scheduled(self::CRON_HOOK_SM_REFRESH)) {
      wp_schedule_event(time() + 600, 'daily', self::CRON_HOOK_SM_REFRESH);
    }

    // Tabela anual do seguro-desemprego: descoberta e validação automáticas.
    if (!wp_next_scheduled(self::CRON_HOOK_SD_PARAMS_REFRESH)) {
      wp_schedule_event(time() + 900, 'daily', self::CRON_HOOK_SD_PARAMS_REFRESH);
    }
  }

  public function cron_refresh_sm(): void {
    // Força recálculo/refresh do salário mínimo
    delete_transient(self::SM_CACHE_KEY);
    $payload = $this->get_sm_payload(true);
    $this->log('cron_refresh_sm executado', [
      'source' => isset($payload['source']) ? $payload['source'] : null,
      'ok' => isset($payload['ok']) ? $payload['ok'] : null,
      'error' => isset($payload['error']) ? $payload['error'] : null,
    ]);
  }

  public function cron_refresh_sd_params(): void {
    delete_transient(self::SD_PARAMS_CACHE_KEY);
    $payload = $this->get_sd_params_payload(true);
    $this->log('cron_refresh_sd_params executado', [
      'source' => isset($payload['source']) ? $payload['source'] : null,
      'ok' => isset($payload['ok']) ? $payload['ok'] : null,
      'reference_year' => isset($payload['reference_year']) ? $payload['reference_year'] : null,
      'error' => isset($payload['error']) ? $payload['error'] : null,
    ]);
  }

  /* ===== Seguro-desemprego: parâmetros oficiais evergreen ===== */

  private function sd_params_payload_shape_valid($payload): bool {
    return is_array($payload)
      && array_key_exists('ok', $payload)
      && array_key_exists('status', $payload)
      && array_key_exists('reference_year', $payload)
      && array_key_exists('effective_from', $payload)
      && array_key_exists('floor', $payload)
      && array_key_exists('first_band_limit', $payload)
      && array_key_exists('first_band_rate', $payload)
      && array_key_exists('second_band_limit', $payload)
      && array_key_exists('second_band_excess_rate', $payload)
      && array_key_exists('second_band_base', $payload)
      && array_key_exists('cap', $payload)
      && array_key_exists('source_url', $payload)
      && array_key_exists('source', $payload)
      && array_key_exists('error', $payload)
      && array_key_exists('fetched_at', $payload);
  }

  private function sd_params_last_good_is_fresh($lg): bool {
    if (!is_array($lg) || !isset($lg['ts']) || !is_numeric($lg['ts'])) return false;
    $age = time() - (int)$lg['ts'];
    return $age >= 0 && $age <= $this->sd_params_last_good_max_age();
  }

  private function sd_parse_rate(string $raw): ?float {
    $raw = trim(str_replace(',', '.', $raw));
    $isPercent = strpos($raw, '%') !== false;
    $raw = trim(str_replace('%', '', $raw));
    if ($raw === '' || !is_numeric($raw)) return null;

    $value = (float)$raw;
    if ($isPercent || $value > 1) {
      if ($value <= 0 || $value > 100) return null;
      $value = $value / 100.0;
    }

    return ($value > 0 && $value < 1) ? $value : null;
  }

  private function sd_parse_currency(string $raw): ?float {
    $value = $this->normalize_currency_input($raw);
    return ($value >= 100.0 && $value <= 100000.0) ? (float)$value : null;
  }

  private function sd_extract_effective_from(string $plain, int $targetYear): ?string {
    $year = preg_quote((string)$targetYear, '/');
    $months = [
      'janeiro'=>1, 'fevereiro'=>2, 'março'=>3, 'marco'=>3, 'abril'=>4,
      'maio'=>5, 'junho'=>6, 'julho'=>7, 'agosto'=>8, 'setembro'=>9,
      'outubro'=>10, 'novembro'=>11, 'dezembro'=>12,
    ];

    if (preg_match(
      '/(?:vig[eê]ncia|valer|vigor|passa\s+a\s+valer).{0,140}?(\d{1,2})\s+de\s+([[:alpha:]çãáâéêíóôõú]+)\s+de\s+' . $year . '/iu',
      $plain,
      $m
    )) {
      $day = (int)$m[1];
      $monthName = function_exists('mb_strtolower') ? mb_strtolower($m[2], 'UTF-8') : strtolower($m[2]);
      if (isset($months[$monthName])) {
        $month = (int)$months[$monthName];
        if (checkdate($month, $day, $targetYear)) {
          return sprintf('%04d-%02d-%02d', $targetYear, $month, $day);
        }
      }
    }

    if (preg_match(
      '/(?:vig[eê]ncia|valer|vigor|passa\s+a\s+valer).{0,140}?(\d{1,2})\/(\d{1,2})\/' . $year . '/iu',
      $plain,
      $m
    )) {
      $day = (int)$m[1];
      $month = (int)$m[2];
      if (checkdate($month, $day, $targetYear)) {
        return sprintf('%04d-%02d-%02d', $targetYear, $month, $day);
      }
    }

    return null;
  }

  private function sd_extract_from_official_html(string $html, int $targetYear): array {
    $plain = wp_strip_all_tags($html, true);
    $plain = html_entity_decode($plain, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    $plain = preg_replace('/\s+/u', ' ', $plain);

    if (!is_string($plain) || trim($plain) === '') {
      return ['ok' => false, 'error' => 'empty_official_page'];
    }

    $year = preg_quote((string)$targetYear, '/');
    $yearMarkers = [
      '/tabela\s+anual.{0,140}?' . $year . '/iu',
      '/per[ií]odo\s*:\s*ano\s+de\s+' . $year . '/iu',
      '/vig[eê]ncia.{0,120}?' . $year . '/iu',
      '/vigente\s+para\s+o\s+ano\s+de\s+' . $year . '/iu',
    ];
    $hasYearMarker = false;
    foreach ($yearMarkers as $pattern) {
      if (preg_match($pattern, $plain)) {
        $hasYearMarker = true;
        break;
      }
    }
    if (!$hasYearMarker) {
      return ['ok' => false, 'error' => 'current_reference_year_marker_missing'];
    }

    $firstPattern = '/at[eé]\s+R\$\s*([\d\.\,]+).{0,120}?multiplica-se(?:\s+o)?\s+sal[aá]rio\s+m[eé]dio\s+por\s+([0-9]+(?:[\.,][0-9]+)?\s*%?)/iu';
    $secondPattern = '/de\s+R\$\s*([\d\.\,]+)\s+at[eé]\s+R\$\s*([\d\.\,]+).{0,180}?exceder(?:\s+a|\s+de)?\s*R\$\s*([\d\.\,]+).{0,120}?multiplica-se\s+por\s+([0-9]+(?:[\.,][0-9]+)?\s*%?).{0,140}?soma-se\s+(?:com|a)\s+R\$\s*([\d\.\,]+)/iu';
    $thirdPattern = '/acima\s+de\s+R\$\s*([\d\.\,]+).{0,140}?valor\s+ser[aá]\s+invari[aá]vel\s+de\s+R\$\s*([\d\.\,]+)/iu';

    if (!preg_match($firstPattern, $plain, $m1)) {
      return ['ok' => false, 'error' => 'first_band_missing'];
    }
    if (!preg_match($secondPattern, $plain, $m2)) {
      return ['ok' => false, 'error' => 'second_band_missing'];
    }
    if (!preg_match($thirdPattern, $plain, $m3)) {
      return ['ok' => false, 'error' => 'cap_band_missing'];
    }

    $floor = null;
    $floorPatterns = [
      '/n[aã]o\s+(?:poder[aá]\s+)?ser[aá]\s+inferior.{0,180}?R\$\s*([\d\.\,]+)/iu',
      '/sal[aá]rio\s+m[ií]nimo\s*:?\s*R\$\s*([\d\.\,]+)/iu',
    ];
    foreach ($floorPatterns as $pattern) {
      if (preg_match($pattern, $plain, $mf)) {
        $floor = $this->sd_parse_currency($mf[1]);
        if ($floor !== null) break;
      }
    }
    if ($floor === null) {
      return ['ok' => false, 'error' => 'floor_missing'];
    }

    $firstLimit = $this->sd_parse_currency($m1[1]);
    $firstRate = $this->sd_parse_rate($m1[2]);
    $secondStart = $this->sd_parse_currency($m2[1]);
    $secondLimit = $this->sd_parse_currency($m2[2]);
    $secondExcessBase = $this->sd_parse_currency($m2[3]);
    $secondRate = $this->sd_parse_rate($m2[4]);
    $secondBase = $this->sd_parse_currency($m2[5]);
    $thirdStart = $this->sd_parse_currency($m3[1]);
    $cap = $this->sd_parse_currency($m3[2]);
    $effectiveFrom = $this->sd_extract_effective_from($plain, $targetYear);

    foreach ([
      $firstLimit, $firstRate, $secondStart, $secondLimit,
      $secondExcessBase, $secondRate, $secondBase, $thirdStart, $cap
    ] as $value) {
      if ($value === null) {
        return ['ok' => false, 'error' => 'parsed_parameter_invalid'];
      }
    }
    if ($effectiveFrom === null) {
      return ['ok' => false, 'error' => 'effective_date_missing'];
    }

    // Invariantes semânticos: se a estrutura anual mudar, falha fechado.
    if (abs($secondStart - ($firstLimit + 0.01)) > 0.011) {
      return ['ok' => false, 'error' => 'band_continuity_mismatch'];
    }
    if (abs($secondExcessBase - $firstLimit) > 0.011) {
      return ['ok' => false, 'error' => 'second_band_reference_mismatch'];
    }
    if (abs($thirdStart - $secondLimit) > 0.011) {
      return ['ok' => false, 'error' => 'cap_threshold_mismatch'];
    }

    $expectedSecondBase = round($firstLimit * $firstRate, 2, PHP_ROUND_HALF_UP);
    if (abs($secondBase - $expectedSecondBase) > 0.011) {
      return ['ok' => false, 'error' => 'second_band_base_mismatch'];
    }

    if ($firstLimit <= $floor || $secondLimit <= $firstLimit || $cap < $floor || $secondBase <= 0) {
      return ['ok' => false, 'error' => 'numeric_invariant_mismatch'];
    }

    return [
      'ok' => true,
      'status' => 'candidate',
      'reference_year' => $targetYear,
      'effective_from' => $effectiveFrom,
      'floor' => round($floor, 2),
      'first_band_limit' => round($firstLimit, 2),
      'first_band_rate' => $firstRate,
      'second_band_limit' => round($secondLimit, 2),
      'second_band_excess_rate' => $secondRate,
      'second_band_base' => round($secondBase, 2),
      'cap' => round($cap, 2),
      'parse_rule' => 'sd_three_bands_plus_floor_and_effective_date_v1',
      'error' => null,
    ];
  }

  private function sd_params_http_args(): array {
    return [
      'timeout' => 25,
      'redirection' => 4,
      'sslverify' => $this->sd_params_sslverify(),
      'headers' => [
        'User-Agent' => 'Mozilla/5.0 (compatible; DOBR/1.4.10; WordPress)',
        'Accept' => 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection' => 'keep-alive',
      ],
    ];
  }

  private function sd_params_discover_links(string $url, int $targetYear): array {
    $host = (string) parse_url($url, PHP_URL_HOST);
    if ($host === '' || !$this->can_request_host($host)) return [];

    $res = wp_remote_get($url, $this->sd_params_http_args());
    if (is_wp_error($res) || (int)wp_remote_retrieve_response_code($res) !== 200) return [];

    $body = (string) wp_remote_retrieve_body($res);
    if ($body === '') return [];

    preg_match_all('/href\s*=\s*["\']([^"\']+)["\']/iu', $body, $matches);
    $out = [];
    foreach (($matches[1] ?? []) as $href) {
      $candidate = html_entity_decode(trim((string)$href), ENT_QUOTES | ENT_HTML5, 'UTF-8');
      if (!preg_match('#^https://#i', $candidate)) continue;
      $candidateHost = strtolower((string)parse_url($candidate, PHP_URL_HOST));
      if (!in_array($candidateHost, ['portalfat.mte.gov.br', 'portalfat.trabalho.gov.br'], true)) continue;
      $path = strtolower((string)parse_url($candidate, PHP_URL_PATH));
      if (strpos($path, 'seguro-desemprego') === false) continue;
      $out[] = $candidate;
      if (count($out) >= 20) break;
    }

    return array_values(array_unique($out));
  }

  private function sd_params_candidate_urls(int $targetYear): array {
    $urls = [
      // Slug estável observado no FAT.
      'https://portalfat.mte.gov.br/mte-reajusta-valores-do-beneficio-seguro-desemprego/',
      'https://portalfat.trabalho.gov.br/mte-reajusta-valores-do-beneficio-seguro-desemprego/',
      // Página operacional MTE: só será aceita se contiver a referência esperada.
      'https://www.gov.br/trabalho-e-emprego/pt-br/servicos/trabalhador/seguro-desemprego/seguro-desemprego-formal',
    ];

    $discoveryPages = [
      'https://portalfat.mte.gov.br/category/noticias/',
      'https://portalfat.trabalho.gov.br/category/noticias/',
      'https://portalfat.mte.gov.br/?s=seguro-desemprego+' . $targetYear,
      'https://portalfat.trabalho.gov.br/?s=seguro-desemprego+' . $targetYear,
    ];
    foreach ($discoveryPages as $page) {
      foreach ($this->sd_params_discover_links($page, $targetYear) as $found) {
        $urls[] = $found;
      }
    }

    $urls = apply_filters('dobr_sd_params_auto_urls', $urls, $targetYear);
    $normalized = [];
    foreach ((array)$urls as $url) {
      $url = trim((string)$url);
      if ($url === '' || !preg_match('#^https://#i', $url)) continue;
      $host = strtolower((string)parse_url($url, PHP_URL_HOST));
      if (!in_array($host, ['www.gov.br', 'gov.br', 'portalfat.mte.gov.br', 'portalfat.trabalho.gov.br'], true)) continue;
      $normalized[] = $url;
    }

    return array_values(array_unique($normalized));
  }

  private function fetch_sd_params_auto_from_official(int $targetYear): array {
    $urls = $this->sd_params_candidate_urls($targetYear);
    $attempts = 0;
    $lastError = 'no_candidate_succeeded';
    $lastHttpCode = null;
    $lastEndpoint = null;
    $targetYearEvidence = false;

    foreach ($urls as $url) {
      $host = (string)parse_url($url, PHP_URL_HOST);
      if ($host === '' || !$this->can_request_host($host)) {
        $lastError = 'external_http_blocked_for_' . $host;
        continue;
      }

      $lastEndpoint = $url;
      $attempts++;
      $res = wp_remote_get($url, $this->sd_params_http_args());

      if (is_wp_error($res)) {
        $lastError = 'wp_error: ' . $res->get_error_message();
        continue;
      }

      $code = (int)wp_remote_retrieve_response_code($res);
      $lastHttpCode = $code;
      if ($code !== 200) {
        $lastError = 'http_status_' . $code;
        continue;
      }

      $parsed = $this->sd_extract_from_official_html((string)wp_remote_retrieve_body($res), $targetYear);
      if (empty($parsed['ok'])) {
        $lastError = isset($parsed['error']) ? (string)$parsed['error'] : 'parse_failed';
        if ($lastError !== 'current_reference_year_marker_missing') {
          $targetYearEvidence = true;
        }
        continue;
      }

      $targetYearEvidence = true;
      $parsed['source_url'] = $url;
      $parsed['http_code'] = 200;
      $parsed['attempts'] = $attempts;
      return $parsed;
    }

    return [
      'ok' => false,
      'error' => $lastError,
      'http_code' => $lastHttpCode,
      'endpoint' => $lastEndpoint,
      'attempts' => $attempts,
      'target_year_evidence' => $targetYearEvidence,
    ];
  }

  private function sd_active_candidate(array $fetchedByYear): array {
    $now = (int)current_time('timestamp');
    $active = [];

    foreach ($fetchedByYear as $year => $candidate) {
      if (empty($candidate['ok'])) continue;
      $effectiveTs = strtotime((string)$candidate['effective_from'] . ' 00:00:00');
      if (!$effectiveTs || $effectiveTs > $now) continue;
      $active[(int)$year] = $candidate;
    }

    if (!$active) return [];
    krsort($active, SORT_NUMERIC);
    return reset($active);
  }

  private function get_sd_params_payload(bool $forceRefresh = false): array {
    $years = $this->sd_reference_year_candidates();
    $currentYear = (int)$years[0];
    $now = (int)current_time('timestamp');
    $cached = get_transient(self::SD_PARAMS_CACHE_KEY);

    if (!$forceRefresh && $this->sd_params_payload_shape_valid($cached)) {
      $cachedTs = !empty($cached['effective_from'])
        ? strtotime((string)$cached['effective_from'] . ' 00:00:00')
        : false;

      if (
        !empty($cached['ok'])
        && in_array((int)$cached['reference_year'], $years, true)
        && $cachedTs
        && $cachedTs <= $now
      ) {
        return $cached;
      }

      delete_transient(self::SD_PARAMS_CACHE_KEY);
    }

    if ($forceRefresh) {
      delete_transient(self::SD_PARAMS_CACHE_KEY);
    }

    $fetchedByYear = [];
    foreach ($years as $year) {
      $fetchedByYear[(int)$year] = $this->fetch_sd_params_auto_from_official((int)$year);
    }

    $currentProbe = $fetchedByYear[$currentYear] ?? [];
    $active = $this->sd_active_candidate($fetchedByYear);
    $blockingError = null;

    // Se a fonte já expõe inequivocamente a competência corrente, mas o parser
    // não consegue validar sua estrutura, não mascaramos a mudança com a tabela anterior.
    if (
      empty($currentProbe['ok'])
      && !empty($currentProbe['target_year_evidence'])
    ) {
      $active = [];
      $blockingError = $currentProbe['error'] ?? 'current_year_evidence_unparseable';
    }

    // Corroboração independente para a competência do ano corrente.
    if (!empty($active) && (int)$active['reference_year'] === $currentYear) {
      $sm = $this->get_sm_effective();
      if ($this->sm_effective_is_current($sm)) {
        if (abs((float)$active['floor'] - (float)$sm['valor']) > 0.011) {
          $active = [];
          $blockingError = 'minimum_wage_crosscheck_mismatch';
        }
      }
    }

    if (!empty($active['ok'])) {
      $payload = [
        'ok' => true,
        'status' => 'current',
        'reference_year' => (int)$active['reference_year'],
        'effective_from' => (string)$active['effective_from'],
        'floor' => (float)$active['floor'],
        'first_band_limit' => (float)$active['first_band_limit'],
        'first_band_rate' => (float)$active['first_band_rate'],
        'second_band_limit' => (float)$active['second_band_limit'],
        'second_band_excess_rate' => (float)$active['second_band_excess_rate'],
        'second_band_base' => (float)$active['second_band_base'],
        'cap' => (float)$active['cap'],
        'source_url' => (string)$active['source_url'],
        'source' => 'official_auto',
        'error' => null,
        'fetched_at' => time(),
        'meta' => [
          'reference_year_candidates' => $years,
          'parse_rule' => $active['parse_rule'] ?? null,
          'attempts' => $active['attempts'] ?? null,
        ],
      ];

      update_option(self::SD_PARAMS_LAST_GOOD_OPTION, [
        'ts' => time(),
        'payload' => $payload,
      ], false);

      set_transient(self::SD_PARAMS_CACHE_KEY, $payload, $this->sd_params_ttl_success());
      return $payload;
    }

    $lastGood = get_option(self::SD_PARAMS_LAST_GOOD_OPTION);
    if (
      !$blockingError
      && $this->sd_params_last_good_is_fresh($lastGood)
      && isset($lastGood['payload'])
      && $this->sd_params_payload_shape_valid($lastGood['payload'])
      && !empty($lastGood['payload']['ok'])
      && in_array((int)$lastGood['payload']['reference_year'], $years, true)
    ) {
      $lastGoodEffectiveTs = !empty($lastGood['payload']['effective_from'])
        ? strtotime((string)$lastGood['payload']['effective_from'] . ' 00:00:00')
        : false;

      if ($lastGoodEffectiveTs && $lastGoodEffectiveTs <= $now) {
        $payload = $lastGood['payload'];
        $payload['source'] = 'last_good';
        $payload['error'] = 'official_sources_temporarily_unavailable';
        $payload['fetched_at'] = time();

        set_transient(self::SD_PARAMS_CACHE_KEY, $payload, $this->sd_params_ttl_fallback());
        return $payload;
      }
    }

    $payload = [
      'ok' => false,
      'status' => 'unavailable',
      'reference_year' => null,
      'effective_from' => null,
      'floor' => null,
      'first_band_limit' => null,
      'first_band_rate' => null,
      'second_band_limit' => null,
      'second_band_excess_rate' => null,
      'second_band_base' => null,
      'cap' => null,
      'source_url' => null,
      'source' => 'none',
      'error' => $blockingError ?: 'official_source_unavailable',
      'fetched_at' => time(),
      'meta' => [
        'reference_year_candidates' => $years,
        'current_probe_error' => $currentProbe['error'] ?? null,
        'current_probe_evidence' => !empty($currentProbe['target_year_evidence']),
      ],
    ];

    set_transient(self::SD_PARAMS_CACHE_KEY, $payload, $this->sd_params_ttl_fallback());
    return $payload;
  }

  public function sc_sd_parametros_json(): string {
    $payload = $this->get_sd_params_payload(false);
    $public = [
      'status' => !empty($payload['ok']) ? 'current' : 'unavailable',
      'reference_year' => isset($payload['reference_year']) ? (int)$payload['reference_year'] : null,
      'effective_from' => $payload['effective_from'] ?? null,
      'floor' => $payload['floor'] ?? null,
      'first_band_limit' => $payload['first_band_limit'] ?? null,
      'first_band_rate' => $payload['first_band_rate'] ?? null,
      'second_band_limit' => $payload['second_band_limit'] ?? null,
      'second_band_excess_rate' => $payload['second_band_excess_rate'] ?? null,
      'second_band_base' => $payload['second_band_base'] ?? null,
      'cap' => $payload['cap'] ?? null,
      'source_url' => $payload['source_url'] ?? null,
    ];

    if (empty($payload['ok'])) {
      $public['error'] = $payload['error'] ?? 'unavailable';
    }

    return wp_json_encode($public, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  }

  public function sc_sd_debug(): string {
    if (!current_user_can('manage_options')) {
      return '<em>Debug restrito ao administrador.</em>';
    }

    $payload = $this->get_sd_params_payload(false);
    $out = [
      'plugin_version_esperada' => '1.4.10',
      'reference_year_candidates' => $this->sd_reference_year_candidates(),
      'sd_params_payload' => $payload,
      'sd_params_transient' => get_transient(self::SD_PARAMS_CACHE_KEY),
      'sd_params_last_good' => get_option(self::SD_PARAMS_LAST_GOOD_OPTION),
      'sslverify' => $this->sd_params_sslverify(),
      'wp_http_block_external' => defined('WP_HTTP_BLOCK_EXTERNAL') ? WP_HTTP_BLOCK_EXTERNAL : '(não definido)',
      'wp_accessible_hosts' => defined('WP_ACCESSIBLE_HOSTS') ? WP_ACCESSIBLE_HOSTS : '(não definido)',
    ];

    return '<pre style="white-space:pre-wrap;word-break:break-word;background:#111;color:#eee;padding:12px;border-radius:8px;font-size:12px;line-height:1.45;">'
      . esc_html(print_r($out, true))
      . '</pre>';
  }

  /* ===== Salário mínimo (auto híbrido) ===== */

  private function sm_payload_shape_valid($payload): bool {
    return is_array($payload)
      && array_key_exists('ok', $payload)
      && array_key_exists('valor', $payload)
      && array_key_exists('vigencia', $payload)
      && array_key_exists('source', $payload)
      && array_key_exists('error', $payload)
      && array_key_exists('fetched_at', $payload)
      && array_key_exists('meta', $payload);
  }

  private function sm_is_value_sane($valor): bool {
    return is_numeric($valor) && (float)$valor >= 500 && (float)$valor <= 10000;
  }

  private function sm_parse_currency_from_text($txt): ?float {
    if (!is_string($txt) || $txt === '') return null;
    $v = $this->normalize_currency_input($txt);
    return $this->sm_is_value_sane($v) ? (float)$v : null;
  }

  private function sm_extract_from_gov_html(string $html, int $targetYear): array {
    $plain = wp_strip_all_tags($html, true);
    $plain = html_entity_decode($plain, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    $plain = preg_replace('/\s+/u', ' ', $plain);

    if (!is_string($plain) || $plain === '') {
      return ['ok' => false, 'error' => 'empty_official_page', 'valor' => null];
    }

    $year = preg_quote((string) $targetYear, '/');
    $markerPatterns = [
      '/tabelas?\s+v[aá]lidas?\s+a\s+partir\s+da\s+compet[eê]ncia\s+janeiro\s+de\s+' . $year . '/iu',
      '/compet[eê]ncia\s+janeiro\s+de\s+' . $year . '/iu',
    ];

    $offset = null;
    foreach ($markerPatterns as $pattern) {
      if (preg_match($pattern, $plain, $m, PREG_OFFSET_CAPTURE)) {
        $offset = (int) $m[0][1];
        break;
      }
    }

    if ($offset === null) {
      return ['ok' => false, 'error' => 'current_year_marker_missing', 'valor' => null];
    }

    $window = substr($plain, $offset, 12000);
    if (!is_string($window) || $window === '') {
      return ['ok' => false, 'error' => 'current_year_window_empty', 'valor' => null];
    }

    if (!preg_match(
      '/1\.\s*Para\s+Empregado,?\s*Empregado\s+Dom[eé]stico\s+e\s+Trabalhador\s+Avulso\s*:?\s*.*?At[eé]\s*R\$\s*([\d\.\,]+)/iu',
      $window,
      $m
    )) {
      return ['ok' => false, 'error' => 'employee_first_bracket_missing', 'valor' => null];
    }

    $valor = $this->sm_parse_currency_from_text($m[1]);
    if ($valor === null) {
      return ['ok' => false, 'error' => 'employee_first_bracket_invalid', 'valor' => null];
    }

    // Confirma o mesmo piso em duas linhas independentes da seção 2 (5% e 11%).
    // Se a estrutura previdenciária mudar, falha fechado em vez de assumir que
    // o teto da primeira faixa continua sendo automaticamente o salário mínimo.
    $confirmationPatterns = [
      '/2\.\s*Para\s+Contribuinte\s+Individual,?\s*Facultativo.*?R\$\s*([\d\.\,]+)\s*5\s*%/iu',
      '/2\.\s*Para\s+Contribuinte\s+Individual,?\s*Facultativo.*?R\$\s*[\d\.\,]+\s*5\s*%.*?R\$\s*([\d\.\,]+)\s*11\s*%/iu',
    ];
    $confirmedValues = [];
    foreach ($confirmationPatterns as $pattern) {
      if (!preg_match($pattern, $window, $mm) || !isset($mm[1])) {
        return ['ok' => false, 'error' => 'contributor_floor_confirmation_missing', 'valor' => null];
      }
      $confirmed = $this->sm_parse_currency_from_text($mm[1]);
      if ($confirmed === null) {
        return ['ok' => false, 'error' => 'contributor_floor_confirmation_invalid', 'valor' => null];
      }
      $confirmedValues[] = (float)$confirmed;
    }
    foreach ($confirmedValues as $confirmed) {
      if (abs((float)$confirmed - (float)$valor) >= 0.005) {
        return ['ok' => false, 'error' => 'contributor_floor_confirmation_mismatch', 'valor' => null];
      }
    }

    return [
      'ok' => true,
      'valor' => (float) $valor,
      'vigencia' => sprintf('%04d-01-01', $targetYear),
      'error' => null,
      'parse_rule' => 'inss_current_year_employee_bracket_plus_contributor_floor',
      'matches_count' => 1 + count($confirmedValues),
    ];
  }

  private function sm_get_candidate_urls(int $year): array {
    // Endpoint oficial estável do INSS; o parser exige marcador do ano corrente.
    $urls = [
      'https://www.gov.br/inss/pt-br/direitos-e-deveres/inscricao-e-contribuicao/tabela-de-contribuicao-mensal',
    ];

    $urls = apply_filters('dobr_sm_auto_urls', $urls, $year);
    $norm = [];
    foreach ((array)$urls as $u) {
      $u = trim((string)$u);
      if ($u !== '') $norm[] = $u;
    }
    return array_values(array_unique($norm));
  }


  /**
   * Busca automática do salário mínimo em gov.br com retry em 5xx.
   * @return array {ok, valor, vigencia, error, http_code, endpoint, attempts, body_sample, parse_rule, matches_count}
   */
  private function fetch_sm_auto_from_gov(int $year): array {
    if (!$this->sm_auto_enabled()) {
      return [
        'ok' => false,
        'valor' => null,
        'vigencia' => null,
        'error' => 'sm_auto_disabled',
        'http_code' => null,
        'endpoint' => null,
        'attempts' => 0,
        'body_sample' => null,
      ];
    }

    // Host whitelist check (quando WP_HTTP_BLOCK_EXTERNAL estiver ativo)
    if (!$this->can_request_host('www.gov.br') && !$this->can_request_host('gov.br')) {
      return [
        'ok' => false,
        'valor' => null,
        'vigencia' => null,
        'error' => 'external_http_blocked_for_gov.br',
        'http_code' => null,
        'endpoint' => null,
        'attempts' => 0,
        'body_sample' => null,
      ];
    }

    $urls = $this->sm_get_candidate_urls($year);

    $args = [
      'timeout'     => 25,
      'redirection' => 3,
      'sslverify'   => $this->sm_sslverify(),
      'headers'     => [
        'User-Agent' => 'Mozilla/5.0 (compatible; DOBR/1.4.10; WordPress)',
        'Accept'     => 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection' => 'keep-alive',
      ],
    ];

    $last_error = 'unknown_error';
    $last_code = null;
    $last_endpoint = null;
    $last_body_sample = null;
    $attempts_total = 0;

    foreach ($urls as $url) {
      $last_endpoint = $url;

      for ($i = 1; $i <= 3; $i++) {
        $attempts_total++;
        $res = wp_remote_get($url, $args);

        if (is_wp_error($res)) {
          $last_error = 'wp_error: ' . $res->get_error_message();
          $last_code = null;
          $last_body_sample = null;

          if ($i < 3) {
            usleep(300000 * $i);
            continue;
          }
          break;
        }

        $code = (int) wp_remote_retrieve_response_code($res);
        $body = (string) wp_remote_retrieve_body($res);

        $last_code = $code;
        $last_body_sample = $this->body_sample($body, 260);

        if ($code === 200) {
          $parsed = $this->sm_extract_from_gov_html($body, $year);

          if (!empty($parsed['ok']) && isset($parsed['valor'])) {
            return [
              'ok' => true,
              'valor' => (float)$parsed['valor'],
              'vigencia' => isset($parsed['vigencia']) ? (string)$parsed['vigencia'] : sprintf('%04d-01-01', $year),
              'error' => null,
              'http_code' => 200,
              'endpoint' => $url,
              'attempts' => $attempts_total,
              'body_sample' => null,
              'parse_rule' => isset($parsed['parse_rule']) ? $parsed['parse_rule'] : null,
              'matches_count' => isset($parsed['matches_count']) ? (int)$parsed['matches_count'] : null,
            ];
          }

          $last_error = isset($parsed['error']) ? (string)$parsed['error'] : 'parse_failed';
          // tenta próximo endpoint
          break;
        }

        $last_error = 'http_status_' . $code;

        // Retry só em 5xx
        if ($code >= 500 && $code < 600 && $i < 3) {
          usleep(400000 * $i);
          continue;
        }

        // 4xx / última tentativa deste endpoint
        break;
      }
    }

    return [
      'ok' => false,
      'valor' => null,
      'vigencia' => null,
      'error' => $last_error,
      'http_code' => $last_code,
      'endpoint' => $last_endpoint,
      'attempts' => $attempts_total,
      'body_sample' => $last_body_sample,
    ];
  }

  /**
   * Payload efetivo do salário mínimo:
   * auto_api -> last_good -> history_auto -> manual_settings -> seed
   */
  private function get_sm_payload(bool $forceRefresh = false): array {
    $currentYear = (int) current_time('Y');
    $cache = get_transient(self::SM_CACHE_KEY);
    if (!$forceRefresh && $this->sm_payload_shape_valid($cache)) {
      $cacheYear = 0;
      if (isset($cache['meta']['current_year']) && is_numeric($cache['meta']['current_year'])) {
        $cacheYear = (int)$cache['meta']['current_year'];
      } elseif (isset($cache['vigencia']) && preg_match('/^(\d{4})-/', (string)$cache['vigencia'], $cm)) {
        $cacheYear = (int)$cm[1];
      }
      if ($cacheYear === $currentYear) return $cache;
      // Virada anual: não conserva transient do ano anterior nem estado "none" antigo.
      delete_transient(self::SM_CACHE_KEY);
    }

    if ($forceRefresh) {
      delete_transient(self::SM_CACHE_KEY);
    }

    $manual = $this->get_manual_sm_config();
    $yearMap = $this->get_sm_year_map();

    $ok = false;
    $valor = null;
    $vigencia = null;
    $source = 'none';
    $error = null;
    $meta = [
      'current_year' => $currentYear,
      'manual_year' => (int) $manual['year'],
      'manual_valor' => (float) $manual['valor'],
    ];

    // 1) AUTO API (gov.br) para ano corrente
    $resAuto = $this->fetch_sm_auto_from_gov($currentYear);
    if (!empty($resAuto['ok']) && $this->sm_is_value_sane($resAuto['valor'])) {
      $valor = (float) $resAuto['valor'];
      $vigencia = preg_match('/^\d{4}-\d{2}-\d{2}$/', (string)$resAuto['vigencia']) ? (string)$resAuto['vigencia'] : sprintf('%04d-01-01', $currentYear);
      $ok = true;
      $source = 'api_auto';

      $meta['api_endpoint'] = $resAuto['endpoint'];
      $meta['api_attempts'] = $resAuto['attempts'];
      if (isset($resAuto['parse_rule'])) $meta['api_parse_rule'] = $resAuto['parse_rule'];
      if (isset($resAuto['matches_count'])) $meta['api_matches_count'] = $resAuto['matches_count'];

      update_option(self::SM_LAST_GOOD_OPTION, [
        'valor' => (float)$valor,
        'vigencia' => (string)$vigencia,
        'year' => (int) substr($vigencia, 0, 4),
        'ts' => time(),
      ], false);

      // Opcional: sincroniza config manual automaticamente
      if ($this->sm_auto_sync_settings()) {
        $currOpt = get_option(self::OPT, []);
        if (!is_array($currOpt)) $currOpt = [];
        $currOpt['valor'] = (float)$valor;
        $currOpt['vigencia'] = (string)$vigencia;
        update_option(self::OPT, $currOpt, false);
      }
    } else {
      $error = 'sm_auto_failed';
      $meta['api_error'] = isset($resAuto['error']) ? $resAuto['error'] : 'unknown';
      $meta['api_http_code'] = isset($resAuto['http_code']) ? $resAuto['http_code'] : null;
      $meta['api_endpoint'] = isset($resAuto['endpoint']) ? $resAuto['endpoint'] : null;
      $meta['api_attempts'] = isset($resAuto['attempts']) ? $resAuto['attempts'] : 0;
      if (!empty($resAuto['body_sample'])) $meta['api_body_sample'] = $resAuto['body_sample'];

      if (defined('WP_HTTP_BLOCK_EXTERNAL') && WP_HTTP_BLOCK_EXTERNAL) {
        $meta['wp_http_block_external'] = 1;
        $meta['wp_accessible_hosts'] = defined('WP_ACCESSIBLE_HOSTS') ? (string)WP_ACCESSIBLE_HOSTS : '';
      }
    }

    // 2) last_good (auto)
    if (!$ok) {
      $lg = get_option(self::SM_LAST_GOOD_OPTION);
      if (
        is_array($lg)
        && isset($lg['valor'], $lg['vigencia'])
        && $this->sm_is_value_sane($lg['valor'])
        && preg_match('/^\d{4}-\d{2}-\d{2}$/', (string)$lg['vigencia'])
        && (int) substr((string)$lg['vigencia'], 0, 4) === $currentYear
      ) {
        $valor = (float)$lg['valor'];
        $vigencia = (string)$lg['vigencia'];
        $ok = true;
        $source = 'last_good';

        if (isset($lg['year']) && is_numeric($lg['year'])) $meta['last_good_year'] = (int)$lg['year'];
        if (isset($lg['ts']) && is_numeric($lg['ts'])) $meta['last_good_ts'] = (int)$lg['ts'];
      }
    }

    // 3) history_auto (se já houver valor para o ano corrente no histórico/manual consolidado)
    if (!$ok) {
      $key = (string)$currentYear;
      if (isset($yearMap[$key]) && $this->sm_is_value_sane($yearMap[$key])) {
        $valor = (float)$yearMap[$key];
        $vigencia = sprintf('%04d-01-01', $currentYear);
        $ok = true;
        $source = 'history_auto';
      }
    }

    // 4) manual_settings (config plugin)
    if (!$ok) {
      if ($this->sm_is_value_sane($manual['valor']) && preg_match('/^\d{4}-\d{2}-\d{2}$/', (string)$manual['vigencia']) && (int)$manual['year'] === $currentYear) {
        $valor = (float)$manual['valor'];
        $vigencia = (string)$manual['vigencia'];
        $ok = true;
        $source = 'manual_settings';
      }
    }

    // 5) seed
    if (!$ok) {
      $seedValor = $this->sm_seed_valor();
      $seedVig = $this->sm_seed_vigencia();
      if ($this->sm_is_value_sane($seedValor) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $seedVig) && (int) substr($seedVig, 0, 4) === $currentYear) {
        $valor = (float)$seedValor;
        $vigencia = $seedVig;
        $ok = true;
        $source = 'seed';
      }
    }

    $payload = [
      'ok'        => (bool)$ok,
      'valor'     => $this->sm_is_value_sane($valor) ? (float)$valor : null,
      'vigencia'  => (is_string($vigencia) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $vigencia)) ? $vigencia : null,
      'source'    => $source, // api_auto | last_good | history_auto | manual_settings | seed | none
      'error'     => $error,
      'fetched_at'=> time(),
      'meta'      => $meta,
    ];

    // Arquiva pelo ano da vigência; nunca relabelled pelo ano do servidor.
    if (
      !empty($payload['ok'])
      && isset($payload['valor'], $payload['vigencia'])
      && $this->sm_is_value_sane($payload['valor'])
      && preg_match('/^(\d{4})-\d{2}-\d{2}$/', (string)$payload['vigencia'], $ym)
    ) {
      $payloadYear = (int) $ym[1];
      if ($payloadYear === $currentYear) {
        $hist = $this->get_history();
        $key = (string) $payloadYear;
        if (!isset($hist[$key]) || abs((float)$hist[$key] - (float)$payload['valor']) > 0.0001) {
          $hist[$key] = (float)$payload['valor'];
          $this->set_history($hist);
        }
      }
    }

    $ttl = ($payload['source'] === 'api_auto') ? $this->sm_ttl_success() : $this->sm_ttl_fallback();
    set_transient(self::SM_CACHE_KEY, $payload, $ttl);

    $this->log('sm payload atualizado', [
      'source' => $payload['source'],
      'ok' => $payload['ok'],
      'error' => $payload['error'],
      'ttl' => $ttl,
      'meta' => $payload['meta'],
    ]);

    return $payload;
  }

  private function get_sm_effective(): array {
    $p = $this->get_sm_payload(false);

    // Garante estrutura mínima para os shortcodes
    if (!is_array($p)) {
      return ['valor' => 0.0, 'vigencia' => '1970-01-01', 'source' => 'none'];
    }

    return [
      'valor' => (isset($p['valor']) && is_numeric($p['valor'])) ? (float)$p['valor'] : 0.0,
      'vigencia' => (isset($p['vigencia']) && preg_match('/^\d{4}-\d{2}-\d{2}$/', (string)$p['vigencia'])) ? (string)$p['vigencia'] : '1970-01-01',
      'source' => isset($p['source']) ? (string)$p['source'] : 'none',
      'payload' => $p,
    ];
  }

  /* ===== Shortcodes salário mínimo etc. (agora usam payload efetivo) ===== */

  private function sm_effective_is_current(array $sm): bool {
    if (!isset($sm['valor'], $sm['vigencia']) || !$this->sm_is_value_sane($sm['valor'])) return false;
    if (!preg_match('/^(\d{4})-\d{2}-\d{2}$/', (string)$sm['vigencia'], $m)) return false;
    return (int)$m[1] === (int)current_time('Y');
  }

  private function sm_unavailable_text(): string {
    return '—';
  }

  private function sm_ratio_round_up_cent(float $valor, int $divisor): float {
    if ($divisor <= 0) return 0.0;
    $cents = (int) ceil((($valor * 100.0) / $divisor) - 1e-12);
    return $cents / 100.0;
  }

  public function sc_sm_valor($atts=[], $content=''): string {
    $a = shortcode_atts(['format'=>'br'], $atts);
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    return esc_html($this->money_format($sm['valor'], $a['format']));
  }

  public function sc_sm_valor_raw(): string {
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    return esc_html($this->money_format($sm['valor'], 'raw'));
  }

  public function sc_sm_vigencia(): string {
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    $ts = strtotime((string)$sm['vigencia']);
    if (!$ts) return esc_html($this->sm_unavailable_text());
    return esc_html(date_i18n(get_option('date_format','d/m/Y'), $ts));
  }

  public function sc_sm_dia($atts=[]): string {
    $a = shortcode_atts(['dias'=>'', 'format'=>'br'], $atts);
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    $dDefault = (int)apply_filters('dobr_days_per_month', 30);
    $d = (int)($a['dias'] !== '' ? $a['dias'] : $dDefault);
    if ($d <= 0) $d = $dDefault;
    $v = $this->sm_ratio_round_up_cent((float)$sm['valor'], $d);
    return esc_html($this->money_format($v, $a['format']));
  }

  public function sc_sm_hora($atts=[]): string {
    $a = shortcode_atts(['horas'=>'', 'format'=>'br'], $atts);
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    $hDefault = (int)apply_filters('dobr_hours_per_month', 220);
    $h = (int)($a['horas'] !== '' ? $a['horas'] : $hDefault);
    if ($h <= 0) $h = $hDefault;
    $v = $this->sm_ratio_round_up_cent((float)$sm['valor'], $h);
    return esc_html($this->money_format($v, $a['format']));
  }

  public function sc_pis_valor($atts=[]): string {
    $a = shortcode_atts(['meses'=>'0', 'format'=>'br'], $atts);
    $m = max(0, min(12, (int)$a['meses']));
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    $v = $m === 0 ? 0.0 : (float) ceil(((float)$sm['valor'] / 12.0) * $m - 1e-12);
    return esc_html($this->money_format($v, $a['format']));
  }

  public function sc_sd_min_parcela($atts=[]): string {
    $a = shortcode_atts(['format'=>'br'], $atts);
    $sm = $this->get_sm_effective();
    if (!$this->sm_effective_is_current($sm)) return esc_html($this->sm_unavailable_text());
    return esc_html($this->money_format($sm['valor'], $a['format']));
  }

  public function sc_sm_serie_json(): string {
    $hist = $this->get_history();
    $sm = $this->get_sm_effective();
    if ($this->sm_effective_is_current($sm) && preg_match('/^(\d{4})-/', (string)$sm['vigencia'], $m)) {
      $hist[(string)((int)$m[1])] = (float)$sm['valor'];
    }
    ksort($hist, SORT_NUMERIC);
    return wp_json_encode($hist, JSON_UNESCAPED_UNICODE);
  }

  public function sc_sm_debug($atts=[]): string {
    if (!current_user_can('manage_options')) {
      return '<em>Debug restrito ao administrador.</em>';
    }

    $payload = $this->get_sm_payload(false);

    $out = [
      'plugin_version_esperada' => '1.4.10',
      'sm_payload' => $payload,
      'sm_transient' => get_transient(self::SM_CACHE_KEY),
      'sm_last_good_option' => get_option(self::SM_LAST_GOOD_OPTION),
      'manual_settings_option' => get_option(self::OPT),
      'manual_settings_normalized' => $this->get_manual_sm_config(),
      'sm_year_map' => $this->get_sm_year_map(),
      'cron_next_run' => wp_next_scheduled(self::CRON_HOOK_SM_REFRESH),
      'sm_auto_enabled' => $this->sm_auto_enabled(),
      'sm_sslverify' => $this->sm_sslverify(),
      'wp_http_block_external' => defined('WP_HTTP_BLOCK_EXTERNAL') ? WP_HTTP_BLOCK_EXTERNAL : '(não definido)',
      'wp_accessible_hosts' => defined('WP_ACCESSIBLE_HOSTS') ? WP_ACCESSIBLE_HOSTS : '(não definido)',
    ];

    return '<pre style="white-space:pre-wrap;word-break:break-word;background:#111;color:#eee;padding:12px;border-radius:8px;font-size:12px;line-height:1.45;">'
      . esc_html(print_r($out, true))
      . '</pre>';
  }

  /* ===== Desemprego PNAD Contínua via IpeaData (v1.4.2+) ===== */

  private function desemprego_cache_top(int $top): int {
    return max(1, min((int) $top, 24));
  }

  private function desemprego_cache_key(int $top): string {
    return self::DESEMPREGO_CACHE_KEY . '_v2_' . $this->desemprego_cache_top($top);
  }

  private function desemprego_cache_json_compat_key(int $top): string {
    return self::DESEMPREGO_CACHE_KEY_JSON_COMPAT . '_v2_' . $this->desemprego_cache_top($top);
  }

  private function clear_desemprego_cache(): void {
    delete_transient(self::DESEMPREGO_CACHE_KEY);
    delete_transient(self::DESEMPREGO_CACHE_KEY_JSON_COMPAT);

    for ($i = 1; $i <= 24; $i++) {
      delete_transient($this->desemprego_cache_key($i));
      delete_transient($this->desemprego_cache_json_compat_key($i));
    }
  }

  private function get_admin_refresh_url(array $args = []): string {
    $url = add_query_arg($args, admin_url('edit.php'));
    return wp_nonce_url($url, 'dobr_refresh_cache', 'dobr_nonce');
  }


  /**
   * Busca série no IpeaData com retry em 5xx e diagnóstico.
   * @return array {ok,data,error,http_code,endpoint,attempts,body_sample}
   */
  private function fetch_ipea_series(string $sercode, int $top=5): array {
    $top = max(1, min($top, 24));

    if (!$this->can_request_host('www.ipeadata.gov.br')) {
      return [
        'ok' => false,
        'data' => [],
        'error' => 'external_http_blocked_for_www.ipeadata.gov.br',
        'http_code' => null,
        'endpoint' => null,
        'attempts' => 0,
        'body_sample' => null,
      ];
    }

    $args = [
      'timeout'     => 25,
      'redirection' => 3,
      'sslverify'   => $this->desemprego_sslverify(),
      'headers'     => [
        'User-Agent' => 'Mozilla/5.0 (compatible; DOBR/1.4.10; WordPress)',
        'Accept'     => 'application/json, text/plain, */*',
        'Connection' => 'keep-alive',
      ],
    ];

    // Endpoints equivalentes / encoded-safe
    $urls = [
      "https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='" . rawurlencode($sercode) . "')?\$orderby=VALDATA%20desc&\$top={$top}",
      "https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{$sercode}')?\$orderby=VALDATA%20desc&\$top={$top}",
    ];

    $last_error = 'unknown_error';
    $last_code = null;
    $last_body = null;
    $last_endpoint = null;
    $attempts_total = 0;

    foreach ($urls as $url) {
      $last_endpoint = $url;

      for ($i = 1; $i <= 3; $i++) {
        $attempts_total++;
        $res = wp_remote_get($url, $args);

        if (is_wp_error($res)) {
          $last_error = 'wp_error: ' . $res->get_error_message();
          $last_code = null;
          $last_body = null;
          if ($i < 3) {
            usleep(300000 * $i);
            continue;
          }
          break;
        }

        $code = (int) wp_remote_retrieve_response_code($res);
        $body = (string) wp_remote_retrieve_body($res);
        $last_code = $code;
        $last_body = $this->body_sample($body, 260);

        if ($code === 200) {
          $json = json_decode($body, true);
          $rows = is_array($json) && isset($json['value']) && is_array($json['value']) ? $json['value'] : [];

          if (!$rows) {
            $last_error = 'empty_or_invalid_json';
            break;
          }

          $out = [];
          foreach ($rows as $r) {
            $raw = isset($r['VALVALOR']) ? (string) $r['VALVALOR'] : '';
            $val = is_numeric($raw) ? (float) $raw : null;
            $dt  = isset($r['VALDATA']) ? strtotime((string) $r['VALDATA']) : null;
            if ($val === null || !$dt) continue;
            $label = date_i18n('M/Y', $dt); // trimestre móvel encerrado no mês
            $out[(int) $dt] = [
              'label' => $label,
              'value' => (float) number_format($val, 1, '.', ''),
            ];
          }

          if (!$out) {
            $last_error = 'parsed_rows_empty';
            break;
          }

          ksort($out, SORT_NUMERIC); // força ordem cronológica ascendente
          if ($top > 0 && count($out) > $top) {
            $out = array_slice($out, -$top, null, true);
          }

          $ordered = [];
          foreach ($out as $item) {
            if (!is_array($item) || !isset($item['label'])) continue;
            $ordered[(string) $item['label']] = isset($item['value']) ? (float) $item['value'] : null;
          }

          return [
            'ok' => true,
            'data' => $ordered,
            'error' => null,
            'http_code' => 200,
            'endpoint' => $url,
            'attempts' => $attempts_total,
            'body_sample' => null,
          ];
        }

        $last_error = 'http_status_' . $code;

        // Retry apenas para 5xx
        if ($code >= 500 && $code < 600 && $i < 3) {
          usleep(400000 * $i);
          continue;
        }

        // 4xx ou última tentativa deste endpoint
        break;
      }
    }

    return [
      'ok' => false,
      'data' => [],
      'error' => $last_error,
      'http_code' => $last_code,
      'endpoint' => $last_endpoint,
      'attempts' => $attempts_total,
      'body_sample' => $last_body,
    ];
  }

  private function is_payload_shape_valid($payload): bool {
    return is_array($payload)
      && array_key_exists('ok', $payload)
      && array_key_exists('series', $payload)
      && array_key_exists('source', $payload)
      && array_key_exists('error', $payload)
      && array_key_exists('fetched_at', $payload)
      && array_key_exists('meta', $payload);
  }

  /**
   * Core do desemprego com cache + fallback (api -> last_good)
   */
  private function get_desemprego_payload(int $top = 5): array {
    $top = $this->desemprego_cache_top($top);
    $cache_key = $this->desemprego_cache_key($top);
    $compat_key = $this->desemprego_cache_json_compat_key($top);

    $payload = get_transient($cache_key);
    if ($this->is_payload_shape_valid($payload)) {
      if (($payload['source'] ?? '') !== 'last_good') return $payload;
      $cachedLastGoodTs = $payload['meta']['last_good_ts'] ?? null;
      if ($this->desemprego_last_good_is_fresh(['ts' => $cachedLastGoodTs])) return $payload;
      delete_transient($cache_key);
      delete_transient($compat_key);
      $this->log('stale desemprego fallback cache invalidated');
    }

    // Invalida caches legados fixos da 1.4.3-
    $legacy_payload = get_transient(self::DESEMPREGO_CACHE_KEY);
    if ($this->is_payload_shape_valid($legacy_payload)) {
      delete_transient(self::DESEMPREGO_CACHE_KEY);
      $this->log('legacy desemprego payload cache invalidated');
    }

    $legacy_json = get_transient(self::DESEMPREGO_CACHE_KEY_JSON_COMPAT);
    if (is_string($legacy_json) && $legacy_json !== '') {
      delete_transient(self::DESEMPREGO_CACHE_KEY_JSON_COMPAT);
      $this->log('legacy desemprego json cache invalidated');
    }

    $ok = false;
    $series = [];
    $source = 'none';
    $error = null;
    $meta = [];

    $res = $this->fetch_ipea_series('PNADC12_TDESOC12', $top);
    if ($res['ok']) {
      $series = $res['data'];
      $ok = true;
      $source = 'api';
      $meta['endpoint'] = $res['endpoint'];
      $meta['attempts'] = $res['attempts'];
      $meta['top'] = $top;

      update_option(self::DESEMPREGO_LAST_GOOD_OPTION, [
        'series' => $series,
        'ts' => time(),
        'top' => $top,
      ], false);
    } else {
      $error = 'api_failed';
      $meta['api_error'] = $res['error'];
      $meta['http_code'] = $res['http_code'];
      $meta['endpoint'] = $res['endpoint'];
      $meta['attempts'] = $res['attempts'];
      $meta['top'] = $top;
      if (!empty($res['body_sample'])) $meta['body_sample'] = $res['body_sample'];

      if (defined('WP_HTTP_BLOCK_EXTERNAL') && WP_HTTP_BLOCK_EXTERNAL) {
        $meta['wp_http_block_external'] = 1;
        $meta['wp_accessible_hosts'] = defined('WP_ACCESSIBLE_HOSTS') ? (string) WP_ACCESSIBLE_HOSTS : '';
      }
    }

    // Fallback: último dado bom real, somente dentro da janela máxima de frescor.
    if (!$ok) {
      $lg = get_option(self::DESEMPREGO_LAST_GOOD_OPTION);
      if (
        $this->desemprego_last_good_is_fresh($lg)
        && isset($lg['series'])
        && is_array($lg['series'])
        && !empty($lg['series'])
      ) {
        $series = $lg['series'];
        if ($top > 0 && count($series) > $top) {
          $series = array_slice($series, -$top, null, true);
        }
        $ok = true;
        $source = 'last_good';
        $meta['last_good_ts'] = (int)$lg['ts'];
        $meta['last_good_age_seconds'] = max(0, time() - (int)$lg['ts']);
        $meta['last_good_max_age_seconds'] = $this->desemprego_last_good_max_age();
        if (isset($lg['top']) && is_numeric($lg['top'])) $meta['last_good_top'] = (int) $lg['top'];
      } elseif (is_array($lg) && !empty($lg['series'])) {
        $meta['last_good_stale'] = 1;
        $meta['last_good_ts'] = isset($lg['ts']) && is_numeric($lg['ts']) ? (int)$lg['ts'] : null;
        $meta['last_good_max_age_seconds'] = $this->desemprego_last_good_max_age();
      }
    }

    $payload = [
      'ok'        => (bool) $ok,
      'series'    => is_array($series) ? $series : [],
      'source'    => $source,
      'error'     => $error,
      'fetched_at'=> time(),
      'meta'      => $meta,
    ];

    $ttl = ($source === 'api') ? $this->desemprego_ttl_success() : $this->desemprego_ttl_fallback();
    set_transient($cache_key, $payload, $ttl);

    // Compatibilidade: mantém transient JSON por faixa top
    set_transient($compat_key, wp_json_encode($payload['series'], JSON_UNESCAPED_UNICODE), $ttl);

    $this->log('desemprego payload atualizado', [
      'source' => $source,
      'ok' => $payload['ok'],
      'error' => $error,
      'ttl' => $ttl,
      'cache_key' => $cache_key,
      'meta' => $meta,
    ]);

    return $payload;
  }

  public function sc_desemprego_serie_json($atts=[]): string {
    $a = shortcode_atts(['top' => 5], $atts);
    $top = (int) $a['top'];
    $payload = $this->get_desemprego_payload($top);
    $series = isset($payload['series']) && is_array($payload['series']) ? $payload['series'] : [];
    if (!$series) return '{}';

    $parts = [];
    foreach ($series as $label => $value) {
      if (!is_numeric($value)) continue;
      $parts[] = wp_json_encode((string) $label, JSON_UNESCAPED_UNICODE) . ':' . number_format((float) $value, 1, '.', '');
    }

    return '{' . implode(',', $parts) . '}';
  }

  public function sc_desemprego_valor($atts=[]): string {
    $a = shortcode_atts(['decimais' => 1], $atts);
    $dec = (int) $a['decimais'];
    if ($dec < 0) $dec = 0;
    if ($dec > 4) $dec = 4;

    $payload = $this->get_desemprego_payload(5);
    if (!is_array($payload) || empty($payload['series']) || !is_array($payload['series'])) {
      return esc_html('—');
    }

    $lastVal = end($payload['series']);
    if (!is_numeric($lastVal)) return esc_html('—');

    return esc_html(number_format((float)$lastVal, $dec, ',', '.') . '%');
  }

  public function sc_desemprego_debug($atts=[]): string {
    if (!current_user_can('manage_options')) {
      return '<em>Debug restrito ao administrador.</em>';
    }

    $a = shortcode_atts(['top' => 5], $atts);
    $top = (int) $a['top'];
    $payload = $this->get_desemprego_payload($top);

    $out = [
      'plugin_version_esperada' => '1.4.10',
      'desemprego_payload' => $payload,
      'transient_payload' => get_transient($this->desemprego_cache_key($top)),
      'transient_payload_key' => $this->desemprego_cache_key($top),
      'compat_transient_json' => get_transient($this->desemprego_cache_json_compat_key($top)),
      'compat_transient_json_key' => $this->desemprego_cache_json_compat_key($top),
      'legacy_transient_payload' => get_transient(self::DESEMPREGO_CACHE_KEY),
      'legacy_transient_json' => get_transient(self::DESEMPREGO_CACHE_KEY_JSON_COMPAT),
      'last_good_option' => get_option(self::DESEMPREGO_LAST_GOOD_OPTION),
      'wp_http_block_external' => defined('WP_HTTP_BLOCK_EXTERNAL') ? WP_HTTP_BLOCK_EXTERNAL : '(não definido)',
      'wp_accessible_hosts' => defined('WP_ACCESSIBLE_HOSTS') ? WP_ACCESSIBLE_HOSTS : '(não definido)',
      'sslverify' => $this->desemprego_sslverify(),
    ];

    return '<pre style="white-space:pre-wrap;word-break:break-word;background:#111;color:#eee;padding:12px;border-radius:8px;font-size:12px;line-height:1.45;">'
      . esc_html(print_r($out, true))
      . '</pre>';
  }

  /* ===== Admin ===== */

  public function admin_menu() {
    add_options_page('Dados Oficiais BR','Dados Oficiais BR','manage_options','dobr',[$this,'render_page']);
  }

  public function admin_init() {
    register_setting(self::OPT, self::OPT, [
      'type' => 'array',
      'sanitize_callback' => function($input){
        $out = [];
        $out['valor'] = isset($input['valor']) ? $this->normalize_currency_input($input['valor']) : 0.0;
        $out['vigencia'] = isset($input['vigencia']) ? trim(sanitize_text_field($input['vigencia'])) : '';

        if ($out['vigencia'] !== '' && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $out['vigencia'])) {
          $t = strtotime($out['vigencia']);
          $out['vigencia'] = $t ? date('Y-m-d', $t) : '';
        }
        return $out;
      }
    ]);

    add_settings_section('dobr_main','', '__return_false', 'dobr');

    add_settings_field('dobr_valor','Salário mínimo (R$)', function(){
      $s = $this->get_settings();
      printf(
        '<input type="text" name="%s[valor]" value="%s" class="regular-text" />',
        esc_attr(self::OPT),
        esc_attr(((float)$s['valor'] > 0) ? number_format((float)$s['valor'],2,',','.') : '')
      );
      echo '<p class="description">Fallback manual opcional. Só é aceito como valor atual quando a vigência pertence ao ano corrente; normalmente a fonte oficial é suficiente.</p>';
    }, 'dobr', 'dobr_main');

    add_settings_field('dobr_vigencia','Vigência (YYYY-mm-dd)', function(){
      $s = $this->get_settings();
      printf(
        '<input type="text" name="%s[vigencia]" value="%s" class="regular-text" />',
        esc_attr(self::OPT),
        esc_attr($s['vigencia'])
      );
      echo '<p class="description">Vigência do fallback manual opcional. Pode permanecer vazia quando a automação oficial estiver funcionando.</p>';
    }, 'dobr', 'dobr_main');
  }

  public function on_update_settings($old_value, $value, $option) {
    // Arquiva automaticamente o ano anterior ao trocar de vigência
    $old_year = isset($old_value['vigencia']) ? (int) substr((string)$old_value['vigencia'],0,4) : 0;
    $new_year = isset($value['vigencia'])     ? (int) substr((string)$value['vigencia'],0,4)     : 0;

    if ($old_year && $new_year && $new_year !== $old_year) {
      $hist = $this->get_history();
      $key = (string)$old_year;
      if (!isset($hist[$key])) {
        $val_ant = isset($old_value['valor']) ? (float)$this->normalize_currency_input($old_value['valor']) : 0.0;
        if ($val_ant > 0) {
          $hist[$key] = $val_ant;
          $this->set_history($hist);
        }
      }
    }

    // Limpa cache do salário mínimo para refletir mudança manual imediatamente
    delete_transient(self::SM_CACHE_KEY);
  }

  public function admin_handle_clear_cache(): void {
    if (!current_user_can('manage_options')) return;

    $has_desemprego_refresh = isset($_GET['dobr_force_refresh']) && sanitize_text_field(wp_unslash($_GET['dobr_force_refresh'])) === '1';
    $has_sm_refresh = isset($_GET['dobr_sm_force_refresh']) && sanitize_text_field(wp_unslash($_GET['dobr_sm_force_refresh'])) === '1';

    if (!$has_desemprego_refresh && !$has_sm_refresh) return;

    check_admin_referer('dobr_refresh_cache', 'dobr_nonce');

    $did_any = false;
    $cleared_d_last = false;
    $cleared_sm_last = false;

    // Desemprego
    if ($has_desemprego_refresh) {
      $this->clear_desemprego_cache();
      $did_any = true;

      if (isset($_GET['dobr_clear_last_good']) && sanitize_text_field(wp_unslash($_GET['dobr_clear_last_good'])) === '1') {
        delete_option(self::DESEMPREGO_LAST_GOOD_OPTION);
        $cleared_d_last = true;
      }
    }

    // Salário mínimo
    if ($has_sm_refresh) {
      delete_transient(self::SM_CACHE_KEY);
      $did_any = true;

      if (isset($_GET['dobr_sm_clear_last_good']) && sanitize_text_field(wp_unslash($_GET['dobr_sm_clear_last_good'])) === '1') {
        delete_option(self::SM_LAST_GOOD_OPTION);
        $cleared_sm_last = true;
      }

      // Opcional: já força uma tentativa imediata (útil no clique)
      $this->get_sm_payload(true);
    }

    if (!$did_any) return;

    $url = add_query_arg([
      'dobr_cleared' => '1',
      'dobr_cleared_d_last' => $cleared_d_last ? '1' : '0',
      'dobr_cleared_sm_last' => $cleared_sm_last ? '1' : '0',
    ], admin_url('edit.php'));

    wp_safe_redirect($url);
    exit;
  }

  public function admin_notices(): void {
    if (!current_user_can('manage_options')) return;
    if (!isset($_GET['dobr_cleared']) || $_GET['dobr_cleared'] !== '1') return;

    $parts = ['caches atualizados.'];
    if (isset($_GET['dobr_cleared_d_last']) && sanitize_text_field(wp_unslash($_GET['dobr_cleared_d_last'])) === '1') {
      $parts[] = 'Last-good de desemprego apagado.';
    }
    if (isset($_GET['dobr_cleared_sm_last']) && sanitize_text_field(wp_unslash($_GET['dobr_cleared_sm_last'])) === '1') {
      $parts[] = 'Last-good de salário mínimo apagado.';
    }

    echo '<div class="notice notice-success is-dismissible"><p><strong>DOBR:</strong> ' . esc_html(implode(' ', $parts)) . '</p></div>';

    // Aviso amigável se vigência manual estiver atrás do ano corrente
    $manual = $this->get_manual_sm_config();
    $currYear = (int) current_time('Y');
    if ($manual['year'] > 0 && $manual['year'] < $currYear) {
      echo '<div class="notice notice-warning is-dismissible"><p>'
         . '<strong>DOBR:</strong> a configuração manual do salário mínimo está em '
         . esc_html((string)$manual['year'])
         . '. Essa configuração será ignorada como valor atual; a automação continua consultando a fonte oficial.'
         . '</p></div>';
    }
  }

  public function admin_bar_menu($wp_admin_bar): void {
    if (!is_admin_bar_showing() || !current_user_can('manage_options')) return;

    // Nó principal
    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh',
      'title' => 'DOBR: Atualizar',
      'href' => $this->get_admin_refresh_url([
        'dobr_force_refresh' => '1',
      ]),
      'meta' => ['title' => 'Atualizar cache (desemprego)'],
    ]);

    // Desemprego
    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh_desemprego',
      'parent' => 'dobr_refresh',
      'title' => 'Desemprego (cache)',
      'href' => $this->get_admin_refresh_url([
        'dobr_force_refresh' => '1',
      ]),
      'meta' => ['title' => 'Limpar cache de desemprego'],
    ]);

    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh_desemprego_full',
      'parent' => 'dobr_refresh',
      'title' => 'Desemprego: Forçar do zero',
      'href' => $this->get_admin_refresh_url([
        'dobr_force_refresh' => '1',
        'dobr_clear_last_good' => '1',
      ]),
      'meta' => ['title' => 'Limpar cache + last_good de desemprego'],
    ]);

    // Salário mínimo
    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh_sm',
      'parent' => 'dobr_refresh',
      'title' => 'Salário mínimo (auto)',
      'href' => $this->get_admin_refresh_url([
        'dobr_sm_force_refresh' => '1',
      ]),
      'meta' => ['title' => 'Atualizar salário mínimo (auto + fallback)'],
    ]);

    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh_sm_full',
      'parent' => 'dobr_refresh',
      'title' => 'Salário mínimo: Forçar do zero',
      'href' => $this->get_admin_refresh_url([
        'dobr_sm_force_refresh' => '1',
        'dobr_sm_clear_last_good' => '1',
      ]),
      'meta' => ['title' => 'Limpar cache + last_good do salário mínimo'],
    ]);

    // Tudo
    $wp_admin_bar->add_node([
      'id' => 'dobr_refresh_all',
      'parent' => 'dobr_refresh',
      'title' => 'Tudo (cache)',
      'href' => $this->get_admin_refresh_url([
        'dobr_force_refresh' => '1',
        'dobr_sm_force_refresh' => '1',
      ]),
      'meta' => ['title' => 'Atualizar desemprego + salário mínimo'],
    ]);
  }

  public function render_page() {
    if (!current_user_can('manage_options')) return;

    $smPayload = $this->get_sm_payload(false);
    $smSource = isset($smPayload['source']) ? (string)$smPayload['source'] : 'none';
    $smValor = isset($smPayload['valor']) && is_numeric($smPayload['valor']) ? $this->money_format($smPayload['valor'], 'br') : '—';
    $smVig = isset($smPayload['vigencia']) ? $smPayload['vigencia'] : '—';
    ?>
    <div class="wrap">
      <h1>Dados Oficiais BR</h1>

      <form method="post" action="options.php">
        <?php settings_fields(self::OPT); do_settings_sections('dobr'); submit_button(); ?>
      </form>

      <hr>

      <h2>Status do Salário Mínimo (v1.4.10)</h2>
      <p><strong>Valor efetivo atual:</strong> <?php echo esc_html($smValor); ?></p>
      <p><strong>Vigência efetiva:</strong> <?php echo esc_html($smVig); ?></p>
      <p><strong>Origem:</strong> <code><?php echo esc_html($smSource); ?></code> (api_auto | last_good | history_auto | manual_settings | seed | none)</p>
      <p><strong>Cache:</strong> <code><?php echo esc_html(self::SM_CACHE_KEY); ?></code> | <strong>Last-good:</strong> <code><?php echo esc_html(self::SM_LAST_GOOD_OPTION); ?></code></p>
      <p><strong>Cron hook:</strong> <code><?php echo esc_html(self::CRON_HOOK_SM_REFRESH); ?></code> (diário)</p>
      <p><strong>Atalhos na barra superior:</strong> <em>DOBR: Atualizar → Salário mínimo (auto)</em></p>
      <p><strong>Debug (admin):</strong> <code>[sm_debug]</code></p>

      <hr>

      <h2>Série histórica (Salário Mínimo)</h2>
      <p>O plugin completa anos-base se estiver vazio e inclui o ano atual dinamicamente no shortcode <code>[sm_serie_json]</code> com base no valor efetivo (auto/fallback).</p>
      <pre style="background:#0b1220;color:#e5e7eb;padding:8px;border-radius:6px;overflow:auto"><?php
        echo esc_html( wp_json_encode( $this->get_history(), JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE ) );
      ?></pre>

      <hr>

      <h2>Desemprego (PNAD Contínua / IpeaData)</h2>
      <p><strong>Cache:</strong> <code><?php echo esc_html(self::DESEMPREGO_CACHE_KEY); ?>_N</code> (payload por top) + <code><?php echo esc_html(self::DESEMPREGO_CACHE_KEY_JSON_COMPAT); ?>_N</code> (compat por top).</p>
      <p><strong>Atalho:</strong> <em>DOBR: Atualizar → Desemprego (cache)</em></p>
      <p><strong>Debug (admin):</strong> <code>[desemprego_debug]</code></p>

      <hr>

      <h2>Shortcodes</h2>
      <p>
        <code>[sm_valor]</code>, <code>[sm_valor_raw]</code>, <code>[sm_vigencia]</code>, <code>[sm_hora]</code>, <code>[sm_dia]</code>,
        <code>[pis_valor meses="6"]</code>, <code>[sd_min_parcela]</code>, <code>[sm_serie_json]</code>,
        <code>[desemprego_serie_json]</code>, <code>[desemprego_valor]</code>,
        <code>[sm_debug]</code>, <code>[desemprego_debug]</code>
      </p>
    </div>
    <?php
  }
}

// Seed inicial do histórico (SM)
register_activation_hook(__FILE__, function(){
  if (get_option(DOBR_Plugin::OPT_HIST, null) === null) {
    add_option(DOBR_Plugin::OPT_HIST, ['2021'=>1100.00,'2022'=>1212.00,'2023'=>1320.00,'2024'=>1412.00,'2025'=>1518.00], '', false);
  }
});

// Agenda cron ao ativar (se possível)
register_activation_hook(__FILE__, function(){
  if (!wp_next_scheduled(DOBR_Plugin::CRON_HOOK_SM_REFRESH)) {
    wp_schedule_event(time() + 600, 'daily', DOBR_Plugin::CRON_HOOK_SM_REFRESH);
  }
});

// Remove cron ao desativar (se plugin padrão)
register_deactivation_hook(__FILE__, function(){
  $ts = wp_next_scheduled(DOBR_Plugin::CRON_HOOK_SM_REFRESH);
  if ($ts) wp_unschedule_event($ts, DOBR_Plugin::CRON_HOOK_SM_REFRESH);
});

new DOBR_Plugin();

/* ===== Shortcodes também na META (SEO) ===== */
function dobr_do_shortcode_meta($s){
  if (is_string($s) && strpos($s,'[')!==false) {
    $s = do_shortcode($s);
    $s = wp_strip_all_tags($s, true);
  }
  return $s;
}

add_filter('wpseo_metadesc','dobr_do_shortcode_meta',11);
add_filter('wpseo_title','dobr_do_shortcode_meta',11);
add_filter('wpseo_opengraph_desc','dobr_do_shortcode_meta',11);
add_filter('wpseo_twitter_description','dobr_do_shortcode_meta',11);

add_filter('rank_math/frontend/description','dobr_do_shortcode_meta',11);
add_filter('rank_math/frontend/title','dobr_do_shortcode_meta',11);
add_filter('rank_math/opengraph/facebook/description','dobr_do_shortcode_meta',11);
add_filter('rank_math/opengraph/twitter/description','dobr_do_shortcode_meta',11);

add_filter('seopress_titles_desc','dobr_do_shortcode_meta',11);
add_filter('seopress_social_og_desc','dobr_do_shortcode_meta',11);

add_filter('aioseo_description','dobr_do_shortcode_meta',11);
add_filter('aioseo_title','dobr_do_shortcode_meta',11);
add_filter('aioseo_twitter_description','dobr_do_shortcode_meta',11);

add_filter('the_seo_framework_title_from_custom_field','dobr_do_shortcode_meta',11);
add_filter('the_seo_framework_description_from_custom_field','dobr_do_shortcode_meta',11);

// Opt-in: evitar execução global de shortcodes em excerpts/widgets.
// Para reativar conscientemente, defina as constantes abaixo como true no wp-config.php.
if (defined('DOBR_ENABLE_SHORTCODES_IN_EXCERPTS') && DOBR_ENABLE_SHORTCODES_IN_EXCERPTS) {
  add_filter('the_excerpt','do_shortcode');
}

if (defined('DOBR_ENABLE_SHORTCODES_IN_WIDGETS') && DOBR_ENABLE_SHORTCODES_IN_WIDGETS) {
  add_filter('widget_text','do_shortcode');
  add_filter('widget_text_content','do_shortcode');
}