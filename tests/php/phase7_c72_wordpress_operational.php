<?php
/**
 * C7.2 operational harness for the real WordPress fiscal traits.
 *
 * It stubs only WordPress I/O primitives. Contract validation, cache/last-good
 * policy, successor detection and REST behavior execute the production traits.
 */

define('ABSPATH', __DIR__);
define('HOUR_IN_SECONDS', 3600);
define('MINUTE_IN_SECONDS', 60);

$GLOBALS['c72_transients'] = [];
$GLOBALS['c72_options'] = [];
$GLOBALS['c72_network'] = [];
$GLOBALS['c72_calls'] = [];

final class WP_Error {
  public $code;
  public $message;
  public $data;
  public function __construct($code, $message = '', $data = null){
    $this->code = $code;
    $this->message = $message;
    $this->data = $data;
  }
  public function get_error_message(){ return $this->message; }
}
final class WP_REST_Request {}
final class C72_REST_Response {
  public $data;
  public $headers = [];
  public function __construct($data){ $this->data = $data; }
  public function header($name, $value){ $this->headers[$name] = $value; }
}

function wp_json_encode($value, $flags = 0, $depth = 512){ return json_encode($value, $flags, $depth); }
function is_wp_error($value){ return $value instanceof WP_Error; }
function rest_ensure_response($data){ return new C72_REST_Response($data); }
function register_rest_route($namespace, $route, $args){ return true; }
function __return_true(){ return true; }

function get_transient($key){
  return array_key_exists($key, $GLOBALS['c72_transients']) ? $GLOBALS['c72_transients'][$key]['value'] : false;
}
function set_transient($key, $value, $ttl){
  $GLOBALS['c72_transients'][$key] = ['value' => $value, 'ttl' => (int)$ttl];
  return true;
}
function delete_transient($key){ unset($GLOBALS['c72_transients'][$key]); return true; }
function get_option($key){ return $GLOBALS['c72_options'][$key] ?? false; }
function update_option($key, $value, $autoload = false){ $GLOBALS['c72_options'][$key] = $value; return true; }
function delete_option($key){ unset($GLOBALS['c72_options'][$key]); return true; }

function wp_remote_get($url, $args = []){
  $GLOBALS['c72_calls'][] = ['url' => $url, 'headers' => $args['headers'] ?? []];
  if (!isset($GLOBALS['c72_network'][$url]) || !$GLOBALS['c72_network'][$url]) {
    return new WP_Error('c72_unplanned_network', 'No planned response for '.$url);
  }
  $response = array_shift($GLOBALS['c72_network'][$url]);
  if (isset($response['error'])) return new WP_Error('c72_network_error', (string)$response['error']);
  return [
    'response' => ['code' => (int)($response['code'] ?? 500)],
    'body' => (string)($response['body'] ?? ''),
    'headers' => $response['headers'] ?? [],
  ];
}
function wp_remote_retrieve_response_code($response){ return (int)($response['response']['code'] ?? 0); }
function wp_remote_retrieve_body($response){ return (string)($response['body'] ?? ''); }
function wp_remote_retrieve_header($response, $name){
  foreach (($response['headers'] ?? []) as $key => $value) {
    if (strcasecmp((string)$key, (string)$name) === 0) return $value;
  }
  return '';
}

if ($argc !== 7) {
  fwrite(STDERR, "usage: php phase7_c72_wordpress_operational.php <contract-trait> <network-trait> <previous-manifest> <previous-artifact> <current-manifest> <current-artifact>\n");
  exit(2);
}

require $argv[1];
require $argv[2];

final class C72_Fiscal_Runtime {
  const VERSION = '2.7.0';
  const CURRENT_JSON_URL_DEFAULT = 'https://fixture.invalid/fiscal-v1/current.json';
  const RELEASE_BASE_URL_DEFAULT = 'https://fixture.invalid/fiscal-v1/';
  const CONTRACT_ID = 'br.sanida.fiscal';
  const SUPPORTED_SCHEMA_VERSION = '1.2.0';
  const SUPPORTED_CONTRACT_API_VERSION = '1.2.0';
  const SUPPORTED_MANIFEST_VERSION = '1.0.0';
  const T_CACHE = 'sfa_fiscal_v12_cache';
  const OPT_LAST_GOOD = 'sfa_fiscal_v12_last_good';
  const OPT_CURRENT_ETAG = 'sfa_fiscal_v12_current_etag';
  const TTL_SUCCESS = 12 * HOUR_IN_SECONDS;
  const TTL_FAIL = 15 * MINUTE_IN_SECONDS;

  use Sanida_Fiscais_Fiscal_Contract_Trait;
  use Sanida_Fiscais_Fiscal_Network_Trait;

  public function package(){ return $this->get_release_package(); }
  public function rest(){ return $this->rest_get_fiscal_release(new WP_REST_Request()); }
  public function valid($package){ return $this->validate_release_package($package); }
}

function c72_json_file($path){
  $raw = file_get_contents($path);
  $json = json_decode($raw, true);
  if (!is_array($json)) throw new RuntimeException('invalid JSON fixture: '.$path);
  return [$raw, $json];
}
function c72_plan($url, $responses){ $GLOBALS['c72_network'][$url] = $responses; }
function c72_clear_calls(){ $GLOBALS['c72_calls'] = []; }
function c72_expire_transient(){ delete_transient(C72_Fiscal_Runtime::T_CACHE); }
function c72_release_id($package){ return is_array($package) ? ($package['release']['release_id'] ?? null) : null; }
function c72_origin($package){ return is_array($package) ? ($package['_runtime']['origin'] ?? null) : null; }
function c72_ttl(){ return $GLOBALS['c72_transients'][C72_Fiscal_Runtime::T_CACHE]['ttl'] ?? null; }
function c72_rest_summary($response){
  if ($response instanceof WP_Error) {
    return ['kind' => 'error', 'code' => $response->code, 'status' => $response->data['status'] ?? null];
  }
  return [
    'kind' => 'response',
    'release_id' => $response->data['release_id'] ?? null,
    'header_release_id' => $response->headers['X-Sanida-Fiscal-Release'] ?? null,
    'cache_control' => $response->headers['Cache-Control'] ?? null,
  ];
}

[$previousManifestRaw, $previousManifest] = c72_json_file($argv[3]);
[$previousArtifactRaw, $previousRelease] = c72_json_file($argv[4]);
[$currentManifestRaw, $currentManifest] = c72_json_file($argv[5]);
[$currentArtifactRaw, $currentRelease] = c72_json_file($argv[6]);

$currentUrl = C72_Fiscal_Runtime::CURRENT_JSON_URL_DEFAULT;
$previousArtifactUrl = C72_Fiscal_Runtime::RELEASE_BASE_URL_DEFAULT . ltrim($previousManifest['artifact'], '/');
$currentArtifactUrl = C72_Fiscal_Runtime::RELEASE_BASE_URL_DEFAULT . ltrim($currentManifest['artifact'], '/');
$runtime = new C72_Fiscal_Runtime();
$out = [
  'previous_release_id' => $previousRelease['release_id'],
  'current_release_id' => $currentRelease['release_id'],
  'scenarios' => [],
];

// 1. Cold start on the predecessor: fetch pointer + immutable artifact and seed last-good/cache/ETag.
c72_plan($currentUrl, [[
  'code' => 200,
  'body' => $previousManifestRaw,
  'headers' => ['etag' => '"etag-prev"'],
]]);
c72_plan($previousArtifactUrl, [[ 'code' => 200, 'body' => $previousArtifactRaw ]]);
c72_clear_calls();
$package = $runtime->package();
$out['scenarios']['cold_predecessor'] = [
  'valid' => $runtime->valid($package),
  'release_id' => c72_release_id($package),
  'origin' => c72_origin($package),
  'network_calls' => $GLOBALS['c72_calls'],
  'etag' => get_option(C72_Fiscal_Runtime::OPT_CURRENT_ETAG),
  'last_good_release_id' => c72_release_id(get_option(C72_Fiscal_Runtime::OPT_LAST_GOOD)),
  'transient_ttl' => c72_ttl(),
];

// 2. Shared transient must suppress network I/O until expiry.
c72_clear_calls();
$cached = $runtime->package();
$out['scenarios']['transient_cache'] = [
  'valid' => $runtime->valid($cached),
  'release_id' => c72_release_id($cached),
  'origin' => c72_origin($cached),
  'network_call_count' => count($GLOBALS['c72_calls']),
  'transient_ttl' => c72_ttl(),
];

// 3. After cache expiry, source outage with no known successor may use validated last-good.
c72_expire_transient();
c72_plan($currentUrl, [[ 'error' => 'simulated current.json outage' ]]);
c72_clear_calls();
$outageFallback = $runtime->package();
$out['scenarios']['source_outage_last_good'] = [
  'valid' => $runtime->valid($outageFallback),
  'release_id' => c72_release_id($outageFallback),
  'origin' => c72_origin($outageFallback),
  'known_successor' => $outageFallback['_runtime']['known_successor'] ?? null,
  'historical_release_relabelled_as_current' => $outageFallback['_runtime']['historical_release_relabelled_as_current'] ?? null,
  'last_fetch_error' => $outageFallback['_runtime']['last_fetch_error'] ?? null,
  'transient_ttl' => c72_ttl(),
];

// 4. A 304 with valid last-good is a safe no-change path.
c72_expire_transient();
c72_plan($currentUrl, [[ 'code' => 304, 'body' => '', 'headers' => ['etag' => '"etag-prev"'] ]]);
c72_clear_calls();
$notModified = $runtime->package();
$out['scenarios']['etag_304'] = [
  'valid' => $runtime->valid($notModified),
  'release_id' => c72_release_id($notModified),
  'origin' => c72_origin($notModified),
  'transient_ttl' => c72_ttl(),
  'request_headers' => $GLOBALS['c72_calls'][0]['headers'] ?? [],
];

// 5. Once current.json announces a successor, failure to fetch/verify it must block predecessor fallback.
c72_expire_transient();
c72_plan($currentUrl, [[
  'code' => 200,
  'body' => $currentManifestRaw,
  'headers' => ['etag' => '"etag-current"'],
]]);
c72_plan($currentArtifactUrl, [[ 'code' => 503, 'body' => 'artifact unavailable' ]]);
c72_clear_calls();
$blocked = $runtime->package();
$out['scenarios']['known_successor_artifact_failure'] = [
  'valid' => $runtime->valid($blocked),
  'release_id' => c72_release_id($blocked),
  'origin' => c72_origin($blocked),
  'known_successor' => $blocked['_runtime']['known_successor'] ?? null,
  'last_fetch_error' => $blocked['_runtime']['last_fetch_error'] ?? null,
  'last_good_still_predecessor' => c72_release_id(get_option(C72_Fiscal_Runtime::OPT_LAST_GOOD)),
  'network_calls' => $GLOBALS['c72_calls'],
  'rest' => c72_rest_summary($runtime->rest()),
];

// The REST call above may perform another fetch; isolate recovery from any unplanned network side effect.
c72_expire_transient();

// 6. Recovery: same successor pointer + valid immutable artifact promotes the successor and refreshes ETag/last-good.
c72_plan($currentUrl, [[
  'code' => 200,
  'body' => $currentManifestRaw,
  'headers' => ['etag' => '"etag-current"'],
]]);
c72_plan($currentArtifactUrl, [[ 'code' => 200, 'body' => $currentArtifactRaw ]]);
c72_clear_calls();
$recovered = $runtime->package();
$restRecovered = $runtime->rest();
$out['scenarios']['recovery_to_successor'] = [
  'valid' => $runtime->valid($recovered),
  'release_id' => c72_release_id($recovered),
  'origin' => c72_origin($recovered),
  'etag' => get_option(C72_Fiscal_Runtime::OPT_CURRENT_ETAG),
  'last_good_release_id' => c72_release_id(get_option(C72_Fiscal_Runtime::OPT_LAST_GOOD)),
  'transient_ttl' => c72_ttl(),
  'rest' => c72_rest_summary($restRecovered),
  'network_calls' => $GLOBALS['c72_calls'],
];
$out['recovered_release'] = $recovered['release'] ?? null;

// 7. After successor is last-good, a later origin outage preserves that exact successor without relabeling.
c72_expire_transient();
c72_plan($currentUrl, [[ 'error' => 'simulated post-recovery outage' ]]);
c72_clear_calls();
$postRecoveryFallback = $runtime->package();
$out['scenarios']['post_recovery_last_good'] = [
  'valid' => $runtime->valid($postRecoveryFallback),
  'release_id' => c72_release_id($postRecoveryFallback),
  'origin' => c72_origin($postRecoveryFallback),
  'historical_release_relabelled_as_current' => $postRecoveryFallback['_runtime']['historical_release_relabelled_as_current'] ?? null,
  'transient_ttl' => c72_ttl(),
];

// 8. A 304 without any valid last-good must fail closed instead of inventing data.
$GLOBALS['c72_transients'] = [];
$GLOBALS['c72_options'] = [];
c72_plan($currentUrl, [[ 'code' => 304, 'body' => '' ]]);
c72_clear_calls();
$empty304 = $runtime->package();
$out['scenarios']['etag_304_without_last_good'] = [
  'valid' => $runtime->valid($empty304),
  'release_id' => c72_release_id($empty304),
  'origin' => c72_origin($empty304),
  'last_fetch_error' => $empty304['_runtime']['last_fetch_error'] ?? null,
  'rest' => c72_rest_summary($runtime->rest()),
];

// 9. Corrupted last-good must not survive validation when the source is unavailable.
$GLOBALS['c72_transients'] = [];
$GLOBALS['c72_options'] = [C72_Fiscal_Runtime::OPT_LAST_GOOD => ['manifest' => $previousManifest, 'release' => $previousRelease, 'artifact_body' => '{}']];
c72_plan($currentUrl, [[ 'error' => 'simulated outage with corrupt last-good' ]]);
c72_clear_calls();
$corruptFallback = $runtime->package();
$out['scenarios']['corrupt_last_good'] = [
  'valid' => $runtime->valid($corruptFallback),
  'release_id' => c72_release_id($corruptFallback),
  'origin' => c72_origin($corruptFallback),
  'last_fetch_error' => $corruptFallback['_runtime']['last_fetch_error'] ?? null,
];

fwrite(STDOUT, json_encode($out, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
