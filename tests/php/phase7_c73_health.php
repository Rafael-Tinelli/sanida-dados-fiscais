<?php
/** C7.3 health endpoint harness using the production fiscal traits. */
define('ABSPATH', __DIR__);
define('HOUR_IN_SECONDS', 3600);
define('MINUTE_IN_SECONDS', 60);

$GLOBALS['c73_transients'] = [];
$GLOBALS['c73_options'] = [];
$GLOBALS['c73_network'] = [];

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
final class C73_REST_Response {
  public $data;
  public $headers = [];
  public function __construct($data){ $this->data = $data; }
  public function header($name, $value){ $this->headers[$name] = $value; }
}

function wp_json_encode($value, $flags = 0, $depth = 512){ return json_encode($value, $flags, $depth); }
function is_wp_error($value){ return $value instanceof WP_Error; }
function rest_ensure_response($data){ return new C73_REST_Response($data); }
function register_rest_route($namespace, $route, $args){ return true; }
function __return_true(){ return true; }
function get_transient($key){ return array_key_exists($key, $GLOBALS['c73_transients']) ? $GLOBALS['c73_transients'][$key] : false; }
function set_transient($key, $value, $ttl){ $GLOBALS['c73_transients'][$key] = $value; return true; }
function delete_transient($key){ unset($GLOBALS['c73_transients'][$key]); return true; }
function get_option($key){ return $GLOBALS['c73_options'][$key] ?? false; }
function update_option($key, $value, $autoload = false){ $GLOBALS['c73_options'][$key] = $value; return true; }
function delete_option($key){ unset($GLOBALS['c73_options'][$key]); return true; }

function wp_remote_get($url, $args = []){
  if (!isset($GLOBALS['c73_network'][$url]) || !$GLOBALS['c73_network'][$url]) {
    return new WP_Error('c73_unplanned_network', 'No planned response for '.$url);
  }
  $response = array_shift($GLOBALS['c73_network'][$url]);
  if (isset($response['error'])) return new WP_Error('c73_network_error', (string)$response['error']);
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
  fwrite(STDERR, "usage: php phase7_c73_health.php <contract> <network> <admin> <manifest> <artifact> <expected-release-id>\n");
  exit(2);
}

require $argv[1];
require $argv[2];
require $argv[3];

final class C73_Fiscal_Runtime {
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
  const OPT_KNOWN_SUCCESSOR = 'sfa_fiscal_v12_known_successor';
  const TTL_SUCCESS = 12 * HOUR_IN_SECONDS;
  const TTL_FAIL = 15 * MINUTE_IN_SECONDS;

  use Sanida_Fiscais_Fiscal_Contract_Trait;
  use Sanida_Fiscais_Fiscal_Network_Trait;
  use Sanida_Fiscais_Admin_Debug_Trait;

  public function health(){ return $this->rest_get_fiscal_health(new WP_REST_Request()); }
}

function c73_plan($url, $responses){ $GLOBALS['c73_network'][$url] = $responses; }
function c73_summary($response){
  if ($response instanceof WP_Error) {
    return [
      'kind' => 'error',
      'code' => $response->code,
      'status' => $response->data['status'] ?? null,
      'health' => $response->data['health'] ?? null,
    ];
  }
  return [
    'kind' => 'response',
    'data' => $response->data,
    'headers' => $response->headers,
  ];
}

$manifest_raw = file_get_contents($argv[4]);
$artifact_raw = file_get_contents($argv[5]);
$manifest = json_decode($manifest_raw, true);
$artifact = json_decode($artifact_raw, true);
if (!is_array($manifest) || !is_array($artifact)) throw new RuntimeException('invalid release fixtures');
$expected = $argv[6];
if (($manifest['release_id'] ?? null) !== $expected || ($artifact['release_id'] ?? null) !== $expected) {
  throw new RuntimeException('release fixture identity mismatch');
}

$current_url = C73_Fiscal_Runtime::CURRENT_JSON_URL_DEFAULT;
$artifact_url = C73_Fiscal_Runtime::RELEASE_BASE_URL_DEFAULT . ltrim($manifest['artifact'], '/');
$runtime = new C73_Fiscal_Runtime();

c73_plan($current_url, [[
  'code' => 200,
  'body' => $manifest_raw,
  'headers' => ['etag' => '"c73-current"'],
]]);
c73_plan($artifact_url, [[
  'code' => 200,
  'body' => $artifact_raw,
  'headers' => [],
]]);
$healthy = c73_summary($runtime->health());

delete_transient(C73_Fiscal_Runtime::T_CACHE);
c73_plan($current_url, [['error' => 'simulated canonical source outage']]);
$degraded = c73_summary($runtime->health());

delete_transient(C73_Fiscal_Runtime::T_CACHE);
update_option(C73_Fiscal_Runtime::OPT_KNOWN_SUCCESSOR, [
  'release_id' => 'fiscal-v1-sha256-'.str_repeat('f', 64),
  'artifact_sha256' => str_repeat('e', 64),
  'observed_at_utc' => gmdate('c'),
], false);
c73_plan($current_url, [['error' => 'simulated outage with known successor']]);
$blocked = c73_summary($runtime->health());

delete_transient(C73_Fiscal_Runtime::T_CACHE);
delete_option(C73_Fiscal_Runtime::OPT_LAST_GOOD);
delete_option(C73_Fiscal_Runtime::OPT_KNOWN_SUCCESSOR);
delete_option(C73_Fiscal_Runtime::OPT_CURRENT_ETAG);
c73_plan($current_url, [['error' => 'simulated total outage']]);
$unavailable = c73_summary($runtime->health());

print(json_encode([
  'checkpoint' => 'C7.3',
  'expected_release_id' => $expected,
  'healthy' => $healthy,
  'degraded' => $degraded,
  'blocked' => $blocked,
  'unavailable' => $unavailable,
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
