<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Fiscal_Contract_Trait {

  private function required_rule_ids(){
    return [
      'inss.employee.progressive_table',
      'irrf.deductions_by_income_type',
      'irrf.dependent_deduction',
      'irrf.income_type',
      'irrf.monthly.progressive_table',
      'irrf.reduction.2026',
      'irrf.simplified_monthly_discount',
      'technical.contract_vigency_and_quality',
      'technical.money_decimal_and_rounding',
      'termination.acquired_and_overdue_vacation',
      'termination.partial_output_scope',
      'termination.reason_scope',
      'termination.salary_balance',
      'termination.thirteenth_proportional',
      'termination.vacation_indemnity_tax_treatment',
      'termination.vacation_proportional',
      'thirteenth.accrual.twelfths',
      'thirteenth.advance',
      'thirteenth.inss.separate_assessment',
      'thirteenth.irrf.exclusive_assessment',
      'thirteenth.irrf.reduction.2026',
      'thirteenth.reference_remuneration',
      'thirteenth.variable_remuneration',
      'vacation.abono.ir_exemption',
      'vacation.abono_constitutional_third.ir_incidence',
      'vacation.abono_pecuniario',
      'vacation.acquisition_period',
      'vacation.entitlement_days_by_absences',
      'vacation.inss.enjoyed',
      'vacation.irrf.reduction.2026',
      'vacation.irrf.separate_assessment',
      'vacation.remuneration_and_constitutional_third',
    ];
  }

  private function supported_payload_types(){
    return [
      'progressive_table', 'scalar', 'affine_reduction', 'threshold_accrual',
      'fraction', 'entitlement_bands', 'eligibility_matrix', 'incidence_profile',
      'policy', 'remuneration_reference', 'variable_remuneration',
      'thirteenth_advance', 'period_rule', 'component_formula',
      'scope_declaration', 'proration', 'code_eligibility', 'period_state',
    ];
  }

  private function valid_iso_date($value){
    if (!is_string($value) || !preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $value, $m)) return false;
    return checkdate((int)$m[2], (int)$m[3], (int)$m[1]);
  }

  private function current_fiscal_date(){
    $date = current_time('Y-m-d');
    return $this->valid_iso_date($date) ? $date : gmdate('Y-m-d');
  }

  private function rule_within_effective_window($rule, $as_of = null){
    if (!is_array($rule) || !isset($rule['vigency']) || !is_array($rule['vigency'])) return false;
    $vigency = $rule['vigency'];
    $effective_from = $vigency['effective_from'] ?? null;
    if (!$this->valid_iso_date($effective_from)) return false;

    $date = $as_of === null ? $this->current_fiscal_date() : $as_of;
    if (!$this->valid_iso_date($date)) return false;
    if ($date < $effective_from) return false;

    if (array_key_exists('effective_until', $vigency) && $vigency['effective_until'] !== null) {
      $effective_until = $vigency['effective_until'];
      if (!$this->valid_iso_date($effective_until)) return false;
      if ($date > $effective_until) return false;
    }
    return true;
  }

  private function release_within_effective_window($release, $as_of = null){
    if (!is_array($release) || !isset($release['rules']) || !is_array($release['rules'])) return false;
    $date = $as_of === null ? $this->current_fiscal_date() : $as_of;
    if (!$this->valid_iso_date($date)) return false;
    foreach ($release['rules'] as $rule) {
      if (!$this->rule_within_effective_window($rule, $date)) return false;
    }
    return true;
  }

  private function validate_manifest($m){
    if (!is_array($m)) return false;
    foreach (['artifact','artifact_sha256','contract_api_version','contract_id','published_at_utc','release_id','schema_version','schema_version_contract'] as $k) {
      if (!isset($m[$k]) || !is_string($m[$k]) || trim($m[$k]) === '') return false;
    }

    if ($m['schema_version'] !== self::SUPPORTED_MANIFEST_VERSION) return false;
    if ($m['schema_version_contract'] !== self::SUPPORTED_SCHEMA_VERSION) return false;
    if ($m['contract_api_version'] !== self::SUPPORTED_CONTRACT_API_VERSION) return false;
    if ($m['contract_id'] !== self::CONTRACT_ID) return false;
    if (!preg_match('/^fiscal-v1-sha256-[0-9a-f]{64}$/', $m['release_id'])) return false;
    if (!preg_match('/^[0-9a-f]{64}$/', $m['artifact_sha256'])) return false;
    if ($m['artifact'] !== 'releases/'.$m['release_id'].'.json') return false;
    if (strtotime($m['published_at_utc']) === false) return false;
    if (isset($m['supersedes_release_id']) && $m['supersedes_release_id'] !== null) {
      if (!is_string($m['supersedes_release_id']) || !preg_match('/^fiscal-v1-sha256-[0-9a-f]{64}$/', $m['supersedes_release_id'])) return false;
    }
    return true;
  }

  private function canonicalize_json_value($value){
    if (is_object($value)) {
      $vars = get_object_vars($value);
      ksort($vars, SORT_STRING);
      $out = new stdClass();
      foreach ($vars as $key => $item) {
        $out->{$key} = $this->canonicalize_json_value($item);
      }
      return $out;
    }
    if (is_array($value)) {
      $out = [];
      foreach ($value as $item) $out[] = $this->canonicalize_json_value($item);
      return $out;
    }
    return $value;
  }

  private function canonical_json_object($value){
    $json = wp_json_encode(
      $this->canonicalize_json_value($value),
      JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
    );
    return is_string($json) ? $json : null;
  }

  private function release_from_raw_body($raw_body){
    if (!is_string($raw_body) || $raw_body === '') return null;
    $decoded = json_decode($raw_body, true);
    return is_array($decoded) ? $decoded : null;
  }

  private function release_artifact_sha256($raw_body){
    return is_string($raw_body) && $raw_body !== '' ? hash('sha256', $raw_body) : null;
  }

  private function expected_release_id_from_raw_body($raw_body){
    if (!is_string($raw_body) || $raw_body === '') return null;
    $release = json_decode($raw_body);
    if (!is_object($release)) return null;

    $required = [
      'schema_version', 'contract_id', 'jurisdiction', 'source_registry_version',
      'rule_inventory_version', 'versioning_policy', 'consumer_compatibility',
      'last_good_policy', 'rules', 'governance_source_registry_version',
    ];
    foreach ($required as $key) {
      if (!property_exists($release, $key)) return null;
    }

    $payload = new stdClass();
    $payload->schema_version = $release->schema_version;
    $payload->contract_id = $release->contract_id;
    $payload->jurisdiction = $release->jurisdiction;
    $payload->source_registry_version = $release->source_registry_version;
    $payload->rule_inventory_version = $release->rule_inventory_version;
    $payload->versioning_policy = $release->versioning_policy;
    $payload->consumer_compatibility = $release->consumer_compatibility;
    $payload->supersedes_release_id = property_exists($release, 'supersedes_release_id')
      ? $release->supersedes_release_id
      : null;
    $payload->last_good_policy = $release->last_good_policy;
    $payload->rules = $release->rules;
    $payload->governance_source_registry_version = $release->governance_source_registry_version;

    $json = $this->canonical_json_object($payload);
    return is_string($json) ? 'fiscal-v1-sha256-'.hash('sha256', $json) : null;
  }

  private function validate_release($r, $manifest = null, $raw_body = null){
    if (!is_array($r)) return false;
    foreach (['schema_version','contract_id','release_id','status','consumer_compatibility','lifecycle','last_good_policy','rules'] as $k) {
      if (!array_key_exists($k, $r)) return false;
    }

    if ($r['schema_version'] !== self::SUPPORTED_SCHEMA_VERSION) return false;
    if ($r['contract_id'] !== self::CONTRACT_ID) return false;
    if (!is_string($r['release_id']) || !preg_match('/^fiscal-v1-sha256-[0-9a-f]{64}$/', $r['release_id'])) return false;
    $expected_release_id = $this->expected_release_id_from_raw_body($raw_body);
    if (!is_string($expected_release_id) || !hash_equals($expected_release_id, $r['release_id'])) return false;
    if ($r['status'] !== 'PUBLISHED') return false;

    $cc = $r['consumer_compatibility'];
    if (!is_array($cc)) return false;
    $expected_cc = [
      'schema_version' => self::SUPPORTED_SCHEMA_VERSION,
      'contract_api_version' => self::SUPPORTED_CONTRACT_API_VERSION,
      'schema_match' => 'exact',
      'contract_api_match' => 'exact',
      'unknown_fields' => 'reject',
      'unknown_payload_types' => 'reject',
      'unsupported_behavior' => 'hard_fail',
    ];
    foreach ($expected_cc as $k => $v) {
      if (($cc[$k] ?? null) !== $v) return false;
    }
    $consumers = $cc['consumers'] ?? null;
    if (!is_array($consumers) || array_values($consumers) !== ['H26','H27','H28','H29']) return false;

    $lifecycle = $r['lifecycle'];
    if (!is_array($lifecycle)) return false;
    if (!in_array($lifecycle['approval_mode'] ?? null, ['HUMAN_REVIEWED','AUTO_VALIDATED'], true)) return false;
    if (empty($lifecycle['approval_reference']) || !is_string($lifecycle['approval_reference'])) return false;
    if (empty($lifecycle['validated_at_utc']) || empty($lifecycle['published_at_utc'])) return false;
    if (!empty($lifecycle['block_reasons'])) return false;

    $lg = $r['last_good_policy'];
    if (!is_array($lg)) return false;
    if (($lg['allow_when_source_unavailable'] ?? null) !== true) return false;
    if (($lg['allow_relabel_historical_as_current'] ?? null) !== false) return false;
    if (($lg['on_expired'] ?? null) !== 'hard_fail') return false;
    if (($lg['on_unknown_vigency'] ?? null) !== 'hard_fail') return false;
    if (($lg['require_validated_rule'] ?? null) !== true) return false;
    if (($lg['require_within_effective_window'] ?? null) !== true) return false;
    if (($lg['require_no_known_successor'] ?? null) !== true) return false;

    if (!is_array($r['rules']) || count($r['rules']) !== 32) return false;
    $required = $this->required_rule_ids();
    $supported_payloads = $this->supported_payload_types();
    $seen = [];
    foreach ($r['rules'] as $rule) {
      if (!is_array($rule)) return false;
      $rule_id = $rule['rule_id'] ?? null;
      if (!is_string($rule_id) || !in_array($rule_id, $required, true) || isset($seen[$rule_id])) return false;
      $seen[$rule_id] = true;
      if (($rule['quality']['status'] ?? null) !== 'VALIDATED') return false;
      if (!isset($rule['payload']) || !is_array($rule['payload'])) return false;
      $payload_type = $rule['payload']['type'] ?? null;
      if (!is_string($payload_type) || !in_array($payload_type, $supported_payloads, true)) return false;
      if (empty($rule['provenance']) || !is_array($rule['provenance'])) return false;
      $has_hashed_available = false;
      foreach ($rule['provenance'] as $evidence) {
        if (!is_array($evidence)) continue;
        if (($evidence['status'] ?? null) === 'AVAILABLE' && preg_match('/^[0-9a-f]{64}$/', (string)($evidence['snapshot_sha256'] ?? ''))) {
          $has_hashed_available = true;
          break;
        }
      }
      if (!$has_hashed_available) return false;
    }
    if (count($seen) !== count($required)) return false;
    foreach ($required as $rule_id) {
      if (!isset($seen[$rule_id])) return false;
    }
    if (!$this->release_within_effective_window($r)) return false;

    if (is_array($manifest)) {
      if (!$this->validate_manifest($manifest)) return false;
      if ($manifest['release_id'] !== $r['release_id']) return false;
      if ($manifest['contract_id'] !== $r['contract_id']) return false;
      if ($manifest['schema_version_contract'] !== $r['schema_version']) return false;
      if (($r['consumer_compatibility']['contract_api_version'] ?? null) !== $manifest['contract_api_version']) return false;
    }

    return true;
  }

  private function validate_release_package($package){
    if (!is_array($package) || !isset($package['manifest'], $package['release'], $package['artifact_body'])) return false;
    if (!$this->validate_manifest($package['manifest'])) return false;
    $decoded = $this->release_from_raw_body($package['artifact_body']);
    if (!is_array($decoded) || $decoded !== $package['release']) return false;
    if (!$this->validate_release($decoded, $package['manifest'], $package['artifact_body'])) return false;
    $artifact_sha = $this->release_artifact_sha256($package['artifact_body']);
    return is_string($artifact_sha)
      && hash_equals($package['manifest']['artifact_sha256'], $artifact_sha);
  }

  private function package_allows_last_good($package, $known_successor = false){
    if (!$this->validate_release_package($package)) return false;
    $p = $package['release']['last_good_policy'] ?? [];
    if (!is_array($p)) return false;
    if (($p['allow_when_source_unavailable'] ?? null) !== true) return false;
    if (($p['allow_relabel_historical_as_current'] ?? null) !== false) return false;
    if (($p['on_expired'] ?? null) !== 'hard_fail') return false;
    if (($p['on_unknown_vigency'] ?? null) !== 'hard_fail') return false;
    if (($p['require_validated_rule'] ?? null) !== true) return false;
    if (($p['require_within_effective_window'] ?? null) !== true) return false;
    if (($p['require_no_known_successor'] ?? null) !== true) return false;
    if (!$this->release_within_effective_window($package['release'])) return false;
    if ($known_successor) return false;
    return true;
  }

}
