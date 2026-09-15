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

  private function is_list_array($value){
    if (!is_array($value)) return false;
    $i = 0;
    foreach ($value as $k => $_) {
      if ($k !== $i) return false;
      $i++;
    }
    return true;
  }

  private function canonicalize_json_value($value){
    if (!is_array($value)) return $value;
    if ($this->is_list_array($value)) {
      $out = [];
      foreach ($value as $item) $out[] = $this->canonicalize_json_value($item);
      return $out;
    }
    ksort($value, SORT_STRING);
    $out = [];
    foreach ($value as $k => $item) $out[$k] = $this->canonicalize_json_value($item);
    return $out;
  }

  private function canonical_json($value){
    $json = wp_json_encode(
      $this->canonicalize_json_value($value),
      JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
    );
    return is_string($json) ? $json : null;
  }

  private function release_artifact_sha256($release){
    $json = $this->canonical_json($release);
    return is_string($json) ? hash('sha256', $json."\n") : null;
  }

  private function expected_release_id($release){
    if (!is_array($release)) return null;
    $required = [
      'schema_version', 'contract_id', 'jurisdiction', 'source_registry_version',
      'rule_inventory_version', 'versioning_policy', 'consumer_compatibility',
      'last_good_policy', 'rules', 'governance_source_registry_version',
    ];
    foreach ($required as $key) {
      if (!array_key_exists($key, $release)) return null;
    }
    $payload = [
      'schema_version' => $release['schema_version'],
      'contract_id' => $release['contract_id'],
      'jurisdiction' => $release['jurisdiction'],
      'source_registry_version' => $release['source_registry_version'],
      'rule_inventory_version' => $release['rule_inventory_version'],
      'versioning_policy' => $release['versioning_policy'],
      'consumer_compatibility' => $release['consumer_compatibility'],
      'supersedes_release_id' => array_key_exists('supersedes_release_id', $release)
        ? $release['supersedes_release_id']
        : null,
      'last_good_policy' => $release['last_good_policy'],
      'rules' => $release['rules'],
      'governance_source_registry_version' => $release['governance_source_registry_version'],
    ];
    $json = $this->canonical_json($payload);
    return is_string($json) ? 'fiscal-v1-sha256-'.hash('sha256', $json) : null;
  }

  private function validate_release($r, $manifest = null){
    if (!is_array($r)) return false;
    foreach (['schema_version','contract_id','release_id','status','consumer_compatibility','lifecycle','last_good_policy','rules'] as $k) {
      if (!array_key_exists($k, $r)) return false;
    }

    if ($r['schema_version'] !== self::SUPPORTED_SCHEMA_VERSION) return false;
    if ($r['contract_id'] !== self::CONTRACT_ID) return false;
    if (!is_string($r['release_id']) || !preg_match('/^fiscal-v1-sha256-[0-9a-f]{64}$/', $r['release_id'])) return false;
    $expected_release_id = $this->expected_release_id($r);
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
    if (!is_array($package) || !isset($package['manifest'], $package['release'])) return false;
    if (!$this->validate_manifest($package['manifest'])
        || !$this->validate_release($package['release'], $package['manifest'])) return false;
    $artifact_sha = $this->release_artifact_sha256($package['release']);
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
    if ($known_successor) return false;
    return true;
  }

}
