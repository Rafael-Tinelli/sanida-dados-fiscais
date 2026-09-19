from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
CONTRACT_TRAIT = PLUGIN.parent / "includes/trait-sanida-fiscal-contract.php"
TAXAS_TRAIT = PLUGIN.parent / "includes/trait-sanida-taxas.php"
STORE_ROOT = ROOT / "releases/fiscal-v1"
TAXAS = ROOT / "taxas_bacen.json"


def _php() -> str | None:
    return shutil.which("php")


def test_v271_contract_enforces_effective_window_at_year_boundary() -> None:
    php = _php()
    if php is None:
        return

    manifest = json.loads((STORE_ROOT / "current.json").read_text(encoding="utf-8"))
    artifact = STORE_ROOT / manifest["artifact"]

    harness = r"""<?php
    define('ABSPATH', __DIR__);
    function current_time($type) {
      return $type === 'Y-m-d' ? '2026-09-19' : time();
    }
    require $argv[1];

    final class V271_Contract_Harness {
      use Sanida_Fiscais_Fiscal_Contract_Trait;
      public function within($release, $date) {
        return $this->release_within_effective_window($release, $date);
      }
    }

    $release = json_decode(file_get_contents($argv[2]), true);
    $h = new V271_Contract_Harness();
    echo json_encode([
      '2026-12-31' => $h->within($release, '2026-12-31'),
      '2027-01-01' => $h->within($release, '2027-01-01'),
    ]);
    ?>"""

    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "contract-window.php"
        script.write_text(harness, encoding="utf-8")
        completed = subprocess.run(
            [php, str(script), str(CONTRACT_TRAIT), str(artifact)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert json.loads(completed.stdout) == {
        "2026-12-31": True,
        "2027-01-01": False,
    }


def test_v271_cdi_freshness_boundary_and_persistent_selic() -> None:
    php = _php()
    if php is None:
        return

    harness = r"""<?php
    define('ABSPATH', __DIR__);
    const DAY_IN_SECONDS = 86400;
    $GLOBALS['v271_today'] = '2026-09-19';
    function current_time($type) {
      return $type === 'Y-m-d' ? $GLOBALS['v271_today'] : time();
    }
    require $argv[1];

    final class V271_Taxas_Harness {
      const CDI_MAX_OBSERVATION_AGE_DAYS = 7;
      use Sanida_Fiscais_Taxas_Trait;
      public function fresh($source, $date) {
        return $this->cdi_observation_is_fresh($source, $date);
      }
      public function valid($payload) {
        return $this->validate_taxas_payload($payload);
      }
    }

    $payload = json_decode(file_get_contents($argv[2]), true);
    $h = new V271_Taxas_Harness();
    $cdi = $payload['meta']['sources']['cdi'];

    $GLOBALS['v271_today'] = '2026-09-19';
    $selic_old = $payload;
    $selic_old['meta']['sources']['selic']['source_observation_date'] = '2020-01-01';

    $cdi_stale = $payload;
    $cdi_stale['meta']['sources']['cdi']['source_observation_date'] = '2026-09-11';

    echo json_encode([
      'age7' => $h->fresh($cdi, '2026-09-24'),
      'age8' => $h->fresh($cdi, '2026-09-25'),
      'selic_old_still_valid' => $h->valid($selic_old),
      'stale_cdi_rejected' => !$h->valid($cdi_stale),
    ]);
    ?>"""

    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "taxas-freshness.php"
        script.write_text(harness, encoding="utf-8")
        completed = subprocess.run(
            [php, str(script), str(TAXAS_TRAIT), str(TAXAS)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert json.loads(completed.stdout) == {
        "age7": True,
        "age8": False,
        "selic_old_still_valid": True,
        "stale_cdi_rejected": True,
    }


def test_v271_consumer_has_no_numeric_rate_fallbacks() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [PLUGIN, *sorted((PLUGIN.parent / "includes").glob("*.php"))]
    )
    assert "Version:     2.7.1" in text
    for forbidden in (
        "'selic' => 15.00",
        "'cdi'   => 14.90",
        "isset($d['taxas']['selic']) ? (float)$d['taxas']['selic'] : 0",
        "isset($d['taxas']['cdi']) ? (float)$d['taxas']['cdi'] : 0",
    ):
        assert forbidden not in text
