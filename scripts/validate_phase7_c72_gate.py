#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from scripts.run_phase7_c72_operational_e2e import run_operational_e2e

STORE = ROOT / "releases/fiscal-v1"
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
NETWORK = ROOT / "consumers/wordpress/includes/trait-sanida-fiscal-network.php"
ADMIN = ROOT / "consumers/wordpress/includes/trait-sanida-admin-debug.php"
DOC = ROOT / "docs/phase7-c72-evergreen-e2e.md"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
PUBLICATION_WORKFLOW = ROOT / ".github/workflows/fiscal-release-v12.yml"
DEPLOYMENT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.2 gate failed: {message}")


def main() -> int:
    for path in (PLUGIN, NETWORK, ADMIN, DOC, README, CI, PUBLICATION_WORKFLOW, DEPLOYMENT_MANIFEST):
        require(path.is_file(), f"required C7.2 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current schema is not 1.2.0")
    require(release.consumer_compatibility.contract_api_version == "1.2.0", "current API is not 1.2.0")
    require(len(release.rules) == 32, "current release is not 32/32")
    require(isinstance(release.supersedes_release_id, str) and bool(release.supersedes_release_id), "current release lacks predecessor for C7.2 successor proof")

    deployment = json.loads(DEPLOYMENT_MANIFEST.read_text(encoding="utf-8"))
    require(deployment.get("production_deployed") is False, "C7.2 must remain pre-deploy")
    require(deployment.get("status") == "ready_not_deployed", "C7.1 deployment manifest status drift")

    plugin_text = PLUGIN.read_text(encoding="utf-8")
    network = NETWORK.read_text(encoding="utf-8")
    admin = ADMIN.read_text(encoding="utf-8")
    for marker in (
        "Version:     2.8.0",
        "OPT_KNOWN_SUCCESSOR",
        "sfa_fiscal_v12_known_successor",
        "/releases/fiscal-v1/current.json",
        "RELEASE_BASE_URL_DEFAULT",
    ):
        require(marker in plugin_text, f"plugin missing C7.2 marker: {marker}")

    for marker in (
        "known_successor_state",
        "persist_known_successor",
        "known_successor_blocks_package",
        "resolve_known_successor_with_verified_package",
        "current_manifest_regressed_behind_known_successor",
        "current_304_with_known_successor",
        "known_successor_release_id",
        "origin' => (string)$origin",
        "'transient_cache'",
        "cached_from_origin",
    ):
        require(marker in network, f"network runtime missing C7.2 safety/observability marker: {marker}")

    clear_cache_block = re.search(
        r"public function clear_cache\(\).*?public function sc_debug\(",
        admin,
        flags=re.S,
    )
    require(clear_cache_block is not None, "could not inspect administrative cache-refresh boundary")
    assert clear_cache_block is not None
    require(
        "delete_option(self::OPT_KNOWN_SUCCESSOR)" not in clear_cache_block.group(0),
        "manual cache refresh erases known-successor safety latch",
    )
    require("'fiscais_known_successor' => $this->known_successor_state()" in admin, "debug does not expose known successor state")

    publication = PUBLICATION_WORKFLOW.read_text(encoding="utf-8")
    for marker in (
        'schedule:',
        'cron: "40 9 * * *"',
        'group: sanida-dados-fiscais-writes-main',
        'python scripts/publish_fiscal_release_v12.py',
        'git pull --rebase origin main',
        'git push origin main',
        "Human review packet persisted",
    ):
        require(marker in publication, f"publication workflow missing evergreen marker: {marker}")
    require("--scheduled" in publication, "scheduled publication mode is not explicit")
    require(
        "schedule may prepare review, but never approves bootstrap" in publication.lower(),
        "scheduled human-review boundary is undocumented",
    )

    result = run_operational_e2e()
    require(result.get("production_deployed") is False, "operational E2E claims production deployment")
    previous = result["previous_release_id"]
    current = result["current_release_id"]
    require(previous != current, "successor proof did not use two distinct real releases")
    require(result.get("bundle_release_id") == current, "bundle is not bound to current release")
    scenarios = result["wordpress"]["scenarios"]

    cold = scenarios["cold_predecessor"]
    require(cold["valid"] is True and cold["release_id"] == previous, "cold predecessor bootstrap failed")
    require(cold["origin"] == "remote_verified_release", "cold predecessor origin drift")

    cached = scenarios["transient_cache"]
    require(cached["release_id"] == previous and cached["origin"] == "transient_cache", "transient cache path failed")
    require(cached["network_call_count"] == 0, "valid transient unexpectedly performed network I/O")

    outage = scenarios["source_outage_last_good"]
    require(outage["valid"] is True and outage["release_id"] == previous, "allowed last-good outage path failed")
    require(outage["origin"] == "option_last_good", "outage did not identify last-good origin")
    require(outage["historical_release_relabelled_as_current"] is False, "last-good was relabelled as current")

    unchanged = scenarios["etag_304"]
    require(unchanged["origin"] == "current_304_last_good", "304 path failed")
    require(unchanged["request_headers"].get("If-None-Match") == '"etag-prev"', "ETag revalidation header missing")

    blocked = scenarios["known_successor_artifact_failure"]
    require(blocked["valid"] is False and blocked["origin"] == "unavailable", "known-successor artifact failure did not fail closed")
    require(blocked["known_successor"] is True, "known successor was not marked in runtime")
    require(blocked["known_successor_release_id"] == current, "runtime known successor identity drift")
    require(blocked["persistent_known_successor"]["release_id"] == current, "known successor latch was not persisted")
    require(blocked["last_good_still_predecessor"] == previous, "failed successor mutated last-good")
    rest_blocked = blocked["rest_after_second_request"]
    require(rest_blocked == {"kind": "error", "code": "sfa_fiscal_release_unavailable", "status": 503}, "second request resurrected predecessor after known successor")

    recovered = scenarios["recovery_to_successor"]
    require(recovered["valid"] is True and recovered["release_id"] == current, "successor recovery failed")
    require(recovered["last_good_release_id"] == current, "successor did not replace last-good")
    require(recovered["known_successor_after_recovery"] is None, "known-successor latch did not resolve after verification")
    require(recovered["rest"]["release_id"] == current, "REST did not expose recovered successor")
    require(recovered["rest"]["header_release_id"] == current, "REST release header drift")

    post = scenarios["post_recovery_last_good"]
    require(post["valid"] is True and post["release_id"] == current, "post-recovery outage did not preserve successor")
    require(post["historical_release_relabelled_as_current"] is False, "post-recovery last-good relabelled data")

    empty304 = scenarios["etag_304_without_last_good"]
    require(empty304["valid"] is False and empty304["origin"] == "unavailable", "304 without last-good did not fail closed")
    corrupt = scenarios["corrupt_last_good"]
    require(corrupt["valid"] is False and corrupt["origin"] == "unavailable", "corrupt last-good did not fail closed")

    require(result["consumer_release_ids"] == {"H26": current, "H27": current, "H28": current, "H29": current}, "H26-H29 did not consume the recovered successor")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/validate_phase7_c72_gate.py" in ci, "Remake CI does not execute C7.2 gate")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "OPT_KNOWN_SUCCESSOR",
        "503",
        "H26–H29",
        "production_deployed=false",
        "C7.3",
    ):
        require(marker in doc, f"C7.2 documentation missing marker: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.2 — E2E operacional, falhas e recuperação evergreen",
        "sucessora conhecida",
        "C7.3",
        "production_deployed=false",
    ):
        require(marker in readme, f"README missing C7.2 state marker: {marker}")

    print(
        "Phase 7 C7.2 gate: PASS "
        f"(previous={previous}, successor={current}, cache=verified, known_successor_latch=persistent, recovery=verified, consumers=H26-H29, production_deployed=false)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
