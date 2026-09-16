#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_phase7_c71_bundle import build_bundle

STORE = ROOT / "releases/fiscal-v1"
DEPLOYMENT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
PHP_HARNESS = ROOT / "tests/php/phase7_c72_wordpress_operational.php"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _read_json(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(data, dict), f"expected JSON object: {path}")
    return data


def _previous_manifest(release: dict, raw: bytes) -> dict:
    release_id = release["release_id"]
    compatibility = release["consumer_compatibility"]
    lifecycle = release["lifecycle"]
    return {
        "artifact": f"releases/{release_id}.json",
        "artifact_sha256": sha256(raw).hexdigest(),
        "contract_api_version": compatibility["contract_api_version"],
        "contract_id": release["contract_id"],
        "published_at_utc": lifecycle["published_at_utc"],
        "release_id": release_id,
        "schema_version": "1.0.0",
        "schema_version_contract": release["schema_version"],
        "supersedes_release_id": release.get("supersedes_release_id"),
    }


def _run_node(runtime: Path, args: list[Path]) -> dict:
    node = shutil.which("node")
    require(node is not None, "node is required for C7.2")
    completed = subprocess.run(
        [node, str(runtime), *(str(arg) for arg in args)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    require(completed.returncode == 0, f"runtime failed: {runtime.name}: {completed.stderr or completed.stdout}")
    payload = json.loads(completed.stdout)
    require(isinstance(payload, dict), f"runtime did not return object: {runtime.name}")
    return payload


def run_operational_e2e() -> dict:
    php = shutil.which("php")
    require(php is not None, "php is required for C7.2 operational WordPress E2E")
    require(PHP_HARNESS.is_file(), "C7.2 PHP operational harness is missing")

    current_manifest = _read_json(STORE / "current.json")
    current_release_id = current_manifest["release_id"]
    previous_release_id = current_manifest.get("supersedes_release_id")
    require(isinstance(previous_release_id, str) and previous_release_id, "current release has no real predecessor for successor E2E")

    current_artifact = STORE / current_manifest["artifact"]
    previous_artifact = STORE / "releases" / f"{previous_release_id}.json"
    require(current_artifact.is_file(), "current immutable artifact missing")
    require(previous_artifact.is_file(), "predecessor immutable artifact missing")

    current_raw = current_artifact.read_bytes()
    previous_raw = previous_artifact.read_bytes()
    require(sha256(current_raw).hexdigest() == current_manifest["artifact_sha256"], "current artifact SHA mismatch")
    current_release = json.loads(current_raw)
    previous_release = json.loads(previous_raw)
    require(current_release["release_id"] == current_release_id, "current artifact release_id mismatch")
    require(previous_release["release_id"] == previous_release_id, "predecessor artifact release_id mismatch")
    require(current_release.get("supersedes_release_id") == previous_release_id, "current release does not supersede predecessor")

    with tempfile.TemporaryDirectory(prefix="c72-operational-") as raw_dir:
        temp = Path(raw_dir)
        bundle_dir = temp / "bundle"
        bundle_manifest = build_bundle(DEPLOYMENT_MANIFEST, bundle_dir)
        require(bundle_manifest["release"]["release_id"] == current_release_id, "bundle is not bound to current release")
        require(bundle_manifest["production_deployed"] is False, "C7.2 bundle unexpectedly claims production deployment")

        previous_manifest_path = temp / "previous-current.json"
        previous_manifest_path.write_text(
            json.dumps(_previous_manifest(previous_release, previous_raw), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

        contract_trait = bundle_dir / "wordpress-plugin/includes/trait-sanida-fiscal-contract.php"
        network_trait = bundle_dir / "wordpress-plugin/includes/trait-sanida-fiscal-network.php"
        require(contract_trait.is_file() and network_trait.is_file(), "bundle lacks WordPress fiscal traits")

        completed = subprocess.run(
            [
                php,
                str(PHP_HARNESS),
                str(contract_trait),
                str(network_trait),
                str(previous_manifest_path),
                str(previous_artifact),
                str(STORE / "current.json"),
                str(current_artifact),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        require(completed.returncode == 0, f"C7.2 PHP operational harness failed: {completed.stderr or completed.stdout}")
        wordpress = json.loads(completed.stdout)
        require(isinstance(wordpress, dict), "C7.2 PHP harness did not return object")
        require(wordpress.get("previous_release_id") == previous_release_id, "PHP harness predecessor mismatch")
        require(wordpress.get("current_release_id") == current_release_id, "PHP harness current release mismatch")

        recovered_release = wordpress.pop("recovered_release", None)
        require(isinstance(recovered_release, dict), "PHP harness did not expose recovered release")
        require(recovered_release.get("release_id") == current_release_id, "recovered WordPress release is not current")
        recovered_path = temp / "recovered-release.json"
        recovered_path.write_text(json.dumps(recovered_release, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

        assets = bundle_dir / "site-root/financas/calculadoras/assets"
        runtimes = {
            "H26": (
                ROOT / "tests/js/phase6_c63_h26_runtime.cjs",
                [assets / "folha-core.js", assets / "salario-liquido.js", recovered_path],
            ),
            "H27": (
                ROOT / "tests/js/phase6_c64_h27_runtime.cjs",
                [assets / "folha-core.js", assets / "folha-thirteenth.js", assets / "decimo-terceiro.js", recovered_path],
            ),
            "H28": (
                ROOT / "tests/js/phase6_c65_h28_runtime.cjs",
                [assets / "folha-core.js", assets / "folha-vacation.js", assets / "ferias-clt.js", recovered_path],
            ),
            "H29": (
                ROOT / "tests/js/phase6_c66_h29_runtime.cjs",
                [assets / "folha-core.js", assets / "folha-termination.js", assets / "rescisao-clt.js", recovered_path],
            ),
        }
        consumers = {name: _run_node(runtime, args) for name, (runtime, args) in runtimes.items()}
        for name, payload in consumers.items():
            require(payload.get("release_id") == current_release_id, f"{name} did not consume recovered successor")

        return {
            "schema_version": "1.0.0",
            "checkpoint": "C7.2",
            "production_deployed": False,
            "bundle_release_id": bundle_manifest["release"]["release_id"],
            "previous_release_id": previous_release_id,
            "current_release_id": current_release_id,
            "wordpress": wordpress,
            "consumer_release_ids": {name: payload.get("release_id") for name, payload in consumers.items()},
        }


def main() -> int:
    result = run_operational_e2e()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
