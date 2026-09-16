#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

DEFAULT_AUTHORIZED_COMMIT = "ccc5a31c3da7c1c93570df0337e553e5a06404ac"
EXPECTED_ASSETS = {
    "folha-core.js",
    "salario-liquido.js",
    "folha-thirteenth.js",
    "decimo-terceiro.js",
    "folha-vacation.js",
    "ferias-clt.js",
    "folha-termination.js",
    "rescisao-clt.js",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.5 external delivery evidence validation failed: {message}")


def load(path: Path) -> dict:
    require(path.is_file(), f"missing evidence file: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), "evidence root must be object")
    return value


def normalize_commit(value: str) -> str:
    commit = str(value or "").strip().lower()
    require(len(commit) == 40 and all(ch in "0123456789abcdef" for ch in commit), "authorized commit must be a 40-character hexadecimal SHA")
    return commit


def validate(evidence: dict, authorized_commit: str = DEFAULT_AUTHORIZED_COMMIT) -> dict:
    expected_commit = normalize_commit(authorized_commit)
    require(evidence.get("schema_version") == "1.0.0", "schema version drift")
    require(evidence.get("checkpoint") == "C7.5", "checkpoint drift")
    require(evidence.get("mode") == "external_client_public_delivery_validation", "mode drift")
    require(evidence.get("status") == "PASS", "external delivery evidence is not PASS")
    require(evidence.get("production_mutated") is False, "external client probe must be read-only")
    require(normalize_commit(str(evidence.get("authorized_commit") or "")) == expected_commit, "authorized commit drift")
    require(evidence.get("assets_expected") == len(EXPECTED_ASSETS), "asset expectation drift")
    require(evidence.get("assets_matching") == len(EXPECTED_ASSETS), "not all public assets matched")
    require((evidence.get("block_reasons") or []) == [], "external delivery evidence contains block reasons")

    assets = evidence.get("assets") or []
    require(len(assets) == len(EXPECTED_ASSETS), "external evidence does not contain exactly 8 assets")
    names = {str(item.get("asset") or "") for item in assets}
    require(names == EXPECTED_ASSETS, "external asset set drift")

    for item in assets:
        name = str(item.get("asset") or "")
        require(item.get("expected_http_status") == 200, f"raw expected source unavailable for {name}")
        require(item.get("public_http_status") == 200, f"public asset not HTTP 200 for {name}")
        expected_sha = str(item.get("expected_sha256") or "")
        public_sha = str(item.get("public_sha256") or "")
        require(len(expected_sha) == 64, f"expected SHA malformed for {name}")
        require(len(public_sha) == 64, f"public SHA malformed for {name}")
        require(expected_sha == public_sha, f"public SHA differs from authorized source for {name}")
        require(item.get("match") is True, f"match flag false for {name}")
        require(expected_commit in str(item.get("expected_source_url") or ""), f"expected source is not pinned to authorized commit for {name}")
        require(str(item.get("public_url") or "").startswith("https://sanida.com.br/financas/calculadoras/assets/"), f"unexpected public URL for {name}")

    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.5",
        "status": "PASS",
        "production_mutated": False,
        "authorized_commit": expected_commit,
        "public_assets": len(EXPECTED_ASSETS),
        "external_delivery_verified": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate C7.5 external-client public delivery evidence")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--authorized-commit", default=DEFAULT_AUTHORIZED_COMMIT)
    args = parser.parse_args()
    print(json.dumps(validate(load(args.evidence), args.authorized_commit), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
