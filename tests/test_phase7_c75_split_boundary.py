from __future__ import annotations

import json
from pathlib import Path
import tempfile

import pytest

from scripts.simulate_phase7_c75_postdeploy_validation import BASE_URL, build_fixture, validate_fixture
from scripts.validate_phase7_c75_remote_evidence import validate as validate_host


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_host_validator_accepts_delivery_only_block_but_requires_external_evidence() -> None:
    with tempfile.TemporaryDirectory(prefix="c75-boundary-") as raw:
        fixture = build_fixture(Path(raw))
        stale_target = next(
            item
            for item in fixture["records"]
            if item["target_root"] == "site_root" and item["target_path"].endswith(".js")
        )
        stale_url = BASE_URL + "/" + stale_target["target_path"]

        def blocked_delivery_fetcher(url: str) -> dict:
            result = fixture["fetcher"](url)
            if url == stale_url:
                return {"url": url, "http_status": 403, "headers": {"server": "cloudflare"}, "body": b"edge denied", "error": None}
            return result

        evidence = validate_fixture(fixture, fetcher=blocked_delivery_fetcher)
        assert evidence["status"] == "BLOCKED"
        assert all(str(reason).startswith("public_js_delivery_drift:") for reason in evidence["block_reasons"])

        summary = validate_host(evidence, load(fixture["c74_state"]), load(fixture["c74_record"]))
        assert summary["status"] == "PASS"
        assert summary["host_origin_validation"] == "PASS"
        assert summary["external_delivery_required"] is True
        assert summary["phase7_close_recommended"] is False
        assert summary["host_public_js_unverified"] == 1


def test_host_validator_rejects_any_non_delivery_block() -> None:
    with tempfile.TemporaryDirectory(prefix="c75-boundary-drift-") as raw:
        fixture = build_fixture(Path(raw))
        first = fixture["records"][0]
        root = fixture["site"] if first["target_root"] == "site_root" else fixture["plugin"]
        target = root / first["target_path"]
        target.write_bytes(target.read_bytes() + b"drift")

        evidence = validate_fixture(fixture)
        assert evidence["status"] == "BLOCKED"
        assert any(str(reason).startswith("managed_file_drift:") for reason in evidence["block_reasons"])

        with pytest.raises(SystemExit):
            validate_host(evidence, load(fixture["c74_state"]), load(fixture["c74_record"]))
