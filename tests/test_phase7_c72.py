from __future__ import annotations

import shutil

import pytest

from scripts.run_phase7_c72_operational_e2e import run_operational_e2e


def _scenarios() -> tuple[dict, dict]:
    if shutil.which("php") is None or shutil.which("node") is None:
        pytest.skip("C7.2 operational E2E requires PHP and Node")
    result = run_operational_e2e()
    return result, result["wordpress"]["scenarios"]


def test_c72_wordpress_cache_last_good_successor_and_recovery() -> None:
    result, scenarios = _scenarios()
    previous = result["previous_release_id"]
    current = result["current_release_id"]

    assert result["production_deployed"] is False
    assert previous != current
    assert result["bundle_release_id"] == current

    cold = scenarios["cold_predecessor"]
    assert cold["valid"] is True
    assert cold["release_id"] == previous
    assert cold["origin"] == "remote_verified_release"
    assert cold["last_good_release_id"] == previous
    assert cold["etag"] == '"etag-prev"'
    assert cold["known_successor"] is None
    assert cold["transient_ttl"] == 12 * 60 * 60
    assert len(cold["network_calls"]) == 2

    cached = scenarios["transient_cache"]
    assert cached["valid"] is True
    assert cached["release_id"] == previous
    assert cached["origin"] == "transient_cache"
    assert cached["cached_from_origin"] == "remote_verified_release"
    assert cached["network_call_count"] == 0

    outage = scenarios["source_outage_last_good"]
    assert outage["valid"] is True
    assert outage["release_id"] == previous
    assert outage["origin"] == "option_last_good"
    assert outage["known_successor"] is False
    assert outage["historical_release_relabelled_as_current"] is False
    assert outage["transient_ttl"] == 15 * 60

    unchanged = scenarios["etag_304"]
    assert unchanged["valid"] is True
    assert unchanged["release_id"] == previous
    assert unchanged["origin"] == "current_304_last_good"
    assert unchanged["request_headers"]["If-None-Match"] == '"etag-prev"'
    assert unchanged["transient_ttl"] == 12 * 60 * 60

    blocked = scenarios["known_successor_artifact_failure"]
    assert blocked["valid"] is False
    assert blocked["release_id"] is None
    assert blocked["origin"] == "unavailable"
    assert blocked["known_successor"] is True
    assert blocked["known_successor_release_id"] == current
    assert blocked["persistent_known_successor"]["release_id"] == current
    assert blocked["last_good_still_predecessor"] == previous
    assert blocked["rest_after_second_request"] == {
        "kind": "error",
        "code": "sfa_fiscal_release_unavailable",
        "status": 503,
    }

    recovered = scenarios["recovery_to_successor"]
    assert recovered["valid"] is True
    assert recovered["release_id"] == current
    assert recovered["origin"] == "remote_verified_release"
    assert recovered["etag"] == '"etag-current"'
    assert recovered["last_good_release_id"] == current
    assert recovered["known_successor_after_recovery"] is None
    assert recovered["rest"]["kind"] == "response"
    assert recovered["rest"]["release_id"] == current
    assert recovered["rest"]["header_release_id"] == current
    assert recovered["rest"]["cache_control"] == "public, max-age=600, s-maxage=600"

    later_outage = scenarios["post_recovery_last_good"]
    assert later_outage["valid"] is True
    assert later_outage["release_id"] == current
    assert later_outage["origin"] == "option_last_good"
    assert later_outage["historical_release_relabelled_as_current"] is False


def test_c72_fails_closed_without_valid_last_good_and_feeds_all_consumers_after_recovery() -> None:
    result, scenarios = _scenarios()
    current = result["current_release_id"]

    empty_304 = scenarios["etag_304_without_last_good"]
    assert empty_304["valid"] is False
    assert empty_304["release_id"] is None
    assert empty_304["origin"] == "unavailable"
    assert empty_304["last_fetch_error"] == "current_304_without_valid_last_good"
    assert empty_304["rest_after_second_request"] == {
        "kind": "error",
        "code": "sfa_fiscal_release_unavailable",
        "status": 503,
    }

    corrupt = scenarios["corrupt_last_good"]
    assert corrupt["valid"] is False
    assert corrupt["release_id"] is None
    assert corrupt["origin"] == "unavailable"

    assert result["consumer_release_ids"] == {
        "H26": current,
        "H27": current,
        "H28": current,
        "H29": current,
    }
