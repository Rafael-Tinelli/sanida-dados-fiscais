from __future__ import annotations

import json
from pathlib import Path

from scripts import publish_fiscal_release_v12 as publisher


def test_same_review_key_preserves_review_packet_bytes(tmp_path: Path, monkeypatch) -> None:
    review_path = tmp_path / "review.json"
    monkeypatch.setattr(publisher, "REVIEW_PATH", review_path)

    first = {
        "schema_version": "1.0.0",
        "review_key": "a" * 64,
        "created_at_utc": "2026-09-14T12:00:00Z",
        "rules": [],
    }
    later_same_decision = {
        **first,
        "created_at_utc": "2026-09-15T12:00:00Z",
    }

    assert publisher._write_review(first) is True
    original = review_path.read_bytes()
    assert publisher._write_review(later_same_decision) is False
    assert review_path.read_bytes() == original


def test_same_pending_review_key_preserves_last_attempt_bytes(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "last-attempt.json"
    monkeypatch.setattr(publisher, "STATE_PATH", state_path)

    first = {
        "schema_version": "1.0.0",
        "attempted_at_utc": "2026-09-14T12:00:00Z",
        "previous_release_id": None,
        "publication_status": "REVIEW_REQUIRED",
        "promotion_outcome": "REVIEW_REQUIRED",
        "review_key": "b" * 64,
    }
    later_same_decision = {
        **first,
        "attempted_at_utc": "2026-09-15T12:00:00Z",
    }

    assert publisher._write_pending_review_state(first) is True
    original = state_path.read_bytes()
    assert publisher._write_pending_review_state(later_same_decision) is False
    assert state_path.read_bytes() == original


def test_new_review_key_replaces_pending_review_state(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "last-attempt.json"
    monkeypatch.setattr(publisher, "STATE_PATH", state_path)

    first = {
        "schema_version": "1.0.0",
        "attempted_at_utc": "2026-09-14T12:00:00Z",
        "previous_release_id": None,
        "publication_status": "REVIEW_REQUIRED",
        "review_key": "c" * 64,
    }
    changed = {
        **first,
        "attempted_at_utc": "2026-09-15T12:00:00Z",
        "review_key": "d" * 64,
    }

    publisher._write_pending_review_state(first)
    assert publisher._write_pending_review_state(changed) is True
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["review_key"] == "d" * 64
