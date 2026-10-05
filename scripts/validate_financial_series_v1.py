from __future__ import annotations

import argparse
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sanida_fiscal.financial_series_v1 import (
    CDI_SOURCE_ID,
    SELIC_SOURCE_ID,
    financial_history_segments,
    validate_financial_series_artifact,
)


def safe_source_id(source_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in source_id)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"financial series validation failed: {message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", default="taxas_bacen_series.json")
    parser.add_argument("--runtime-root", default="evidence/financial-series-v1")
    args = parser.parse_args()

    artifact_path = Path(args.artifact)
    runtime_root = Path(args.runtime_root)
    require(artifact_path.is_file(), f"artifact missing: {artifact_path}")

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    end_date = date.fromisoformat(artifact["meta"]["window"]["end_date"])
    ok, errors = validate_financial_series_artifact(artifact, as_of_date=end_date)
    require(ok, f"artifact contract errors: {errors}")

    sources = artifact["meta"]["sources"]
    for label, source_id in (("selic", SELIC_SOURCE_ID), ("cdi", CDI_SOURCE_ID)):
        meta = sources[label]
        state_path = runtime_root / "state" / f"{safe_source_id(source_id)}.json"
        require(state_path.is_file(), f"{source_id}: state missing")
        state = json.loads(state_path.read_text(encoding="utf-8"))

        require(state.get("source_id") == source_id, f"{source_id}: state source_id mismatch")
        require(state.get("source_url") == meta.get("url"), f"{source_id}: source_url mismatch")
        require(state.get("last_parse_status") == "PARSED", f"{source_id}: last parse not PARSED")
        require(
            state.get("last_candidate_sha256") == meta.get("candidate_sha256"),
            f"{source_id}: candidate fingerprint mismatch",
        )
        require(
            state.get("last_parsed_snapshot_sha256") == meta.get("snapshot_sha256"),
            f"{source_id}: snapshot fingerprint mismatch",
        )

        candidate_rel = state.get("last_candidate_path")
        snapshot_rel = state.get("last_parsed_snapshot_path")
        require(isinstance(candidate_rel, str), f"{source_id}: candidate path missing")
        require(isinstance(snapshot_rel, str), f"{source_id}: snapshot path missing")

        candidate_path = runtime_root / "candidates" / candidate_rel
        snapshot_path = runtime_root / "snapshots" / snapshot_rel
        require(candidate_path.is_file(), f"{source_id}: candidate file missing")
        require(snapshot_path.is_file(), f"{source_id}: snapshot file missing")

        require(
            sha256(candidate_path.read_bytes()).hexdigest() == meta["candidate_sha256"],
            f"{source_id}: candidate bytes hash mismatch",
        )
        require(
            sha256(snapshot_path.read_bytes()).hexdigest() == meta["snapshot_sha256"],
            f"{source_id}: snapshot bytes hash mismatch",
        )

        segments = meta.get("segments")
        if segments is not None:
            require(isinstance(segments, list) and segments, f"{source_id}: segments invalid")
            window = artifact["meta"]["window"]
            expected_segments = financial_history_segments(
                date.fromisoformat(window["start_date"]),
                date.fromisoformat(window["end_date"]),
            )
            require(
                len(segments) == len(expected_segments),
                f"{source_id}: segment count mismatch",
            )
            for index, (segment, expected_window) in enumerate(zip(segments, expected_segments)):
                require(isinstance(segment, dict), f"{source_id}: segment {index} not object")
                expected_start, expected_end = expected_window
                require(
                    segment.get("start_date") == expected_start.isoformat()
                    and segment.get("end_date") == expected_end.isoformat(),
                    f"{source_id}: segment {index} boundary mismatch",
                )
                candidate_rel = segment.get("candidate_path")
                snapshot_rel = segment.get("snapshot_path")
                candidate_sha = segment.get("candidate_sha256")
                snapshot_sha = segment.get("snapshot_sha256")
                require(
                    all(isinstance(value, str) for value in (candidate_rel, snapshot_rel, candidate_sha, snapshot_sha)),
                    f"{source_id}: segment {index} provenance missing",
                )
                candidate_file = runtime_root / "candidates" / candidate_rel
                snapshot_file = runtime_root / "snapshots" / snapshot_rel
                require(candidate_file.is_file(), f"{source_id}: segment {index} candidate missing")
                require(snapshot_file.is_file(), f"{source_id}: segment {index} snapshot missing")
                require(
                    sha256(candidate_file.read_bytes()).hexdigest() == candidate_sha,
                    f"{source_id}: segment {index} candidate hash mismatch",
                )
                require(
                    sha256(snapshot_file.read_bytes()).hexdigest() == snapshot_sha,
                    f"{source_id}: segment {index} snapshot hash mismatch",
                )
                payload = json.loads(candidate_file.read_text(encoding="utf-8"))
                require(
                    payload.get("source_id") == source_id
                    and payload.get("start_date") == expected_start.isoformat()
                    and payload.get("end_date") == expected_end.isoformat(),
                    f"{source_id}: segment {index} normalized payload mismatch",
                )

    print(
        "Financial historical series validation: PASS "
        f"(months={len(artifact['points'])}, "
        f"window={artifact['meta']['window']['start_date']}..{artifact['meta']['window']['end_date']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
