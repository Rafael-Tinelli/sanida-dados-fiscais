#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RUNTIME_ASSETS = (
    {
        "source": "consumers/frontend/folha-core.js",
        "target": "financas/calculadoras/assets/folha-core.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/folha-core.js",
    },
    {
        "source": "consumers/runtime/salario-liquido-runtime.js",
        "target": "financas/calculadoras/assets/salario-liquido-runtime.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/salario-liquido-runtime.js",
    },
    {
        "source": "consumers/frontend/folha-thirteenth.js",
        "target": "financas/calculadoras/assets/folha-thirteenth.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/folha-thirteenth.js",
    },
    {
        "source": "consumers/runtime/decimo-terceiro-runtime.js",
        "target": "financas/calculadoras/assets/decimo-terceiro-runtime.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/decimo-terceiro-runtime.js",
    },
    {
        "source": "consumers/frontend/folha-vacation.js",
        "target": "financas/calculadoras/assets/folha-vacation.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/folha-vacation.js",
    },
    {
        "source": "consumers/frontend/folha-termination.js",
        "target": "financas/calculadoras/assets/folha-termination.js",
        "public_url": "https://sanida.com.br/financas/calculadoras/assets/folha-termination.js",
    },
)

CACHE_POLICY_SOURCE = "ops/fiscal-runtime-assets.htaccess"
CACHE_POLICY_TARGET = "financas/calculadoras/assets/.htaccess"
PENDING_STATUS = "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL"
FINAL_STATUS = "APPLIED_EXTERNALLY_HEALTHY"


class RuntimeDeploymentError(RuntimeError):
    pass


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path: Path, data: bytes, mode: int = 0o644) -> None:
    if not path.parent.is_dir():
        raise RuntimeDeploymentError(f"target parent missing: {path.parent}")
    fd, tmp_name = tempfile.mkstemp(prefix=".sanida-runtime-", suffix=".tmp", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        try:
            dfd = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except (AttributeError, OSError):
            pass
    finally:
        if tmp.exists():
            tmp.unlink()


def read_json(path: Path) -> dict:
    if not path.is_file():
        raise RuntimeDeploymentError(f"required JSON missing: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeDeploymentError(f"JSON root must be object: {path}")
    return value


def ensure_authorized_commit(authorized_commit: str) -> None:
    if not authorized_commit or len(authorized_commit) != 40:
        raise RuntimeDeploymentError("authorized commit must be a full 40-char SHA")
    try:
        actual = subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except Exception as exc:
        raise RuntimeDeploymentError(f"cannot resolve repository HEAD: {exc}") from exc
    if actual != authorized_commit:
        raise RuntimeDeploymentError(
            f"authorized commit mismatch: expected={authorized_commit} actual={actual}"
        )


def payload_rows() -> list[dict]:
    rows: list[dict] = []
    for item in RUNTIME_ASSETS:
        source_path = ROOT / item["source"]
        if not source_path.is_file() or source_path.is_symlink():
            raise RuntimeDeploymentError(f"invalid runtime source: {item['source']}")
        data = source_path.read_bytes()
        rows.append(
            {
                "kind": "fiscal_runtime",
                "source": item["source"],
                "target": item["target"],
                "public_url": item["public_url"],
                "sha256": sha256_bytes(data),
                "size": len(data),
                "data": data,
            }
        )

    policy_path = ROOT / CACHE_POLICY_SOURCE
    if not policy_path.is_file() or policy_path.is_symlink():
        raise RuntimeDeploymentError(f"cache policy source missing: {CACHE_POLICY_SOURCE}")
    policy_data = policy_path.read_bytes()
    rows.append(
        {
            "kind": "cache_policy",
            "source": CACHE_POLICY_SOURCE,
            "target": CACHE_POLICY_TARGET,
            "public_url": None,
            "sha256": sha256_bytes(policy_data),
            "size": len(policy_data),
            "data": policy_data,
        }
    )
    return rows


def manifest_for(rows: list[dict], *, authorized_commit: str, authorization_id: str) -> dict:
    manifest_rows = [
        {k: row[k] for k in ("kind", "source", "target", "public_url", "sha256", "size")}
        for row in rows
    ]
    base = {
        "schema_version": "1.0.0",
        "deployment_kind": "fiscal_runtime_evergreen",
        "authorized_commit": authorized_commit,
        "authorization_id": authorization_id,
        "site_root": "/home1/sanid210/public_html",
        "managed_files": manifest_rows,
        "frontend_mutation_allowed": False,
        "fiscal_release_mutation_allowed": False,
        "rollback_policy": "exact_bytes_changed_targets_only",
        "cache_policy": "no-store for fiscal runtime assets",
    }
    canonical = json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    base["manifest_sha256"] = sha256_bytes(canonical)
    return base


def current_state(target: Path) -> dict:
    if target.is_symlink():
        raise RuntimeDeploymentError(f"symlink target forbidden: {target}")
    if not target.exists():
        return {"exists": False, "sha256": None, "size": None, "mode": None}
    if not target.is_file():
        raise RuntimeDeploymentError(f"target is not regular file: {target}")
    return {
        "exists": True,
        "sha256": sha256_file(target),
        "size": target.stat().st_size,
        "mode": target.stat().st_mode & 0o777,
    }


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    atomic_write(path, payload, 0o600)


def deploy(
    *,
    site_root: Path,
    journal_root: Path,
    authorized_commit: str,
    authorization_id: str,
    verify_git_head: bool = True,
) -> dict:
    site_root = site_root.resolve()
    journal_root = journal_root.resolve()

    if str(site_root) != "/home1/sanid210/public_html" and verify_git_head:
        raise RuntimeDeploymentError(f"unexpected production site root: {site_root}")
    if verify_git_head:
        ensure_authorized_commit(authorized_commit)

    rows = payload_rows()
    manifest = manifest_for(rows, authorized_commit=authorized_commit, authorization_id=authorization_id)
    journal = journal_root / authorization_id
    if journal.exists():
        raise RuntimeDeploymentError(f"single-use journal already exists: {journal}")
    journal.mkdir(parents=True, mode=0o700)

    preflight_rows: list[dict] = []
    changed_rows: list[dict] = []
    try:
        for row in rows:
            target = (site_root / row["target"]).resolve()
            try:
                target.relative_to(site_root)
            except ValueError as exc:
                raise RuntimeDeploymentError(f"target escapes site root: {row['target']}") from exc
            if not target.parent.is_dir():
                raise RuntimeDeploymentError(f"target parent missing: {row['target']}")
            if not os.access(target.parent, os.W_OK):
                raise RuntimeDeploymentError(f"target parent not writable: {row['target']}")
            before = current_state(target)

            if row["kind"] == "cache_policy" and before["exists"] and before["sha256"] != row["sha256"]:
                raise RuntimeDeploymentError(
                    "unmanaged calculadoras/assets/.htaccess already exists; refusing to overwrite"
                )

            preflight_rows.append(
                {
                    "target": row["target"],
                    "incoming_sha256": row["sha256"],
                    "incoming_size": row["size"],
                    "before": before,
                    "needs_write": before["sha256"] != row["sha256"],
                }
            )
            if before["sha256"] != row["sha256"]:
                changed_rows.append(row)

        preflight = {
            "schema_version": "1.0.0",
            "status": "PASS",
            "mode": "READ_ONLY_BEFORE_WRITE",
            "authorized_commit": authorized_commit,
            "authorization_id": authorization_id,
            "manifest_sha256": manifest["manifest_sha256"],
            "production_modified": False,
            "targets": preflight_rows,
            "changed_target_count": len(changed_rows),
            "observed_at_utc": utcnow(),
        }
        write_json(journal / "manifest.json", manifest)
        write_json(journal / "preflight.json", preflight)

        backup_records = []
        for row in changed_rows:
            target = site_root / row["target"]
            before = current_state(target)
            backup_rel = Path("backup") / row["target"]
            if before["exists"]:
                backup_path = journal / backup_rel
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup_path)
                if sha256_file(backup_path) != before["sha256"]:
                    raise RuntimeDeploymentError(f"backup checksum mismatch: {row['target']}")
            backup_records.append(
                {
                    "target": row["target"],
                    "existed_before": before["exists"],
                    "before_sha256": before["sha256"],
                    "before_size": before["size"],
                    "before_mode": before["mode"],
                    "incoming_sha256": row["sha256"],
                    "backup_path": str(backup_rel) if before["exists"] else None,
                }
            )
        write_json(
            journal / "rollback-manifest.json",
            {
                "schema_version": "1.0.0",
                "authorization_id": authorization_id,
                "authorized_commit": authorized_commit,
                "records": backup_records,
            },
        )

        applied: list[str] = []
        post: list[dict] = []
        try:
            for row in changed_rows:
                target = site_root / row["target"]
                before = current_state(target)
                mode = int(before["mode"] or 0o644)
                atomic_write(target, row["data"], mode)
                if sha256_file(target) != row["sha256"]:
                    raise RuntimeDeploymentError(f"post-write checksum mismatch: {row['target']}")
                applied.append(row["target"])

            # Aggregate origin verification belongs to the same transaction.
            # Any mismatch after the first production write must roll back all
            # changed targets instead of being mislabeled as a pre-write block.
            for row in rows:
                target = site_root / row["target"]
                after = current_state(target)
                if after["sha256"] != row["sha256"] or after["size"] != row["size"]:
                    raise RuntimeDeploymentError(f"origin verification failed: {row['target']}")
                post.append(
                    {
                        "target": row["target"],
                        "sha256": after["sha256"],
                        "size": after["size"],
                    }
                )
        except BaseException as exc:
            rollback_actions: list[str] = []
            for record in reversed(backup_records):
                target = site_root / record["target"]
                if record["existed_before"]:
                    backup = journal / record["backup_path"]
                    if not backup.is_file() or sha256_file(backup) != record["before_sha256"]:
                        raise RuntimeDeploymentError(
                            f"apply failed and rollback backup is invalid for {record['target']}"
                        ) from exc
                    atomic_write(target, backup.read_bytes(), int(record["before_mode"] or 0o644))
                    if sha256_file(target) != record["before_sha256"]:
                        raise RuntimeDeploymentError(
                            f"apply failed and rollback verification failed for {record['target']}"
                        ) from exc
                    rollback_actions.append(f"restore:{record['target']}")
                elif target.exists():
                    if target.is_symlink() or not target.is_file():
                        raise RuntimeDeploymentError(
                            f"apply failed and created target is unsafe for rollback: {record['target']}"
                        ) from exc
                    target.unlink()
                    rollback_actions.append(f"delete:{record['target']}")
            state = {
                "schema_version": "1.0.0",
                "status": "ROLLED_BACK",
                "authorization_id": authorization_id,
                "authorized_commit": authorized_commit,
                "manifest_sha256": manifest["manifest_sha256"],
                "production_deployed": False,
                "origin_health_verified": False,
                "external_delivery_verified": False,
                "rollback_performed": True,
                "applied_before_failure": applied,
                "rollback_actions": rollback_actions,
                "error": str(exc),
                "completed_at_utc": utcnow(),
            }
            write_json(journal / "deployment-state.json", state)
            raise

        state = {
            "schema_version": "1.0.0",
            "status": PENDING_STATUS,
            "authorization_id": authorization_id,
            "authorized_commit": authorized_commit,
            "manifest_sha256": manifest["manifest_sha256"],
            "production_deployed": bool(changed_rows),
            "origin_health_verified": True,
            "external_delivery_verified": False,
            "final_health_status": "PENDING_EXTERNAL_PROOF",
            "rollback_performed": False,
            "changed_targets": [row["target"] for row in changed_rows],
            "managed_target_count": len(rows),
            "origin_files": post,
            "started_at_utc": preflight["observed_at_utc"],
            "completed_at_utc": utcnow(),
        }
        write_json(journal / "deployment-state.json", state)
        return state
    except BaseException:
        if not (journal / "deployment-state.json").exists():
            state = {
                "schema_version": "1.0.0",
                "status": "BLOCKED_BEFORE_WRITE",
                "authorization_id": authorization_id,
                "authorized_commit": authorized_commit,
                "production_deployed": False,
                "origin_health_verified": False,
                "external_delivery_verified": False,
                "rollback_performed": False,
                "completed_at_utc": utcnow(),
            }
            try:
                write_json(journal / "deployment-state.json", state)
            except Exception:
                pass
        raise


def finalize(
    *,
    journal_dir: Path,
    external_evidence: Path,
    authorized_commit: str,
    authorization_id: str,
) -> dict:
    state_path = journal_dir / "deployment-state.json"
    state = read_json(state_path)
    evidence = read_json(external_evidence)

    if state.get("status") != PENDING_STATUS:
        raise RuntimeDeploymentError(f"deployment is not pending external proof: {state.get('status')}")
    if state.get("authorization_id") != authorization_id:
        raise RuntimeDeploymentError("authorization_id mismatch")
    if state.get("authorized_commit") != authorized_commit:
        raise RuntimeDeploymentError("authorized_commit mismatch")
    if evidence.get("status") != "PASS":
        raise RuntimeDeploymentError("external evidence is not PASS")
    if evidence.get("authorized_commit") != authorized_commit:
        raise RuntimeDeploymentError("external evidence commit mismatch")
    if evidence.get("assets_expected") != len(RUNTIME_ASSETS):
        raise RuntimeDeploymentError("unexpected external asset count")
    if evidence.get("assets_matching") != len(RUNTIME_ASSETS):
        raise RuntimeDeploymentError("not all external assets match")
    if evidence.get("block_reasons") not in ([], None):
        raise RuntimeDeploymentError("external evidence contains block reasons")

    assets = evidence.get("assets") or []
    if len(assets) != len(RUNTIME_ASSETS):
        raise RuntimeDeploymentError("external asset evidence is incomplete")
    for asset in assets:
        if asset.get("match") is not True:
            raise RuntimeDeploymentError(f"external asset mismatch: {asset.get('target')}")
        cache_control = str(asset.get("cache_control") or "").lower()
        if "no-store" not in cache_control:
            raise RuntimeDeploymentError(
                f"runtime cache policy not active externally: {asset.get('target')}"
            )

    final = dict(state)
    final.update(
        {
            "status": FINAL_STATUS,
            "external_delivery_verified": True,
            "final_health_status": "HEALTHY",
            "external_delivery": evidence,
            "finalized_at_utc": utcnow(),
        }
    )
    write_json(state_path, final)
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description="Evergreen deployment for Sanida fiscal runtime assets")
    sub = parser.add_subparsers(dest="command", required=True)

    p_deploy = sub.add_parser("deploy")
    p_deploy.add_argument("--site-root", required=True, type=Path)
    p_deploy.add_argument("--journal-root", required=True, type=Path)
    p_deploy.add_argument("--authorized-commit", required=True)
    p_deploy.add_argument("--authorization-id", required=True)

    p_finalize = sub.add_parser("finalize")
    p_finalize.add_argument("--journal-dir", required=True, type=Path)
    p_finalize.add_argument("--external-evidence", required=True, type=Path)
    p_finalize.add_argument("--authorized-commit", required=True)
    p_finalize.add_argument("--authorization-id", required=True)

    args = parser.parse_args()
    try:
        if args.command == "deploy":
            result = deploy(
                site_root=args.site_root,
                journal_root=args.journal_root,
                authorized_commit=args.authorized_commit,
                authorization_id=args.authorization_id,
                verify_git_head=True,
            )
        else:
            result = finalize(
                journal_dir=args.journal_dir,
                external_evidence=args.external_evidence,
                authorized_commit=args.authorized_commit,
                authorization_id=args.authorization_id,
            )
    except (RuntimeDeploymentError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())