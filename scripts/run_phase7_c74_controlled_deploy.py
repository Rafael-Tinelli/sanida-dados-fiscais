#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import tempfile
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.run_phase7_c73_host_preflight import run_preflight, safe_relative, path_is_within


class DeploymentError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path) -> dict:
    if not path.is_file():
        raise DeploymentError(f"required JSON missing: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentError(f"invalid JSON {path}: {exc}") from exc


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def target_for(record: dict, roots: dict[str, Path]) -> Path:
    root_name = str(record.get("target_root", ""))
    if root_name not in roots:
        raise DeploymentError(f"unknown target root: {root_name}")
    return roots[root_name] / safe_relative(str(record.get("target_path", "")))


def load_bundle(bundle_dir: Path) -> tuple[dict, Path]:
    manifest_path = bundle_dir / "bundle-manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("schema_version") != "1.0.0" or manifest.get("checkpoint") != "C7.1":
        raise DeploymentError("unsupported deployment bundle")
    if manifest.get("production_deployed") is not False:
        raise DeploymentError("bundle is not a pre-production C7.1 bundle")
    if len(manifest.get("files") or []) != 32:
        raise DeploymentError("bundle must contain exactly 32 managed files")
    if len(manifest.get("preexisting_dependencies") or []) != 11:
        raise DeploymentError("bundle must declare exactly 11 preexisting dependencies")
    return manifest, manifest_path


def load_authorization(path: Path, authorization_id: str) -> dict:
    auth = load_json(path)
    if auth.get("schema_version") != "1.0.0" or auth.get("checkpoint") != "C7.4":
        raise DeploymentError("unsupported C7.4 authorization record")
    if auth.get("status") != "AUTHORIZED_READY_TO_DEPLOY":
        raise DeploymentError("C7.4 authorization is not ready to deploy")
    if auth.get("authorization_id") != authorization_id:
        raise DeploymentError("authorization id mismatch")
    if auth.get("deployment_authorized") is not True:
        raise DeploymentError("deployment is not explicitly authorized")
    if auth.get("production_deployed") is not False:
        raise DeploymentError("authorization record already claims production deployment")
    if auth.get("single_use_authorization") is not True:
        raise DeploymentError("authorization must be single-use")
    return auth


def compare_c73_state(previous: dict, fresh: dict) -> None:
    if previous.get("status") != "PASS" or previous.get("technical_go_no_go") != "GO":
        raise DeploymentError("stored C7.3 evidence is not PASS/GO")
    if previous.get("production_mutated") is not False:
        raise DeploymentError("stored C7.3 evidence claims production mutation")
    if fresh.get("status") != "PASS" or fresh.get("technical_go_no_go") != "GO":
        raise DeploymentError(f"fresh preflight blocked: {fresh.get('block_reasons')}")

    def index(items: list[dict]) -> dict[tuple[str, str], dict]:
        return {
            (str(item.get("target_root")), str(item.get("relative_path"))): item
            for item in items
        }

    old_deps = index(previous.get("dependencies") or [])
    new_deps = index(fresh.get("dependencies") or [])
    if set(old_deps) != set(new_deps):
        raise DeploymentError("preexisting dependency inventory drift since C7.3")
    for key in sorted(old_deps):
        old = old_deps[key]
        new = new_deps[key]
        for field in ("exists", "is_file", "is_symlink", "sha256"):
            if old.get(field) != new.get(field):
                raise DeploymentError(f"dependency state drift since C7.3: {key}: {field}")

    old_managed = index(previous.get("managed_targets") or [])
    new_managed = index(fresh.get("managed_targets") or [])
    if set(old_managed) != set(new_managed):
        raise DeploymentError("managed target inventory drift since C7.3")
    for key in sorted(old_managed):
        old = old_managed[key]
        new = new_managed[key]
        for field in ("exists", "is_file", "is_symlink", "sha256"):
            if old.get(field) != new.get(field):
                raise DeploymentError(f"managed target state drift since C7.3: {key}: {field}")

    old_dirs = previous.get("planned_directory_creations") or []
    new_dirs = fresh.get("planned_directory_creations") or []
    if old_dirs != new_dirs:
        raise DeploymentError("planned directory creation set drift since C7.3")


def verify_bindings(bundle: dict, manifest_path: Path, c73_path: Path, c73: dict, auth: dict) -> None:
    candidate = auth.get("candidate") or {}
    manifest_sha = sha256_file(manifest_path)
    c73_sha = sha256_file(c73_path)
    release_id = (bundle.get("release") or {}).get("release_id")
    if candidate.get("bundle_manifest_sha256") != manifest_sha:
        raise DeploymentError("bundle manifest SHA is not the authorized candidate")
    if candidate.get("c73_remote_evidence_sha256") != c73_sha:
        raise DeploymentError("C7.3 remote evidence SHA is not the authorized evidence")
    if candidate.get("release_id") != release_id:
        raise DeploymentError("bundle release_id is not the authorized release")
    if c73.get("bundle_manifest_sha256") != manifest_sha:
        raise DeploymentError("C7.3 evidence is bound to another bundle manifest")
    if (c73.get("release") or {}).get("release_id") != release_id:
        raise DeploymentError("C7.3 evidence is bound to another release")
    if candidate.get("managed_files") != 32 or candidate.get("preexisting_dependencies") != 11:
        raise DeploymentError("authorization inventory count drift")


def deployment_priority(record: dict) -> tuple[int, str]:
    root = str(record.get("target_root"))
    path = str(record.get("target_path"))
    if root == "wordpress_plugin_dir" and path.startswith("includes/"):
        return (10, path)
    if root == "site_root" and path.endswith(".js"):
        return (20, path)
    if root == "site_root" and "/parts/" in path:
        return (30, path)
    if root == "site_root" and path.endswith("/index.php"):
        return (40, path)
    if root == "wordpress_plugin_dir" and path == "sanida-fiscais-auto.php":
        return (50, path)
    return (35, f"{root}:{path}")


def snapshot_managed(bundle: dict, roots: dict[str, Path], journal_dir: Path, fresh: dict) -> dict:
    backup_dir = journal_dir / "backup"
    backup_dir.mkdir(parents=True, exist_ok=False)
    states: dict[str, dict] = {}
    fresh_map = {
        (str(item["target_root"]), str(item["relative_path"])): item
        for item in fresh.get("managed_targets") or []
    }
    for record in bundle["files"]:
        destination = target_for(record, roots)
        key = record["bundle_path"]
        preflight = fresh_map[(record["target_root"], record["target_path"])]
        existed = bool(preflight["exists"])
        state = {
            "target_root": record["target_root"],
            "target_path": record["target_path"],
            "existed": existed,
            "sha256": preflight.get("sha256"),
            "mode": preflight.get("mode"),
            "backup_path": None,
        }
        if existed:
            body = destination.read_bytes()
            observed = sha256(body).hexdigest()
            if observed != preflight.get("sha256"):
                raise DeploymentError(f"managed target changed during snapshot: {key}")
            backup_path = backup_dir / safe_relative(key)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_bytes(body)
            if sha256_file(backup_path) != observed:
                raise DeploymentError(f"backup verification failed: {key}")
            state["backup_path"] = backup_path.relative_to(journal_dir).as_posix()
        states[key] = state

    dependencies = {}
    for item in fresh.get("dependencies") or []:
        key = f"{item['target_root']}:{item['relative_path']}"
        dependencies[key] = {
            "target_root": item["target_root"],
            "target_path": item["relative_path"],
            "sha256": item["sha256"],
        }
    snapshot = {
        "schema_version": "1.0.0",
        "checkpoint": "C7.4",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "managed": states,
        "dependencies": dependencies,
        "planned_directory_creations": fresh.get("planned_directory_creations") or [],
    }
    atomic_json(journal_dir / "snapshot.json", snapshot)
    return snapshot


def ensure_planned_parent(
    parent: Path,
    root_name: str,
    root: Path,
    allowed: set[tuple[str, str]],
    created_dirs: list[dict],
) -> None:
    try:
        parent.relative_to(root)
    except ValueError as exc:
        raise DeploymentError(f"parent escapes root: {parent}") from exc
    missing: list[Path] = []
    cursor = parent
    while cursor != root and not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    if not cursor.exists() or not cursor.is_dir() or cursor.is_symlink():
        raise DeploymentError(f"nearest existing parent is unsafe: {cursor}")
    for directory in reversed(missing):
        rel = directory.relative_to(root).as_posix()
        if (root_name, rel) not in allowed:
            raise DeploymentError(f"directory creation was not authorized by preflight: {root_name}:{rel}")
        directory.mkdir()
        created_dirs.append({"target_root": root_name, "relative_path": rel})


def atomic_write_managed(destination: Path, body: bytes, mode: int) -> None:
    fd, raw = tempfile.mkstemp(prefix=f".{destination.name}.c74-", dir=str(destination.parent))
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, destination)
    finally:
        if tmp.exists():
            tmp.unlink()


def verify_dependencies(snapshot: dict, roots: dict[str, Path]) -> None:
    for state in snapshot["dependencies"].values():
        path = roots[state["target_root"]] / safe_relative(state["target_path"])
        if not path.is_file() or path.is_symlink():
            raise DeploymentError(f"preexisting dependency disappeared or became unsafe: {state['target_path']}")
        if sha256_file(path) != state["sha256"]:
            raise DeploymentError(f"preexisting dependency mutated: {state['target_path']}")


def verify_deployed(bundle: dict, roots: dict[str, Path]) -> None:
    for record in bundle["files"]:
        path = target_for(record, roots)
        if not path.is_file() or path.is_symlink():
            raise DeploymentError(f"deployed managed target missing or unsafe: {record['bundle_path']}")
        if sha256_file(path) != record["sha256"]:
            raise DeploymentError(f"deployed hash mismatch: {record['bundle_path']}")


def verify_restored(snapshot: dict, roots: dict[str, Path]) -> None:
    for key, state in snapshot["managed"].items():
        path = roots[state["target_root"]] / safe_relative(state["target_path"])
        if state["existed"]:
            if not path.is_file() or path.is_symlink() or sha256_file(path) != state["sha256"]:
                raise DeploymentError(f"rollback did not restore exact managed state: {key}")
        elif path.exists() or path.is_symlink():
            raise DeploymentError(f"rollback did not remove newly introduced managed file: {key}")
    verify_dependencies(snapshot, roots)


def rollback(snapshot: dict, roots: dict[str, Path], journal_dir: Path, created_dirs: list[dict]) -> dict:
    backup_dir = journal_dir / "backup"
    errors: list[str] = []
    for key, state in snapshot["managed"].items():
        destination = roots[state["target_root"]] / safe_relative(state["target_path"])
        try:
            if state["existed"]:
                backup = journal_dir / safe_relative(state["backup_path"])
                body = backup.read_bytes()
                if sha256(body).hexdigest() != state["sha256"]:
                    raise DeploymentError(f"backup corrupt during rollback: {key}")
                mode = int(str(state.get("mode") or "0644"), 8)
                destination.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_managed(destination, body, mode)
            elif destination.exists() and destination.is_file() and not destination.is_symlink():
                destination.unlink()
        except Exception as exc:
            errors.append(f"file:{key}:{exc}")

    nonempty_dirs: list[str] = []
    for item in sorted(created_dirs, key=lambda row: len(Path(row["relative_path"]).parts), reverse=True):
        directory = roots[item["target_root"]] / safe_relative(item["relative_path"])
        try:
            directory.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            nonempty_dirs.append(f"{item['target_root']}:{item['relative_path']}")

    try:
        verify_restored(snapshot, roots)
    except Exception as exc:
        errors.append(f"verify:{exc}")

    return {
        "rollback_verified": not errors,
        "errors": errors,
        "nonempty_created_directories_preserved": nonempty_dirs,
    }


def request_url(url: str, *, timeout: int = 20) -> tuple[int, bytes, dict[str, str]]:
    request = Request(url, headers={"User-Agent": "Sanida-C7.4-Deployment-Health/1.0"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read(), {k.lower(): v for k, v in response.headers.items()}
    except HTTPError as exc:
        return int(exc.code), exc.read(), {k.lower(): v for k, v in exc.headers.items()}
    except URLError as exc:
        raise DeploymentError(f"health request failed for {url}: {exc}") from exc


def network_health_check(base_url: str, release_id: str) -> dict:
    base = base_url.rstrip("/")
    results: dict[str, dict] = {}

    status, body, headers = request_url(base + "/blog/wp-json/sfa/v1/fiscal-health")
    if status != 200:
        raise DeploymentError(f"fiscal-health returned HTTP {status}")
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise DeploymentError(f"fiscal-health returned invalid JSON: {exc}") from exc
    if payload.get("status") != "healthy" or payload.get("release_id") != release_id:
        raise DeploymentError(f"fiscal-health is not healthy on candidate release: {payload}")
    if headers.get("x-sanida-fiscal-release") not in (None, release_id):
        raise DeploymentError("fiscal-health release header mismatch")
    results["fiscal_health"] = {"http_status": status, "status": payload.get("status"), "release_id": payload.get("release_id")}

    status, body, headers = request_url(base + "/blog/wp-json/sfa/v1/fiscal-release")
    if status != 200:
        raise DeploymentError(f"fiscal-release returned HTTP {status}")
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise DeploymentError(f"fiscal-release returned invalid JSON: {exc}") from exc
    if payload.get("release_id") != release_id:
        raise DeploymentError("fiscal-release release_id mismatch")
    if headers.get("x-sanida-fiscal-release") not in (None, release_id):
        raise DeploymentError("fiscal-release header mismatch")
    results["fiscal_release"] = {"http_status": status, "release_id": payload.get("release_id")}

    status, _, _ = request_url(base + "/blog/wp-json/sfa/v1/folha")
    if status != 410:
        raise DeploymentError(f"legacy folha endpoint returned HTTP {status}, expected 410")
    results["legacy_folha"] = {"http_status": status}

    pages = {
        "H26": "/financas/calculadoras/salario-liquido-clt/",
        "H27": "/financas/calculadoras/decimo-terceiro/",
        "H28": "/financas/calculadoras/ferias-clt/",
        "H29": "/financas/calculadoras/rescisao-clt/",
    }
    results["calculators"] = {}
    for consumer, path in pages.items():
        status, body, _ = request_url(base + path)
        text = body.decode("utf-8", errors="ignore")
        if status != 200:
            raise DeploymentError(f"{consumer} page returned HTTP {status}")
        if "Fatal error" in text or "Parse error" in text:
            raise DeploymentError(f"{consumer} page contains PHP fatal/parse error")
        results["calculators"][consumer] = {"http_status": status, "bytes": len(body)}
    return results


def default_health_checker(base_url: str, release_id: str) -> dict:
    return network_health_check(base_url, release_id)


def run_controlled_deployment(
    *,
    bundle_dir: Path,
    site_root: Path,
    wordpress_plugin_dir: Path,
    c73_evidence_path: Path,
    authorization_path: Path,
    authorization_id: str,
    journal_dir: Path,
    health_base_url: str,
    health_checker: Callable[[str, str], dict] = default_health_checker,
    fail_after_writes: int | None = None,
) -> dict:
    bundle_dir = bundle_dir.resolve()
    site_root = site_root.resolve()
    wordpress_plugin_dir = wordpress_plugin_dir.resolve()
    c73_evidence_path = c73_evidence_path.resolve()
    authorization_path = authorization_path.resolve()
    journal_dir = journal_dir.resolve()
    roots = {"site_root": site_root, "wordpress_plugin_dir": wordpress_plugin_dir}

    if path_is_within(journal_dir, site_root) or path_is_within(journal_dir, wordpress_plugin_dir):
        raise DeploymentError("journal must be outside production roots")
    if journal_dir.exists():
        raise DeploymentError("single-use deployment journal already exists")
    if journal_dir.name != authorization_id:
        raise DeploymentError("journal directory name must equal authorization_id")

    bundle, manifest_path = load_bundle(bundle_dir)
    auth = load_authorization(authorization_path, authorization_id)
    c73 = load_json(c73_evidence_path)
    verify_bindings(bundle, manifest_path, c73_evidence_path, c73, auth)

    fresh = run_preflight(manifest_path, site_root, wordpress_plugin_dir)
    compare_c73_state(c73, fresh)

    journal_dir.mkdir(parents=True, exist_ok=False)
    atomic_json(journal_dir / "fresh-preflight.json", fresh)
    shutil.copy2(authorization_path, journal_dir / "authorization.json")
    shutil.copy2(c73_evidence_path, journal_dir / "c73-evidence.json")
    shutil.copy2(manifest_path, journal_dir / "bundle-manifest.json")

    snapshot = snapshot_managed(bundle, roots, journal_dir, fresh)
    verify_dependencies(snapshot, roots)

    state = {
        "schema_version": "1.0.0",
        "checkpoint": "C7.4",
        "authorization_id": authorization_id,
        "status": "AUTHORIZED_SNAPSHOT_VERIFIED",
        "deployment_authorized": True,
        "production_deployed": False,
        "rollback_performed": False,
        "release_id": (bundle.get("release") or {}).get("release_id"),
        "bundle_manifest_sha256": sha256_file(manifest_path),
        "c73_evidence_sha256": sha256_file(c73_evidence_path),
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "created_directories": [],
        "applied_files": [],
        "health": None,
        "error": None,
    }
    atomic_json(journal_dir / "deployment-state.json", state)

    allowed_dirs = {
        (str(item["target_root"]), str(item["relative_path"]))
        for item in fresh.get("planned_directory_creations") or []
    }
    created_dirs: list[dict] = []
    records = sorted(bundle["files"], key=deployment_priority)
    writes = 0

    def emergency_signal(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    old_int = signal.signal(signal.SIGINT, emergency_signal)
    old_term = signal.signal(signal.SIGTERM, emergency_signal)
    try:
        state["status"] = "APPLYING"
        atomic_json(journal_dir / "deployment-state.json", state)
        for record in records:
            source = bundle_dir / safe_relative(record["bundle_path"])
            if not source.is_file():
                raise DeploymentError(f"bundle file missing: {record['bundle_path']}")
            body = source.read_bytes()
            if sha256(body).hexdigest() != record["sha256"]:
                raise DeploymentError(f"bundle file hash mismatch before write: {record['bundle_path']}")
            destination = target_for(record, roots)
            ensure_planned_parent(
                destination.parent,
                record["target_root"],
                roots[record["target_root"]],
                allowed_dirs,
                created_dirs,
            )
            snap = snapshot["managed"][record["bundle_path"]]
            mode = int(str(snap.get("mode") or "0644"), 8) if snap["existed"] else 0o644
            atomic_write_managed(destination, body, mode)
            if sha256_file(destination) != record["sha256"]:
                raise DeploymentError(f"hash mismatch after write: {record['bundle_path']}")
            writes += 1
            state["created_directories"] = list(created_dirs)
            state["applied_files"].append(record["bundle_path"])
            atomic_json(journal_dir / "deployment-state.json", state)
            if fail_after_writes is not None and writes >= fail_after_writes:
                raise DeploymentError("injected apply failure")

        verify_deployed(bundle, roots)
        verify_dependencies(snapshot, roots)
        state["status"] = "APPLIED_AWAITING_HEALTH"
        atomic_json(journal_dir / "deployment-state.json", state)

        health = health_checker(health_base_url, state["release_id"])
        verify_deployed(bundle, roots)
        verify_dependencies(snapshot, roots)
        state["health"] = health
        state["status"] = "APPLIED_HEALTHY"
        state["production_deployed"] = True
        state["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(journal_dir / "deployment-state.json", state)
        return state
    except BaseException as exc:
        state["error"] = f"{type(exc).__name__}: {exc}"
        rb = rollback(snapshot, roots, journal_dir, created_dirs)
        state["rollback_performed"] = True
        state["rollback"] = rb
        state["production_deployed"] = False
        state["status"] = "ROLLED_BACK" if rb["rollback_verified"] else "ROLLBACK_INCOMPLETE"
        state["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(journal_dir / "deployment-state.json", state)
        return state
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)


def main() -> int:
    parser = argparse.ArgumentParser(description="C7.4 single-use controlled production deployment")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--c73-evidence", type=Path, required=True)
    parser.add_argument("--authorization-file", type=Path, required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--journal-dir", type=Path, required=True)
    parser.add_argument("--health-base-url", required=True)
    args = parser.parse_args()

    try:
        result = run_controlled_deployment(
            bundle_dir=args.bundle_dir,
            site_root=args.site_root,
            wordpress_plugin_dir=args.wordpress_plugin_dir,
            c73_evidence_path=args.c73_evidence,
            authorization_path=args.authorization_file,
            authorization_id=args.authorization_id,
            journal_dir=args.journal_dir,
            health_base_url=args.health_base_url,
        )
    except (DeploymentError, SystemExit) as exc:
        print(json.dumps({"checkpoint": "C7.4", "status": "BLOCKED_BEFORE_WRITE", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("status") == "APPLIED_HEALTHY" and result.get("production_deployed") is True:
        return 0
    if result.get("status") == "ROLLED_BACK":
        return 4
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
