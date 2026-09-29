import json
from pathlib import Path

import pytest

from scripts import deploy_fiscal_runtime_assets as runtime_deploy


def _site_root(tmp_path: Path) -> Path:
    root = tmp_path / "public_html"
    assets = root / "financas" / "calculadoras" / "assets"
    assets.mkdir(parents=True)
    for item in runtime_deploy.RUNTIME_ASSETS:
        (root / item["target"]).write_text("legacy\n", encoding="utf-8")
    return root


def test_runtime_publication_scope_is_exact_and_non_visual() -> None:
    sources = {item["source"] for item in runtime_deploy.RUNTIME_ASSETS}
    targets = {item["target"] for item in runtime_deploy.RUNTIME_ASSETS}

    assert sources == {
        "consumers/frontend/folha-core.js",
        "consumers/runtime/salario-liquido-runtime.js",
        "consumers/frontend/folha-thirteenth.js",
        "consumers/runtime/decimo-terceiro-runtime.js",
        "consumers/frontend/folha-vacation.js",
        "consumers/frontend/folha-termination.js",
    }
    assert targets == {
        "financas/calculadoras/assets/folha-core.js",
        "financas/calculadoras/assets/salario-liquido-runtime.js",
        "financas/calculadoras/assets/folha-thirteenth.js",
        "financas/calculadoras/assets/decimo-terceiro-runtime.js",
        "financas/calculadoras/assets/folha-vacation.js",
        "financas/calculadoras/assets/folha-termination.js",
    }
    assert all(not target.endswith(".css") for target in targets)
    assert all("-ui.js" not in target for target in targets)
    assert all("/parts/" not in target and not target.endswith(".php") for target in targets)


def test_deploy_applies_exact_runtime_bytes_and_scoped_cache_policy(tmp_path: Path) -> None:
    site_root = _site_root(tmp_path)
    journals = tmp_path / "journals"

    state = runtime_deploy.deploy(
        site_root=site_root,
        journal_root=journals,
        authorized_commit="a" * 40,
        authorization_id="runtime-test-1",
        verify_git_head=False,
    )

    assert state["status"] == runtime_deploy.PENDING_STATUS
    assert state["production_deployed"] is True
    assert state["origin_health_verified"] is True
    assert state["external_delivery_verified"] is False
    assert state["rollback_performed"] is False
    assert len(state["changed_targets"]) == 7

    for item in runtime_deploy.RUNTIME_ASSETS:
        source = runtime_deploy.ROOT / item["source"]
        target = site_root / item["target"]
        assert target.read_bytes() == source.read_bytes()

    cache_target = site_root / runtime_deploy.CACHE_POLICY_TARGET
    assert cache_target.read_bytes() == (runtime_deploy.ROOT / runtime_deploy.CACHE_POLICY_SOURCE).read_bytes()

    journal = journals / "runtime-test-1"
    assert (journal / "manifest.json").is_file()
    assert (journal / "preflight.json").is_file()
    assert (journal / "rollback-manifest.json").is_file()
    assert (journal / "deployment-state.json").is_file()


def test_deploy_refuses_to_overwrite_unmanaged_assets_htaccess(tmp_path: Path) -> None:
    site_root = _site_root(tmp_path)
    cache_target = site_root / runtime_deploy.CACHE_POLICY_TARGET
    cache_target.write_text("# unrelated policy\n", encoding="utf-8")
    before = {
        item["target"]: (site_root / item["target"]).read_bytes()
        for item in runtime_deploy.RUNTIME_ASSETS
    }

    with pytest.raises(runtime_deploy.RuntimeDeploymentError, match="unmanaged"):
        runtime_deploy.deploy(
            site_root=site_root,
            journal_root=tmp_path / "journals",
            authorized_commit="b" * 40,
            authorization_id="runtime-test-2",
            verify_git_head=False,
        )

    for item in runtime_deploy.RUNTIME_ASSETS:
        assert (site_root / item["target"]).read_bytes() == before[item["target"]]


def test_finalize_requires_exact_external_bytes_and_no_store(tmp_path: Path) -> None:
    site_root = _site_root(tmp_path)
    journals = tmp_path / "journals"
    runtime_deploy.deploy(
        site_root=site_root,
        journal_root=journals,
        authorized_commit="c" * 40,
        authorization_id="runtime-test-3",
        verify_git_head=False,
    )

    assets = []
    for item in runtime_deploy.RUNTIME_ASSETS:
        expected = runtime_deploy.sha256_file(runtime_deploy.ROOT / item["source"])
        assets.append(
            {
                "target": item["target"],
                "expected_sha256": expected,
                "public_sha256": expected,
                "match": True,
                "cache_control": "no-store, max-age=0",
            }
        )

    evidence = {
        "schema_version": "1.0.0",
        "status": "PASS",
        "authorized_commit": "c" * 40,
        "assets_expected": len(runtime_deploy.RUNTIME_ASSETS),
        "assets_matching": len(runtime_deploy.RUNTIME_ASSETS),
        "assets": assets,
        "block_reasons": [],
    }
    evidence_path = tmp_path / "external.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    final = runtime_deploy.finalize(
        journal_dir=journals / "runtime-test-3",
        external_evidence=evidence_path,
        authorized_commit="c" * 40,
        authorization_id="runtime-test-3",
    )
    assert final["status"] == runtime_deploy.FINAL_STATUS
    assert final["external_delivery_verified"] is True
    assert final["final_health_status"] == "HEALTHY"


def test_finalize_blocks_when_cache_policy_is_not_visible_externally(tmp_path: Path) -> None:
    site_root = _site_root(tmp_path)
    journals = tmp_path / "journals"
    runtime_deploy.deploy(
        site_root=site_root,
        journal_root=journals,
        authorized_commit="d" * 40,
        authorization_id="runtime-test-4",
        verify_git_head=False,
    )

    assets = []
    for item in runtime_deploy.RUNTIME_ASSETS:
        expected = runtime_deploy.sha256_file(runtime_deploy.ROOT / item["source"])
        assets.append(
            {
                "target": item["target"],
                "expected_sha256": expected,
                "public_sha256": expected,
                "match": True,
                "cache_control": "public, max-age=31536000",
            }
        )

    evidence = {
        "status": "PASS",
        "authorized_commit": "d" * 40,
        "assets_expected": len(runtime_deploy.RUNTIME_ASSETS),
        "assets_matching": len(runtime_deploy.RUNTIME_ASSETS),
        "assets": assets,
        "block_reasons": [],
    }
    evidence_path = tmp_path / "external-blocked.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(runtime_deploy.RuntimeDeploymentError, match="cache policy"):
        runtime_deploy.finalize(
            journal_dir=journals / "runtime-test-4",
            external_evidence=evidence_path,
            authorized_commit="d" * 40,
            authorization_id="runtime-test-4",
        )

def test_production_workflow_is_runtime_only_and_automatic_on_main() -> None:
    workflow = (
        runtime_deploy.ROOT / ".github" / "workflows" / "fiscal-runtime-production.yml"
    ).read_text(encoding="utf-8")

    assert "branches: [main]" in workflow
    assert "workflow_dispatch:" in workflow
    assert "deploy_fiscal_runtime_assets.py" in workflow
    assert "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL" in workflow
    assert "APPLIED_EXTERNALLY_HEALTHY" in workflow
    assert "Cache-Control" in workflow
    assert "no-store" in workflow

    for forbidden in (
        "src/pages/",
        "src/adapters/",
        "src/assets/",
        "wp-content/plugins",
        "Purge Everything",
    ):
        assert forbidden not in workflow
