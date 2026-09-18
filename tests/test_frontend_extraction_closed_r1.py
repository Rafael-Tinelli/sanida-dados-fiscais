from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

FORBIDDEN_ACTIVE_FRONTEND_TOKENS = (
    "consumers/frontend",
    "/financas/calculadoras/",
    "phase7-c71-deployment-manifest-v2.json",
    "build_phase7_c71_bundle_v2.py",
    "run_phase7_c75_postdeploy_validation.py",
    "run_phase7_c75_external_delivery_probe.ps1",
)


def test_active_workflows_do_not_reclaim_frontend_ownership() -> None:
    files = sorted([*WORKFLOWS.glob("*.yml"), *WORKFLOWS.glob("*.yaml")])
    assert files, "no active workflows found"

    violations: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN_ACTIVE_FRONTEND_TOKENS:
            if token in text:
                violations.append(f"{path.relative_to(ROOT)} -> {token}")

    assert violations == [], (
        "frontend extraction is closed; active fiscal workflows must not publish "
        "or validate the public calculator frontend again:\n" + "\n".join(violations)
    )


def test_legacy_hostgator_frontend_workflow_remains_non_active() -> None:
    active = WORKFLOWS / "h25-v2-hostgator-preflight.yml"
    historical = ROOT / "ops" / "workflows" / "h25-v2-hostgator-preflight.yml"

    assert not active.exists()
    assert historical.exists(), (
        "historical deployment evidence may remain under ops/, but it must not "
        "be promoted back into .github/workflows"
    )
