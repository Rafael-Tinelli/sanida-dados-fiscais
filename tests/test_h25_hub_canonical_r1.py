from __future__ import annotations

import json
from pathlib import Path

from scripts.build_phase7_c71_bundle_v2 import build_bundle_v2
from scripts.simulate_phase7_c71_deployment import run_simulation

ROOT = Path(__file__).resolve().parents[1]
H25 = ROOT / "consumers/frontend/calculadoras/index.php"
BASE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
EXTENSION_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"


def test_h25_is_canonical_distributor_for_h26_h29() -> None:
    text = H25.read_text(encoding="utf-8")
    lower = text.lower()

    for marker in (
        "https://sanida.com.br/financas/calculadoras/",
        "/PHP/config-site.php",
        "/PHP/head-global.php",
        "/PHP/header-menu.php",
        "/PHP/section-produtos.php",
        "/PHP/cta-final.php",
        "/PHP/section-formulario.php",
        "/PHP/footer.php",
        "/PHP/btn-whatsapp.php",
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
        "collectionpage",
        "itemlist",
    ):
        assert marker.lower() in lower

    assert "salário bruto → líquido" in text
    assert "estimativa parcial" in lower
    assert "fgts, aviso prévio e seguro-desemprego ficam fora do cálculo automático" in lower
    assert "<span>FGTS</span>" not in text
    assert "<span>aviso prévio</span>" not in text


def test_h25_extension_preserves_historical_c71_manifest() -> None:
    base = json.loads(BASE_MANIFEST.read_text(encoding="utf-8"))
    extension = json.loads(EXTENSION_MANIFEST.read_text(encoding="utf-8"))

    assert len(base["managed_files"]) == 32
    assert all(
        item["target_path"] != "financas/calculadoras/index.php"
        for item in base["managed_files"]
    )

    assert extension["base_manifest"] == "docs/phase7-c71-deployment-manifest-v1.json"
    assert extension["base_managed_file_count"] == 32
    assert extension["resulting_managed_file_count"] == 33
    assert extension["production_deployed"] is False
    assert extension["managed_files_add"] == [
        {
            "source": "consumers/frontend/calculadoras/index.php",
            "target_root": "site_root",
            "target_path": "financas/calculadoras/index.php",
        }
    ]


def test_h25_extended_bundle_is_deterministic_and_rollback_safe(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    bundle = build_bundle_v2(EXTENSION_MANIFEST, bundle_dir)

    assert bundle["managed_file_count"] == 33
    assert bundle["production_deployed"] is False
    assert bundle["source_manifest_chain"] == [
        "docs/phase7-c71-deployment-manifest-v1.json",
        "docs/phase7-c71-deployment-manifest-v2.json",
    ]

    central = next(
        item
        for item in bundle["files"]
        if item["target_path"] == "financas/calculadoras/index.php"
    )
    assert central["source"] == "consumers/frontend/calculadoras/index.php"
    assert (bundle_dir / central["bundle_path"]).read_bytes() == H25.read_bytes()

    simulation = run_simulation(bundle_dir)
    assert simulation == {
        "checkpoint": "C7.1",
        "managed_files": 33,
        "apply_verified": True,
        "rollback_exact_bytes_verified": True,
        "new_file_cleanup_verified": True,
        "preexisting_dependencies_preserved": True,
        "unmanaged_files_preserved": True,
        "production_mutated": False,
    }
