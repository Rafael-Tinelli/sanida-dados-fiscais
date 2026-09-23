from __future__ import annotations

from pathlib import Path
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "integrations/wordpress/dados-oficiais-br/dados-oficiais-br.php"
ASSET = ROOT / "integrations/wordpress/dados-oficiais-br/assets/seguro-desemprego-calculadora.js"


def test_sd_calculator_asset_is_externalized_and_enqueued() -> None:
    plugin = PLUGIN.read_text(encoding="utf-8")
    asset = ASSET.read_text(encoding="utf-8")

    assert "enqueue_sd_calculator_asset" in plugin
    assert "wp_enqueue_scripts" in plugin
    assert "plugins_url('assets/seguro-desemprego-calculadora.js', __FILE__)" in plugin
    assert "data-sd-params" in plugin
    assert "data-sd-calc" in plugin
    assert "sd_parametros_json" in plugin
    assert "get_queried_object_id" in plugin
    assert "=== 944" in plugin
    assert "strip_legacy_sd_inline_script" in plugin
    assert "filemtime" in plugin
    assert "Version: 1.4.12" in plugin

    assert "window.__SANIDA_SD_B14_LOADED__ = true;" in asset
    assert "DOMContentLoaded" in asset
    assert "data-sd-params" in asset
    assert "data-sd-parcel-rule" in asset
    assert "Math.ceil" in asset
    assert "=> " not in asset
    assert "`" not in asset


def test_sd_calculator_asset_node_syntax() -> None:
    node = shutil.which("node")
    if node is None:
        return

    run = subprocess.run(
        [node, "--check", str(ASSET)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stderr or run.stdout
