from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
H28 = ROOT / "consumers/frontend/ferias-clt"
PARTS = H28 / "parts"
MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def source() -> str:
    return "\n".join(
        (PARTS / name).read_text(encoding="utf-8")
        for name in (
            "01-head-hero.php",
            "02-calculator.php",
            "03-guide.php",
            "04-faq-footer.php",
        )
    )


def test_h28_r1_is_integrated_with_global_site_shell() -> None:
    text = source()
    for marker in (
        "/PHP/config-site.php",
        "/PHP/head-global.php",
        "/PHP/header-menu.php",
        "/PHP/section-produtos.php",
        "/PHP/cta-final.php",
        "/PHP/section-formulario.php",
        "/PHP/footer.php",
        "/PHP/btn-whatsapp.php",
        "/financas/calculadoras/assets/calculadoras-ui.css",
    ):
        assert marker in text
    assert '<main id="calc-page">' in text
    assert 'id="calc-ferias-clt"' in text


def test_h28_r1_preserves_primary_seo_and_adds_real_abono_semantics() -> None:
    text = source().lower()
    assert "calculadora de férias clt" in text
    assert "1/3 constitucional" in text
    assert "abono pecuniário" in text
    assert "venda de 1/3" in text
    assert "vender 10 dias" in text
    assert "softwareapplication" in text
    assert "faqpage" in text
    assert "https://sanida.com.br/financas/calculadoras/ferias-clt/" in text

    # O H28 atual não é uma calculadora de férias proporcionais por tempo de vínculo.
    assert "calculadora de férias proporcionais" not in text
    assert "quanto recebo quando volto de férias" not in text
    assert "cálculo exato" not in text


def test_h28_r1_keeps_runtime_contract_and_defensive_payment_date_disclosure() -> None:
    text = source()
    for marker in (
        'name="base_ferias"',
        'name="faltas_injustificadas"',
        'name="data_pagamento"',
        'type="date" required',
        "não presume a data de hoje",
        'name="vender_um_terco"',
        'name="dependentes"',
        'name="pensao"',
        'data-row="direito"',
        'data-row="gozo"',
        'data-row="abono-dias"',
        'data-row="abono-principal"',
        'data-row="abono-terco"',
        'data-row="base-inss"',
        'data-row="renda-ir"',
        'data-row="base-ir"',
        'data-row="renda-redutor"',
        'data-row="release-id"',
        "Release fiscal usada",
        "Fora do escopo automático",
    ):
        assert marker in text


def test_h28_r1_strengthens_calculator_cluster_without_claiming_unmodeled_intent() -> None:
    text = source()
    for href in (
        "/financas/calculadoras/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/rescisao-clt/",
        "/financas/calculadoras/salario-liquido-clt/",
    ):
        assert href in text


def test_h28_r1_manifest_declares_global_dependencies() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    deps = {
        item["target_path"]: set(item["required_by"])
        for item in manifest["preexisting_dependencies"]
    }
    for path in (
        "PHP/config-site.php",
        "PHP/head-global.php",
        "PHP/header-menu.php",
        "PHP/section-produtos.php",
        "PHP/cta-final.php",
        "PHP/section-formulario.php",
        "PHP/footer.php",
        "PHP/btn-whatsapp.php",
        "financas/calculadoras/assets/calculadoras-ui.css",
    ):
        assert "H28" in deps[path]
