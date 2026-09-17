from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
H29 = ROOT / "consumers/frontend/rescisao-clt"
PARTS = H29 / "parts"
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


def test_h29_r1_is_integrated_with_global_site_shell() -> None:
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
    assert 'id="calc-rescisao-clt"' in text


def test_h29_r1_has_correct_seo_contract_without_overpromising() -> None:
    text = source().lower()

    assert "calculadora de rescisão clt" in text
    assert "pedido de demissão" in text
    assert "demissão sem justa causa" in text
    assert "rescisão por acordo" in text
    assert "justa causa" in text
    assert "estimativa parcial" in text

    assert "cálculo exato rescisão" not in text
    assert "quanto vou receber de rescisão" not in text

    assert "softwareapplication" in text
    assert "faqpage" in text
    assert "https://sanida.com.br/financas/calculadoras/rescisao-clt/" in text


def test_h29_r1_keeps_backend_input_contract_but_translates_labels() -> None:
    text = source()

    for marker in (
        'name="motivo_esocial"',
        'value="01"',
        'value="02"',
        'value="07"',
        'value="33"',
        'name="regime_emprego"',
        'name="prazo_contrato"',
        'value="indefinite"',
        'name="data_admissao"',
        'name="data_desligamento"',
        'name="salario_base_mensal"',
        'name="dias_computados"',
        'name="remuneracao_mes_desligamento"',
    ):
        assert marker in text

    assert "Como terminou o contrato?" in text
    assert "Fui demitido sem justa causa" in text
    assert "Pedi demissão" in text
    assert "Fiz acordo com a empresa" in text
    assert "Fui demitido por justa causa" in text


def test_h29_r1_preserves_partial_scope_and_audit_surface() -> None:
    text = source()

    for marker in (
        "Esta não é uma calculadora de “total da rescisão”",
        "Fora do cálculo automático",
        'data-kpi="saldo"',
        'data-kpi="decimo"',
        'data-kpi="ferias"',
        'data-row="saldo-formula"',
        'data-row="13-avos"',
        'data-row="ferias-periodo"',
        'data-row="ferias-avos"',
        'data-row="promessa"',
        'data-row="referencia"',
        'data-row="release-id"',
        'data-list="incluidos"',
        'data-list="excluidos"',
        "multa e saque do FGTS",
        "aviso prévio",
    ):
        assert marker in text


def test_h29_r1_strengthens_internal_calculator_cluster() -> None:
    text = source()

    for href in (
        "/financas/calculadoras/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/salario-liquido-clt/",
    ):
        assert href in text


def test_h29_r1_manifest_declares_existing_global_dependencies_for_h29() -> None:
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
        assert "H29" in deps[path]
