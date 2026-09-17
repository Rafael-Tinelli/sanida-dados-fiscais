from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
H27_DIR = ROOT / "consumers/frontend/decimo-terceiro-clt"
PARTS = H27_DIR / "parts"
H27_JS = ROOT / "consumers/frontend/decimo-terceiro.js"


def page_source() -> str:
    return "\n".join(
        (PARTS / name).read_text(encoding="utf-8")
        for name in (
            "01-head-hero.php",
            "02-calculator.php",
            "03-guide.php",
            "04-faq-footer.php",
        )
    )


def test_h27_r3_keeps_global_shell_and_canonical_seo() -> None:
    text = page_source()
    lower = text.lower()

    for marker in (
        "/PHP/config-site.php",
        "/PHP/head-global.php",
        "/PHP/header-menu.php",
        "/PHP/section-produtos.php",
        "/PHP/cta-final.php",
        "/PHP/section-formulario.php",
        "/PHP/footer.php",
        "/PHP/btn-whatsapp.php",
        "https://sanida.com.br/financas/calculadoras/decimo-terceiro/",
        "softwareapplication",
        "faqpage",
    ):
        assert marker.lower() in lower


def test_h27_r3_covers_real_search_territories_without_seo_leakage() -> None:
    lower = page_source().lower()

    for marker in (
        "calculadora de décimo terceiro salário",
        "primeira parcela",
        "segunda parcela",
        "décimo terceiro proporcional",
        "regra dos 15 dias",
        "inss",
        "irrf",
        "comissão",
        "horas extras",
    ):
        assert marker in lower

    for forbidden in (
        "intenções de busca",
        "intenção de busca",
        "esta página foi estruturada para responder buscas",
        "evergreen e de baixa manutenção",
        "cálculo exato",
        "keyword",
    ):
        assert forbidden not in lower


def test_h27_r3_result_prioritizes_total_first_and_second_installments() -> None:
    page = page_source()
    js = H27_JS.read_text(encoding="utf-8")

    for marker in (
        'data-kpi="total13"',
        'data-kpi="primeira"',
        'data-kpi="segunda-liquida"',
        'data-row="status-liquidacao"',
        'data-row="saldo-antes-piso"',
        'data-row="insuficiencia"',
        'data-row="release-id"',
    ):
        assert marker in page

    assert "setText('[data-kpi=\"primeira\"]'" in js
    assert "setText('[data-kpi=\"segunda-liquida\"]'" in js
    assert "settlementStatusLabel(calculation.settlement)" in js
    assert "calculation.settlement.insufficiency_amount" in js


def test_h27_r3_does_not_reopen_fiscal_engine_or_overpromise_variable_math() -> None:
    text = page_source().lower()
    js = H27_JS.read_text(encoding="utf-8")

    assert "média monetária de variáveis já apurada" in text
    assert "a calculadora não apura essa média" in text
    assert "não calcula horas" in text

    for required in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.THIRTEENTH.calculateAccrual(",
        "SFA.THIRTEENTH.calculateAdvance(",
        "SFA.THIRTEENTH.assessFiscal(",
        "buildSettlement(",
    ):
        assert required in js

    for forbidden in (
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "total13 * 0.5",
        "h27_advance_exceeds_gross",
    ):
        assert forbidden not in js


def test_h27_r3_keeps_cluster_links_contextual() -> None:
    text = page_source()
    for href in (
        "/financas/calculadoras/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
        "/financas/calculadoras/salario-liquido-clt/",
    ):
        assert href in text
