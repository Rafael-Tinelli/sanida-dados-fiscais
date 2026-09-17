from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
H26_PAGE = ROOT / "consumers/frontend/salario-liquido-clt/index.php"
H26_JS = ROOT / "consumers/frontend/salario-liquido.js"


def page_source() -> str:
    return H26_PAGE.read_text(encoding="utf-8")


def test_h26_r1_keeps_global_shell_canonical_and_stable_assets() -> None:
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
        "https://sanida.com.br/financas/calculadoras/salario-liquido-clt/",
        "softwareapplication",
        "faqpage",
    ):
        assert marker.lower() in lower

    assert text.count("?v=20260916-f06f10") == 2
    assert "time()" not in text


def test_h26_r1_prioritizes_gross_deductions_net_and_progressive_explanation() -> None:
    text = page_source()
    lower = text.lower()

    for marker in (
        "calculadora de salário líquido clt",
        "salário bruto",
        "salário líquido",
        "inss",
        "irrf",
        "dependentes",
        "pensão alimentícia",
        "como chegamos a esse valor",
        "entender o cálculo de inss e irrf",
        "regras fiscais e detalhes técnicos",
    ):
        assert marker in lower

    for marker in (
        'data-kpi="liquido"',
        'data-kpi="bruto"',
        'data-kpi="descontos"',
        'data-row="base-ir"',
        'data-row="ir-antes-reducao"',
        'data-row="renda-redutor"',
        'data-row="reducao"',
        'data-row="referencia"',
        'data-row="release-id"',
    ):
        assert marker in text


def test_h26_r1_does_not_overpromise_variable_calculators_or_reopen_fiscal_engine() -> None:
    lower = page_source().lower()
    js = H26_JS.read_text(encoding="utf-8")

    assert "média já apurada de variáveis" in lower
    assert "esta calculadora não calcula horas, percentuais ou dsr" in lower
    assert "não calcula horas extras, adicional noturno, periculosidade ou insalubridade" in lower

    for forbidden in (
        "calculadora de hora extra",
        "calculadora de adicional noturno",
        "calculadora de periculosidade",
        "calculadora de insalubridade",
        "cálculo exato",
        "intenções de busca",
        "intenção de busca",
        "keyword",
    ):
        assert forbidden not in lower

    for required in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.assessInss(",
        "SFA.assessIrrf(",
        "SFA.H26",
    ):
        assert required in js


def test_h26_r1_keeps_cluster_links_and_runtime_order() -> None:
    text = page_source()
    for href in (
        "/financas/calculadoras/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
    ):
        assert href in text

    core = text.index("/financas/calculadoras/assets/folha-core.js")
    adapter = text.index("/financas/calculadoras/assets/salario-liquido.js")
    assert core < adapter
