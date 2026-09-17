from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
H25 = ROOT / "consumers/frontend/calculadoras/index.php"


def source() -> str:
    return H25.read_text(encoding="utf-8")


def test_h25_is_a_router_not_a_competing_calculator() -> None:
    text = source()
    lower = text.lower()

    assert "https://sanida.com.br/financas/calculadoras/" in text
    assert "calculadoras trabalhistas clt" in lower
    assert "esta página não faz cálculos" in lower

    for href in (
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
    ):
        assert href in text

    for forbidden in (
        "data-kpi=",
        "data-row=",
        "folha-core.js",
        "salario-liquido.js",
        "decimo-terceiro.js",
        "ferias-clt.js",
        "rescisao-clt.js",
    ):
        assert forbidden not in text


def test_h25_describes_each_tool_with_current_product_boundaries() -> None:
    lower = source().lower()

    for marker in (
        "salário bruto para líquido",
        "primeira e segunda parcela",
        "abono pecuniário",
        "estimativa parcial",
        "sem justa causa",
        "pedido de demissão",
        "acordo do art. 484-a",
        "justa causa",
        "não calcula fgts, multa de 40%, aviso prévio, seguro-desemprego nem o total completo do trct",
    ):
        assert marker in lower

    assert "rescisão com fgts" not in lower
    assert "cálculo exato" not in lower


def test_h25_keeps_global_shell_and_shared_stylesheet() -> None:
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
        "/financas/calculadoras/assets/calculadoras-ui.css?v=20260916-f06f10",
    ):
        assert marker in text
