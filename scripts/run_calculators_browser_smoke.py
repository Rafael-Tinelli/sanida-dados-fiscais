from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
import shutil
import threading
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "consumers" / "frontend"
STORE = ROOT / "releases" / "fiscal-v1"
ASSETS = {
    "folha-core.js", "salario-liquido.js", "folha-thirteenth.js", "decimo-terceiro.js",
    "folha-vacation.js", "ferias-clt.js", "folha-termination.js", "rescisao-clt.js",
}

manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
RELEASE = json.loads((STORE / manifest["artifact"]).read_text(encoding="utf-8"))
RELEASE_ID = RELEASE["release_id"]


def rows(*names: str) -> str:
    return "".join(f'<span data-row="{name}">—</span>' for name in names)


def kpis(*names: str) -> str:
    return "".join(f'<span data-kpi="{name}">—</span>' for name in names)


def shell(root_id: str, fields: str, scripts: list[str], result_body: str) -> str:
    tags = "\n".join(f'<script src="/assets/{name}"></script>' for name in scripts)
    return f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Smoke</title></head>
<body><section id="{root_id}">
<div data-alert role="alert" aria-live="assertive" aria-atomic="true" style="display:none"></div>
<form novalidate>{fields}<button type="submit">Calcular</button></form>
<aside data-result role="status" aria-live="polite" aria-atomic="true" style="display:block">{result_body}</aside>
</section>{tags}</body></html>"""


def h26() -> str:
    fields = """
    <input name="salario" value="6000.00"><input name="variaveis" value="0.00">
    <input name="dependentes" value="0"><input name="pensao" value="0.00"><input name="outros" value="0.00">
    """
    result = kpis("bruto", "liquido", "descontos") + rows(
        "inss", "irrf", "pensao", "outros", "base-ir", "ir-antes-reducao", "renda-redutor",
        "modo", "reducao", "aliquotas", "ano", "referencia", "release-id",
    )
    return shell("calc-salario-liquido", fields, ["folha-core.js", "salario-liquido.js"], result)


def h27() -> str:
    fields = """
    <input name="salario" value="4000.00"><input name="variaveis" value="0.00">
    <input type="radio" name="modo_avos" value="manual" checked>
    <select name="meses"><option value="12" selected>12</option></select>
    <input name="data_inicio"><input name="data_fim"><input name="data_quitacao" type="date" value="2026-12-20">
    <input name="adiantamento_pago" value="2000.00"><input name="salario_mes_anterior"><input name="data_adiantamento">
    <input type="checkbox" name="admissao_ano"><input name="dependentes" value="0"><input name="pensao" value="0.00">
    <input type="checkbox" name="liquido" checked><div data-manual-avos></div><div data-auto-avos hidden></div>
    """
    result = kpis("base", "total13") + rows(
        "avos", "primeira", "status-adiantamento", "segunda-bruta", "segunda-liquida", "status-liquidacao",
        "saldo-antes-piso", "insuficiencia", "inss13", "ir13", "base-ir", "ir-antes-reducao",
        "renda-redutor", "modo", "reducao", "ano-fiscal", "referencia", "release-id",
    )
    return shell("calc-decimo-terceiro", fields, ["folha-core.js", "folha-thirteenth.js", "decimo-terceiro.js"], result)


def h28() -> str:
    fields = """
    <input name="base_ferias" value="4000.00"><input name="faltas_injustificadas" value="0">
    <input name="data_pagamento" type="date" value="2026-09-15"><input type="checkbox" name="vender_um_terco" checked>
    <input name="dependentes" value="0"><input name="pensao" value="0.00">
    """
    result = kpis("bruto", "liquido", "dias") + rows(
        "direito", "gozo", "abono-dias", "principal-gozo", "terco-gozo", "abono-principal", "abono-terco",
        "base-inss", "inss", "renda-ir", "base-ir", "modo-ir", "ir-antes-reducao", "renda-redutor",
        "reducao", "irrf", "pensao", "referencia", "release-id",
    )
    return shell("calc-ferias-clt", fields, ["folha-core.js", "folha-vacation.js", "ferias-clt.js"], result)


def h29() -> str:
    fields = """
    <select name="motivo_esocial"><option value="02" selected>02</option></select>
    <select name="regime_emprego"><option value="monthly" selected>monthly</option></select>
    <select name="prazo_contrato"><option value="indefinite" selected>indefinite</option></select>
    <input name="data_admissao" type="date" value="2025-09-01"><input name="data_desligamento" type="date" value="2026-03-31">
    <input name="salario_base_mensal" value="3100.00"><input name="dias_computados" value="31">
    <div data-conditional="thirteenth"><input name="remuneracao_mes_desligamento" value="3600.00"></div>
    """
    result = kpis("saldo", "decimo", "ferias") + rows(
        "motivo", "saldo-formula", "saldo", "13-avos", "13-referencia", "13-bruto", "ferias-periodo",
        "ferias-avos", "ferias-adquiridas", "promessa", "referencia", "release-id",
    ) + '<ul data-list="incluidos"></ul><ul data-list="excluidos"></ul>'
    return shell("calc-rescisao-clt", fields, ["folha-core.js", "folha-termination.js", "rescisao-clt.js"], result)


PAGES = {"/h26/": h26, "/h27/": h27, "/h28/": h28, "/h29/": h29}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args) -> None:
        return

    def send_body(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/blog/wp-json/sfa/v1/fiscal-release":
            body = json.dumps(RELEASE, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("X-Sanida-Fiscal-Release", RELEASE_ID)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path.startswith("/assets/"):
            name = Path(path).name
            if name not in ASSETS:
                self.send_body(404, b"not found", "text/plain")
                return
            file = FRONTEND / name
            self.send_body(200, file.read_bytes(), mimetypes.guess_type(file.name)[0] or "application/javascript")
            return
        factory = PAGES.get(path)
        if factory:
            self.send_body(200, factory().encode("utf-8"), "text/html; charset=utf-8")
            return
        self.send_body(404, b"not found", "text/plain")


@contextmanager
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)
        httpd.server_close()


def driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,900")
    chrome = shutil.which("google-chrome") or shutil.which("google-chrome-stable") or shutil.which("chromium")
    if chrome:
        options.binary_location = chrome
    return webdriver.Chrome(options=options)


def wait_runtime(drv: webdriver.Chrome, consumer: str) -> None:
    WebDriverWait(drv, 15).until(lambda d: d.execute_script(
        "return Boolean(window.SFA_FOLHA && window.SFA_FOLHA[arguments[0]])", consumer
    ))


def assert_a11y(drv: webdriver.Chrome) -> None:
    alert = drv.find_element(By.CSS_SELECTOR, "[data-alert]")
    result = drv.find_element(By.CSS_SELECTOR, "[data-result]")
    assert (alert.get_attribute("role"), alert.get_attribute("aria-live"), alert.get_attribute("aria-atomic")) == ("alert", "assertive", "true")
    assert (result.get_attribute("role"), result.get_attribute("aria-live"), result.get_attribute("aria-atomic")) == ("status", "polite", "true")


def set_value(drv: webdriver.Chrome, name: str, value: str) -> None:
    element = drv.find_element(By.CSS_SELECTOR, f'[name="{name}"]')
    drv.execute_script("arguments[0].value = arguments[1]", element, value)


def submit(drv: webdriver.Chrome) -> None:
    drv.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()


def wait_success(drv: webdriver.Chrome) -> None:
    WebDriverWait(drv, 15).until(lambda d: d.find_element(By.CSS_SELECTOR, "[data-result]").get_attribute("data-release-id") == RELEASE_ID)


def wait_error(drv: webdriver.Chrome) -> str:
    WebDriverWait(drv, 15).until(lambda d: bool(d.find_element(By.CSS_SELECTOR, "[data-alert]").text.strip()))
    assert drv.find_element(By.CSS_SELECTOR, "[data-result]").value_of_css_property("display") == "none"
    return drv.find_element(By.CSS_SELECTOR, "[data-alert]").text.strip()


def run_case(drv: webdriver.Chrome, base: str, path: str, runtime: str, success_selector: str, invalid_name: str, invalid_value: str, error_fragment: str | None = None) -> None:
    drv.get(base + path)
    wait_runtime(drv, runtime)
    assert_a11y(drv)
    submit(drv)
    wait_success(drv)
    assert "R$" in drv.find_element(By.CSS_SELECTOR, success_selector).text
    set_value(drv, invalid_name, invalid_value)
    submit(drv)
    message = wait_error(drv)
    if error_fragment:
        assert error_fragment.lower() in message.lower()


def main() -> int:
    with server() as base:
        drv = driver()
        try:
            run_case(drv, base, "/h26/", "H26", '[data-kpi="liquido"]', "salario", "-1.00", "Revise os dados informados")
            assert "release fiscal" not in drv.find_element(By.CSS_SELECTOR, "[data-alert]").text.lower()
            run_case(drv, base, "/h27/", "H27", '[data-row="segunda-liquida"]', "salario", "-1.00")
            run_case(drv, base, "/h28/", "H28", '[data-kpi="liquido"]', "data_pagamento", "", "data de pagamento")
            run_case(drv, base, "/h29/", "H29_UI", '[data-kpi="saldo"]', "salario_base_mensal", "", "Salário-base mensal")
        finally:
            drv.quit()
    print(f"Calculator browser smoke: PASS (H26-H29, release={RELEASE_ID})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
