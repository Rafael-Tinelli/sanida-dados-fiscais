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
ALLOWED_ASSETS = {
    "folha-core.js",
    "salario-liquido.js",
    "folha-thirteenth.js",
    "decimo-terceiro.js",
    "folha-vacation.js",
    "ferias-clt.js",
    "folha-termination.js",
    "rescisao-clt.js",
}


def _release() -> dict:
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    return json.loads(artifact.read_text(encoding="utf-8"))


RELEASE = _release()
RELEASE_ID = RELEASE["release_id"]


def _rows(*names: str) -> str:
    return "".join(f'<span data-row="{name}">—</span>' for name in names)


def _kpis(*names: str) -> str:
    return "".join(f'<span data-kpi="{name}">—</span>' for name in names)


def _shell(root_id: str, fields: str, scripts: list[str], result_body: str) -> str:
    tags = "\n".join(f'<script src="/assets/{name}"></script>' for name in scripts)
    return f"""<!doctype html>
<html lang="pt-BR">
<head><meta charset="utf-8"><title>Sanida browser smoke</title></head>
<body>
<section id="{root_id}">
  <div data-alert style="display:none"></div>
  <form novalidate>
    {fields}
    <button type="submit">Calcular</button>
  </form>
  <aside data-result style="display:block">{result_body}</aside>
</section>
{tags}
</body>
</html>"""


def _h26() -> str:
    fields = """
      <input name="salario" value="6000.00">
      <input name="variaveis" value="0.00">
      <input name="dependentes" value="0">
      <input name="pensao" value="0.00">
      <input name="outros" value="0.00">
    """
    result = _kpis("bruto", "liquido", "descontos") + _rows(
        "inss", "irrf", "pensao", "outros", "base-ir", "ir-antes-reducao",
        "renda-redutor", "modo", "reducao", "aliquotas", "ano", "referencia", "release-id"
    )
    return _shell(
        "calc-salario-liquido",
        fields,
        ["folha-core.js", "salario-liquido.js"],
        result,
    )


def _h27() -> str:
    fields = """
      <input name="salario" value="4000.00">
      <input name="variaveis" value="0.00">
      <input type="radio" name="modo_avos" value="manual" checked>
      <select name="meses"><option value="12" selected>12</option></select>
      <input name="data_inicio" value="">
      <input name="data_fim" value="">
      <input name="data_quitacao" type="date" value="2026-12-20">
      <input name="adiantamento_pago" value="2000.00">
      <input name="salario_mes_anterior" value="">
      <input name="data_adiantamento" value="">
      <input type="checkbox" name="admissao_ano">
      <input name="dependentes" value="0">
      <input name="pensao" value="0.00">
      <input type="checkbox" name="liquido" checked>
      <div data-manual-avos></div>
      <div data-auto-avos hidden></div>
    """
    result = _kpis("base", "total13") + _rows(
        "avos", "primeira", "status-adiantamento", "segunda-bruta", "segunda-liquida",
        "status-liquidacao", "saldo-antes-piso", "insuficiencia", "inss13", "ir13",
        "base-ir", "ir-antes-reducao", "renda-redutor", "modo", "reducao",
        "ano-fiscal", "referencia", "release-id"
    )
    return _shell(
        "calc-decimo-terceiro",
        fields,
        ["folha-core.js", "folha-thirteenth.js", "decimo-terceiro.js"],
        result,
    )


def _h28() -> str:
    fields = """
      <input name="base_ferias" value="4000.00">
      <input name="faltas_injustificadas" value="0">
      <input name="data_pagamento" type="date" value="2026-09-15">
      <input type="checkbox" name="vender_um_terco" checked>
      <input name="dependentes" value="0">
      <input name="pensao" value="0.00">
    """
    result = _kpis("bruto", "liquido", "dias") + _rows(
        "direito", "gozo", "abono-dias", "principal-gozo", "terco-gozo", "abono-principal",
        "abono-terco", "base-inss", "inss", "renda-ir", "base-ir", "modo-ir",
        "ir-antes-reducao", "renda-redutor", "reducao", "irrf", "pensao", "referencia", "release-id"
    )
    return _shell(
        "calc-ferias-clt",
        fields,
        ["folha-core.js", "folha-vacation.js", "ferias-clt.js"],
        result,
    )


def _h29() -> str:
    fields = """
      <select name="motivo_esocial"><option value="02" selected>02</option></select>
      <select name="regime_emprego"><option value="monthly" selected>monthly</option></select>
      <select name="prazo_contrato"><option value="indefinite" selected>indefinite</option></select>
      <input name="data_admissao" type="date" value="2025-09-01">
      <input name="data_desligamento" type="date" value="2026-03-31">
      <input name="salario_base_mensal" value="3100.00">
      <input name="dias_computados" value="31">
      <div data-conditional="thirteenth"><input name="remuneracao_mes_desligamento" value="3600.00"></div>
    """
    result = _kpis("saldo", "decimo", "ferias") + _rows(
        "motivo", "saldo-formula", "saldo", "13-avos", "13-referencia", "13-bruto",
        "ferias-periodo", "ferias-avos", "ferias-adquiridas", "promessa", "referencia", "release-id"
    ) + '<ul data-list="incluidos"></ul><ul data-list="excluidos"></ul>'
    return _shell(
        "calc-rescisao-clt",
        fields,
        ["folha-core.js", "folha-termination.js", "rescisao-clt.js"],
        result,
    )


PAGES = {
    "/h26/": _h26,
    "/h27/": _h27,
    "/h28/": _h28,
    "/h29/": _h29,
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args) -> None:
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
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
            if name not in ALLOWED_ASSETS:
                self._send(404, b"not found", "text/plain")
                return
            file = FRONTEND / name
            content_type = mimetypes.guess_type(file.name)[0] or "application/javascript"
            self._send(200, file.read_bytes(), content_type)
            return
        factory = PAGES.get(path)
        if factory:
            self._send(200, factory().encode("utf-8"), "text/html; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain")


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


def _driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,900")
    chrome = (
        shutil.which("google-chrome")
        or shutil.which("google-chrome-stable")
        or shutil.which("chromium")
        or shutil.which("chromium-browser")
    )
    if chrome:
        options.binary_location = chrome
    return webdriver.Chrome(options=options)


def _wait_runtime(driver: webdriver.Chrome, consumer: str) -> None:
    WebDriverWait(driver, 15).until(
        lambda d: d.execute_script(
            "return Boolean(window.SFA_FOLHA && window.SFA_FOLHA[arguments[0]])", consumer
        )
    )


def _assert_accessibility(driver: webdriver.Chrome) -> None:
    alert = driver.find_element(By.CSS_SELECTOR, "[data-alert]")
    result = driver.find_element(By.CSS_SELECTOR, "[data-result]")
    assert alert.get_attribute("role") == "alert"
    assert alert.get_attribute("aria-live") == "assertive"
    assert alert.get_attribute("aria-atomic") == "true"
    assert result.get_attribute("role") == "status"
    assert result.get_attribute("aria-live") == "polite"
    assert result.get_attribute("aria-atomic") == "true"


def _submit(driver: webdriver.Chrome) -> None:
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()


def _wait_success(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, 15).until(
        lambda d: d.find_element(By.CSS_SELECTOR, "[data-result]").get_attribute("data-release-id") == RELEASE_ID
    )


def _wait_error(driver: webdriver.Chrome) -> str:
    WebDriverWait(driver, 15).until(
        lambda d: bool(d.find_element(By.CSS_SELECTOR, "[data-alert]").text.strip())
    )
    alert = driver.find_element(By.CSS_SELECTOR, "[data-alert]").text.strip()
    display = driver.find_element(By.CSS_SELECTOR, "[data-result]").value_of_css_property("display")
    assert display == "none"
    return alert


def _set(driver: webdriver.Chrome, name: str, value: str) -> None:
    element = driver.find_element(By.CSS_SELECTOR, f'[name="{name}"]')
    driver.execute_script("arguments[0].value = arguments[1]", element, value)


def smoke_h26(driver: webdriver.Chrome, base: str) -> None:
    driver.get(base + "/h26/")
    _wait_runtime(driver, "H26")
    _assert_accessibility(driver)
    _submit(driver)
    _wait_success(driver)
    assert "R$" in driver.find_element(By.CSS_SELECTOR, '[data-kpi="liquido"]').text
    _set(driver, "salario", "-1.00")
    _submit(driver)
    message = _wait_error(driver)
    assert "Revise os dados informados" in message
    assert "release fiscal" not in message.lower()


def smoke_h27(driver: webdriver.Chrome, base: str) -> None:
    driver.get(base + "/h27/")
    _wait_runtime(driver, "H27")
    _assert_accessibility(driver)
    _submit(driver)
    _wait_success(driver)
    assert "R$" in driver.find_element(By.CSS_SELECTOR, '[data-row="segunda-liquida"]').text
    _set(driver, "salario", "-1.00")
    _submit(driver)
    assert _wait_error(driver)


def smoke_h28(driver: webdriver.Chrome, base: str) -> None:
    driver.get(base + "/h28/")
    _wait_runtime(driver, "H28")
    _assert_accessibility(driver)
    _submit(driver)
    _wait_success(driver)
    assert "R$" in driver.find_element(By.CSS_SELECTOR, '[data-kpi="liquido"]').text
    _set(driver, "data_pagamento", "")
    _submit(driver)
    message = _wait_error(driver)
    assert "data de pagamento" in message.lower()


def smoke_h29(driver: webdriver.Chrome, base: str) -> None:
    driver.get(base + "/h29/")
    _wait_runtime(driver, "H29_UI")
    _assert_accessibility(driver)
    _submit(driver)
    _wait_success(driver)
    assert "R$" in driver.find_element(By.CSS_SELECTOR, '[data-kpi="saldo"]').text
    _set(driver, "salario_base_mensal", "")
    _submit(driver)
    message = _wait_error(driver)
    assert "Salário-base mensal" in message


def main() -> int:
    with server() as base:
        driver = _driver()
        try:
            smoke_h26(driver, base)
            smoke_h27(driver, base)
            smoke_h28(driver, base)
            smoke_h29(driver, base)
        finally:
            driver.quit()
    print(f"Calculator browser smoke: PASS (H26-H29, release={RELEASE_ID})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
