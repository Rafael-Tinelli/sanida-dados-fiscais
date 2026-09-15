<?php
if (!defined('ABSPATH')) exit;

trait Sanida_Fiscais_Calc13_Trait {

  public function sc_calc13_assets(){
    static $loaded = false;
    if ($loaded) return '';
    $loaded = true;

    add_action('wp_footer', function(){
      echo "<script id='sfa-calc13-js'>\n";
      echo $this->calc13_js();
      echo "\n</script>\n";
    }, 999);

    return '';
  }

  private function calc13_js(){
    return <<<JS
(function () {
  function brl(n) {
    try { return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(n); }
    catch (e) { return "R$ " + (Math.round(n * 100) / 100).toFixed(2).replace(".", ","); }
  }

  function parseMoney(v) {
    if (v === null || v === undefined) return 0;
    var s = String(v).trim()
      .replace(/\s/g, "")
      .replace(/[R$\u00A0]/g, "")
      .replace(/\./g, "")
      .replace(",", ".")
      .replace(/[^0-9.-]/g, "");
    var n = Number(s);
    return isFinite(n) ? n : NaN;
  }

  function decodeEntities(str) {
    if (!str || str.indexOf("&") === -1) return str;
    var t = document.createElement("textarea");
    t.innerHTML = str;
    return t.value;
  }

  function findRootFrom(el) {
    if (!el) return null;
    return el.closest('[data-sfa-calc="d13"]') || el.closest("#decimo-terceiro-v1");
  }

  function getFolhaData(root) {
    if (!root) return null;

    var folId = root.getAttribute("data-folha-id");
    if (folId) {
      var elBy = document.getElementById(folId);
      if (elBy) {
        try {
          var rawBy = decodeEntities((elBy.textContent || "").trim());
          if (rawBy) return JSON.parse(rawBy);
        } catch (e) {}
      }
    }

    var inside = root.querySelector('script[data-sfa="folha"]');
    if (inside) {
      try {
        var rawIn = decodeEntities((inside.textContent || "").trim());
        if (rawIn) return JSON.parse(rawIn);
      } catch (e) {}
    }

    var el = document.getElementById("d13-folha-v1");
    if (!el) return null;
    try {
      var raw = decodeEntities((el.textContent || "").trim());
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) {
      return null;
    }
  }

  function calcINSS(base, faixas) {
    if (!faixas || !faixas.length) return 0;
    var inss = 0, prev = 0;

    for (var i = 0; i < faixas.length; i++) {
      var lim = Number(faixas[i].limite);
      var ali = Number(faixas[i].aliquota);
      if (!isFinite(lim) || !isFinite(ali)) continue;

      var trecho = Math.min(base, lim) - prev;
      if (trecho > 0) inss += trecho * ali;

      prev = lim;
      if (base <= lim) break;
    }
    return Math.max(0, inss);
  }

  function calcIRTabela(base, tabela) {
    if (base <= 0) return 0;
    if (!tabela || !tabela.length) return 0;

    for (var i = 0; i < tabela.length; i++) {
      var lim = Number(tabela[i].limite);
      var ali = Number(tabela[i].aliquota);
      var ded = Number(tabela[i].deducao);
      if (!isFinite(lim) || !isFinite(ali) || !isFinite(ded)) continue;
      if (base <= lim) return Math.max(0, (base * ali) - ded);
    }

    var last = tabela[tabela.length - 1] || {};
    return Math.max(0, (base * (Number(last.aliquota) || 0)) - (Number(last.deducao) || 0));
  }

  function aplicarReducao(base, ir, r) {
    if (!r || typeof r !== "object") return { ir: ir, aplicada: 0 };

    var isenta_ate = Number(r.isenta_ate);
    var reduz_ate = Number(r.reduz_ate);
    var a = Number(r.a);
    var b = Number(r.b);

    if (isFinite(isenta_ate) && base <= isenta_ate) {
      return { ir: 0, aplicada: Math.max(0, ir) };
    }

    if (isFinite(isenta_ate) && isFinite(reduz_ate) && base > isenta_ate && base <= reduz_ate && isFinite(a) && isFinite(b)) {
      var red = Math.max(0, (a - (b * base)));
      var aplicada = Math.min(Math.max(0, ir), red);
      return { ir: Math.max(0, ir - aplicada), aplicada: aplicada };
    }

    return { ir: ir, aplicada: 0 };
  }

  function calcIRRF13(total13, inss13, deps, pensao, data) {
    var depVal = Number(data.dep) || 0;
    var simp = Number((data.irrf || {}).simplificado) || 0;
    var tabela = (data.irrf || {}).tabela || [];
    var reducao = (data.irrf || {}).reducao_mensal || null;

    var baseLegal = total13 - inss13 - (deps * depVal) - pensao;
    var baseSimp = total13 - simp;

    var irLegalPre = calcIRTabela(baseLegal, tabela);
    var irSimpPre = calcIRTabela(baseSimp, tabela);

    var legal = aplicarReducao(baseLegal, irLegalPre, reducao);
    var simpR = aplicarReducao(baseSimp, irSimpPre, reducao);

    var useSimp = (simpR.ir < legal.ir);

    return {
      ir: useSimp ? simpR.ir : legal.ir,
      modo: useSimp ? "Simplificado" : "Deduções legais",
      ajuste_aplicado: useSimp ? simpR.aplicada : legal.aplicada
    };
  }

  function run(root) {
    var sal = root.querySelector('[data-d13="sal"]');
    var vari = root.querySelector('[data-d13="var"]');
    var meses = root.querySelector('[data-d13="meses"]');
    var dep = root.querySelector('[data-d13="dep"]');
    var pensao = root.querySelector('[data-d13="pensao"]');
    var chk = root.querySelector('[data-d13="liq"]');

    var out = root.querySelector('[data-d13="out"]');
    var oBase = root.querySelector('[data-d13-out="base"]');
    var oTotal = root.querySelector('[data-d13-out="total"]');
    var oP1 = root.querySelector('[data-d13-out="p1"]');
    var oP2b = root.querySelector('[data-d13-out="p2b"]');
    var oINSS = root.querySelector('[data-d13-out="inss"]');
    var oIRRF = root.querySelector('[data-d13-out="irrf"]');
    var oP2l = root.querySelector('[data-d13-out="p2l"]');
    var oInfo = root.querySelector('[data-d13-out="info"]');

    if (!sal || !meses || !out || !oBase || !oTotal || !oP1 || !oP2b || !oINSS || !oIRRF || !oP2l || !oInfo) {
      alert("Não foi possível calcular. Recarregue a página.");
      return;
    }

    var salario = parseMoney(sal.value);
    var vvar = parseMoney(vari ? vari.value : "");
    var m = Number(meses.value);

    if (!isFinite(salario) || salario <= 0) { alert("Informe um salário bruto válido (ex.: 3000,00)."); sal.focus(); return; }
    if (!isFinite(vvar) || vvar < 0) { alert("A média de variáveis deve ser um número válido (ou deixe em branco)."); if (vari) vari.focus(); return; }
    if (!isFinite(m) || m < 1 || m > 12) { alert("Selecione os meses trabalhados (1 a 12)."); meses.focus(); return; }

    var deps = Math.max(0, Math.floor(Number(dep ? dep.value : 0) || 0));
    var pens = parseMoney(pensao ? pensao.value : "");
    if (!isFinite(pens) || pens < 0) { alert("Pensão alimentícia deve ser um número válido (ou deixe em branco)."); if (pensao) pensao.focus(); return; }

    var base = salario + vvar;
    var total13 = base * (m / 12);
    var p1 = total13 / 2;
    var p2b = total13 - p1;

    oBase.textContent = brl(base);
    oTotal.textContent = brl(total13);
    oP1.textContent = brl(p1);
    oP2b.textContent = brl(p2b);

    var data = getFolhaData(root);
    var liqOn = !!(chk && chk.checked && data && data.inss && data.irrf);

    if (liqOn) {
      var inss13 = calcINSS(total13, data.inss);
      var irObj = calcIRRF13(total13, inss13, deps, pens, data);
      var irrf13 = irObj.ir;
      var p2l = total13 - p1 - inss13 - irrf13;

      oINSS.textContent = brl(inss13);
      oIRRF.textContent = brl(irrf13);
      oP2l.textContent = brl(p2l);

      var info = "IRRF (" + irObj.modo + ").";
      if (irObj.ajuste_aplicado > 0.005) info += " Ajuste aplicado: " + brl(irObj.ajuste_aplicado) + ".";
      info += " Estimativa: pode variar por particularidades de folha.";
      oInfo.textContent = info;
    } else {
      oINSS.textContent = "—";
      oIRRF.textContent = "—";
      oP2l.textContent = "—";
      oInfo.textContent = (chk && chk.checked)
        ? "Para estimar os descontos, recarregue a página. Se preferir, desative o modo líquido."
        : "Modo bruto: sem estimativa de descontos.";
    }

    out.style.display = "block";
    out.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  document.addEventListener("click", function (e) {
    var btn = e.target.closest && e.target.closest('[data-d13-action="calc"]');
    if (!btn) return;

    var root = findRootFrom(btn);
    if (!root) return;

    run(root);
  });

  document.addEventListener("keydown", function (e) {
    if (e.key !== "Enter") return;

    var root = findRootFrom(e.target);
    if (!root) return;

    var inside = e.target && (e.target.matches("input") || e.target.matches("select"));
    if (!inside) return;

    e.preventDefault();
    run(root);
  });
})();
JS;
  }

}
