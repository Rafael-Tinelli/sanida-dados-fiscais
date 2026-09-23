(function () {
  'use strict';

  function ready(fn) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', fn, false);
    } else {
      fn();
    }
  }

  function init() {
    var root = document.getElementById('sd-guia');
    var statusEl, averageEl, installmentEl, countEl, noteEl;
    var requestEl, monthsEl, calcBtn, exampleBtn, resetBtn, salaryEls;
    var money, percent, params, rules;

    if (!root || root.getAttribute('data-sd-ready') === '1') return;

    statusEl = root.querySelector('[data-sd-status]');
    averageEl = root.querySelector('[data-sd-average]');
    installmentEl = root.querySelector('[data-sd-installment]');
    countEl = root.querySelector('[data-sd-count]');
    noteEl = root.querySelector('[data-sd-result-note]');
    requestEl = root.querySelector('[data-sd-request]');
    monthsEl = root.querySelector('[data-sd-months]');
    calcBtn = root.querySelector('[data-sd-calc]');
    exampleBtn = root.querySelector('[data-sd-example]');
    resetBtn = root.querySelector('[data-sd-reset]');
    salaryEls = root.querySelectorAll('[data-sd-salary]');

    if (!statusEl || !averageEl || !installmentEl || !countEl || !noteEl ||
        !requestEl || !monthsEl || !calcBtn || !exampleBtn || !resetBtn ||
        !salaryEls || salaryEls.length !== 3) {
      if (window.console && console.error) {
        console.error('[Sanida seguro-desemprego] Estrutura do estimador incompleta.');
      }
      return;
    }

    root.setAttribute('data-sd-ready', '1');
    window.__SANIDA_SD_B14_LOADED__ = true;

    money = new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });

    percent = new Intl.NumberFormat('pt-BR', {
      style: 'percent',
      minimumFractionDigits: 0,
      maximumFractionDigits: 2
    });

    function q(selector) {
      return root.querySelector(selector);
    }

    function qa(selector) {
      return root.querySelectorAll(selector);
    }

    function setText(selector, value) {
      var nodes = qa(selector);
      var i;
      for (i = 0; i < nodes.length; i += 1) nodes[i].textContent = value;
    }

    function parseMoney(value) {
      var text = String(value || '').replace(/\s+/g, '').replace(/R\$/gi, '').replace(/[^0-9,.\-]/g, '');
      var dots, n;

      if (!text) return NaN;

      if (text.indexOf(',') !== -1) {
        text = text.replace(/\./g, '').replace(',', '.');
      } else {
        dots = (text.match(/\./g) || []).length;
        if (/^-?\d{1,3}(\.\d{3})+$/.test(text)) {
          text = text.replace(/\./g, '');
        } else if (dots > 1) {
          return NaN;
        }
      }

      n = Number(text);
      return isFinite(n) ? n : NaN;
    }

    function positive(value) {
      var n = Number(value);
      return isFinite(n) && n > 0 ? n : NaN;
    }

    function parseIso(value) {
      var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || ''));
      var y, mo, d, stamp, date;
      if (!m) return null;

      y = Number(m[1]);
      mo = Number(m[2]);
      d = Number(m[3]);
      stamp = Date.UTC(y, mo - 1, d);
      date = new Date(stamp);

      if (date.getUTCFullYear() !== y || date.getUTCMonth() !== mo - 1 || date.getUTCDate() !== d) return null;
      return { year: y, month: mo, day: d, stamp: stamp };
    }

    function formatDate(value) {
      var p = parseIso(value);
      if (!p) return '';
      return String(p.day).padStart(2, '0') + '/' + String(p.month).padStart(2, '0') + '/' + String(p.year);
    }

    function officialUrl(value) {
      var a = document.createElement('a');
      var host;
      a.href = String(value || '');
      host = String(a.hostname || '').toLowerCase();

      if (a.protocol !== 'https:') return '';
      if (host !== 'gov.br' && host !== 'www.gov.br' &&
          host !== 'portalfat.mte.gov.br' && host !== 'portalfat.trabalho.gov.br') return '';

      return a.href;
    }

    function loadParams() {
      var holder = q('[data-sd-params]');
      var raw, payload, effective, now, todayUtc, p, expectedBase;

      if (!holder) throw new Error('Contrato de par\u00e2metros ausente.');

      raw = String(holder.textContent || '').replace(/^\s+|\s+$/g, '');
      if (!raw || raw.indexOf('[sd_parametros_json]') !== -1) {
        throw new Error('Par\u00e2metros oficiais n\u00e3o resolvidos.');
      }

      try {
        payload = JSON.parse(raw);
      } catch (e) {
        throw new Error('Contrato de par\u00e2metros inv\u00e1lido.');
      }

      if (!payload || typeof payload !== 'object' || payload.status !== 'current') {
        throw new Error('Par\u00e2metros oficiais indispon\u00edveis.');
      }

      effective = parseIso(payload.effective_from);
      now = new Date();
      todayUtc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());

      p = {
        floor: positive(payload.floor),
        firstLimit: positive(payload.first_band_limit),
        firstRate: positive(payload.first_band_rate),
        secondLimit: positive(payload.second_band_limit),
        secondRate: positive(payload.second_band_excess_rate),
        secondBase: positive(payload.second_band_base),
        cap: positive(payload.cap),
        effectiveFrom: String(payload.effective_from || ''),
        sourceUrl: officialUrl(payload.source_url)
      };

      if (!isFinite(p.floor) || !isFinite(p.firstLimit) || !isFinite(p.firstRate) ||
          !isFinite(p.secondLimit) || !isFinite(p.secondRate) || !isFinite(p.secondBase) ||
          !isFinite(p.cap) || !effective || effective.stamp > todayUtc || !p.sourceUrl ||
          p.firstLimit >= p.secondLimit || p.floor > p.cap ||
          p.firstRate >= 1 || p.secondRate >= 1) {
        throw new Error('Invariantes do contrato n\u00e3o atendidos.');
      }

      expectedBase = Math.round((p.firstLimit * p.firstRate + Number.EPSILON) * 100) / 100;
      if (Math.abs(expectedBase - p.secondBase) > 0.011) {
        throw new Error('Base intermedi\u00e1ria inconsistente.');
      }

      return p;
    }

    function readRules() {
      var rows = qa('[data-sd-parcel-rule]');
      var maxWindow = Number(monthsEl.getAttribute('max'));
      var out = [];
      var i, r;

      if (!rows.length || !isFinite(maxWindow) || maxWindow <= 0) return null;

      for (i = 0; i < rows.length; i += 1) {
        r = {
          request: String(rows[i].getAttribute('data-request') || ''),
          min: Number(rows[i].getAttribute('data-min')),
          max: Number(rows[i].getAttribute('data-max')),
          count: Number(rows[i].getAttribute('data-count'))
        };

        if ((r.request !== '1' && r.request !== '2' && r.request !== '3') ||
            !isFinite(r.min) || !isFinite(r.max) || !isFinite(r.count) ||
            Math.floor(r.min) !== r.min || Math.floor(r.max) !== r.max ||
            Math.floor(r.count) !== r.count || r.min < 0 || r.max < r.min ||
            r.max > maxWindow || r.count < 1) {
          return null;
        }

        out.push(r);
      }

      return out;
    }

    function installment(average, p) {
      var raw;

      if (average <= p.firstLimit) {
        raw = average * p.firstRate;
      } else if (average <= p.secondLimit) {
        raw = p.secondBase + ((average - p.firstLimit) * p.secondRate);
      } else {
        return p.cap;
      }

      if (raw <= p.floor) return p.floor;
      if (raw >= p.cap) return p.cap;

      return Math.min(p.cap, Math.ceil(raw - 0.000000001));
    }

    function parcelCount(request, months) {
      var i;
      if (!rules) return null;

      for (i = 0; i < rules.length; i += 1) {
        if (rules[i].request === request && months >= rules[i].min && months <= rules[i].max) {
          return rules[i].count;
        }
      }

      return 0;
    }

    function readSalaries() {
      var raw = [], values = [], i, value;

      for (i = 0; i < salaryEls.length; i += 1) {
        raw.push(String(salaryEls[i].value || '').replace(/^\s+|\s+$/g, ''));
      }

      if (!raw[0]) throw new Error('Informe o sal\u00e1rio mais recente.');
      if (!raw[1] && raw[2]) throw new Error('Preencha o segundo sal\u00e1rio antes do terceiro.');

      for (i = 0; i < raw.length; i += 1) {
        if (!raw[i]) continue;
        value = parseMoney(raw[i]);
        if (!isFinite(value) || value <= 0) throw new Error('Confira os sal\u00e1rios informados.');
        values.push(value);
      }

      return values;
    }

    function readMonths() {
      var min = Number(monthsEl.getAttribute('min') || 0);
      var max = Number(monthsEl.getAttribute('max'));
      var value;

      if (monthsEl.value === '') {
        monthsEl.removeAttribute('aria-invalid');
        return { state: 'blank', value: NaN, min: min, max: max };
      }

      value = Number(monthsEl.value);

      if (!isFinite(value) || Math.floor(value) !== value || value < min || value > max) {
        monthsEl.setAttribute('aria-invalid', 'true');
        return { state: 'invalid', value: value, min: min, max: max };
      }

      monthsEl.removeAttribute('aria-invalid');
      return { state: 'valid', value: value, min: min, max: max };
    }

    function renderParams(p) {
      var source = q('[data-sd-source]');

      setText('[data-sd-floor]', money.format(p.floor));
      setText('[data-sd-first-limit]', money.format(p.firstLimit));
      setText('[data-sd-first-rate]', percent.format(p.firstRate));
      setText('[data-sd-second-limit]', money.format(p.secondLimit));
      setText('[data-sd-second-rate]', percent.format(p.secondRate));
      setText('[data-sd-second-base]', money.format(p.secondBase));
      setText('[data-sd-cap]', money.format(p.cap));
      setText('[data-sd-effective]', formatDate(p.effectiveFrom));

      if (source) {
        source.href = p.sourceUrl;
        source.removeAttribute('hidden');
      }

      statusEl.textContent = 'Tabela oficial vigente desde ' + formatDate(p.effectiveFrom) + '.';
      statusEl.classList.remove('is-error');
    }

    function renderUnavailable(message) {
      var i;
      var selectors = [
        '[data-sd-floor]', '[data-sd-first-limit]', '[data-sd-first-rate]',
        '[data-sd-second-limit]', '[data-sd-second-rate]', '[data-sd-second-base]',
        '[data-sd-cap]', '[data-sd-effective]', '[data-sd-example-average]',
        '[data-sd-example-installment]'
      ];

      for (i = 0; i < salaryEls.length; i += 1) salaryEls[i].disabled = true;
      requestEl.disabled = true;
      monthsEl.disabled = true;
      calcBtn.disabled = true;
      exampleBtn.disabled = true;

      statusEl.textContent = message || 'Estimador temporariamente indispon\u00edvel.';
      statusEl.classList.add('is-error');
      noteEl.textContent = 'Nenhum c\u00e1lculo \u00e9 feito sem par\u00e2metros oficiais validados.';

      for (i = 0; i < selectors.length; i += 1) setText(selectors[i], 'Indispon\u00edvel');
    }

    function minimumWage() {
      var holder = q('[data-sd-sm-raw]');
      return holder ? parseMoney(holder.textContent) : NaN;
    }

    function renderExample() {
      var sm = minimumWage();
      var avg;

      if (!isFinite(sm) || sm <= 0) {
        setText('[data-sd-example-average]', 'Indispon\u00edvel');
        setText('[data-sd-example-installment]', 'Indispon\u00edvel');
        return;
      }

      avg = sm * 1.5;
      setText('[data-sd-example-average]', money.format(avg));
      setText('[data-sd-example-installment]', money.format(installment(avg, params)));
    }

    try {
      params = loadParams();
      rules = readRules();
      renderParams(params);
      renderExample();
    } catch (e) {
      if (window.console && console.error) console.error('[Sanida seguro-desemprego]', e);
      renderUnavailable('Estimador temporariamente indispon\u00edvel: os par\u00e2metros oficiais n\u00e3o puderam ser validados.');
      return;
    }

    function calculate() {
      var values, total = 0, avg, value, request, m, count, countText = '\u2014', extra = '', i;

      try {
        values = readSalaries();
        for (i = 0; i < values.length; i += 1) total += values[i];

        avg = total / values.length;
        value = installment(avg, params);
        request = requestEl.value;
        m = readMonths();

        averageEl.textContent = money.format(avg);
        installmentEl.textContent = money.format(value);

        if (m.state === 'invalid') {
          countText = 'Confira os meses';
          extra = ' Informe um n\u00famero inteiro entre ' + m.min + ' e ' + m.max + '.';
        } else if (request && m.state === 'valid') {
          if (!rules) {
            countText = 'Indispon\u00edvel';
            extra = ' A regra de parcelas n\u00e3o p\u00f4de ser validada.';
          } else {
            count = parcelCount(request, m.value);
            if (count === 0) {
              countText = 'Abaixo da faixa m\u00ednima';
              extra = ' A quantidade informada n\u00e3o alcan\u00e7a a faixa m\u00ednima desta solicita\u00e7\u00e3o.';
            } else {
              countText = String(count) + (count === 1 ? ' parcela' : ' parcelas');
              extra = ' A quantidade ainda depende da valida\u00e7\u00e3o oficial dos demais requisitos.';
            }
          }
        } else if (request || m.state === 'valid') {
          countText = 'Complete os dois campos';
          extra = ' Para estimar as parcelas, informe a solicita\u00e7\u00e3o e os meses comput\u00e1veis.';
        }

        countEl.textContent = countText;
        noteEl.textContent = 'Estimativa calculada com a tabela oficial vigente.' + extra + ' O resultado n\u00e3o confirma a concess\u00e3o do benef\u00edcio.';
      } catch (e) {
        averageEl.textContent = '\u2014';
        installmentEl.textContent = '\u2014';
        countEl.textContent = '\u2014';
        noteEl.textContent = e && e.message ? e.message : 'Confira os dados informados.';
      }
    }

    function fillExample() {
      var sm = minimumWage();
      var formatted;
      var i;

      if (!isFinite(sm) || sm <= 0) {
        noteEl.textContent = 'O sal\u00e1rio m\u00ednimo vigente n\u00e3o est\u00e1 dispon\u00edvel para o exemplo.';
        return;
      }

      formatted = new Intl.NumberFormat('pt-BR', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(sm * 1.5);

      for (i = 0; i < salaryEls.length; i += 1) salaryEls[i].value = formatted;
      requestEl.value = '';
      monthsEl.value = '';
      monthsEl.removeAttribute('aria-invalid');
      calculate();
    }

    function reset() {
      var i;
      for (i = 0; i < salaryEls.length; i += 1) salaryEls[i].value = '';
      requestEl.value = '';
      monthsEl.value = '';
      monthsEl.removeAttribute('aria-invalid');
      averageEl.textContent = '\u2014';
      installmentEl.textContent = '\u2014';
      countEl.textContent = '\u2014';
      noteEl.textContent = 'Informe ao menos um sal\u00e1rio para come\u00e7ar.';
    }

    monthsEl.addEventListener('input', function () {
      readMonths();
    }, false);

    calcBtn.addEventListener('click', calculate, false);
    exampleBtn.addEventListener('click', fillExample, false);
    resetBtn.addEventListener('click', reset, false);

    for (var i = 0; i < salaryEls.length; i += 1) {
      salaryEls[i].addEventListener('keydown', function (event) {
        if (event.key === 'Enter') {
          event.preventDefault();
          calculate();
        }
      }, false);
    }
  }

  ready(init);
  window.addEventListener('load', init, false);
}());
