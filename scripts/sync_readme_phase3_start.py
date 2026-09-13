from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

old_phase3 = '''### Fase 3 — Biblioteca fiscal e testes

**Status: PENDENTE**

Objetivos:

- implementar funções fiscais puras;
- usar `Decimal`;
- converter exemplos oficiais em testes;
- criar property-based tests e invariantes.
'''

new_phase3 = '''### Fase 3 — Biblioteca fiscal e testes

**Status: EM ANDAMENTO**

Objetivos:

- implementar funções fiscais puras;
- usar `Decimal`;
- converter exemplos oficiais em testes;
- criar property-based tests e invariantes.

Primeira rodada executável:

- `sanida_fiscal/money.py` — entrada decimal estrita e quantização por `RoundingPolicy`;
- `sanida_fiscal/engine_v1.py` — tabelas progressivas, redutor afim e memória de IRRF;
- `tests/test_engine_v1.py` — casos RFB, regressão A01 e property-based tests;
- `docs/phase3-library-v1.md` — escopo e checkpoints da biblioteca fiscal.

Estado do primeiro checkpoint:

- cinco casos oficiais RFB de 2026 executados contra o engine;
- A01 reproduz `382.88` para renda tributável de `6000.00`;
- executor marginal com teto implementado para a família usada pelo INSS;
- `float`/`bool` rejeitados no caminho fiscal;
- Hypothesis integrado ao CI;
- suíte completa: **63 testes verdes** no primeiro run da Fase 3.
'''

old_next = '''## 20. Próxima etapa

Iniciar a **Fase 3 — Biblioteca fiscal e testes** a partir de `docs/phase2-to-phase3-handoff.md`.

A Fase 3 deverá implementar funções fiscais puras e determinísticas sobre o contrato congelado, com `Decimal`, casos oficiais executáveis e property-based testing. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.

O pipeline de produção continua congelado: `scraper.py`, `update_taxas.py`, `dados_fiscais.json`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram migrados.
'''

new_next = '''## 20. Próxima etapa

Continuar a **Fase 3 — Biblioteca fiscal e testes** a partir do primeiro núcleo já verde.

Próximos checkpoints: deduções por contexto e apurações separadas; 13º/avos; período aquisitivo e férias; saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.

O pipeline de produção continua congelado: `scraper.py`, `update_taxas.py`, `dados_fiscais.json`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram migrados.
'''

if old_phase3 in text:
    text = text.replace(old_phase3, new_phase3, 1)
elif new_phase3 not in text:
    raise SystemExit("phase3 block not found")

if old_next in text:
    text = text.replace(old_next, new_next, 1)
elif new_next not in text:
    raise SystemExit("next-stage block not found")

marker = "### 2026-09-13 — fechamento da Fase 2"
entry = '''### 2026-09-13 — início da Fase 3

- aberta a biblioteca fiscal determinística sobre o Contrato v1;
- primitives de `Decimal`, tabela progressiva e redutor afim implementados;
- cinco casos oficiais RFB e regressão A01 tornados executáveis;
- Hypothesis integrado; primeiro checkpoint com 63 testes verdes.

'''
if entry not in text:
    if marker not in text:
        raise SystemExit("changelog marker not found")
    text = text.replace(marker, entry + marker, 1)

path.write_text(text, encoding="utf-8")
print("README Phase 3 start synchronized")
