from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

old = '''Estado do primeiro checkpoint:

- cinco casos oficiais RFB de 2026 executados contra o engine;
- A01 reproduz `382.88` para renda tributável de `6000.00`;
- executor marginal com teto implementado para a família usada pelo INSS;
- `float`/`bool` rejeitados no caminho fiscal;
- Hypothesis integrado ao CI;
- suíte completa: **63 testes verdes** no primeiro run da Fase 3.
'''

new = '''Estado dos checkpoints executados:

- cinco casos oficiais RFB de 2026 executados contra o engine;
- A01 reproduz `382.88` para renda tributável de `6000.00`;
- executor marginal com teto implementado para a família usada pelo INSS;
- `float`/`bool` rejeitados no caminho fiscal;
- Hypothesis integrado ao CI;
- identidade explícita de apuração (`monthly`, `thirteenth`, `vacation`) separada do contexto de origem;
- `termination` tratado como origem que pode conter apurações mensal e de 13º distintas, nunca como quarto tipo de IRRF;
- previdência, dependentes e pensão vinculados a uma única apuração e impedidos de vazar entre contextos;
- bundles de regras selecionados pelo tipo de rendimento, inclusive dentro de H29;
- suíte completa: **73 testes verdes** após o checkpoint de deduções por contexto.
'''

old_next = '''Próximos checkpoints: deduções por contexto e apurações separadas; 13º/avos; período aquisitivo e férias; saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests nas fronteiras legais e monetárias.'''
new_next = '''Próximos checkpoints: 13º/avos; período aquisitivo e férias; saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests nas fronteiras legais e monetárias.'''

if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("Phase 3 checkpoint block not found")

if old_next in text:
    text = text.replace(old_next, new_next, 1)
elif new_next not in text:
    raise SystemExit("Phase 3 next-checkpoints block not found")

marker = "### 2026-09-13 — início da Fase 3"
entry = '''### 2026-09-13 — checkpoint de apurações separadas na Fase 3

- `monthly`, `thirteenth` e `vacation` passam a ter identidade explícita de apuração;
- `termination` permanece contexto de origem, com mensal e 13º isolados entre si;
- deduções legais são vinculadas à apuração e não podem vazar entre contextos;
- seleção de regras e redutor é feita pelo tipo de rendimento;
- suíte completa chega a 73 testes verdes.

'''
if entry not in text:
    if marker not in text:
        raise SystemExit("changelog marker not found")
    text = text.replace(marker, entry + marker, 1)

path.write_text(text, encoding="utf-8")
print("README Phase 3 context checkpoint synchronized")
