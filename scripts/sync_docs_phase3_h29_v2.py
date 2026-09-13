from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]

readme_path = ROOT / "README.md"
readme = readme_path.read_text(encoding="utf-8")

old_checkpoint = """- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;
- suíte completa: **114 testes verdes** após o checkpoint de férias/A02.
"""
new_checkpoint = """- principal do abono (`IRRF não / CP não`) e terço constitucional sobre o abono (`IRRF sim / CP não`) permanecem componentes separados até a formação das bases tributárias;
- H29 ganhou núcleo limitado próprio, com matriz eSocial `01/02/07/33`, sem expansão para motivos não suportados;
- motivo `01` mantém saldo salarial e bloqueia 13º/férias proporcionais; `02/07/33` habilitam ambos;
- saldo salarial usa `salário-base mensal normalizado × dias considerados / dias civis do mês`, sem divisor 30 universal;
- 13º rescisório continua no calendário anual enquanto férias proporcionais continuam no período aquisitivo — no caso `01/09/2025 → 31/03/2026`, isso produz 3/12 de 13º e 7/12 de férias;
- H29 continua declarando `partial_estimate`, com aviso prévio, FGTS rescisório, seguro-desemprego, estabilidade, prazo determinado e demais verbas não modeladas fora do total;
- suíte completa: **130 testes verdes** após o checkpoint H29 limitado.
"""
if old_checkpoint in readme:
    readme = readme.replace(old_checkpoint, new_checkpoint, 1)
elif new_checkpoint not in readme:
    raise SystemExit("README checkpoint marker not found")

old_next = "Próximos checkpoints: saldo salarial e matriz H29 limitada; memória de cálculo comum; expansão dos property-based tests. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine."
new_next = "Próximos checkpoints: memória de cálculo comum e expansão dos property-based tests nas fronteiras legais e monetárias. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine."
if old_next in readme:
    readme = readme.replace(old_next, new_next, 1)
elif new_next not in readme:
    raise SystemExit("README next checkpoint marker not found")

old_decision = "25. O limiar proporcional de férias não pode existir como constante jurídica escondida no engine: `vacation.acquisition_period` v1.1 declara `proportional_qualifying_days=15` e o método de aquisição proporcional; leitores 1.0.0 não aceitam silenciosamente o contrato 1.1.0.\n"
new_decision = old_decision + "26. H29 deve executar a matriz eSocial `01/02/07/33` como escopo fechado e cruzar a matriz geral com as regras específicas de 13º e férias proporcionais; divergência ou motivo fora do conjunto suportado é erro, não aproximação.\n27. O saldo salarial de H29 v1 não usa divisor 30 universal. O denominador é o número de dias civis do mês de desligamento e o numerador é fornecido explicitamente como dias considerados até o desligamento; regimes fora de mensalista/quinzenalista normalizado ficam fora do escopo.\n"
if old_decision in readme:
    readme = readme.replace(old_decision, new_decision, 1)
elif "26. H29 deve executar a matriz eSocial" not in readme:
    raise SystemExit("README decision marker not found")

changelog_marker = "### 2026-09-13 — checkpoint de férias/A02 na Fase 3"
changelog_entry = """### 2026-09-13 — checkpoint H29 limitado na Fase 3

- criado `sanida_fiscal/termination_v1.py` com escopo fail-closed para mensalista/quinzenalista normalizado, contrato por prazo indeterminado e motivos eSocial `01/02/07/33`;
- materializada no `CANDIDATE` a regra já inventariada `termination.vacation_proportional`, elevando o exemplo para 21 regras sem criar nova família de payload;
- matriz geral e regras específicas de 13º/férias proporcionais passam a ser cruzadas em runtime;
- motivo `01` bloqueia proporcionais; `02/07/33` habilitam 13º e férias proporcionais;
- saldo salarial usa os dias civis reais do mês e reproduz R$ 3.100 / 31 × 10 = R$ 1.000;
- no caso A02, H29 preserva simultaneamente 3/12 de 13º no ano civil e 7/12 de férias no período aquisitivo;
- resultado continua `partial_estimate` e não incorpora aviso, FGTS, seguro-desemprego ou demais verbas excluídas;
- suíte completa chega a **130 testes verdes**.

"""
if changelog_entry not in readme:
    if changelog_marker not in readme:
        raise SystemExit("README changelog marker not found")
    readme = readme.replace(changelog_marker, changelog_entry + changelog_marker, 1)

readme_path.write_text(readme, encoding="utf-8")

phase3_path = ROOT / "docs" / "phase3-library-v1.md"
doc = phase3_path.read_text(encoding="utf-8")

section = """## 9. Quinto checkpoint — saldo salarial e matriz H29 limitada

O núcleo rescisório limitado vive em:

```text
sanida_fiscal/termination_v1.py
tests/test_termination_v1.py
```

O objetivo não é transformar H29 em calculadora universal de rescisão. O engine executa apenas o subconjunto fechado na Fase 1/2 e devolve explicitamente `partial_estimate`.

### 9.1 Matriz de motivos suportados

`select_termination_rule_bundle()` cruza três fontes computacionais do próprio contrato:

```text
termination.reason_scope
termination.thirteenth_proportional
termination.vacation_proportional
```

A regra de férias proporcionais já existia no inventário/coverage, mas ainda não estava materializada no `CANDIDATE`. Ela foi adicionada como 21ª regra representativa, usando a família já existente `code_eligibility`; nenhuma família de schema nova foi criada.

A matriz executada é:

```text
01  justa causa pelo empregador
    saldo salarial             sim
    13º proporcional           não
    férias proporcionais       não

02  sem justa causa
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim

07  pedido de demissão
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim

33  acordo art. 484-A
    saldo salarial             sim
    13º proporcional           sim
    férias proporcionais       sim
```

Motivo fora desse conjunto é `UNSUPPORTED`. O bundle exige ainda que as regras específicas de 13º e férias cubram exatamente os mesmos quatro códigos e reproduzam os mesmos booleans da matriz. Divergência contratual falha antes do cálculo.

### 9.2 Limite de vínculo suportado

A aplicabilidade de `termination.reason_scope` é executada:

```text
employment_regime ∈ {monthly, biweekly}
contract_term      = indefinite
```

No caminho monetário, `monthly_base_salary` é a base mensal normalizada declarada pelo `ProrationPayload`. Horista, diarista, semanalista e contrato a prazo determinado não são convertidos silenciosamente para esse modelo.

### 9.3 Saldo salarial

`calculate_salary_balance()` executa:

```text
monthly_base_salary
× days_counted_through_termination
÷ calendar_days_in_month
```

O denominador vem do calendário do próprio mês de desligamento. Não existe divisor 30 universal. O caso de referência fica executável:

```text
3100.00 / 31 × 10 = 1000.00
```

O mesmo numerador em abril de 2026 produz `1033.33`, demonstrando que a diferença 30/31 não é apagada. O numerador não pode exceder o dia do desligamento e o resultado não pode ultrapassar a base mensal.

### 9.4 13º proporcional e férias proporcionais permanecem calendários distintos

Para motivos `02/07/33`, `calculate_h29_limited_estimate()` reutiliza os núcleos já testados:

- 13º: `calculate_thirteenth_accrual()` + referência remuneratória rescisória + fórmula proporcional do 13º;
- férias: `calculate_proportional_vacation_accrual()` ancorada no período aquisitivo.

No mesmo caso:

```text
admissão       01/09/2025
desligamento   31/03/2026

13º rescisório        3/12  (jan-mar/2026)
férias proporcionais  7/12  (set/2025-mar/2026)
```

O engine não reutiliza a contagem de um direito como se fosse a do outro. No motivo `01`, os cálculos proporcionais nem são executados. A fronteira de 15 dias também foi integrada.

### 9.5 A saída continua parcial

`H29LimitedEstimate` carrega:

```text
result_promise = partial_estimate
user_disclosure_required = true
reason
salary_balance
thirteenth_proportional
vacation_proportional
acquired_vacation_if_due
included_items
excluded_items
```

Neste checkpoint, `acquired_vacation_if_due` é a elegibilidade da matriz; o cálculo monetário de períodos adquiridos/vencidos não foi expandido a partir do antigo checkbox.

Continuam fora do total, conforme `ScopeDeclarationPayload`:

- aviso prévio ou desconto de aviso;
- multa e saque do FGTS;
- seguro-desemprego;
- indenizações de estabilidade;
- verbas específicas de CCT;
- regras de contrato por prazo determinado;
- rescisão indireta sem contexto judicial resolvido;
- itens variáveis rescisórios não explicitamente modelados.

A saída não pode ser apresentada como “total universal da rescisão”.

"""
if section not in doc:
    doc = doc.replace("## 9. Estado do CI", "## 10. Estado do CI", 1)
    doc = doc.replace("## 10. Limites preservados", "## 11. Limites preservados", 1)
    doc = doc.replace("## 11. Próximos checkpoints da Fase 3", "## 12. Próximos checkpoints da Fase 3", 1)
    if "## 10. Estado do CI" not in doc:
        raise SystemExit("phase3 CI marker not found")
    doc = doc.replace("## 10. Estado do CI", section + "## 10. Estado do CI", 1)

ci_re = re.compile(r"## 10\. Estado do CI\n.*?(?=\n## 11\. Limites preservados)", re.S)
new_ci = """## 10. Estado do CI

Após o checkpoint H29 limitado:

```text
Repository baseline       PASS
Fiscal Contract v1.1      PASS
Candidate rules           21
Inventory coverage        32/32
Payload families          18/18
Fiscal engine             PASS
Property-based tests      PASS

130 passed
```

Run de validação do checkpoint: `34770991820`.
"""
if not ci_re.search(doc):
    raise SystemExit("phase3 CI section not found")
doc = ci_re.sub(new_ci.rstrip(), doc, count=1)

old_limit = "- saldo salarial e matriz de elegibilidade completa do H29;\n"
new_limit = "- cálculo monetário de períodos integrais adquiridos/vencidos na rescisão além da elegibilidade já exposta;\n- incidências e memória fiscal final do saldo salarial no orquestrador H29, que serão compostas a partir dos primitives já existentes sem ampliar o escopo de verbas;\n"
if old_limit in doc:
    doc = doc.replace(old_limit, new_limit, 1)
elif new_limit not in doc:
    raise SystemExit("phase3 H29 limit marker not found")

old_next = """1. implementar saldo salarial e matriz H29 limitada;
2. consolidar memória de cálculo comum;
3. ampliar invariantes/property-based tests nas fronteiras legais e monetárias.
"""
new_next = """1. consolidar memória de cálculo comum entre os núcleos já implementados;
2. ampliar invariantes/property-based tests nas fronteiras legais e monetárias;
3. preparar o gate de fechamento da Fase 3 sem antecipar collectors, publicação ou migração de consumidores.
"""
if old_next in doc:
    doc = doc.replace(old_next, new_next, 1)
elif new_next not in doc:
    raise SystemExit("phase3 next checkpoint marker not found")

phase3_path.write_text(doc, encoding="utf-8")
print("README and Phase 3 H29 checkpoint synchronized")
