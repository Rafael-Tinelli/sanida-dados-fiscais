#!/usr/bin/env python3
from pathlib import Path

README = Path(__file__).resolve().parents[1] / "README.md"


def replace_once(text: str, old: str, new: str) -> str:
    if old not in text:
        raise RuntimeError(f"README block not found:\n{old[:160]}")
    if text.count(old) != 1:
        raise RuntimeError("README block is not unique")
    return text.replace(old, new, 1)


def main() -> int:
    text = README.read_text(encoding="utf-8")

    text = replace_once(text,
'''## 9. Contrato Fiscal Canônico v1

A primeira grande entrega deste remake será a especificação e implementação do **Contrato Fiscal Canônico v1**.

Cada regra deverá ser capaz de expressar, conforme aplicável:

- `rule_id` estável;
- versão do schema;
- jurisdição;
- domínio;
- descrição técnica;
- variável/objeto jurídico ao qual se aplica;
- dependências;
- ordem de cálculo;
- parâmetros;
- unidade;
- regras de arredondamento;
- vigência inicial;
- vigência final;
- competência;
- fonte normativa;
- fonte operacional;
- exemplos oficiais associados;
- método de extração;
- hash/snapshot da fonte;
- data da última verificação;
- estado de qualidade;
- política de atualização;
- classificação da última mudança.

O schema definitivo ainda será desenhado. Esta seção descreve requisitos, não uma estrutura já congelada.
''',
'''## 9. Contrato Fiscal Canônico v1

A **Fase 2 está em andamento** e o primeiro corte executável do Contrato Fiscal Canônico v1 já foi materializado. A especificação detalhada da fase está em `docs/phase2-contract-v1.md`.

O contrato possui duas representações sincronizadas:

- modelos Pydantic v2 em `sanida_fiscal/types_v1.py` e `sanida_fiscal/contract_v1.py`;
- JSON Schema público gerado deterministicamente em `contracts/fiscal-contract-v1.schema.json`.

Cada regra é representada por `FiscalRuleV1` e expressa, conforme aplicável:

- `rule_id` e `rule_version`;
- domínio;
- consumidores e contextos explícitos de apuração;
- target semântico (`applies_to`);
- predicados de aplicabilidade;
- dependências e ordem de cálculo;
- política de competência e janela de vigência;
- política de arredondamento;
- payload tipado;
- proveniência;
- estado de qualidade;
- classe da mudança;
- política de atualização.

O primeiro corte admite payloads pequenos e auditáveis (`scalar`, `progressive_table`, `affine_reduction`, `threshold_accrual`, `fraction`, `entitlement_bands`, `eligibility_matrix`, `incidence_profile` e `policy`). A criação de nova família de payload é mudança estrutural do schema.

Um exemplo `CANDIDATE` vive em `contracts/examples/fiscal-contract-v1.example.json`. Ele **não é release de produção**: releases `VALIDATED`/`PUBLISHED` exigem evidência oficial disponível com hash de snapshot por regra. Snapshots reais pertencem à camada de fontes da Fase 4.

Os gates atuais já rejeitam, entre outras situações:

- regra fora da vigência ou seleção ambígua;
- sobreposição do mesmo `rule_id` no mesmo contexto;
- dependência inexistente;
- regra estrutural com autopublicação;
- regra estrutural validada sem revisão humana;
- `last-good` expirado, não validado ou com sucessora conhecida;
- tentativa de relabelar dado histórico como corrente;
- regressão do target semântico de A01;
- fusão indevida entre principal e terço do abono;
- expansão silenciosa do escopo de H29.

O schema v1 ainda está sob validação da Fase 2; alterações estruturais continuam permitidas nesta branch até o fechamento formal da fase, sempre acompanhadas de testes e atualização deste README.
''')

    text = replace_once(text,
'''### Fase 2 — Contrato Fiscal Canônico v1

**Status: PENDENTE**

Objetivos:

- definir modelos Pydantic;
- definir JSON Schema;
- definir estados de qualidade e mudança;
- definir política de `last-good`;
- definir proveniência.
''',
'''### Fase 2 — Contrato Fiscal Canônico v1

**Status: EM ANDAMENTO**

Objetivos:

- definir modelos Pydantic;
- definir JSON Schema;
- definir estados de qualidade e mudança;
- definir política de `last-good`;
- definir proveniência.

Primeiro corte executável:

- `sanida_fiscal/types_v1.py`;
- `sanida_fiscal/contract_v1.py`;
- `contracts/fiscal-contract-v1.schema.json`;
- `contracts/examples/fiscal-contract-v1.example.json`;
- `scripts/generate_contract_schema.py`;
- `scripts/validate_contract_v1.py`;
- `tests/test_contract_v1.py`;
- `docs/phase2-contract-v1.md`;
- `requirements-contract.txt`;
- `requirements-dev.txt`.

Estado do CI no início da fase:

- geração/validação determinística do schema: ativa;
- validação cruzada com source registry, rule inventory e reference cases: ativa;
- suíte de contrato: ativa em `Remake CI`.
''')

    text = replace_once(text,
'''Após o fechamento da Fase 1, as questões remanescentes são de **design de software e operação**, não lacunas jurídicas P0 do inventário:

- composição exata das classes Pydantic do Contrato Fiscal Canônico v1;
- `$id`, modularização e compatibilidade dos JSON Schemas;
- organização física dos contratos por regra, domínio e competência;
- política exata de retenção e hash de snapshots;
- critérios de confirmação multi-fonte para mudanças paramétricas;
- política de versionamento de schema e release;
- mecanismo de semantic diff;
- mecanismo de distribuição para WordPress/SFA;
- política temporal específica de `last-good` por domínio/regra;
- estratégia de compatibilidade durante a migração dos consumidores;
- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência.
''',
'''Após o fechamento da Fase 1, as questões remanescentes são de **design de software e operação**, não lacunas jurídicas P0 do inventário. O primeiro corte da Fase 2 já fechou a estrutura-base de classes, o `$id` inicial do schema, os estados de qualidade/mudança e as invariantes mínimas de `last-good`. Permanecem em aberto:

- cobertura de todas as famílias do inventário pelo contrato v1 antes do fechamento da Fase 2;
- eventual modularização futura do JSON Schema sem quebrar `contract_api_version=1.0.0`;
- organização física das releases canônicas por competência e domínio;
- política exata de retenção e hash de snapshots;
- critérios de confirmação multi-fonte para mudanças paramétricas;
- política de versionamento de schema e release após o v1 inicial;
- mecanismo de semantic diff;
- mecanismo de distribuição para WordPress/SFA;
- política temporal específica de `last-good` por domínio/regra além das invariantes globais já implementadas;
- estratégia de compatibilidade durante a migração dos consumidores;
- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência.
''')

    text = replace_once(text,
'''## 20. Próxima etapa

Iniciar a **Fase 2 — Contrato Fiscal Canônico v1**.

A especificação jurídica de entrada está congelada em `docs/phase1-closure.md`, `docs/rule-inventory-v1.json` e `docs/source-registry-v1.json`. A Fase 2 deverá transformar essa base em:

- modelos Pydantic tipados;
- JSON Schema público e versionado;
- invariantes de vigência, qualidade e proveniência;
- seleção de regra por competência/contexto;
- política de `last-good`;
- classificação de mudanças;
- vínculos entre `rule_id`, fonte oficial e casos de referência.

O pipeline de produção ainda não deve ser migrado antes de o Contrato v1 e seus gates estarem executáveis.
''',
'''## 20. Próxima etapa

Continuar a **Fase 2 — Contrato Fiscal Canônico v1** até que o contrato cubra, de forma tipada, todas as famílias necessárias do inventário fechado da Fase 1.

O primeiro corte já materializa modelos Pydantic, JSON Schema, seleção por vigência/contexto, política global de `last-good`, proveniência, qualidade e gates de regressão. As próximas entregas dentro da própria Fase 2 são:

- completar a cobertura do inventário de regras, sem antecipar o motor de cálculo da Fase 3;
- endurecer invariantes de proveniência, competência e compatibilidade de consumidores;
- ampliar testes negativos de schema e seleção;
- consolidar a política de release/status (`CANDIDATE`, `VALIDATED`, `PUBLISHED`, `SUPERSEDED`, `BLOCKED`);
- fechar a forma canônica de serialização e versionamento;
- preparar o handoff formal para a Fase 3.

O pipeline de produção continua congelado: `scraper.py`, `update_taxas.py`, `dados_fiscais.json`, `taxas_bacen.json` e os consumidores não serão migrados antes do fechamento do Contrato v1.
''')

    marker = "## 22. Changelog do README\n\n"
    entry = '''### 2026-09-13 — início da Fase 2

- Fase 2 marcada como `EM ANDAMENTO`;
- criada branch `refactor/fiscal-contract-v1` a partir do merge da Fase 1;
- materializados modelos Pydantic v2 e JSON Schema determinístico do Contrato Fiscal Canônico v1;
- criado exemplo `CANDIDATE` sem fingir release de produção;
- implementadas invariantes de vigência, qualidade, proveniência, mudança estrutural e `last-good`;
- preservadas como gates executáveis A01, separação do abono e escopo H29;
- CI ampliado para validar schema, contrato e suíte de testes;
- documentação detalhada da fase criada em `docs/phase2-contract-v1.md`.

'''
    if entry not in text:
        text = replace_once(text, marker, marker + entry)

    README.write_text(text, encoding="utf-8")
    print("README synchronized for Phase 2")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
