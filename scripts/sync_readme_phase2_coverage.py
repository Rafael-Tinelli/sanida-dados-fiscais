#!/usr/bin/env python3
from pathlib import Path

README = Path(__file__).resolve().parents[1] / "README.md"


def replace_once(text: str, old: str, new: str) -> str:
    if old not in text:
        raise RuntimeError(f"README block not found: {old[:120]}")
    if text.count(old) != 1:
        raise RuntimeError("README block is not unique")
    return text.replace(old, new, 1)


def main() -> int:
    text = README.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "O primeiro corte admite payloads pequenos e auditáveis (`scalar`, `progressive_table`, `affine_reduction`, `threshold_accrual`, `fraction`, `entitlement_bands`, `eligibility_matrix`, `incidence_profile` e `policy`). A criação de nova família de payload é mudança estrutural do schema.\n",
        "O checkpoint de cobertura integral está documentado em `docs/contract-coverage-v1.json` e `docs/phase2-schema-coverage.md`. As **32 regras** do inventário da Fase 1 estão mapeadas para **18 famílias tipadas de payload**, e o exemplo `CANDIDATE` materializa ao menos uma regra de cada família. O CI exige cobertura exata 32/32 e igualdade entre as famílias usadas pelo mapa e a união admitida pelo schema. Nova família de payload é mudança estrutural do schema.\n",
    )

    text = replace_once(
        text,
        """Estado do CI no início da fase:

- geração/validação determinística do schema: ativa;
- validação cruzada com source registry, rule inventory e reference cases: ativa;
- suíte de contrato: ativa em `Remake CI`.
""",
        """Estado atual do CI:

- geração/validação determinística do schema: ativa;
- cobertura do `rule-inventory-v1.json`: **32/32**;
- famílias tipadas de payload: **18/18 materializadas no CANDIDATE**;
- validação cruzada com source registry, rule inventory, coverage map e reference cases: ativa;
- proveniência, competência e lifecycle de release possuem gates negativos;
- suíte de contrato no checkpoint de cobertura: **43 testes verdes** em `Remake CI`.

Artefatos adicionais do checkpoint:

- `docs/contract-coverage-v1.json`;
- `docs/phase2-schema-coverage.md`.
""",
    )

    text = replace_once(
        text,
        """Após o fechamento da Fase 1, as questões remanescentes são de **design de software e operação**, não lacunas jurídicas P0 do inventário. O primeiro corte da Fase 2 já fechou a estrutura-base de classes, o `$id` inicial do schema, os estados de qualidade/mudança e as invariantes mínimas de `last-good`. Permanecem em aberto:

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
""",
        """Após o fechamento da Fase 1, as questões remanescentes são de **design de software e operação**, não lacunas jurídicas P0 do inventário. A Fase 2 já fechou a expressividade do schema para as 32 regras, com 18 famílias tipadas, e endureceu proveniência, competência e lifecycle. Permanecem em aberto:

- política definitiva de versionamento entre `schema_version`, `contract_api_version`, `rule_version` e `release_id`;
- compatibilidade backward/forward e critérios objetivos para major/minor/patch;
- imutabilidade, supersessão e organização física das releases canônicas;
- revisão final de campos que ainda poderiam exigir inferência do consumidor;
- política exata de retenção de snapshots na futura camada de fontes;
- critérios de confirmação multi-fonte para mudanças paramétricas;
- mecanismo de semantic diff — Fase 5;
- mecanismo de distribuição para WordPress/SFA — Fase 6;
- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência.
""",
    )

    text = replace_once(
        text,
        """Continuar a **Fase 2 — Contrato Fiscal Canônico v1** até que o contrato cubra, de forma tipada, todas as famílias necessárias do inventário fechado da Fase 1.

O primeiro corte já materializa modelos Pydantic, JSON Schema, seleção por vigência/contexto, política global de `last-good`, proveniência, qualidade e gates de regressão. As próximas entregas dentro da própria Fase 2 são:

- completar a cobertura do inventário de regras, sem antecipar o motor de cálculo da Fase 3;
- endurecer invariantes de proveniência, competência e compatibilidade de consumidores;
- ampliar testes negativos de schema e seleção;
- consolidar a política de release/status (`CANDIDATE`, `VALIDATED`, `PUBLISHED`, `SUPERSEDED`, `BLOCKED`);
- fechar a forma canônica de serialização e versionamento;
- preparar o handoff formal para a Fase 3.
""",
        """Continuar a **Fase 2 — Contrato Fiscal Canônico v1** pela rodada final de estabilidade do contrato.

A cobertura de expressividade já está fechada: **32/32 regras**, **18 famílias tipadas**, proveniência/competência/lifecycle endurecidos e testes negativos ativos. As próximas entregas dentro da própria Fase 2 são:

- congelar a política de versionamento (`schema_version`, `contract_api_version`, `rule_version`, `release_id`);
- definir regras de compatibilidade backward/forward;
- fechar imutabilidade e supersessão de releases;
- revisar se resta qualquer campo semântico que force inferência do consumidor;
- documentar o handoff formal para a Fase 3.
""",
    )

    marker = "## 22. Changelog do README\n\n"
    entry = """### 2026-09-13 — checkpoint 32/32 da Fase 2

- percorrido integralmente o `rule-inventory-v1.json`;
- mapeadas **32/32 regras** para famílias expressáveis pelo Contrato v1;
- ampliado o schema para **18 famílias de payload**;
- criado `docs/contract-coverage-v1.json` como gate machine-readable de cobertura;
- criado `docs/phase2-schema-coverage.md` como documentação humana do checkpoint;
- proveniência endurecida com método de obtenção, consistência parser/versão e regras de snapshot;
- competência endurecida com overrides tipados por contexto;
- lifecycle de release endurecido para `DRAFT`, `CANDIDATE`, `VALIDATED`, `PUBLISHED`, `SUPERSEDED` e `BLOCKED`;
- ampliada a suíte negativa; checkpoint validado com **43 testes verdes**;
- próxima etapa reduzida à rodada final de versionamento, compatibilidade, imutabilidade/supersessão e handoff para a Fase 3.

"""
    if entry not in text:
        text = replace_once(text, marker, marker + entry)

    README.write_text(text, encoding="utf-8")
    print("README synchronized for Phase 2 coverage checkpoint")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
