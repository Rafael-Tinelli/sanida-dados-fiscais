#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PHASE2 = ROOT / "docs" / "phase2-contract-v1.md"
README = ROOT / "README.md"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise RuntimeError(f"anchor not found: {label}")
    if text.count(old) != 1:
        raise RuntimeError(f"anchor is not unique: {label}")
    return text.replace(old, new, 1)


def update_phase2() -> None:
    text = PHASE2.read_text(encoding="utf-8")
    text = replace_once(
        text,
        "**Status:** EM ANDAMENTO  \n**Início:** 13/09/2026  \n**Base:** fechamento formal da Fase 1 (`docs/phase1-closure.md`)",
        "**Status:** CONCLUÍDA  \n**Início:** 13/09/2026  \n**Fechamento:** 13/09/2026  \n**Base:** fechamento formal da Fase 1 (`docs/phase1-closure.md`)",
        "phase2 status",
    )
    text = replace_once(
        text,
        "Além da base principal, `context_overrides` permite declarar uma base distinta para um contexto específico sem recorrer a texto livre. Overrides para contextos não declarados são rejeitados. `rule_specific` exige descrição explícita.",
        "Além da base principal, `context_overrides` permite declarar uma base distinta para um contexto específico. Overrides para contextos não declarados são rejeitados. `rule_specific` exige uma `rule_specific_key` tipada; descrição narrativa pode existir para auditoria humana, mas não decide o cálculo.",
        "typed competence",
    )

    start = text.index("## 9. Lifecycle da release\n")
    end = text.index("## 10. Invariantes executáveis\n")
    lifecycle = """## 9. Lifecycle, identidade e imutabilidade da release

Estados suportados:

```text
DRAFT
CANDIDATE
VALIDATED
PUBLISHED
BLOCKED
```

`SUPERSEDED` não é um estado mutável do artefato antigo. Uma release `PUBLISHED` é imutável; sua substituição é declarada pela sucessora em `supersedes_release_id`.

O modelo `ReleaseLifecycle` registra, conforme o estado:

- data de validação;
- data de publicação;
- modo e referência de aprovação;
- motivos de bloqueio.

Gates principais:

- `DRAFT` não pode carregar estado de validação/publicação;
- `CANDIDATE` não pode alegar lifecycle final;
- `VALIDATED` exige `validated_at_utc`, todas as regras validadas e snapshot hashado por regra;
- `PUBLISHED` exige validação, publicação, aprovação e evidência hashada por regra;
- release com mudança estrutural só pode ser `PUBLISHED` com `HUMAN_REVIEWED`;
- `BLOCKED` exige `block_reasons` e não pode estar publicada;
- `VALIDATED`/`PUBLISHED` exigem `release_id` content-addressed do payload fiscal imutável;
- uma sucessora só pode apontar por `supersedes_release_id` para uma predecessora `PUBLISHED` e deve possuir identidade distinta;
- após `PUBLISHED`, qualquer alteração do artefato exige nova release; reescrita do mesmo `release_id` é rejeitada.

A identidade canônica de release é:

```text
fiscal-v1-sha256-<sha256 do immutable payload>
```

O hash de identidade não depende de timestamps de workflow/lifecycle. Ele cobre a substância fiscal e contratual imutável da release.

"""
    text = text[:start] + lifecycle + text[end:]

    start = text.index("## 12. Testes negativos\n")
    end = text.index("## 13. Casos de referência\n")
    tests = """## 12. Testes negativos

A suíte não testa apenas casos felizes. Ela rejeita, entre outros:

- release publicada sem snapshot hashado;
- `release_id` de release validada/publicada que não corresponda ao hash do payload imutável;
- mudança estrutural publicada como `AUTO_VALIDATED`;
- `CANDIDATE` com timestamp de publicação;
- `BLOCKED` sem motivo;
- uso de `SUPERSEDED` como estado mutável da predecessora;
- tentativa de mutar artefato já `PUBLISHED`;
- sucessão sem `supersedes_release_id` coerente;
- versão SemVer regressiva;
- consumidor com `schema_version` ou `contract_api_version` incompatível;
- regra estrutural com autopublicação;
- dependência inexistente ou autorreferente;
- sobreposição de vigência;
- override de competência em contexto não declarado;
- competência `rule_specific` sem chave tipada;
- parser sem versão ou versão sem parser;
- fonte indisponível com snapshot fictício;
- `snapshot_path` sem hash;
- método de tabela progressiva implícito;
- faixas de direito sobrepostas;
- códigos de elegibilidade duplicados/sobrepostos;
- escopo com o mesmo item simultaneamente incluído e excluído;
- campos extras desconhecidos.

Na rodada final da Fase 2, a suíte específica do contrato executou **52 testes com sucesso**.

"""
    text = text[:start] + tests + text[end:]

    start = text.index("## 14. Compatibilidade de consumidores\n")
    end = text.index("## 15. Critério de conclusão da Fase 2\n")
    compatibility = """## 14. Versionamento, compatibilidade e auditoria de inferência

### 14.1. Quatro identidades distintas

O v1 congela quatro conceitos que não podem ser confundidos:

- `schema_version` — SemVer da forma pública JSON/Pydantic;
- `contract_api_version` — SemVer da interface semântica oferecida aos consumidores;
- `rule_version` — SemVer da semântica de cada `rule_id`;
- `release_id` — identidade imutável content-addressed, **não SemVer**.

Critérios de bump:

| Identidade | MAJOR | MINOR | PATCH |
|---|---|---|---|
| `schema_version` | forma/validação/significado incompatível | capacidade aditiva sem alterar significado anterior | correção não incompatível sem mudar significado computacional | 
| `contract_api_version` | interpretação/seleção/contexto/lifecycle incompatível | capacidade semântica aditiva preservando comportamento anterior | correção/clarificação preservando contratos existentes |
| `rule_version` | target, fórmula, incidência, contexto, dependência/ordem ou semântica incompatível | extensão compatível sem alterar casos existentes | parâmetro, vigência, proveniência/referência ou correção sob a mesma semântica |

Mesmo uma versão minor/patch **não é aceita automaticamente no v1**. Compatibilidade precisa ser testada e declarada.

### 14.2. Política fail-closed do v1

```text
schema_match            = exact
contract_api_match      = exact
unknown_fields          = reject
unknown_payload_types   = reject
backward_compatibility  = explicitly_tested_only
forward_compatibility   = not_assumed
unsupported_behavior    = hard_fail
```

O contrato declara H26, H27, H28 e H29 como consumidores-alvo, mas isso não significa migração concluída. Significa que uma incompatibilidade futura será erro explícito, não fallback silencioso.

### 14.3. Auditoria final: nenhum cálculo depende de inferência narrativa

A revisão final eliminou os pontos objetivos em que o engine ainda poderia precisar escolher uma operação a partir de texto livre. Agora são explícitos/tipados:

- método de tabela progressiva (`marginal_by_bracket` × `rate_times_base_minus_deduction`);
- unidade de escalares (`BRL` × `BRL_per_dependent`);
- fórmula e comportamentos de fronteira do redutor afim;
- método do accrual por limiar de dias;
- chave de competência `rule_specific`;
- campos/valores admitidos nos predicados de aplicabilidade;
- estágios de arredondamento;
- assertions de políticas, sem `values` livre;
- componentes e bases das fórmulas de férias;
- componentes de incidência;
- sistema de códigos eSocial;
- itens incluídos/excluídos do escopo H29;
- semântica do prorrateio do saldo de salário;
- `applies_to` das regras representativas, cruzado exatamente com o inventário fechado.

`description`, `notes` e `locator` continuam úteis para auditoria humana, mas não são fonte de decisão computacional.

"""
    text = text[:start] + compatibility + text[end:]

    start = text.index("## 15. Critério de conclusão da Fase 2\n")
    end = text.index("## 16. Delimitação com as próximas fases\n")
    criteria = """## 15. Critério de conclusão da Fase 2

Todos os critérios estão objetivamente atendidos:

- modelos Pydantic e JSON Schema cobrem as famílias necessárias — **ATENDIDO (32/32, 18 famílias)**;
- schema gerado e commitado são byte-a-byte equivalentes — **ATENDIDO**;
- exemplo canônico valida em Pydantic e JSON Schema — **ATENDIDO**;
- source registry, rule inventory, coverage map e reference cases são cruzados automaticamente — **ATENDIDO**;
- seleção por regra/contexto/vigência está testada — **ATENDIDO**;
- qualidade/publicação e `last-good` estão testados — **ATENDIDO**;
- A01, A02, abono e escopo H29 estão preservados por gates — **ATENDIDO**;
- proveniência e competência possuem invariantes negativas — **ATENDIDO**;
- versionamento/compatibilidade está congelado e executável — **ATENDIDO**;
- identidade, imutabilidade e supersessão das releases estão congeladas — **ATENDIDO**;
- nenhum campo semântico essencial exige inferência narrativa do consumidor — **ATENDIDO**;
- CI executa a suíte sem tocar produção — **ATENDIDO (52 testes específicos do contrato)**;
- handoff formal para Fase 3 existe em `docs/phase2-to-phase3-handoff.md` — **ATENDIDO**.

**Conclusão:** Fase 2 encerrada. O Contrato Fiscal Canônico v1 está pronto para ser a entrada formal da Fase 3.

"""
    text = text[:start] + criteria + text[end:]

    start = text.index("## 17. Próximo checkpoint\n")
    closing = """## 17. Fechamento e próximo passo

A Fase 2 está concluída. O contrato cobre **32/32 regras**, possui **18 famílias tipadas**, mantém 20 regras representativas no `CANDIDATE` e fechou a rodada final com **52 testes específicos do contrato**.

O handoff formal está em `docs/phase2-to-phase3-handoff.md`.

A próxima etapa é a **Fase 3 — Biblioteca fiscal e testes**, que deve implementar funções puras e determinísticas sobre este contrato, usando `Decimal`, casos oficiais e property-based testing, sem reabrir silenciosamente a semântica jurídica já congelada.
"""
    text = text[:start] + closing
    PHASE2.write_text(text, encoding="utf-8")


def update_readme() -> None:
    text = README.read_text(encoding="utf-8")
    text = replace_once(
        text,
        "A **Fase 2 está em andamento** e o primeiro corte executável do Contrato Fiscal Canônico v1 já foi materializado. A especificação detalhada da fase está em `docs/phase2-contract-v1.md`.",
        "A **Fase 2 está concluída**. O Contrato Fiscal Canônico v1 foi fechado como entrada formal da biblioteca fiscal da Fase 3. A especificação consolidada está em `docs/phase2-contract-v1.md` e o handoff em `docs/phase2-to-phase3-handoff.md`.",
        "README phase2 intro",
    )
    text = replace_once(
        text,
        "Um exemplo `CANDIDATE` vive em `contracts/examples/fiscal-contract-v1.example.json`. Ele **não é release de produção**: releases `VALIDATED`/`PUBLISHED` exigem evidência oficial disponível com hash de snapshot por regra. Snapshots reais pertencem à camada de fontes da Fase 4.",
        "Um exemplo `CANDIDATE` vive em `contracts/examples/fiscal-contract-v1.example.json`. Ele **não é release de produção**: releases `VALIDATED`/`PUBLISHED` exigem evidência oficial disponível com hash de snapshot por regra. Snapshots reais pertencem à camada de fontes da Fase 4.\n\nO v1 congela SemVer para `schema_version`, `contract_api_version` e `rule_version`, mas adota compatibilidade **exata** e fail-closed até teste explícito. `release_id` não é SemVer: releases validadas/publicadas usam `fiscal-v1-sha256-<hash do payload imutável>`. Releases publicadas são imutáveis; uma sucessora declara `supersedes_release_id` sem reescrever a predecessora.",
        "README candidate/versioning",
    )
    text = replace_once(
        text,
        "O schema v1 ainda está sob validação da Fase 2; alterações estruturais continuam permitidas nesta branch até o fechamento formal da fase, sempre acompanhadas de testes e atualização deste README.",
        "A auditoria final da Fase 2 fechou os pontos de inferência remanescentes: método de tabela progressiva, unidades, fórmula de redução, competência `rule_specific`, predicados, rounding stages, policies, componentes de fórmula/incidência, sistema de códigos, escopo H29 e prorrateio estão tipados ou cruzados contra o inventário. Campos narrativos não decidem o cálculo.",
        "README phase2 final paragraph",
    )
    text = replace_once(text, "**Status: EM ANDAMENTO**", "**Status: CONCLUÍDA**", "README phase2 status")
    text = replace_once(
        text,
        "- suíte de contrato no checkpoint de cobertura: **43 testes verdes** em `Remake CI`.",
        "- suíte final do contrato: **52 testes verdes**;\n- versionamento, compatibilidade exata, identidade content-addressed e imutabilidade/supersessão possuem gates executáveis;\n- auditoria final de inferência semântica concluída.",
        "README test count",
    )
    text = replace_once(
        text,
        "- `docs/phase2-schema-coverage.md`.",
        "- `docs/phase2-schema-coverage.md`;\n- `docs/phase2-to-phase3-handoff.md`.",
        "README phase2 artifacts",
    )
    text = replace_once(
        text,
        "20. Toda PR do remake terá validação automática em `Remake CI`; o CI é read-only e não executa publicação de dados.",
        "20. Toda PR do remake terá validação automática em `Remake CI`; o CI é read-only e não executa publicação de dados.\n21. `schema_version`, `contract_api_version` e `rule_version` seguem SemVer, mas o v1 exige compatibilidade exata e testada antes de aceitar qualquer nova versão.\n22. `release_id` é identidade content-addressed do payload fiscal imutável, não número de versão.\n23. Release `PUBLISHED` é imutável; supersessão é declarada pela sucessora em `supersedes_release_id`, sem mutar a predecessora.\n24. Campos narrativos existem para auditoria humana, mas o engine não pode depender deles para decidir operação fiscal.",
        "README decisions",
    )

    start = text.index("## 19. Questões em aberto\n")
    end = text.index("## 20. Próxima etapa\n")
    open_questions = """## 19. Questões em aberto

Com a Fase 2 concluída, não restam pendências de design do Contrato Fiscal Canônico v1. As questões abertas pertencem às fases posteriores:

- implementação e propriedades matemáticas do engine fiscal — Fase 3;
- política exata de retenção de snapshots e resiliência das fontes — Fase 4;
- critérios de confirmação multi-fonte para mudanças paramétricas — Fase 4/5;
- mecanismo de semantic diff e promoção — Fase 5;
- distribuição para WordPress/SFA e migração dos consumidores — Fase 6;
- estratégia final para CDI dentro do domínio separado de dados financeiros/de referência.

---

"""
    text = text[:start] + open_questions + text[end:]

    start = text.index("## 20. Próxima etapa\n")
    end = text.index("## 21. Convenção de status\n")
    next_stage = """## 20. Próxima etapa

Iniciar a **Fase 3 — Biblioteca fiscal e testes** a partir de `docs/phase2-to-phase3-handoff.md`.

A Fase 3 deverá implementar funções fiscais puras e determinísticas sobre o contrato congelado, com `Decimal`, casos oficiais executáveis e property-based testing. Qualquer necessidade de reinterpretar regra jurídica ou inventar semântica ausente deve voltar explicitamente ao contrato/inventário, não ser resolvida silenciosamente dentro do engine.

O pipeline de produção continua congelado: `scraper.py`, `update_taxas.py`, `dados_fiscais.json`, `taxas_bacen.json`, WordPress, `folha-core` e H26–H29 ainda não foram migrados.

---

"""
    text = text[:start] + next_stage + text[end:]

    marker = "## 22. Changelog do README\n\n"
    entry = """### 2026-09-13 — fechamento da Fase 2

- Fase 2 marcada como `CONCLUÍDA`;
- congelado SemVer para `schema_version`, `contract_api_version` e `rule_version`;
- congelada compatibilidade fail-closed/exata do v1;
- `release_id` definido como identidade content-addressed do payload imutável;
- release publicada tornada imutável e supersessão movida para `supersedes_release_id` da sucessora;
- auditoria final de inferência tipou método de tabela, unidades, competência específica, policies, fórmulas, incidências, códigos, escopo e prorrateio;
- divergência de `applies_to` em `termination.reason_scope` detectada pelo novo gate e alinhada ao inventário da Fase 1;
- suíte específica do contrato fechada com **52 testes verdes**;
- criado `docs/phase2-to-phase3-handoff.md`;
- próxima etapa alterada para **Fase 3 — Biblioteca fiscal e testes**.

"""
    if entry not in text:
        text = replace_once(text, marker, marker + entry, "README changelog")
    README.write_text(text, encoding="utf-8")


def main() -> int:
    update_phase2()
    update_readme()
    print("Phase 2 documentation closed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
