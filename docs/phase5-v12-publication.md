# Fase 5 — Diff semântico, Contrato Fiscal v1.2 e publicação

**Status do documento:** fechamento técnico da Fase 5  
**Escopo:** produtor canônico `sanida-dados-fiscais`; consumidores permanecem na Fase 6.

## 1. O que a Fase 5 fecha

A Fase 5 transforma candidatos e evidências da Fase 4 em uma decisão de promoção auditável. O caminho fechado é:

```text
raw snapshot / normalized candidate
        ↓
assembler do contrato canônico
        ↓
semantic diff computado
        ↓
classification + version gate
        ↓
AUTO_PUBLISH_ALLOWED | REVIEW_REQUIRED | BLOCKED | NO_PUBLISH_REQUIRED
        ↓
release content-addressed imutável
        ↓
current.json atômico
```

A classificação não confia no `change_class` declarado pelo produtor. O valor declarado precisa coincidir com a classe calculada pelo diff.

## 2. Classes e decisão

O classificador distingue:

- `SOURCE_REFRESH_NO_CHANGE`;
- `PARAMETER_CHANGE`;
- `EFFECTIVE_DATE_CHANGE`;
- `STRUCTURAL_CHANGE`;
- `RULE_ADDED`;
- `RULE_REMOVED`.

`SOURCE_UNAVAILABLE` e `PARSER_INCOMPATIBLE` permanecem estados operacionais incapazes de autorizar uma promoção nova.

Uma alteração numérica só é paramétrica quando a forma tipada e todas as partes semânticas do payload permanecem iguais. Mudança de campo, tipo, enum/string semântica, fórmula, target, estrutura de lista/objeto ou classe de regra não pode ser rebaixada para alteração de parâmetro.

Mudança de vigência, mudança estrutural e adição/remoção de regra exigem revisão humana.

## 3. `change_class` é relativo à release

`change_class` descreve a transição que está sendo publicada, não toda a história da regra.

Uma regra que entrou na primeira release como `RULE_ADDED` não permanece com esse rótulo para sempre. Antes da publicação, a Fase 5 reconcilia cada ocorrência contra a release anterior:

- regra efetivamente alterada recebe a classe computada;
- regra comparável inalterada recebe `SOURCE_REFRESH_NO_CHANGE` como metadado da transição;
- regra nova permanece `RULE_ADDED`.

O `release_id` final só é calculado depois dessa reconciliação.

## 4. Por que existe o Contrato Fiscal v1.2

A cobertura da Fase 2 definiu 32 regras, incluindo:

- `technical.money_decimal_and_rounding`;
- `technical.contract_vigency_and_quality`.

Essas duas regras são invariantes de governança computacional da Sanida. Por definição, não possuem autoridade jurídica externa em `docs/source-registry-v1.json`.

O modelo v1.1, entretanto, exigia snapshot hashado em toda regra `PUBLISHED` e a validação de proveniência estava limitada às fontes oficiais externas. Isso tornava impossível publicar legitimamente 32/32 sem atribuir falsamente uma fonte governamental às regras técnicas.

A correção é aditiva e auditável:

```text
histórico preservado: schema/API 1.1.0
release canônica:     schema/API 1.2.0
```

O v1.2 cria `GovernanceEvidenceObservation`, uma trilha hash-addressed separada para `technical_contract_rule`.

### Fronteira obrigatória

- regra jurídico-fiscal não pode usar evidência interna como autoridade;
- regra técnica deve possuir evidência interna de governança;
- regra técnica não pode apresentar fonte oficial externa como se fosse a autoridade de sua própria política computacional.

O registro dessa trilha é `docs/governance-source-registry-v1.json`.

## 5. As 32 regras passam a ser materializadas

O exemplo histórico `contracts/examples/fiscal-contract-v1.example.json` continua sendo um CANDIDATE representativo de 21 regras e **não é publicável**.

No bootstrap v1.2, `release_assembler_v12.py` materializa exatamente as 11 regras que faltavam:

```text
inss.employee.progressive_table
irrf.income_type
irrf.simplified_monthly_discount
technical.contract_vigency_and_quality
technical.money_decimal_and_rounding
termination.vacation_indemnity_tax_treatment
thirteenth.inss.separate_assessment
thirteenth.irrf.exclusive_assessment
vacation.inss.enjoyed
vacation.irrf.reduction.2026
vacation.irrf.separate_assessment
```

A release canônica só pode seguir para publicação se o conjunto de `rule_id` for exatamente igual ao inventário 32/32.

Após o bootstrap, o assembler parte sempre da release v1.2 `PUBLISHED` corrente. O CANDIDATE histórico não é reaplicado sobre sucessores.

## 6. Evidência externa e interpretação automática

Para as 30 regras não técnicas, a autoridade continua limitada a `docs/source-registry-v1.json` e aos `source_ids` autorizados por regra no inventário.

`authority_evidence_v12.py`:

- coleta snapshots oficiais imutáveis;
- deduplica a coleta de uma mesma fonte usada por várias regras;
- usa apenas fonte já autorizada no inventário;
- não converte alteração bruta de fonte estrutural em nova regra;
- só executa parser semântico nos dois bindings já fechados na Fase 4:
  - `RFB_IRRF_TABLE_2026`;
  - `INSS_TABLE_2026`.

As demais fontes funcionam como evidência bruta para detecção/revisão, não como intérpretes automáticos de legislação.

## 7. Autopromoção

Uma mudança conhecida só pode receber `AUTO_PUBLISH_ALLOWED` quando, cumulativamente:

- todas as regras continuam `VALIDATED`;
- existe evidência `AVAILABLE` hashada;
- a classe computada é compatível com autopublicação;
- a regra declara `auto_publish=true`;
- a mudança observada é sustentada por parser versionado quando há alteração de fonte/parâmetro;
- o bump de `rule_version` é coerente;
- não há mudança semântica de contrato que exija revisão.

Mudanças estruturais jamais passam por esse caminho.

## 8. Idempotência

Nova observação não é sinônimo de nova release.

Quando uma fonte apresenta o mesmo `snapshot_sha256`, o assembler reaproveita a proveniência anterior byte a byte. `quality.last_validated_at_utc` também não é atualizado apenas porque o job rodou novamente.

Portanto, uma execução sem delta produz `NO_PUBLISH_REQUIRED` e preserva a release corrente, em vez de gerar churn de hashes/timestamps.

## 9. Bootstrap e workflow

Workflow permanente:

```text
.github/workflows/fiscal-release-v12.yml
```

A primeira release 32/32 não pode nascer pelo `schedule`.

Bootstrap exige `workflow_dispatch` com uma `approval_reference` explícita. O resultado é `HUMAN_REVIEWED`.

Depois de existir `releases/fiscal-v1/current.json`, o workflow agendado pode avaliar sucessores. Mudanças paramétricas seguras podem ser autopublicadas; mudanças que exigem revisão encerram a execução fail-closed e registram o estado acionável.

O writer usa o mesmo grupo de concorrência dos demais writers de `main`:

```text
sanida-dados-fiscais-writes-main
```

## 10. Release store

Estrutura canônica:

```text
releases/fiscal-v1/
├── releases/
│   └── fiscal-v1-sha256-<digest>.json
└── current.json
```

Arquivos de release são imutáveis. `current.json` é o único ponteiro mutável e é trocado atomicamente.

A leitura valida novamente:

- hash do artefato;
- path derivado do `release_id`;
- contrato Pydantic;
- status `PUBLISHED`;
- identidade content-addressed.

## 11. JSON Schema v1.2

`scripts/generate_contract_schema_v12.py` gera deterministicamente:

```text
contracts/fiscal-contract-v1.2.schema.json
```

O schema v1.1 histórico permanece separado. Compatibilidade não é presumida: consumidores devem declarar suporte exato à versão recebida.

## 12. O que a Fase 5 não faz

A Fase 5 não migra:

- `sanida-fiscais-auto`;
- WordPress/cache;
- `folha-core`;
- H26;
- H27;
- H28;
- H29.

Também não declara `dados_fiscais.json` removido. Essas fronteiras pertencem à Fase 6.

## 13. Gate de encerramento

O encerramento formal depende simultaneamente de:

- suíte pytest verde;
- gates históricos das Fases 1–4 preservados;
- semantic diff e publication foundation verdes;
- v1.2 32/32 testada;
- evidência interna separada de autoridade externa;
- schema v1.2 determinístico;
- workflow permanente fail-closed;
- `scripts/validate_phase5_closure_gate.py` verde no mesmo head.

Após esse gate, a próxima etapa é **Fase 6 — Migração dos consumidores**.
