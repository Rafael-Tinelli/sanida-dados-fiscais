# Fase 5 — Contrato Fiscal v1.2 e publicação canônica

## 1. Objetivo

A Fase 5 fecha a fronteira entre um candidato fiscal validado e uma release canônica consumível.

Ela não reinterpreta legislação. Seu papel é:

- classificar mudanças de forma determinística;
- decidir se uma mudança pode ser promovida automaticamente ou exige revisão humana;
- preservar evidência oficial e governança interna separadamente;
- materializar exatamente o inventário fechado de 32 regras;
- publicar releases imutáveis e content-addressed;
- manter um ponteiro `current.json` atômico;
- bloquear qualquer bootstrap ou mudança estrutural sem aprovação humana explícita.

## 2. Resultado da Fase 5

A Fase 5 é considerada **CONCLUÍDA** como implementação e governança.

Isso não significa que a primeira release v1.2 de produção já exista. O primeiro bootstrap continua deliberadamente pendente de:

1. merge da Fase 5 no `main`;
2. coleta das fontes oficiais reais;
3. montagem do candidato exato 32/32;
4. revisão humana explícita;
5. publicação pela automação permanente.

A migração dos consumidores permanece fora deste fechamento e pertence à Fase 6.

## 3. Diff semântico e promoção

O motor `sanida_fiscal/semantic_diff_v1.py` calcula a classe de mudança; não confia em classe declarada pelo produtor.

Resultados possíveis:

```text
NO_PUBLISH_REQUIRED
AUTO_PUBLISH_ALLOWED
REVIEW_REQUIRED
BLOCKED
```

Princípios:

- mudança apenas numérica só é `PARAMETER_CHANGE` quando o shape semântico tipado permanece idêntico;
- mudança de campos estruturais, fórmulas, target, dependências, aplicabilidade ou governança permanece estrutural;
- mudança de `effective_date`, adição, remoção e mudança estrutural exigem revisão humana;
- autopromoção de parâmetro/fonte exige evidência hashada e parser versionado;
- bootstrap sem release anterior é sempre revisão humana;
- `change_class` é relativo à release anterior, não histórico permanente da regra.

## 4. Por que existe o v1.2

O contrato v1.1 exigia proveniência oficial externa em toda regra `PUBLISHED`.

Esse requisito era incompatível com duas regras técnicas do inventário:

```text
technical.money_decimal_and_rounding
technical.contract_vigency_and_quality
```

Essas regras não são normas governamentais. Elas definem governança computacional da própria Sanida. Atribuir uma fonte governamental a elas criaria falsa autoridade.

Por isso:

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

### Fallback de transporte não é fallback de autoridade

`PLANALTO_CLT` permanece vinculado exclusivamente à URL oficial registrada da CLT compilada. Em GitHub-hosted runners, essa URL demonstrou uma incompatibilidade repetível com a pilha `httpx`, encerrando a conexão antes de qualquer resposta HTTP (`RemoteProtocolError`).

A coleta v1.2 admite, somente para esse erro e esse `source_id`, uma segunda tentativa pela pilha `requests/urllib3`. Esse fallback:

- usa exatamente o mesmo `source_id` e a mesma URL oficial;
- não consulta espelho, cache externo, busca ou fonte substituta;
- persiste os bytes retornados no mesmo `SnapshotStore` content-addressed;
- não interpreta automaticamente a CLT;
- continua fail-closed se a segunda pilha também não obtiver resposta oficial válida.

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

O workflow possui:

- `workflow_dispatch` para execução manual;
- schedule diário;
- preparação acionável de revisão humana antes do bootstrap;
- gate de publicação fail-closed;
- geração/materialização do schema público v1.2;
- nova execução de testes e gates antes de persistir uma publicação bem-sucedida.

**Bootstrap exige** revisão humana explícita do candidato 32/32 e aprovação vinculada ao `review_key` exato. O schedule pode detectar e preparar revisão, mas não pode criar a primeira release `PUBLISHED` sem essa aprovação.

## 10. Store imutável

A publicação usa:

```text
releases/fiscal-v1/releases/{release_id}.json
releases/fiscal-v1/current.json
```

O `release_id` é calculado somente depois da reconciliação final do candidato. O ponteiro `current.json` é atualizado atomicamente.

Um caminho de release existente com bytes diferentes é erro fatal.

## 11. Fase 6 — Migração dos consumidores

A Fase 6 só pode migrar consumidores depois de existir uma release v1.2 `PUBLISHED` real e verificável.

Ordem segura:

1. bootstrap humano da primeira release;
2. validar `current.json`, release imutável, schema e evidências;
3. migrar `sanida-fiscais-auto`/cache/REST;
4. migrar `folha-core`;
5. migrar H26–H29;
6. remover caminhos legados somente após paridade comprovada.
