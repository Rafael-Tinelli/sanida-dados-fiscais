# Fase 2 — Contrato Fiscal Canônico v1

**Status:** EM ANDAMENTO  
**Início:** 13/09/2026  
**Base:** fechamento formal da Fase 1 (`docs/phase1-closure.md`)

## 1. Objetivo desta fase

Transformar o inventário jurídico-fiscal fechado na Fase 1 em um contrato executável, versionado e rejeitável por máquina antes de qualquer migração dos scrapers ou consumidores de produção.

```text
fontes / sensores / parsers
          ↓
Contrato Fiscal Canônico v1
          ↓
engine fiscal / consumidores
```

O contrato deve impedir que um consumidor precise adivinhar semântica, vigência, contexto de apuração, qualidade ou proveniência.

## 2. Implementação atual

A implementação materializa:

- modelo Pydantic v2 em `sanida_fiscal/contract_v1.py` e tipos em `sanida_fiscal/types_v1.py`;
- JSON Schema público em `contracts/fiscal-contract-v1.schema.json`;
- exemplo `CANDIDATE` em `contracts/examples/fiscal-contract-v1.example.json`;
- geração determinística do schema em `scripts/generate_contract_schema.py`;
- validação cruzada com inventário, fontes, cobertura e casos de referência em `scripts/validate_contract_v1.py`;
- mapa machine-readable de cobertura em `docs/contract-coverage-v1.json`;
- leitura humana do checkpoint em `docs/phase2-schema-coverage.md`;
- testes de contrato em `tests/test_contract_v1.py`;
- dependências isoladas em `requirements-contract.txt` e `requirements-dev.txt`.

Essa implementação **não substitui** `dados_fiscais.json` e não altera ainda os consumidores H26–H29.

## 3. Estrutura canônica

O documento raiz `FiscalContractV1` contém versão do schema, identificador da release, status, jurisdição, geração, versões do source registry/rule inventory, compatibilidade dos consumidores, lifecycle da release, política de `last-good` e conjunto de regras.

Cada `FiscalRuleV1` materializa:

- `rule_id` e `rule_version`;
- domínio;
- consumidores e contextos de apuração;
- target semântico (`applies_to`);
- predicados de aplicabilidade;
- dependências e ordem de cálculo;
- competência e vigência;
- política de arredondamento;
- payload tipado;
- proveniência;
- qualidade;
- classe da mudança;
- política de atualização.

## 4. Cobertura integral do inventário

O inventário fechado da Fase 1 possui **32 regras**. O arquivo `docs/contract-coverage-v1.json` mapeia exatamente essas 32 regras para o schema v1 e é validado pelo CI.

Estado deste checkpoint:

```text
regras no inventário:       32
regras mapeadas:             32
cobertura:                   32/32
famílias de payload:         18
regras no CANDIDATE:         20
famílias materializadas:     18/18
```

O `CANDIDATE` não precisa duplicar todas as 32 regras do inventário para provar expressividade do schema; ele precisa materializar pelo menos uma regra de **cada família de payload**. A cobertura exata regra → família é garantida pelo mapa machine-readable e pelo validador.

As 18 famílias do v1 são:

- `progressive_table`;
- `scalar`;
- `affine_reduction`;
- `threshold_accrual`;
- `fraction`;
- `entitlement_bands`;
- `eligibility_matrix`;
- `incidence_profile`;
- `policy`;
- `remuneration_reference`;
- `variable_remuneration`;
- `thirteenth_advance`;
- `period_rule`;
- `component_formula`;
- `scope_declaration`;
- `proration`;
- `code_eligibility`;
- `period_state`.

Nova família de payload é mudança estrutural do schema e não pode nascer automaticamente de scraping.

## 5. Políticas tipadas

O payload `policy` não é mais um dicionário semanticamente aberto. Toda política deve declarar um `policy_kind` conhecido:

```text
deductions_by_income_type
income_type_partition
separate_social_security_assessment
exclusive_irrf_assessment
separate_vacation_irrf_assessment
money_decimal_and_rounding
contract_vigency_and_quality
```

O CI exige que todas essas políticas estejam ancoradas em regras reais do inventário da Fase 1.

## 6. Contextos de apuração

O v1 distingue:

```text
monthly
thirteenth
vacation_enjoyed
vacation_cash_allowance
vacation_indemnified
termination
technical
```

Isso impede tratar mensal, 13º e férias como se fossem a mesma apuração fiscal.

## 7. Competência

A competência é parte obrigatória da regra.

O contrato suporta:

```text
payment_date
competence_month
termination_date
acquisition_period
rule_specific
```

Além da base principal, `context_overrides` permite declarar uma base distinta para um contexto específico sem recorrer a texto livre. Overrides para contextos não declarados são rejeitados. `rule_specific` exige descrição explícita.

## 8. Proveniência

Cada evidência referencia `source_id` do registro canônico e declara:

- papel da fonte;
- momento e estado da observação;
- método de obtenção (`manual`, `http_html`, `http_pdf`, `api` ou `dataset`);
- localizador;
- SHA-256 e caminho do snapshot, quando existentes;
- parser e versão, quando automatizado.

Invariantes:

- `parser_id` e `parser_version` aparecem juntos;
- fonte indisponível/incompatível não pode fingir possuir snapshot;
- `snapshot_path` exige `snapshot_sha256`;
- cada fonte usada por uma regra deve estar autorizada pelo `rule-inventory-v1.json`;
- o papel da fonte deve coincidir com `source-registry-v1.json`;
- todo `CANDIDATE` representativo precisa de ao menos uma evidência oficial disponível por regra.

Snapshots reais permanecem responsabilidade da camada de fontes da Fase 4. A Fase 2 define o contrato de proveniência; não inventa hashes para parecer uma release real.

## 9. Lifecycle da release

Estados suportados:

```text
DRAFT
CANDIDATE
VALIDATED
PUBLISHED
SUPERSEDED
BLOCKED
```

O modelo `ReleaseLifecycle` registra, conforme o estado:

- release anterior;
- data de validação;
- data de publicação;
- data de supersessão;
- release sucessora;
- modo e referência de aprovação;
- motivos de bloqueio.

Gates principais:

- `DRAFT` não pode carregar estado de validação/publicação;
- `CANDIDATE` não pode alegar publicação ou supersessão;
- `VALIDATED` exige `validated_at_utc`;
- `PUBLISHED` exige validação, publicação, aprovação e evidência hashada por regra;
- release com mudança estrutural só pode ser `PUBLISHED` com `HUMAN_REVIEWED`;
- `SUPERSEDED` exige histórico de publicação e sucessora explícita;
- `BLOCKED` exige `block_reasons` e não pode estar publicada.

## 10. Invariantes executáveis

### 10.1. Vigência

- `effective_until` não pode ser anterior a `effective_from`;
- versões do mesmo `rule_id` não podem se sobrepor no mesmo contexto;
- seleção fora da vigência falha;
- seleção ambígua ou ausente falha.

### 10.2. Qualidade e publicação

Release `VALIDATED` ou `PUBLISHED` exige todas as regras `VALIDATED` e ao menos uma evidência oficial disponível com `snapshot_sha256` por regra.

O exemplo permanece `CANDIDATE` porque snapshots/hashes reais pertencem à camada de fontes da Fase 4.

### 10.3. Mudança estrutural

As classes `structural_rule`, `product_scope_rule` e `technical_contract_rule`:

- nunca podem `auto_publish=true`;
- quando validadas, exigem `reviewed_by_human=true`.

### 10.4. `last-good`

É impossível configurar `allow_relabel_historical_as_current=true`.

O `last-good` só é elegível quando a regra está validada, a data permanece dentro da vigência e não há sucessora conhecida. Expiração ou vigência desconhecida resultam em `hard_fail`.

### 10.5. Decimais

Valores monetários e coeficientes usam `Decimal` e são serializados canonicamente como strings decimais, evitando float binário no contrato.

## 11. Invariantes herdadas da Fase 1

### A01 — redutor de IR 2026

```text
irrf.reduction.2026.applies_to
=
taxable_income_subject_to_monthly_incidence_before_irrf_deductions
```

O payload repete o mesmo `input_semantic`; ele não pode colapsar para a base de IR após deduções.

### 13º

```text
thirteenth.irrf.reduction.2026
context = thirteenth
```

### Abono pecuniário

```text
cash_allowance_principal:
  IRRF = no
  CP   = no

constitutional_third_on_cash_allowance:
  IRRF = yes
  CP   = no
```

### H29

O exemplo aceita exatamente os motivos eSocial fechados na Fase 1:

```text
01  justa causa pelo empregador
02  sem justa causa pelo empregador
07  pedido de demissão
33  acordo do art. 484-A
```

Outros motivos continuam `UNSUPPORTED` até especificação própria.

## 12. Testes negativos

A suíte não testa apenas casos felizes. Ela rejeita, entre outros:

- release publicada sem snapshot hashado;
- mudança estrutural publicada como `AUTO_VALIDATED`;
- `CANDIDATE` com timestamp de publicação;
- `BLOCKED` sem motivo;
- `SUPERSEDED` sem sucessora;
- regra estrutural com autopublicação;
- dependência inexistente ou autorreferente;
- sobreposição de vigência;
- override de competência em contexto não declarado;
- competência `rule_specific` sem descrição;
- parser sem versão ou versão sem parser;
- fonte indisponível com snapshot fictício;
- `snapshot_path` sem hash;
- faixas de direito sobrepostas;
- códigos de elegibilidade duplicados/sobrepostos;
- escopo com o mesmo item simultaneamente incluído e excluído;
- campos extras desconhecidos.

No checkpoint 32/32, o `Remake CI` executou **43 testes com sucesso**.

## 13. Casos de referência

`reference_case_ids` não são texto livre: o validador exige que cada ID exista em `tests/reference_cases/phase1_reference_cases.json`.

## 14. Compatibilidade de consumidores

O v1 começa com:

```text
contract_api_version = 1.0.0
consumers = H26, H27, H28, H29
unsupported_behavior = hard_fail
```

Isso não significa migração concluída; significa que incompatibilidade futura será erro explícito e não fallback silencioso.

## 15. Critério de conclusão da Fase 2

A Fase 2 só termina quando:

- modelos Pydantic e JSON Schema cobrirem as famílias necessárias do inventário — **ATENDIDO (32/32, 18 famílias)**;
- schema gerado e commitado forem byte-a-byte equivalentes — **ATENDIDO**;
- exemplo canônico validar em Pydantic e JSON Schema — **ATENDIDO**;
- source registry, rule inventory, coverage map e reference cases forem cruzados automaticamente — **ATENDIDO**;
- seleção por regra/contexto/vigência estiver testada — **ATENDIDO**;
- política de qualidade/publicação estiver testada — **ATENDIDO no nível estrutural**;
- política de `last-good` estiver testada — **ATENDIDO**;
- invariantes A01/H28/H29 relevantes ao contrato estiverem cobertas — **ATENDIDO**;
- CI instalar dependências e executar a suíte sem tocar produção — **ATENDIDO**;
- nenhum campo semântico essencial depender de inferência do consumidor — **EM REVISÃO FINAL**;
- política de versionamento/compatibilidade do schema e das releases estar congelada — **PENDENTE**;
- handoff formal para a Fase 3 estar documentado — **PENDENTE**.

## 16. Delimitação com as próximas fases

A Fase 2 define **o contrato e seus gates**.

Ficam para as fases seguintes:

- funções fiscais puras e property-based testing amplo — Fase 3;
- collectors, snapshots reais, hashes e resiliência HTTP — Fase 4;
- diff semântico e promoção automática — Fase 5;
- migração de WordPress/`folha-core`/H26–H29 — Fase 6.

## 17. Próximo checkpoint

A pergunta de expressividade do schema está fechada: **as 32 regras da Fase 1 são representáveis por 18 famílias tipadas e verificadas pelo CI**.

Antes de declarar a Fase 2 concluída, resta a rodada de fechamento sobre:

1. versionamento de schema, `contract_api_version`, `rule_version` e `release_id`;
2. regras de compatibilidade backward/forward;
3. imutabilidade e supersessão de releases;
4. revisão final de campos que ainda poderiam exigir inferência do consumidor;
5. documento formal de handoff para a Fase 3.
