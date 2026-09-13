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

## 2. Primeira implementação

A primeira implementação materializa:

- modelo Pydantic v2 em `sanida_fiscal/contract_v1.py` e tipos em `sanida_fiscal/types_v1.py`;
- JSON Schema público em `contracts/fiscal-contract-v1.schema.json`;
- exemplo `CANDIDATE` em `contracts/examples/fiscal-contract-v1.example.json`;
- geração determinística do schema em `scripts/generate_contract_schema.py`;
- validação cruzada com inventário, fontes e casos de referência em `scripts/validate_contract_v1.py`;
- testes de contrato em `tests/test_contract_v1.py`;
- dependências isoladas em `requirements-contract.txt` e `requirements-dev.txt`.

Essa implementação **não substitui** `dados_fiscais.json` e não altera ainda os consumidores H26–H29.

## 3. Estrutura canônica

O documento raiz `FiscalContractV1` contém versão do schema, identificador da release, status, jurisdição, geração, versões do source registry/rule inventory, compatibilidade dos consumidores, política de `last-good` e conjunto de regras.

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

## 4. Payloads tipados do v1

O schema inicial evita uma rules engine genérica. Ele admite famílias pequenas e auditáveis:

- `scalar`;
- `progressive_table`;
- `affine_reduction`;
- `threshold_accrual`;
- `fraction`;
- `entitlement_bands`;
- `eligibility_matrix`;
- `incidence_profile`;
- `policy`.

Nova família de payload é mudança estrutural do schema e não pode nascer automaticamente de scraping.

## 5. Contextos de apuração

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

## 6. Invariantes executáveis

### 6.1. Vigência

- `effective_until` não pode ser anterior a `effective_from`;
- versões do mesmo `rule_id` não podem se sobrepor no mesmo contexto;
- seleção fora da vigência falha;
- seleção ambígua ou ausente falha.

### 6.2. Qualidade e publicação

Release `VALIDATED` ou `PUBLISHED` exige todas as regras `VALIDATED` e ao menos uma evidência oficial disponível com `snapshot_sha256` por regra.

O exemplo inicial permanece `CANDIDATE` porque snapshots/hashes reais pertencem à camada de fontes da Fase 4.

### 6.3. Mudança estrutural

- regra estrutural nunca pode `auto_publish=true`;
- regra estrutural validada exige `reviewed_by_human=true`.

### 6.4. `last-good`

É impossível configurar `allow_relabel_historical_as_current=true`.

O `last-good` só é elegível quando a regra está validada, a data permanece dentro da vigência e não há sucessora conhecida. Expiração ou vigência desconhecida resultam em `hard_fail`.

### 6.5. Decimais

Valores monetários e coeficientes usam `Decimal` e são serializados canonicamente como strings decimais, evitando float binário no contrato.

## 7. Invariantes herdadas da Fase 1

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

O exemplo inicial aceita exatamente os motivos eSocial fechados na Fase 1:

```text
01  justa causa pelo empregador
02  sem justa causa pelo empregador
07  pedido de demissão
33  acordo do art. 484-A
```

Outros motivos continuam `UNSUPPORTED` até especificação própria.

## 8. Proveniência

Cada evidência referencia `source_id` do registro canônico e declara papel da fonte, momento e estado da observação, localizador, SHA-256 do snapshot quando disponível e parser/versão quando automatizado.

O validador cruza `source_id` e `role` com `docs/source-registry-v1.json`.

## 9. Casos de referência

`reference_case_ids` não são texto livre: o validador exige que cada ID exista em `tests/reference_cases/phase1_reference_cases.json`.

## 10. Compatibilidade de consumidores

O v1 começa com:

```text
contract_api_version = 1.0.0
consumers = H26, H27, H28, H29
unsupported_behavior = hard_fail
```

Isso não significa migração concluída; significa que incompatibilidade futura será erro explícito e não fallback silencioso.

## 11. Critério de conclusão da Fase 2

A Fase 2 só termina quando:

- modelos Pydantic e JSON Schema cobrirem as famílias necessárias do inventário;
- schema gerado e commitado forem byte-a-byte equivalentes;
- exemplo canônico validar em Pydantic e JSON Schema;
- source registry, rule inventory e reference cases forem cruzados automaticamente;
- seleção por regra/contexto/vigência estiver testada;
- política de qualidade/publicação estiver testada;
- política de `last-good` estiver testada;
- invariantes A01/H28/H29 relevantes ao contrato estiverem cobertas;
- CI instalar dependências e executar a suíte sem tocar produção;
- nenhum campo semântico essencial depender de inferência do consumidor.

## 12. Delimitação com as próximas fases

A Fase 2 define **o contrato e seus gates**.

Ficam para as fases seguintes:

- funções fiscais puras e property-based testing amplo — Fase 3;
- collectors, snapshots reais, hashes e resiliência HTTP — Fase 4;
- diff semântico e promoção automática — Fase 5;
- migração de WordPress/`folha-core`/H26–H29 — Fase 6.
