# Fase 6 — C6.1 — consumidor WordPress da release fiscal v1.2

**Data:** 2026-09-15  
**Status:** CONCLUÍDO  
**Escopo:** fronteira de rede/cache/REST do `sanida-fiscais-auto`; não inclui ainda a migração semântica de `folha-core` nem H26–H29.

## 1. Baseline migrado

A implementação parte da versão **2.4.6 efetivamente publicada** do plugin `Sanida - Fiscais (Git Source + Calculadoras)`, fornecida no projeto. Nessa versão, a autoridade fiscal era `raw.githubusercontent.com/.../main/dados_fiscais.json`, o cache validava o shape legado e a ausência de last-good permitia um fallback fiscal mínimo com tabelas/valores hardcoded.

C6.1 versiona no repositório a sucessora **2.5.0** com entrada principal e traits auxiliares em:

```text
consumers/wordpress/sanida-fiscais-auto.php
consumers/wordpress/includes/trait-sanida-fiscal-network.php
consumers/wordpress/includes/trait-sanida-fiscal-contract.php
consumers/wordpress/includes/trait-sanida-taxas.php
consumers/wordpress/includes/trait-sanida-shortcodes-core.php
consumers/wordpress/includes/trait-sanida-calc13.php
consumers/wordpress/includes/trait-sanida-salary-calc.php
consumers/wordpress/includes/trait-sanida-admin-debug.php
```

A fonte canônica de `payroll_fiscal` deixa de ser `dados_fiscais.json`.

## 2. Cadeia de consumo canônica

A fronteira WordPress passa a obedecer estritamente:

```text
releases/fiscal-v1/current.json
        ↓
manifest 1.0.0
        ↓
releases/fiscal-v1/releases/<release_id>.json
        ↓
SHA-256 do artefato + release_id content-addressed
        ↓
schema/API 1.2.0 exatos + PUBLISHED + 32/32
        ↓
cache/last-good validado
        ↓
REST WordPress
```

A baseline de C6.1 é a primeira release v1.2 publicada pelo C6.0, aprovada como `HUMAN_REVIEWED`.

## 3. Validações antes de aceitar bytes

O plugin 2.5.0:

- exige manifest schema `1.0.0`;
- exige `contract_id = br.sanida.fiscal`;
- exige schema e Contract API exatamente `1.2.0`;
- resolve somente `artifact = releases/<release_id>.json`;
- verifica o SHA-256 dos bytes recebidos contra `artifact_sha256`;
- preserva os bytes brutos do artefato no cache/last-good e revalida diretamente seu SHA-256 ao reutilizá-los;
- recalcula a identidade content-addressed da release a partir de uma decodificação que preserva a distinção JSON entre objetos e arrays — inclusive `{}` vazio — e exige igualdade com `release_id`;
- exige `status = PUBLISHED`;
- exige lifecycle final, aprovação e ausência de `block_reasons`;
- exige a política last-good fail-closed declarada pela release;
- exige exatamente as 32 `rule_id` do inventário fechado, sem duplicatas;
- exige `quality.status = VALIDATED` para todas as regras;
- rejeita payload type fora das 18 famílias conhecidas;
- exige em cada regra ao menos uma evidência `AVAILABLE` com snapshot SHA-256.

`CANDIDATE`, schema/API incompatível, release incompleta, artefato com hash divergente ou identidade inválida não entram no cache.

## 4. Cache e last-good

O cache fiscal passa a armazenar o **pacote integral validado**: manifest, bytes brutos do artefato, release decodificada e metadata operacional. O `release_id` declarado continua sendo a identidade da release; sua recomputação local serve apenas como verificação de integridade, nunca como autoridade alternativa.

Se `current.json` aponta para a mesma release e o last-good validado possui o mesmo `artifact_sha256`, o plugin reutiliza o pacote sem rebaixar a garantia de integridade e sem churn de cache.

Falha de rede pode reutilizar last-good somente quando a política da própria release permite. O runtime marca a origem como last-good e mantém os timestamps jurídicos/publicados da release intactos; `checked_at_utc` é apenas relógio operacional. A implementação nunca relabela uma release histórica como se fosse nova.

Se um `current.json` válido já anuncia `release_id` diferente do last-good, existe sucessora conhecida. Nesse caso o requisito `require_no_known_successor` bloqueia o uso da release anterior se a nova não puder ser validada.

Não existe mais fallback fiscal mínimo hardcoded em C6.1.

## 5. REST e auditabilidade

C6.1 mantém temporariamente:

```text
GET /blog/wp-json/sfa/v1/folha
```

como **adaptador de compatibilidade** para o `folha-core` ainda legado. Esse adaptador é derivado mecanicamente de `rule_id` específicos da release canônica; ele não consulta `dados_fiscais.json` e expõe também:

- `release_id`;
- contract/schema version;
- Contract API version;
- `published_at_utc`;
- approval mode;
- SHA-256 do artefato declarado no manifest.

Foi adicionado também:

```text
GET /blog/wp-json/sfa/v1/fiscal-release
```

que expõe diretamente a release v1.2 validada para o próximo consumidor. Ambos falham com HTTP 503 quando não existe release fiscal canônica consumível e enviam `X-Sanida-Fiscal-Release` quando há resposta válida.

## 6. Fronteira com `financial_reference`

C6.1 não mistura o contrato jurídico-fiscal com Selic/CDI. O caminho `taxas_bacen.json` continua separado e conserva sua política própria até eventual fase específica. O fallback histórico de taxas não é tratado por esta mudança; a proibição desta etapa refere-se ao fallback **fiscal** de INSS/IRRF.

## 7. Fronteira com C6.2

C6.1 deliberadamente não reescreve `folha-core.js`. O fallback raw de `folha-core` e a interpretação das regras no JavaScript pertencem ao **C6.2**.

Enquanto C6.2 não for concluído, `/sfa/v1/folha` funciona como ponte transitória. Essa ponte deve desaparecer ou ser reduzida após H26–H29 migrarem para o contrato canônico; ela não constitui uma segunda autoridade fiscal.

## 8. Gate permanente

O Remake CI executa:

```text
python scripts/validate_phase6_c61_gate.py
```

Além da suíte normal, o gate verifica a primeira release v1.2 realmente publicada, o SHA do artefato, inventário 32/32, presença das garantias no consumidor WordPress, ausência da autoridade/fallback fiscal legado e integração do próprio gate ao CI. Se PHP estiver disponível no runner, executa também `php -l` sobre o plugin.

## 9. Implantação

Este fechamento consolida e valida a **fonte canônica do plugin 2.5.0 no repositório**. Ele **não afirma implantação no HostGator**: esta sessão não possui um canal autorizado para gravar os arquivos do WordPress/hosting em produção. A ativação física no site deve usar exatamente o arquivo versionado e só então pode ser verificada E2E.

O próximo item de implementação é **C6.2 — `folha-core`**, sem reabrir C6.0/C6.1 salvo defeito reproduzível.
