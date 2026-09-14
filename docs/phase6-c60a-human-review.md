# Fase 6 — C6.0a — Human Review UX

**Data:** 2026-09-14  
**Status:** IMPLEMENTADO NA BRANCH / AGUARDANDO CI E MERGE  
**Escopo:** revisão humana acionável para `REVIEW_REQUIRED` antes do bootstrap v1.2 e antes de qualquer migração de consumidor.

## 1. Problema resolvido

A Fase 5 já conseguia bloquear uma publicação quando o semantic diff produzia `REVIEW_REQUIRED`, mas o estado anterior ainda era insuficiente para uma decisão humana de boa qualidade: o workflow terminava com um erro, persistia um JSON técnico e dependia de uma `approval_reference` digitada manualmente.

C6.0a transforma esse bloqueio em um processo auditável de decisão:

```text
candidato fiscal
  ↓
semantic diff
  ↓
REVIEW_REQUIRED
  ↓
review packet determinístico
  ↓
regressões completas
  ↓
Issue GitHub atribuída ao owner
  ↓
leitura + decisão humana
  ↓
/approve <review_key>
  ↓
recoleta + remontagem + recálculo do review_key
  ↓
review_key idêntico? sim → publicação autorizada
                     não → aprovação stale, publicação bloqueada
```

## 2. Notificação

Quando existe decisão humana pendente, o workflow cria ou atualiza automaticamente uma Issue no repositório e a atribui ao proprietário do repositório.

Isso gera uma notificação GitHub para o usuário atribuído. **O repositório não promete entrega por e-mail**, porque o envio por e-mail depende das preferências de notificação configuradas na conta GitHub.

O processo não depende mais de perceber que um workflow ficou vermelho para descobrir que existe uma decisão pendente.

## 3. Substrato fornecido pela Issue

A Issue apresenta:

- `review_key` SHA-256 do candidato relevante para a decisão;
- release atual ou indicação explícita de bootstrap;
- schema/API do candidato;
- motivos do `REVIEW_REQUIRED`;
- classes de mudança calculadas;
- regras e versões antes/depois;
- consumidores/calculadoras afetados;
- `changed_paths`;
- diferenças semânticas e de payload relevantes;
- fontes do candidato;
- hashes dos snapshots;
- parser e versão, quando disponíveis;
- casos de referência associados, quando disponíveis;
- resultado da suíte completa de regressões.

O pacote completo machine-readable fica em:

```text
state/fiscal-release-v12-review.json
```

O resultado das regressões fica em:

```text
state/fiscal-release-v12-review-regressions.json
```

## 4. Bootstrap 21 → 32

No primeiro bootstrap não existe release v1.2 publicada anterior. Por isso o relatório **não apresenta falsamente as 32 regras como 32 decisões novas**.

Ele usa o candidato histórico v1.1 como baseline de território e distingue:

- regras já materializadas no candidato histórico;
- regras que precisaram ser materializadas para completar o inventário 32/32;
- eventual delta semântico real contra o baseline histórico;
- mudança de evidência/proveniência;
- evolução arquitetural v1.1 → v1.2.

O fechamento da Fase 5 identificou a lacuna histórica como **21 regras materializadas + 11 regras materializadas no assembler v1.2**.

## 5. `review_key`

O `review_key` é estável para o mesmo candidato decisório.

Ele inclui semântica, payload, diff e identidade/hash da evidência. Relógios operacionais como `observed_at_utc` ou `last_validated_at_utc`, quando os bytes e a semântica não mudaram, não criam uma revisão nova por conveniência.

Mudança de snapshot oficial, payload, semântica ou diff altera o `review_key`.

Isso permite atualizar a mesma Issue enquanto o candidato é realmente o mesmo e abrir uma nova revisão quando o conteúdo a decidir mudou.

## 6. Aprovação

A superfície preferida de aprovação é a própria Issue.

O comando é exatamente:

```text
/approve <review_key>
```

Somente comentário do proprietário do repositório pode acionar a aprovação automática.

A referência persistida na release identifica Issue, comentário, autor e review key:

```text
issue#N@comment#ID@actor:LOGIN@review:SHA256
```

Existe fallback manual por `workflow_dispatch`, mas ele exige **simultaneamente** `approval_reference` e o `review_key` exato.

## 7. Proteção contra aprovação stale

Uma aprovação nunca autoriza genericamente “a próxima release”.

Ao receber a aprovação, o workflow:

1. coleta novamente as fontes;
2. reconstrói o candidato;
3. recalcula semantic diff;
4. recalcula `review_key`;
5. compara com a chave aprovada.

Se a chave for diferente, a publicação é bloqueada. O estado vira `REVIEW_STALE`, o pacote corrente é persistido e a Issue de revisão é atualizada/substituída para o novo candidato.

Portanto, não é possível revisar A e publicar silenciosamente B.

## 8. Regressões

Antes de apresentar a Issue, o workflow executa:

```text
python -m pytest -q tests
```

O resultado entra no pacote de decisão humano.

Antes de persistir uma publicação bem-sucedida, a suíte completa roda novamente, junto dos gates permanentes das Fases 3–5 e do gate C6.0a.

## 9. Comportamento do schedule

O schedule pode **preparar a revisão do bootstrap** e abrir a Issue, mas continua incapaz de aprovar ou publicar a primeira release sem ação humana.

Depois do bootstrap, o mesmo mecanismo se aplica a qualquer sucessora `REVIEW_REQUIRED`.

Mudanças que permaneçam legitimamente `AUTO_PUBLISH_ALLOWED` continuam fora desse caminho de aprovação humana.

## 10. Ciclo de vida das Issues

- mesmo `review_key`: atualizar a mesma Issue;
- novo `review_key`: fechar a revisão pendente anterior como stale e abrir/atualizar a revisão corrente;
- aprovação válida + publicação: comentar a release publicada e fechar a Issue como concluída;
- aprovação stale: não publicar e manter o novo candidato em revisão.

## 11. Fronteira da Fase 6

C6.0a não migra WordPress, `folha-core` nem H26–H29.

A ordem permanece:

```text
C6.0a — revisão humana acionável
  ↓
correção dos bloqueadores operacionais de pré-flight
  ↓
C6.0 — primeira release v1.2 PUBLISHED validada
  ↓
C6.1 — sanida-fiscais-auto / cache / REST
  ↓
C6.2 — folha-core
  ↓
H26 → H27 → H28 → H29
```

O defeito observado no workflow `Atualizar taxas_bacen.json` #2726 permanece um bloqueador operacional separado a ser corrigido antes do bootstrap C6.0, sem misturar `financial_reference` com a autoridade jurídico-fiscal do contrato H26–H29.
