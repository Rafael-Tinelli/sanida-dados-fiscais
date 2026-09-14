# Fase 6 — C6.0a — Human Review UX

**Data:** 2026-09-14  
**Status:** CONCLUÍDO  
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
- hashes brutos dos snapshots;
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

Ele inclui semântica, payload, diff e identidade material da evidência. Relógios operacionais como `observed_at_utc` ou `last_validated_at_utc` não criam uma revisão nova por conveniência.

### 5.1 Evidência parser-backed

Quando uma fonte possui parser conhecido, o `review_key` continua preso ao **SHA-256 bruto do snapshot + identidade do parser**. Uma alteração dos bytes da fonte parser-backed muda a revisão e permanece fail-closed.

### 5.2 HTML estrutural sem parser

Durante o bootstrap C6.0 real foi demonstrado que páginas oficiais HTML do Planalto, e depois páginas do gov.br, podem devolver bytes diferentes em coletas sucessivas sem qualquer delta nas 32 regras ou nas regressões. Prender a aprovação humana ao hash bruto desses documentos criou um ciclo impossível: revisar A → recoletar → bytes de apresentação mudam → A fica stale → revisar B → repetir.

Para fontes `http_html` **sem parser semântico**, o contrato e a release continuam preservando o snapshot bruto content-addressed e seu SHA-256 exato. Porém, a identidade usada apenas pelo `review_key` passa a ser `html-visible-text-links-v1`, calculada sobre:

- texto humano visível em ordem documental, com whitespace normalizado;
- rótulos e destinos (`href`) dos links;
- exclusão de `script`, `style`, `noscript`, `template`, comentários, atributos de apresentação e diferenças de whitespace/markup.

Isso **não interpreta a norma nem extrai uma regra fiscal**. É normalização de evidência para distinguir conteúdo material de ruído de transporte/apresentação.

Consequências fail-closed:

- alteração de texto visível muda o fingerprint e invalida a aprovação;
- alteração de destino de link muda o fingerprint e invalida a aprovação;
- alteração semântica do candidato muda o `review_key` independentemente da evidência;
- alteração parser-backed continua presa ao hash bruto;
- divergência entre o SHA bruto declarado e os bytes persistidos bloqueia o processo;
- somente churn de markup/transporte com conteúdo material idêntico deixa de rotacionar a aprovação.

O pacote de revisão mantém os hashes brutos para auditoria e registra também `review_evidence_identity`, sem alterar o schema público do Fiscal Contract v1.2 nem a identidade content-addressed da release.

O publicador preserva byte a byte o review packet e o estado `REVIEW_REQUIRED` quando a chave continua igual; o gestor de Issues não faz PATCH quando título, corpo e assignee já correspondem ao candidato. Portanto, uma execução agendada repetida não cria churn Git nem nova notificação apenas pelo avanço do relógio ou por markup volátil sem mudança material.

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
4. recalcula a identidade material da evidência;
5. recalcula `review_key`;
6. compara com a chave aprovada.

Se a chave for diferente, a publicação é bloqueada. O estado vira `REVIEW_STALE`, o pacote corrente é persistido e a Issue de revisão é atualizada/substituída para o novo candidato.

Portanto, não é possível revisar A e publicar silenciosamente B. Ao mesmo tempo, duas representações HTML materialmente equivalentes não são tratadas como candidatos jurídicos diferentes só porque o servidor alterou markup dinâmico.

## 8. Regressões

Antes de apresentar a Issue, o workflow executa:

```text
python -m pytest -q tests
```

O resultado entra no pacote de decisão humano.

A regressão de C6.0a exige adicionalmente que:

- markup/atributos/scripts/comentários diferentes com o mesmo texto e links gerem o mesmo fingerprint;
- mudança de texto visível gere fingerprint diferente;
- mudança de `href` gere fingerprint diferente;
- fonte parser-backed continue usando o SHA bruto;
- mismatch entre snapshot persistido e SHA declarado bloqueie fail-closed.

Antes de persistir uma publicação bem-sucedida, a suíte completa roda novamente, junto dos gates permanentes das Fases 3–5 e do gate C6.0a.

## 9. Comportamento do schedule

O schedule pode **preparar a revisão do bootstrap** e abrir a Issue, mas continua incapaz de aprovar ou publicar a primeira release sem ação humana.

Depois do bootstrap, o mesmo mecanismo se aplica a qualquer sucessora `REVIEW_REQUIRED`.

Mudanças que permaneçam legitimamente `AUTO_PUBLISH_ALLOWED` continuam fora desse caminho de aprovação humana.

## 10. Ciclo de vida das Issues

- mesmo `review_key` e mesmo conteúdo: reutilizar a mesma Issue sem PATCH/noise;
- mesmo `review_key` com apresentação materialmente atualizada, como novo resultado de regressão: atualizar a mesma Issue;
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

O bloqueador histórico observado no workflow `Atualizar taxas_bacen.json` #2726 foi tratado antes do bootstrap C6.0. Ele permanece registrado como evidência de pré-flight, sem misturar `financial_reference` com a autoridade jurídico-fiscal do contrato H26–H29.
