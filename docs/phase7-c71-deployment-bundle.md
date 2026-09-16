# Fase 7 — C7.1 — bundle pré-deploy e rollback

**Data:** 2026-09-16  
**Status:** CONCLUÍDO  
**Escopo:** retirar o último adaptador fiscal temporário de apresentação, materializar um bundle determinístico somente com arquivos canônicos do repositório e provar aplicação/rollback em ambiente temporário antes de qualquer alteração de produção.

## 1. Estado recebido

C7.1 começa depois do fechamento formal da Fase 6. A release fiscal continua sendo resolvida dinamicamente por `releases/fiscal-v1/current.json` e precisa estar `PUBLISHED`, em schema/API `1.2.0`, com 32/32 regras e consumidores H26–H29.

C7.1 não altera o contrato fiscal nem publica uma nova release.

## 2. Retirada de `wordpress_table_shortcodes_v1`

O adaptador temporário criado apenas para os shortcodes informativos `ano_ref`, `inss_tabela` e `irrf_tabela` foi retirado no prazo definido por C6.7.

O **plugin 2.7.0** não contém mais:

- `wordpress_table_shortcodes_v1`;
- `build_shortcode_display_adapter`;
- `shortcode-display-v1`;
- metadados `compatibility_adapter`/`adapter_consumers`;
- constante de prazo `SHORTCODE_DISPLAY_ADAPTER_RETIRE_BY`.

Os três shortcodes informativos passam a ler diretamente a release fiscal canônica já validada pelo plugin:

- `ano_ref` usa a vigência de `irrf.monthly.progressive_table`;
- `inss_tabela` apresenta diretamente `inss.employee.progressive_table`;
- `irrf_tabela` apresenta diretamente `irrf.monthly.progressive_table` e `irrf.simplified_monthly_discount`.

A apresentação continua fail-closed: release ou payload incompatível produz indisponibilidade, não tabela reconstruída por fallback.

## 3. Manifesto do bundle

`docs/phase7-c71-deployment-manifest-v1.json` separa duas classes de arquivo.

### Arquivos gerenciados

O bundle contém exatamente **32 arquivos gerenciados** pelo repositório:

- 8 arquivos do plugin WordPress;
- 8 assets JavaScript das calculadoras;
- H26;
- H27 e suas partes;
- H28 e suas partes;
- H29 e suas partes.

O builder remapeia o diretório-fonte de H27 para o caminho público canônico `/financas/calculadoras/decimo-terceiro/`.

### Dependências pré-existentes

Há exatamente **11 dependências pré-existentes** do site que não são versionadas neste repositório, entre includes PHP compartilhados e CSS de H26/H27.

Elas são declaradas nominalmente no manifesto e não são copiadas nem fingidas pelo bundle. Uma implantação real deve verificar a existência dessas dependências no destino antes de aplicar qualquer arquivo.

## 4. Builder determinístico

`scripts/build_phase7_c71_bundle.py`:

1. valida o manifesto C7.1;
2. resolve `current.json` no momento da construção;
3. exige release PUBLISHED, schema/API 1.2.0, 32 regras e H26–H29;
4. verifica o SHA-256 do artefato fiscal imutável;
5. recusa caminhos inseguros e destinos duplicados;
6. copia somente os arquivos gerenciados;
7. produz `bundle-manifest.json` com origem, destino, SHA-256 e tamanho de cada arquivo;
8. registra a release fiscal que foi usada no gate do bundle.

O bundle possui duas raízes lógicas, não caminhos privados inventados:

```text
site-root/
wordpress-plugin/
```

Na implantação futura, o operador deverá fornecer os destinos reais correspondentes.

## 5. E2E sobre os bytes empacotados

O gate C7.1 executa os runtimes reais de H26, H27, H28 e H29 usando os JavaScript copiados para o bundle, não os caminhos-fonte originais.

Todos precisam consumir a mesma release fiscal canônica e preservar `release_id`.

Os PHP empacotados também passam por lint quando PHP está disponível no runner.

## 6. Prova de rollback

`scripts/simulate_phase7_c71_deployment.py` executa aplicação e **rollback** somente em diretórios temporários.

A simulação prova:

- snapshot prévio dos arquivos gerenciados;
- substituição pelos bytes e hashes do bundle;
- restauração byte a byte dos arquivos que já existiam;
- remoção, no rollback, de arquivo gerenciado que não existia antes da aplicação;
- preservação das dependências pré-existentes;
- preservação de arquivos não gerenciados/sentinelas.

A política é `exact_bytes_snapshot_before_apply`. O rollback nunca usa uma versão “equivalente”: restaura os bytes capturados antes da aplicação.

## 7. Fronteira de segurança

C7.1 **não realiza implantação no HostGator**.

Não há SSH, segredo de hospedagem, caminho de conta ou mutação de produção no checkpoint. O resultado é um artefato pré-deploy reproduzível e uma prova local/CI de que sua aplicação pode ser revertida mecanicamente.

`production_deployed` permanece `false` tanto no manifesto-fonte quanto no manifesto produzido pelo builder.

## 8. Gate permanente

`scripts/validate_phase7_c71_gate.py` verifica no mesmo head:

- release fiscal canônica e íntegra;
- ausência do adaptador expirado;
- plugin 2.7.0 e shortcodes informativos diretos;
- manifesto de 32 arquivos e 11 dependências;
- completude das dependências locais referenciadas pelas páginas;
- construção determinística do bundle;
- ausência de autoridades fiscais legadas no bundle;
- E2E H26–H29 sobre os bytes empacotados;
- PHP lint;
- simulação de apply/rollback;
- ausência de afirmação de deploy de produção.

`tests/test_phase7_c71_gate.py` mantém o checkpoint dentro da suíte normal e o Remake CI constrói o bundle antes de executar o gate.

## 9. Resultado e handoff

Com C7.1 verde, a Fase 7 passa a **EM ANDAMENTO**. A migração já possui um bundle pré-deploy reproduzível e rollback comprovado sem alterar produção.

Próximo checkpoint: **C7.2 — E2E operacional, falhas e recuperação evergreen**. Ele deve testar indisponibilidade, cache/last-good, sucessão de release e recuperação no caminho operacional antes da autorização de implantação controlada.
