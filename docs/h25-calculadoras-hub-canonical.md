# H25 — central canônica de calculadoras

**Status:** fonte canônica versionada e bundle estendido pronto, sem deployment em produção.

## Fonte canônica

A página pública `/financas/calculadoras/` passa a ter origem versionada em:

`consumers/frontend/calculadoras/index.php`

O arquivo foi trazido da fotografia efetivamente publicada e harmonizado apenas nos pontos necessários para a família H26–H29:

- H26 recebe descrição e anchor explícitos para `salário bruto → líquido`, INSS, IRRF e dependentes;
- H29 é descrito como `estimativa parcial`;
- FGTS, aviso prévio e seguro-desemprego aparecem somente como itens fora do cálculo automático de H29, não como funcionalidades;
- os quatro destinos H26, H27, H28 e H29 continuam explícitos e a central permanece uma página distribuidora.

## Deployment sem reescrever a história

`docs/phase7-c71-deployment-manifest-v1.json` permanece imutável como baseline histórico de 32 arquivos gerenciados.

A extensão está declarada em:

`docs/phase7-c71-deployment-manifest-v2.json`

Ela adiciona exclusivamente:

`consumers/frontend/calculadoras/index.php -> site_root:financas/calculadoras/index.php`

O bundle resultante contém 33 arquivos gerenciados. Ele é construído por:

```bash
python scripts/build_phase7_c71_bundle_v2.py --output-dir /tmp/c71-h25-bundle
```

O builder reaproveita integralmente o bundle C7.1 validado e acrescenta H25 com SHA-256, tamanho, origem e destino registrados no `bundle-manifest.json`.

## Rollback

O teste `tests/test_h25_hub_canonical_r1.py` executa a simulação C7.1 sobre os 33 arquivos e exige:

- aplicação verificada;
- restauração byte a byte de arquivos preexistentes;
- remoção de arquivos introduzidos pelo bundle quando necessário;
- preservação das dependências preexistentes;
- preservação de arquivos não gerenciados;
- `production_mutated=false`.

Nenhum HostGator é alterado por esta etapa.
