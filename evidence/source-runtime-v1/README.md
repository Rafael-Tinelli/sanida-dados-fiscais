# Evidências operacionais de fontes — v1

Este diretório é o backend persistente do runtime de fontes usado pelos workflows de produção `main.yml` e `taxas.yml`. Nesta branch ele está apenas preparado; a criação de evidências reais começa quando a Fase 4 for ativada em `main`.

A árvore real é criada pelas execuções de produção:

```text
evidence/source-runtime-v1/
├── snapshots/
│   └── <source_id>/<sha-prefix>/<sha256>.<ext>
├── candidates/
│   └── <source_id>/<sha-prefix>/<sha256>.json
└── state/
    └── <source_id>.json
```

Fontes preparadas para usar a árvore:

- `RFB_IRRF_TABLE_2026`;
- `INSS_TABLE_2026`;
- `BCB_SELIC_META_SGS_432`;
- `BCB_CDI_DAILY_SGS_12`.

Regras:

- snapshots brutos são imutáveis e content-addressed por SHA-256;
- candidatos normalizados são imutáveis e content-addressed pelo SHA-256 da serialização canônica do payload;
- `state/` mantém o estado operacional materializado mais recente de cada fonte; o histórico Git preserva estados anteriores;
- nenhum fixture ou dado sintético deve ser copiado para esta árvore;
- uma nova publicação de `dados_fiscais.json` só pode ser commitada depois que `scripts/validate_production_evidence_v1.py` comprovar que os hashes de proveniência resolvem para arquivos realmente persistidos aqui;
- uma nova publicação de `taxas_bacen.json` só pode ser commitada depois que `scripts/validate_financial_evidence_v1.py` comprovar a mesma propriedade para Selic/CDI;
- falhas podem atualizar estado/evidência sem autorizar a reescrita do artefato público;
- não há pruning automático na política v1; arquivos únicos observados em produção são retidos.

A política machine-readable está em `docs/phase4-production-persistence-v1.json`. O gate permanente também é validado por `scripts/validate_phase4_foundation.py` no Remake CI.
