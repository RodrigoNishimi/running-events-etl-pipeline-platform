# Corridas ETL

Pipeline de dados (ETL) que agrega eventos de corrida de rua de múltiplas
organizadoras (Yescom, Iguana Sports, Live!Run, Ativo.com, Ticket Sports, ...)
em uma base canônica única para a plataforma de descoberta.

## Arquitetura (camadas medallion)

```
Fontes  →  [EXTRACT]   →  Bronze/Raw   →  [TRANSFORM]  →  Silver        →  [RESOLVE+ENRICH]  →  Gold          →  Serving
(sites)    conectores     payload bruto    parse+valida    1 reg/fonte       dedup + geocode      evento único     Postgres + busca
```

- **Bronze** (`storage/raw.py`): payload exatamente como veio, com metadados
  (fonte, url, hash, timestamp). Nunca é descartado — permite reprocessar sem
  re-crawlear.
- **Silver**: registro parseado e normalizado *por fonte* (`SourceEventRecord`).
- **Gold**: evento canônico deduplicado (`CanonicalEvent`), gravado no Postgres.
- **Serving**: Postgres + PostGIS (verdade) e, na Fase 1, Meilisearch (busca).

## Estrutura

```
sql/001_schema.sql              DDL do banco (Postgres + PostGIS)
src/corridas_etl/
  config.py                     configuração via variáveis de ambiente
  models.py                     schema canônico (Pydantic)
  db.py                         conexão + upsert idempotente
  storage/raw.py                camada Bronze (raw)
  connectors/base.py            interface comum de conector
  connectors/registry.py        registro de conectores disponíveis
  connectors/exemplo_ativo.py   conector de exemplo (esqueleto)
  pipeline/run.py               orquestração Fase 0 (bronze→silver→gold)
  utils/distances.py            normalização de distâncias
  utils/text.py                 normalização de texto / slug
tests/                          testes de unidade
```

## Rodando localmente (Fase 0)

```bash
# 1. Infra local (Postgres+PostGIS)
docker compose up -d

# 2. Ambiente Python
python -m venv .venv && . .venv/Scripts/activate    # Windows
pip install -e ".[dev]"

# 3. Configuração
cp .env.example .env             # ajuste se necessário

# 4. Criar o schema
psql "$DATABASE_URL" -f sql/001_schema.sql

# 5. Rodar o pipeline para uma fonte
python -m corridas_etl.pipeline.run --source exemplo_ativo
```

## Fontes mapeadas (2026-07-19)

| Fonte | Tipo | Volume | Acesso | Status |
|---|---|---|---|---|
| **Ticket Sports** | Agregador (centenas de organizadoras) | ~770 corridas ativas | Páginas públicas + JSON-LD `SportsEvent`; descoberta agentic (Playwright) no calendário. robots.txt proíbe `/api/` — respeitado. | ✅ conector `ticketsports` |
| **Iguana Sports** | Organizadora grande (Nike SP City Marathon, Run The Bridge) | ~5-7 eventos premium | Shopify `products.json` (distâncias estruturadas, preços, disponibilidade) | ✅ conector `iguanasports` |
| **Yescom** | Organizadora grande (São Silvestre, Maratona de SP) | ~12 eventos grandes | HTML estático, páginas custom por evento | 🔜 planejado |
| **Ativo.com** | Agregador/portal | ? | Calendário JS-rendered; exige Playwright | 🔜 planejado |
| **Live!Run** | Organizadora | ? | não investigado | — |

Limitações conhecidas (candidatas a enriquecimento na Fase 1):
- Ticket Sports: distâncias só quando presentes no título (o conteúdo completo é client-side via a API proibida pelo robots).
- Iguana: data e cidade não vêm no JSON de produto (enriquecer da página do evento).
- Dedup real observada: "Run The Bridge 2026" vs "Brooks Run The Bridge 2026" (mesmo evento, prefixo de patrocinador) — caso-alvo do entity resolution da Fase 1.

## Roadmap

- **Fase 0 (aqui):** 1 conector, schema canônico, upsert idempotente, raw storage.
- **Fase 1:** 3–5 conectores, deduplicação com revisão manual, geocoding, Meilisearch.
- **Fase 2:** orquestrador (Prefect/Dagster), incremental por hash, testes de qualidade, alertas.
