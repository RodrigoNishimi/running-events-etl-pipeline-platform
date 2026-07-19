# Corridas ETL

Pipeline de dados (ETL) que agrega eventos de corrida de rua de múltiplas
organizadoras (Yescom, Iguana Sports, Ticket Sports, ...) em uma base canônica
única para a plataforma de descoberta.

## Arquitetura (camadas medallion)

```
Fontes  →  [EXTRACT]   →  Bronze/Raw   →  [TRANSFORM]  →  Silver        →  [RESOLVE+ENRICH]  →  Gold          →  Serving
(sites)    conectores     payload bruto    parse+valida    1 reg/fonte       dedup + enrich       evento único     Postgres + busca
```

- **Bronze** (`storage/raw.py`): payload exatamente como veio, com metadados
  (fonte, url, hash, timestamp). Nunca é descartado — permite reprocessar sem
  re-crawlear.
- **Silver**: registro parseado e normalizado *por fonte* (`SourceEventRecord`).
- **Gold**: evento canônico deduplicado (`CanonicalEvent`), gravado no Postgres.
- **Serving**: Postgres (verdade); Meilisearch (busca facetada) planejado.

## Estrutura

```
sql/001_schema.sql              DDL do banco (não exige PostGIS)
sql/002_postgis.sql             upgrade geoespacial opcional (PostGIS)
sql/003_dedup.sql               fila de revisão do entity resolution
sql/004_alias.sql               aliases de merge (chave absorvida → sobrevivente)
sql/005_geocode.sql             cache de geocoding (Nominatim)
src/corridas_etl/
  config.py                     configuração via variáveis de ambiente
  models.py                     schema canônico (Pydantic)
  db.py                         conexão + upsert idempotente
  storage/raw.py                camada Bronze (raw)
  connectors/                   um conector por fonte (base + registry)
  resolution/matcher.py         scoring de duplicatas (lógica pura)
  pipeline/run.py               extract+load de UMA fonte
  pipeline/dedup.py             entity resolution + fila de revisão
  pipeline/enrich.py            preenchimento de lacunas (Playwright)
  pipeline/quality.py           checks de saúde/anomalias/cobertura
  pipeline/daily.py             runner completo (todas as etapas)
  utils/                        normalização de texto/distâncias, render
tests/                          testes de unidade (fixtures reais)
```

## Rodando localmente

```bash
# 1. Banco: docker compose up -d  OU um Postgres local (PostGIS é opcional)

# 2. Ambiente Python
python -m venv .venv && . .venv/Scripts/activate    # Windows
pip install -e ".[dev]"
playwright install chromium         # p/ descoberta agentic e enriquecimento

# 3. Configuração
cp .env.example .env                # ajuste DATABASE_URL

# 4. Schema
psql "$DATABASE_URL" -f sql/001_schema.sql -f sql/003_dedup.sql
# opcional, se o servidor tiver PostGIS: -f sql/002_postgis.sql

# 5. Pipeline completo (ou etapas individuais, abaixo)
python -m corridas_etl.pipeline.daily --limit 20
```

### Etapas individuais

```bash
python -m corridas_etl.pipeline.run --source ticketsports --limit 30   # 1 fonte
python -m corridas_etl.pipeline.run --source iguanasports --dry-run
python -m corridas_etl.pipeline.dedup                                  # dedup
python -m corridas_etl.pipeline.dedup --review                         # fila
python -m corridas_etl.pipeline.dedup --resolve 3 merge                # decisão
python -m corridas_etl.pipeline.enrich --step iguana-dates             # lacunas
python -m corridas_etl.pipeline.enrich --step ticketsports-distances --limit 10
python -m corridas_etl.pipeline.enrich --step geocode                  # lat/long
python -m corridas_etl.serving.search                                  # reindexa busca
python -m corridas_etl.serving.search --dry-run --future-only          # docs sem servidor
python -m corridas_etl.pipeline.quality                                # saúde
```

### Agendamento (produção)

O `daily.py` retorna exit code 1 quando alguma fonte falha ou a qualidade acusa
crítico. Agende uma execução diária; no Windows:

```powershell
schtasks /Create /TN "CorridasETL" /SC DAILY /ST 05:00 `
  /TR "C:\caminho\.venv\Scripts\python.exe -m corridas_etl.pipeline.daily"
```

(Em Linux/cloud: cron/systemd timer, ou migrar para Prefect/Dagster quando o
número de fontes justificar.)

## Fontes mapeadas (2026-07-19)

| Fonte | Tipo | Volume | Acesso | Status |
|---|---|---|---|---|
| **Ticket Sports** | Agregador (centenas de organizadoras) | ~770 corridas ativas | Páginas públicas + JSON-LD `SportsEvent`; descoberta agentic (Playwright) no calendário. robots.txt proíbe `/api/` — respeitado. | ✅ `ticketsports` |
| **Iguana Sports** | Organizadora grande (Nike SP City Marathon, Run The Bridge) | ~7 eventos premium | Shopify: `products.json` (catálogo) + `/products/<handle>.js` (detalhe c/ disponibilidade) | ✅ `iguanasports` |
| **Yescom** | Organizadora grande (São Silvestre, Maratona de SP) | ~13 eventos grandes | HTML estático, microsite custom por evento; parse best-effort | ✅ `yescom` |
| **Ativo.com** | Agregador/portal | ~8 corridas futuras | `/eventos.json` (HTTP puro, sem Playwright). ⚠️ O dump é quase todo **arquivo histórico** (eventos desde 2015); filtramos para futuros. | ✅ `ativo` |
| **Live!Run** | Organizadora | ? | não investigado | — |

Limitações conhecidas:
- Ticket Sports: distâncias só do título no parse; o resto vem do passo de
  enriquecimento (renderização da página, limitado por execução).
- Iguana: data/cidade vêm do enriquecimento (cards da homepage); eventos fora
  do carrossel ficam sem data até aparecerem lá.
- Yescom: microsites heterogêneos — data/local são heurísticos (moda das
  datas do ano do evento; mapa de dicas por slug).

## Entity resolution (dedup)

`resolution/matcher.py` pontua pares por evidência disponível (nome fuzzy,
data, local, distâncias) e aplica *guards* (anos diferentes, marcadores
kids/caminhada/virtual) que vetam merge automático. Score ≥ 0.88 mescla
sozinho; 0.70–0.88 vai para a fila `dedup_review` (CLI `--review`/`--resolve`).
Caso real resolvido: "Run The Bridge 2026" + "Brooks Run The Bridge 2026" →
um evento com 2 fontes.

Calibração com dados reais (triagem de 39 pares em 2026-07-19):
- ordinais de edição/anos/números romanos são removidos do nome só para o
  score (a `canonical_key` não muda);
- cidades conhecidas e diferentes aplicam penalidade multiplicativa — nomes
  genéricos iguais em cidades vizinhas caem para "distinto", nomes quase
  idênticos ainda chegam à revisão.

Merges gravam um **alias** (`event_alias`): a chave do evento absorvido passa a
rotear o upsert para o sobrevivente, então a recarga da fonte não recria o
duplicado. O upsert usa COALESCE — fontes preenchem lacunas, nunca apagam dado
enriquecido.

## Busca facetada (Meilisearch)

A camada de serving indexa os eventos no Meilisearch (`serving/search.py`).
O Postgres continua sendo a verdade; o índice é derivado e reconstruível.

```bash
docker compose up -d search           # sobe o Meilisearch (porta 7700)
export MEILI_MASTER_KEY=dev_master_key
python -m corridas_etl.serving.search --future-only
```

Facetas expostas ao app: `state`, `city`, `country`, `distances_km`, `month`,
`year`, `registration_status`, `sources` e `_geo` (para "corridas perto de mim").
Buscável por nome/cidade/organizadora. Exemplos (cliente `meilisearch`):

```python
idx.search("maratona", {"filter": ["state = SP", "distances_km = 42.195"],
                        "sort": ["start_timestamp:asc"]})
idx.search("", {"filter": ["_geoRadius(-23.55, -46.63, 50000)"],   # raio 50km
               "sort": ["_geoPoint(-23.55,-46.63):asc"]})
idx.search("", {"facets": ["state", "month_name"]})                 # distribuição
```

Verificado end-to-end contra os 796 eventos futuros: busca textual, filtros
facetados, distribuição e geo-radius funcionando.

## Roadmap

- **Fase 0 — feita:** schema canônico, upsert idempotente, raw storage, 1ª fonte.
- **Fase 1 — feita:** 3 conectores reais, dedup com fila de revisão, enriquecimento.
- **Fase 2 — feita:** runner diário com isolamento de falhas, quality checks,
  aliases de merge, geocoding (Nominatim), incremental por hash, suporte a
  eventos internacionais (country ISO-2), conector Ativo.com, persistência de
  organizadoras e índice de busca facetada (Meilisearch).
  Falta: dashboard de saúde, orquestrador (Prefect/Dagster) quando o número de
  fontes justificar.
- **Fase 3:** notificações de mudança (preço/status), parcerias com feed oficial.
