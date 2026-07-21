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
- **Serving**: Postgres — fonte da verdade e também a busca facetada do app
  (pg_trgm/unaccent/PostGIS, ver `sql/008_search.sql`).

## Estrutura

```
sql/001_schema.sql              DDL do banco (não exige PostGIS)
sql/002_postgis.sql             upgrade geoespacial opcional (PostGIS)
sql/003_dedup.sql               fila de revisão do entity resolution
sql/004_alias.sql               aliases de merge (chave absorvida → sobrevivente)
sql/005_geocode.sql             cache de geocoding (Nominatim)
sql/006_country.sql             eventos internacionais (country ISO-2)
sql/007_changes.sql             outbox de mudanças de preço/status (trigger)
sql/008_search.sql              busca textual (pg_trgm + unaccent + índice GIN)
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
psql "$DATABASE_URL" -f sql/008_search.sql   # busca do app (pg_trgm + unaccent)
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
python -m corridas_etl.pipeline.notify                                 # feed de mudanças
python -m corridas_etl.pipeline.notify --json --mark-sent              # app consome + despacha
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
| **Running Land** | Organizadora/plataforma (Blue Run, Eco Run, Bota Pra Correr) | ~116 eventos futuros | Magento headless: GraphQL público (`getEventCategoryFull` + mapa de atributos). robots.txt permite; WAF exige UA de browser. Distâncias estruturadas. | ✅ `runningland` |
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

Calibração com dados reais (triagem de 62 pares em 2026-07-19):
- ordinais de edição/anos/números romanos são removidos do nome só para o
  score (a `canonical_key` não muda); quando as cidades são iguais, os tokens
  da cidade também saem (evita que "TECH RUN CURITIBA" × "Eco Run Curitiba"
  pareçam iguais); quando diferem, ficam (distinguem "Meia de Jundiaí" de
  "Meia de SBC") e aplicam penalidade multiplicativa;
- **piso de nome** (0.70): nome é a âncora de identidade — abaixo disso é
  distinto, mesmo com dia/cidade iguais; auto-merge exige nome ≥ 0.85;
- blend 70/30 de token_set/token_sort (subconjunto = padrão de patrocinador;
  os subconjuntos perigosos — kids, caminhada, circuitinho — têm guard).

Merges gravam um **alias** (`event_alias`): a chave do evento absorvido passa a
rotear o upsert para o sobrevivente, então a recarga da fonte não recria o
duplicado. O upsert usa COALESCE — fontes preenchem lacunas, nunca apagam dado
enriquecido.

## Busca facetada (direto no Postgres)

A busca do app roda direto nas tabelas deste schema — não há índice externo
nem etapa de reindexação: o que o pipeline grava já é pesquisável. O
`sql/008_search.sql` prepara o banco:

- **`pg_trgm`** — matching por trigramas: tolerância a erro de digitação
  (`word_similarity`) e substring match indexável (`LIKE`);
- **`unaccent`** (via wrapper imutável `f_unaccent`) — "sao paulo" encontra
  "São Paulo";
- **índice GIN** (`event_search_trgm_idx`, `gin_trgm_ops`) sobre a expressão
  nome + cidade + UF normalizada — a query do app usa a mesma expressão para
  o planner aproveitá-lo;
- geo ("perto de mim") é haversine em SQL puro sobre `latitude`/`longitude`
  — funciona em qualquer Postgres, sem exigir PostGIS (o
  `sql/002_postgis.sql` segue opcional).

Filtros/facetas ficam em SQL comum (`WHERE` + `GROUP BY`) na query do app
(`RunnersHub/src/lib/search.ts`): estado, cidade, distâncias, mês,
status de inscrição, preço. Eventos passados ficam fora da busca via
`start_at >= now()` — sem necessidade do antigo `--future-only`.

## Notificações de mudança (preço/status)

O pipeline **detecta e registra** mudanças de preço e status de inscrição num
outbox (`event_change`), alimentado por um trigger `AFTER UPDATE` no `event` —
qualquer caminho de escrita (upsert, merge) é capturado. A entrega ao usuário
(e-mail/push) é do serviço de notificação do app, que consome o feed:

```bash
python -m corridas_etl.pipeline.notify              # feed legível
python -m corridas_etl.pipeline.notify --json       # estruturado (p/ o app)
python -m corridas_etl.pipeline.notify --mark-sent  # marca despachado (não reenvia)
```

Mensagens prontas: "Inscrições abriram para X", "Y esgotou", "Preço de Z caiu
de R$A para R$B". Detalhes de design:
- **Preço por fonte**: cada `source_record` guarda seu preço; `event.price` é
  derivado como o **menor** entre as fontes — determinístico, então rodadas
  repetidas não geram mudanças espúrias (evita "piscar" quando o mesmo evento
  tem vários produtos/fontes com preços diferentes). Só uma mudança real do
  mínimo entra no feed.
- **Sem ruído inicial**: mudança de preço só é registrada quando havia preço
  antes (null → X é população, não notificação).
- **Status sem flapping**: `unknown` nunca sobrescreve um status conhecido.
- Preço capturado de Iguana (variantes) e Running Land (preço/promoção);
  filtrável e ordenável na busca do app (`price`, `has_price`).

## Roadmap

- **Fase 0 — feita:** schema canônico, upsert idempotente, raw storage, 1ª fonte.
- **Fase 1 — feita:** 3 conectores reais, dedup com fila de revisão, enriquecimento.
- **Fase 2 — feita:** runner diário com isolamento de falhas, quality checks,
  aliases de merge, geocoding (Nominatim), incremental por hash, suporte a
  eventos internacionais (country ISO-2), conector Ativo.com, persistência de
  organizadoras e busca facetada (hoje direto no Postgres — `sql/008_search.sql`;
  o índice Meilisearch original foi aposentado).
- **Fase 3 — parcial:** notificações de mudança de preço/status (outbox
  `event_change` via trigger + feed `pipeline.notify`).
  Falta: parcerias com feed oficial, conector Live!Run, dashboard de saúde,
  orquestrador (Prefect/Dagster) quando o número de fontes justificar.
