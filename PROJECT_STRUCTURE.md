# TrendPulse — Social Media Trend Heatmap

> A real-time intelligence platform that scrapes Instagram, Facebook, TikTok, X, and Reddit, feeds the aggregated signal to an LLM, and renders an hourly-updated heatmap of what's trending across the internet.

---

## 1. Overview

**TrendPulse** ingests public content from five major social networks on a fixed cadence, normalizes it into a unified schema, scores engagement and velocity, sends the distilled signal to an AI model for topic clustering and trend interpretation, and surfaces the results as an interactive geographic + topical heatmap. The full pipeline refreshes **every 60 minutes**.

### 1.1 Core Capabilities

| Capability | Description |
|---|---|
| Multi-platform ingestion | Instagram, Facebook, TikTok, X, Reddit collectors behind a common interface |
| Normalization | Heterogeneous payloads → one canonical `Post` / `Trend` model |
| Trend scoring | Velocity, acceleration, engagement-weighted ranking |
| AI enrichment | LLM clusters posts into topics, labels sentiment, summarizes "why it's trending" |
| Heatmap rendering | Geo heatmap + topic-intensity grid, updated hourly |
| Scheduling | Orchestrated 1-hour refresh with backfill + retry |
| Observability | Metrics, structured logs, scrape-health dashboards |

### 1.2 High-Level Data Flow

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Collectors  │──▶│ Normalizer + │──▶│  AI Enrich   │──▶│   Heatmap    │
│ (5 sources)  │   │   Scoring    │   │   (LLM API)  │   │   API + UI   │
└──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
       │                  │                   │                  │
       ▼                  ▼                   ▼                  ▼
   raw store         posts table         trends table       websocket
   (object/S3)        (Postgres)         (Postgres)          + cache
                            │
                            ▼
                    ┌──────────────┐
                    │  Scheduler   │  ← triggers every 1h, fans out to collectors
                    └──────────────┘
```

---

## 2. Technology Stack

| Layer | Choice | Rationale |
|---|---|---|
| Language (backend) | **Python 3.12** | Rich scraping/data ecosystem, async support |
| Async runtime | `asyncio` + `httpx` | Concurrent collectors without thread overhead |
| Task scheduling | **Celery + Redis** (or APScheduler for simpler deploys) | Reliable hourly jobs, retries, fan-out |
| Database | **PostgreSQL 16** + `TimescaleDB` | Time-series trend history, hypertables |
| Cache / queue | **Redis 7** | Rate-limit buckets, broker, hot-trend cache |
| Object storage | **S3 / MinIO** | Raw scrape payloads, media snapshots |
| AI provider | **Claude API** (Anthropic) | Topic clustering, trend summarization, sentiment |
| API server | **FastAPI** | Typed, async, OpenAPI out of the box |
| Frontend | **Next.js 15 + React 19** | SSR dashboard, streaming updates |
| Map / heatmap | **Mapbox GL JS** + `deck.gl` | GPU heatmap layers |
| Charts | **Recharts / visx** | Topic-intensity, velocity timelines |
| Infra | **Docker + Docker Compose** (dev), **Kubernetes** (prod) | Reproducible, horizontally scalable |
| IaC | **Terraform** | Cloud provisioning |
| CI/CD | **GitHub Actions** | Lint, test, build, deploy |

> **AI integration note:** Use the latest Claude models — `claude-opus-4-8` for high-quality trend synthesis, or `claude-haiku-4-5-20251001` for cheap high-volume classification. Verify model IDs and pricing against the `claude-api` reference before wiring the client.

---

## 3. Directory Structure

```
heatmap/
├── README.md
├── PROJECT_STRUCTURE.md              # this file
├── LICENSE
├── .env.example                      # all required env vars, no secrets
├── .gitignore
├── docker-compose.yml                # local full-stack: db, redis, minio, api, worker, web
├── Makefile                          # make up / test / lint / migrate / seed
│
├── docs/
│   ├── architecture.md               # diagrams, sequence flows
│   ├── data-model.md                 # canonical schemas, ERD
│   ├── scraping-policy.md            # ToS, rate limits, robots, legal/compliance
│   ├── ai-prompts.md                 # prompt templates + versioning
│   └── runbook.md                    # on-call: failures, backfills, key rotation
│
├── backend/
│   ├── pyproject.toml                # deps, tool config (ruff, mypy, pytest)
│   ├── alembic.ini
│   │
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                   # FastAPI app entrypoint
│   │   ├── config.py                 # pydantic-settings, env-driven config
│   │   ├── logging.py                # structured logging setup
│   │   │
│   │   ├── api/                      # HTTP layer
│   │   │   ├── deps.py               # auth, db session, rate-limit deps
│   │   │   ├── routes/
│   │   │   │   ├── trends.py         # GET /trends, /trends/{id}
│   │   │   │   ├── heatmap.py        # GET /heatmap (geo + topic grid)
│   │   │   │   ├── platforms.py      # per-source breakdowns
│   │   │   │   ├── health.py         # liveness/readiness + scrape health
│   │   │   │   └── ws.py             # websocket: live trend pushes
│   │   │   └── schemas/              # pydantic request/response models
│   │   │
│   │   ├── collectors/               # ── SCRAPING LAYER ──
│   │   │   ├── base.py               # BaseCollector ABC: fetch() -> list[RawPost]
│   │   │   ├── registry.py           # maps platform -> collector
│   │   │   ├── instagram.py
│   │   │   ├── facebook.py
│   │   │   ├── tiktok.py
│   │   │   ├── x.py
│   │   │   ├── reddit.py
│   │   │   ├── rate_limiter.py       # token-bucket per platform
│   │   │   ├── proxy_pool.py         # rotating proxies / sessions
│   │   │   └── parsers/              # html/json -> RawPost per platform
│   │   │
│   │   ├── pipeline/                 # ── PROCESSING LAYER ──
│   │   │   ├── normalize.py          # RawPost -> canonical Post
│   │   │   ├── dedupe.py             # cross-platform near-dup detection
│   │   │   ├── scoring.py            # velocity, acceleration, engagement score
│   │   │   ├── geocode.py            # location -> lat/lng / region
│   │   │   └── aggregate.py          # posts -> topic/region buckets
│   │   │
│   │   ├── ai/                       # ── AI ENRICHMENT LAYER ──
│   │   │   ├── client.py            # Claude API client wrapper (retry, backoff)
│   │   │   ├── cluster.py            # group posts -> candidate topics
│   │   │   ├── summarize.py          # "why trending" + label generation
│   │   │   ├── sentiment.py          # sentiment / stance scoring
│   │   │   └── prompts/              # versioned prompt templates (.txt/.jinja)
│   │   │
│   │   ├── models/                   # SQLAlchemy ORM models
│   │   │   ├── post.py
│   │   │   ├── trend.py
│   │   │   ├── platform.py
│   │   │   └── scrape_run.py         # audit of each hourly run
│   │   │
│   │   ├── repositories/             # DB access (no business logic in routes)
│   │   │   ├── posts.py
│   │   │   └── trends.py
│   │   │
│   │   ├── tasks/                    # ── SCHEDULING LAYER ──
│   │   │   ├── celery_app.py         # Celery config + beat schedule (hourly)
│   │   │   ├── orchestrate.py        # the hourly pipeline DAG
│   │   │   ├── collect_tasks.py      # one task per platform (fan-out)
│   │   │   ├── enrich_tasks.py       # AI enrichment task
│   │   │   └── cleanup.py            # retention / raw-data pruning
│   │   │
│   │   └── core/
│   │       ├── cache.py              # Redis helpers
│   │       ├── storage.py            # S3/MinIO raw-payload writer
│   │       └── security.py          # API keys, secret loading
│   │
│   ├── migrations/                   # Alembic versions
│   └── tests/
│       ├── unit/                     # collectors, scoring, normalize
│       ├── integration/              # pipeline end-to-end w/ fixtures
│       ├── fixtures/                 # recorded API/HTML responses
│       └── conftest.py
│
├── frontend/
│   ├── package.json
│   ├── next.config.ts
│   ├── src/
│   │   ├── app/
│   │   │   ├── page.tsx              # main heatmap dashboard
│   │   │   ├── trends/[id]/page.tsx  # trend detail drill-down
│   │   │   └── layout.tsx
│   │   ├── components/
│   │   │   ├── Heatmap.tsx           # deck.gl / Mapbox heatmap layer
│   │   │   ├── TopicGrid.tsx         # topic-intensity matrix
│   │   │   ├── TrendCard.tsx
│   │   │   ├── PlatformFilter.tsx
│   │   │   ├── VelocityChart.tsx
│   │   │   └── LiveTicker.tsx        # websocket-driven updates
│   │   ├── lib/
│   │   │   ├── api.ts                # typed client for backend
│   │   │   └── ws.ts                 # websocket subscription
│   │   ├── hooks/
│   │   └── types/
│   └── public/
│
├── infra/
│   ├── terraform/                    # cloud resources (db, redis, k8s, dns)
│   ├── k8s/                          # deployments, cronjob, services, ingress
│   │   ├── api-deployment.yaml
│   │   ├── worker-deployment.yaml
│   │   ├── beat-cronjob.yaml         # hourly trigger
│   │   └── web-deployment.yaml
│   └── docker/
│       ├── backend.Dockerfile
│       └── frontend.Dockerfile
│
├── scripts/
│   ├── seed_demo.py                  # synthetic data for local dev
│   ├── run_once.py                   # trigger one pipeline run manually
│   └── rotate_keys.py
│
└── .github/
    └── workflows/
        ├── ci.yml                    # lint + typecheck + test
        └── deploy.yml                # build + push + rollout
```

---

## 4. Canonical Data Model

### 4.1 `Post` (normalized, per-item)

```python
Post:
    id: UUID
    platform: Enum[instagram, facebook, tiktok, x, reddit]
    external_id: str                  # platform's own post id
    author_handle: str
    text: str
    media_urls: list[str]
    hashtags: list[str]
    lang: str
    created_at: datetime              # original post time
    collected_at: datetime            # when we scraped it
    metrics: {
        likes: int, comments: int, shares: int,
        views: int | None
    }
    location: { lat: float, lng: float, region: str } | None
    raw_ref: str                      # S3 key to original payload
```

### 4.2 `Trend` (aggregated, AI-enriched)

```python
Trend:
    id: UUID
    run_id: UUID                      # which hourly run produced it
    label: str                        # AI-generated topic name
    summary: str                      # AI "why it's trending"
    sentiment: float                  # -1.0 .. 1.0
    score: float                      # composite ranking score
    velocity: float                   # rate of growth this window
    acceleration: float               # change in velocity vs prev window
    platforms: list[str]              # where it's appearing
    sample_post_ids: list[UUID]
    geo_distribution: dict[region, intensity]
    window_start: datetime
    window_end: datetime
```

### 4.3 `ScrapeRun` (audit / observability)

```python
ScrapeRun:
    id: UUID
    started_at, finished_at: datetime
    status: Enum[running, success, partial, failed]
    per_platform: dict[platform, { fetched: int, errors: int }]
    trends_produced: int
    ai_tokens_used: int
```

---

## 5. The Hourly Pipeline (DAG)

```
[Celery Beat: every 60 min]
        │
        ▼
  orchestrate.run()                         # creates ScrapeRun record
        │
        ├──▶ collect.instagram  ┐
        ├──▶ collect.facebook   │  fan-out, run concurrently,
        ├──▶ collect.tiktok     │  independent rate limits + retries
        ├──▶ collect.x          │
        └──▶ collect.reddit     ┘
                 │  (raw payloads → S3, RawPosts → queue)
                 ▼
        normalize + dedupe + geocode         # → Post rows
                 ▼
        score (velocity / engagement)        # rank candidates
                 ▼
        aggregate into buckets               # topic/region grouping
                 ▼
        ai.cluster → ai.summarize → ai.sentiment   # LLM enrichment
                 ▼
        write Trend rows + update hot cache  # Postgres + Redis
                 ▼
        push to websocket subscribers        # live UI refresh
                 ▼
        finalize ScrapeRun (success/partial)
```

**Resilience rules:**
- Each platform collector is **independent** — one failing source yields a `partial` run, never a total failure.
- Collectors retry with exponential backoff; persistent failures are logged to `ScrapeRun.per_platform`.
- AI enrichment is **batched** and token-budgeted per run; on AI failure, raw scored trends still publish (degraded mode, flagged `ai_enriched=false`).
- Idempotent by `run_id` — a re-run never double-counts.

---

## 6. AI Enrichment Design

| Stage | Input | LLM Task | Output |
|---|---|---|---|
| Cluster | Batch of scored posts | Group semantically related posts into candidate topics | Topic clusters + member post IDs |
| Summarize | One cluster | Generate a short label + "why it's trending" explainer | `label`, `summary` |
| Sentiment | Cluster sample | Aggregate stance/sentiment | `sentiment` ∈ [-1, 1] |

**Practices:**
- Prompts are **versioned files** under `ai/prompts/` — never inline strings — so changes are reviewable and A/B-testable.
- Use **structured output** (tool/JSON schema) so the model returns parseable objects, not prose.
- Apply **prompt caching** for the static instruction block to cut cost across the hourly batch.
- Route by tier: cheap model for clustering/classification at volume, capable model for final summaries.
- Track `ai_tokens_used` per run for cost observability.

---

## 7. Scraping Compliance & Risk

> his is the highest-risk part of the project. Treat it as a first-class concern, not an afterthought.

- **Respect each platform's Terms of Service and `robots.txt`.** Several of these platforms restrict automated scraping. Prefer **official APIs where they exist**:
  - Reddit → official Reddit API
  - X → X API (paid tiers)
  - Instagram/Facebook → Meta Graph API (for content you're authorized to access)
  - TikTok → TikTok Research/Display API
- Only collect **public** data; never authenticate as or impersonate users without authorization.
- Store **rate-limit budgets** per platform in Redis; never burst past documented limits.
- Keep PII handling minimal: store handles/text only as needed, honor deletion, document retention in `docs/scraping-policy.md`.
- Implement **per-platform kill switches** so any source can be disabled without a redeploy.
- Maintain legal review sign-off before production scraping.

---

## 8. API Surface (FastAPI)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/trends` | Ranked trends for the latest run (filter by platform, region) |
| `GET` | `/api/trends/{id}` | Single trend + sample posts |
| `GET` | `/api/heatmap` | Geo intensities + topic grid for map rendering |
| `GET` | `/api/platforms/{name}` | Per-platform trend breakdown |
| `GET` | `/api/runs/latest` | Status of most recent hourly run |
| `WS` | `/ws/trends` | Live push when a new run publishes |
| `GET` | `/healthz` / `/readyz` | Liveness / readiness |

---

## 9. Local Development

```bash
# 1. Bring up infra + services
make up                 # docker-compose: postgres, redis, minio, api, worker, web

# 2. Run migrations + seed demo data
make migrate
make seed

# 3. Trigger one pipeline run by hand (no need to wait an hour)
python scripts/run_once.py

# 4. Open the dashboard
open http://localhost:3000
```

**Required env (`.env`):**
```
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
S3_ENDPOINT=...   S3_BUCKET=...
ANTHROPIC_API_KEY=...
# Per-platform credentials / API keys / proxy config
INSTAGRAM_..., FACEBOOK_..., TIKTOK_..., X_..., REDDIT_...
```

---

## 10. Observability & Operations

- **Metrics** (Prometheus): posts/run/platform, scrape error rate, pipeline duration, AI tokens & cost, websocket connections.
- **Logs**: structured JSON, correlated by `run_id`.
- **Alerts**: pipeline failed, a platform's error rate > threshold, run duration > 50 min (risking the next hour), AI cost spike.
- **Dashboards**: scrape health per platform, trend-publish latency, top trends over time.
- **Runbook** (`docs/runbook.md`): how to backfill a missed hour, rotate keys, disable a failing collector, replay from raw S3 payloads.

---

## 11. Roadmap

| Phase | Scope |
|---|---|
| **M1 — Skeleton** | Repo, Docker, DB schema, one collector (Reddit via API), manual run |
| **M2 — Pipeline** | Normalize + scoring + all 5 collectors behind the common interface |
| **M3 — AI** | Clustering, summarization, sentiment via Claude API |
| **M4 — Heatmap UI** | Next.js dashboard, Mapbox heatmap, topic grid |
| **M5 — Scheduling** | Celery beat hourly orchestration, retries, partial-run handling |
| **M6 — Hardening** | Observability, alerts, compliance review, load testing |

---

*Last updated: 2026-06-14*
