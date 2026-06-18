# TrendPulse — Social Media Trend Heatmap

> A real-time intelligence platform that pulls signal from Instagram, Facebook, TikTok, X, and Reddit — via official APIs where available, scraping as fallback — feeds it to an LLM, and renders an hourly-updated heatmap of what's trending across the internet.

---

## 1. Overview

**TrendPulse** ingests public content from five major social networks on a fixed cadence, normalizes it into a unified schema, scores engagement and velocity, sends the distilled signal to an AI model for topic clustering and trend interpretation, and surfaces the results as an interactive geographic + topical heatmap. The full pipeline refreshes **every 60 minutes**.

### 1.1 Core Capabilities

| Capability | Description |
|---|---|
| Multi-platform ingestion | Instagram, Facebook, TikTok, X, Reddit — API-first, scraper fallback, common interface |
| Normalization | Heterogeneous payloads → one canonical `Post` / `Trend` model |
| Trend scoring | Velocity, acceleration, engagement-weighted ranking |
| AI enrichment | LLM clusters posts into topics, labels sentiment, summarizes "why it's trending" |
| Heatmap rendering | Geo heatmap + topic-intensity grid, updated hourly |
| Scheduling | Celery + Valkey beat schedule, hourly refresh with backfill + retry |
| Observability | Metrics, structured logs, scrape-health dashboards |

### 1.2 High-Level Data Flow

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Collectors  │──▶│ Normalizer + │──▶│  AI Enrich   │──▶│   Heatmap    │
│ (5 sources)  │   │   Scoring    │   │   (LLM API)  │   │   API + UI   │
│ API > scrape │   │              │   │              │   │              │
└──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
       │                  │                   │                  │
       ▼                  ▼                   ▼                  ▼
   raw store         posts table         trends table       Next.js UI
  (filesystem/S3)     (Postgres)         (Postgres)        (+ Valkey cache)
                            │
                            ▼
                    ┌──────────────┐
                    │ Celery Beat  │  ← triggers every 1h, fans out to collectors
                    │  (Valkey)    │
                    └──────────────┘

        ┌─────────────────────────────────────────┐
        │  Go middleware — PLANNED, not yet placed  │
        │  Candidates: API gateway in front of      │
        │  Django, or a dedicated real-time/        │
        │  fan-out service. Decide once the         │
        │  bottleneck it's solving is clear.        │
        └─────────────────────────────────────────┘
```

---

## 2. Technology Stack

| Layer | Choice | Status | Rationale |
|---|---|---|---|
| Backend / API | **Django 6 + DRF** | ✅ working | Mature ORM, admin, batteries-included for a solo-dev pace |
| Async / real-time | **Django Channels** | planned | WebSocket push for live trend updates, consistent with your Televisit24 pattern |
| Task scheduling | **Celery + Valkey** | planned | Hourly fan-out, retries; Valkey as the broker/result backend |
| Database | **PostgreSQL 16** | ✅ working | Relational integrity for posts/trends, JSONB for flexible payloads |
| Cache / broker | **Valkey** (Redis-compatible) | ✅ working (as Redis today) | Drop-in Redis replacement, open license, same client libraries |
| AI provider | **Claude API** (Anthropic) | planned | Topic clustering, trend summarization, sentiment |
| Frontend | **Next.js + React** | planned | SSR dashboard, streaming updates |
| Middleware | **Go** | **planned — placement TBD** | Performance-sensitive layer once the actual bottleneck (gateway vs. fan-out vs. something else) is identified |
| Map / heatmap | **Mapbox GL JS** + `deck.gl` | planned | GPU heatmap layers |
| Charts | **Recharts** | planned | Topic-intensity, velocity timelines |
| Infra (dev) | **Docker Compose** | ✅ working (db, redis) | One command to bring up the full stack locally |
| Infra (prod) | TBD | — | Decide once you're past MVP; Cloudflare tunnel + systemd (your usual pattern) is a reasonable default |

> **AI integration note:** when you wire up the Claude API client, check current model IDs/pricing rather than trusting anything cached in a doc or in a model's training data — verify against Anthropic's docs at integration time.

### 2.1 Valkey, not Redis — naming note

Your `.env` and `docker-compose.yml` still say `redis`/`REDIS_URL`. That's fine functionally (Valkey speaks the Redis protocol, same client libraries work unchanged), but worth standardizing now before more code depends on the name:
- Keep `REDIS_URL` as the env var name if you want zero code churn (Django, Celery, `redis-py` don't care what you call the env var).
- Swap the **image** in `docker-compose.yml` from `redis:7` to `valkey/valkey:8` — this is the only required change to actually run Valkey instead of Redis.
- Optional: rename the env var to `VALKEY_URL` later if you want the naming to be honest end-to-end. Not urgent.

### 2.2 Data acquisition policy: API-first, scrape as fallback

For each platform, check official API availability before writing a scraper:

| Platform | Official API | Fallback |
|---|---|---|
| Reddit | Reddit API (free tier available) | scraper if quota too tight |
| X | X API (paid tiers) | scraper if cost-prohibitive |
| Instagram / Facebook | Meta Graph API (requires app review for most data) | scraper for public-only data |
| TikTok | TikTok Research/Display API (access-gated) | scraper likely needed short-term |

Build collectors behind a single interface (`fetch() -> list[RawPost]`) so swapping a scraper for an API later — or vice versa, if an API gets rate-limited or revoked — doesn't touch anything downstream. Treat this as a per-platform decision revisited over time, not a one-time choice.

---

## 3. Directory Structure

```
heatmap/
├── README.md
├── PROJECT_STRUCTURE.md              # this file
├── LICENSE
├── .env.example                      # all required env vars, no secrets
├── .gitignore
├── docker-compose.yml                # local full-stack: db, valkey, backend, frontend
├── Makefile                          # make up / test / lint / migrate / seed
│
├── docs/
│   ├── architecture.md               # diagrams, sequence flows
│   ├── data-model.md                 # canonical schemas, ERD
│   ├── acquisition-policy.md         # per-platform: API vs scrape, ToS, rate limits
│   ├── ai-prompts.md                 # prompt templates + versioning
│   └── runbook.md                    # on-call: failures, backfills, key rotation
│
├── backend/                          # Django project (existing)
│   ├── manage.py
│   ├── requirements.txt
│   │
│   ├── config/                       # Django project settings (existing)
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   ├── urls.py
│   │   ├── asgi.py                   # already present — Channels-ready
│   │   ├── wsgi.py
│   │   └── celery.py                 # NEW: Celery app + beat schedule
│   │
│   ├── api/                          # existing app — DRF routes/serializers
│   │   ├── __init__.py
│   │   ├── models.py                 # Post, Trend, ScrapeRun
│   │   ├── serializers.py
│   │   ├── views.py                  # /trends, /heatmap, /platforms, /runs
│   │   ├── urls.py
│   │   ├── admin.py
│   │   └── migrations/
│   │
│   ├── collectors/                   # NEW app — ── ACQUISITION LAYER ──
│   │   ├── __init__.py
│   │   ├── base.py                   # BaseCollector ABC: fetch() -> list[RawPost]
│   │   ├── registry.py               # maps platform -> collector impl
│   │   ├── instagram.py
│   │   ├── facebook.py
│   │   ├── tiktok.py
│   │   ├── x.py
│   │   ├── reddit.py
│   │   ├── rate_limiter.py           # token-bucket per platform, backed by Valkey
│   │   ├── proxy_pool.py             # only needed for scraper-mode collectors
│   │   └── parsers/                  # html/json -> RawPost per platform
│   │
│   ├── pipeline/                     # NEW app — ── PROCESSING LAYER ──
│   │   ├── __init__.py
│   │   ├── normalize.py              # RawPost -> canonical Post
│   │   ├── dedupe.py                 # cross-platform near-dup detection
│   │   ├── scoring.py                # velocity, acceleration, engagement score
│   │   ├── geocode.py                # location -> lat/lng / region
│   │   └── aggregate.py              # posts -> topic/region buckets
│   │
│   ├── ai/                           # NEW app — ── AI ENRICHMENT LAYER ──
│   │   ├── __init__.py
│   │   ├── client.py                 # Claude API client wrapper (retry, backoff)
│   │   ├── cluster.py                # group posts -> candidate topics
│   │   ├── summarize.py              # "why trending" + label generation
│   │   ├── sentiment.py              # sentiment / stance scoring
│   │   └── prompts/                  # versioned prompt templates (.txt/.jinja)
│   │
│   ├── tasks/                        # NEW app — ── SCHEDULING LAYER (Celery) ──
│   │   ├── __init__.py
│   │   ├── orchestrate.py            # the hourly pipeline, as a Celery chain/chord
│   │   ├── collect_tasks.py          # one task per platform (fan-out)
│   │   ├── enrich_tasks.py           # AI enrichment task
│   │   └── cleanup.py                # retention / raw-data pruning
│   │
│   ├── core/                         # NEW app — shared helpers
│   │   ├── __init__.py
│   │   ├── cache.py                  # Valkey helpers (via redis-py / django-redis)
│   │   ├── storage.py                # raw-payload writer (local FS now, S3-compatible later)
│   │   └── security.py               # API keys, secret loading
│   │
│   └── tests/
│       ├── unit/                     # collectors, scoring, normalize
│       ├── integration/              # pipeline end-to-end w/ fixtures
│       ├── fixtures/                 # recorded API/HTML responses
│       └── conftest.py
│
├── middleware/                       # Go — PLANNED, placement not yet decided
│   └── README.md                     # notes/decision log until scope is picked
│
├── frontend/                         # Next.js (planned)
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
│   ├── docker/
│   │   ├── backend.Dockerfile
│   │   └── frontend.Dockerfile
│   └── prod/                         # TBD: likely Cloudflare tunnel + systemd, decide later
│
├── scripts/
│   ├── seed_demo.py                  # synthetic data for local dev
│   ├── run_once.py                   # trigger one pipeline run manually
│   └── rotate_keys.py
│
└── .github/
    └── workflows/
        └── ci.yml                    # lint + test
```

**Why apps instead of one big `app/` tree:** Django's convention is small, focused apps (`collectors`, `pipeline`, `ai`, `tasks`, `core`) registered in `INSTALLED_APPS`, each with its own `models.py`/`migrations` if it needs them. `collectors` and `pipeline` likely don't need their own models — they operate on `api`'s `Post`/`Trend` models — but keeping the *code* separated by responsibility still pays off once the project grows past a few files.

---

## 4. Canonical Data Model

These will live in `backend/api/models.py` as standard Django models (shown here in simplified form):

### 4.1 `Post` (normalized, per-item)

```python
class Post(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    platform = models.CharField(choices=Platform.choices)   # instagram, facebook, tiktok, x, reddit
    external_id = models.CharField()                        # platform's own post id
    author_handle = models.CharField()
    text = models.TextField()
    media_urls = models.JSONField(default=list)
    hashtags = models.JSONField(default=list)
    lang = models.CharField(max_length=8)
    created_at = models.DateTimeField()                     # original post time
    collected_at = models.DateTimeField(auto_now_add=True)  # when we acquired it
    likes = models.IntegerField(default=0)
    comments = models.IntegerField(default=0)
    shares = models.IntegerField(default=0)
    views = models.IntegerField(null=True)
    lat = models.FloatField(null=True)
    lng = models.FloatField(null=True)
    region = models.CharField(null=True)
    raw_ref = models.CharField(null=True)                   # path/key to original payload
```

### 4.2 `Trend` (aggregated, AI-enriched)

```python
class Trend(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    run = models.ForeignKey('ScrapeRun', on_delete=models.CASCADE)
    label = models.CharField()                # AI-generated topic name
    summary = models.TextField()              # AI "why it's trending"
    sentiment = models.FloatField()           # -1.0 .. 1.0
    score = models.FloatField()               # composite ranking score
    velocity = models.FloatField()            # rate of growth this window
    acceleration = models.FloatField()        # change in velocity vs prev window
    platforms = models.JSONField(default=list)
    sample_post_ids = models.JSONField(default=list)
    geo_distribution = models.JSONField(default=dict)   # {region: intensity}
    window_start = models.DateTimeField()
    window_end = models.DateTimeField()
    ai_enriched = models.BooleanField(default=True)      # false if AI step degraded
```

### 4.3 `ScrapeRun` (audit / observability)

```python
class ScrapeRun(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True)
    status = models.CharField(choices=Status.choices)   # running, success, partial, failed
    per_platform = models.JSONField(default=dict)        # {platform: {fetched, errors}}
    trends_produced = models.IntegerField(default=0)
    ai_tokens_used = models.IntegerField(default=0)
```

---

## 5. The Hourly Pipeline (Celery)

```
[Celery Beat: every 60 min, broker = Valkey]
        │
        ▼
  orchestrate.run()                         # creates ScrapeRun row
        │
        ├──▶ collect.instagram  ┐
        ├──▶ collect.facebook   │  fan-out via Celery group(), concurrent,
        ├──▶ collect.tiktok     │  independent rate limits + retries
        ├──▶ collect.x          │  (API call if available, else scraper)
        └──▶ collect.reddit     ┘
                 │  (raw payloads → disk/S3, RawPosts → next stage)
                 ▼
        normalize + dedupe + geocode         # → Post rows (Postgres)
                 ▼
        score (velocity / engagement)        # rank candidates
                 ▼
        aggregate into buckets               # topic/region grouping
                 ▼
        ai.cluster → ai.summarize → ai.sentiment   # Claude API enrichment
                 ▼
        write Trend rows + warm Valkey cache  # Postgres + Valkey
                 ▼
        push to websocket subscribers          # Channels, once wired up
                 ▼
        finalize ScrapeRun (success/partial)
```

**Resilience rules (unchanged from original design, still correct here):**
- Each platform collector is **independent** — one failing source yields a `partial` run, never a total failure.
- Collectors retry with exponential backoff; persistent failures are logged to `ScrapeRun.per_platform`.
- AI enrichment is **batched** and token-budgeted per run; on AI failure, raw scored trends still publish (`ai_enriched=False`).
- Idempotent by `run_id` — a re-run never double-counts.

---

## 6. AI Enrichment Design

| Stage | Input | LLM Task | Output |
|---|---|---|---|
| Cluster | Batch of scored posts | Group semantically related posts into candidate topics | Topic clusters + member post IDs |
| Summarize | One cluster | Generate a short label + "why it's trending" explainer | `label`, `summary` |
| Sentiment | Cluster sample | Aggregate stance/sentiment | `sentiment` ∈ [-1, 1] |

**Practices:**
- Prompts are **versioned files** under `backend/ai/prompts/` — never inline strings — so changes are reviewable.
- Use **structured output** (tool/JSON schema) so the model returns parseable objects, not prose.
- Apply **prompt caching** for the static instruction block to cut cost across the hourly batch.
- Track `ai_tokens_used` per run on `ScrapeRun` for cost observability.
- Verify current Claude model IDs/pricing at integration time rather than hardcoding from memory.

---

## 7. Acquisition Compliance & Risk

> This is the highest-risk part of the project. Treat it as a first-class concern, not an afterthought.

- **Check for an official API first, per platform** (see §2.2). Only fall back to scraping where no reasonable API access exists.
- For scraper-mode collectors: respect `robots.txt` and each platform's Terms of Service; only collect **public** data; never authenticate as or impersonate users without authorization.
- Store **rate-limit budgets** per platform in Valkey; never burst past documented limits, whether hitting an API or a scraper target.
- Keep PII handling minimal: store handles/text only as needed, honor deletion requests, document retention in `docs/acquisition-policy.md`.
- Implement **per-platform kill switches** so any source can be disabled without a redeploy.
- Revisit the API-vs-scrape decision periodically — API access (cost, quota, terms) and scraping risk both shift over time.

---

## 8. API Surface (Django REST Framework)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/trends/` | Ranked trends for the latest run (filter by platform, region) |
| `GET` | `/api/trends/{id}/` | Single trend + sample posts |
| `GET` | `/api/heatmap/` | Geo intensities + topic grid for map rendering |
| `GET` | `/api/platforms/{name}/` | Per-platform trend breakdown |
| `GET` | `/api/runs/latest/` | Status of most recent hourly run |
| `WS` | `/ws/trends/` | Live push when a new run publishes (Channels, planned) |
| `GET` | `/healthz/` `/readyz/` | Liveness / readiness |

---

## 9. Local Development Setup

### 9.1 Current state

Django + PostgreSQL + Redis are already working via `docker-compose.yml`. Here's how to get from there to a daily dev loop, plus the changes needed to move Redis → Valkey and add Celery.

### 9.2 One-time setup

**Step 1 — Switch the cache image to Valkey** (functionally a no-op for your code, since Valkey speaks the Redis protocol):

```yaml
# docker-compose.yml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_DB: trendpulse
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  cache:                          # renamed from `redis` for clarity
    image: valkey/valkey:8
    ports:
      - "6379:6379"
    volumes:
      - valkeydata:/data

  backend:
    build: ./backend
    command: python3 manage.py runserver 0.0.0.0:8000
    volumes:
      - ./backend:/app
    ports:
      - "8000:8000"
    depends_on:
      - db
      - cache
    env_file:
      - .env

  worker:                          # NEW: Celery worker
    build: ./backend
    command: celery -A config worker -l info
    volumes:
      - ./backend:/app
    depends_on:
      - db
      - cache
    env_file:
      - .env

  beat:                            # NEW: Celery beat scheduler
    build: ./backend
    command: celery -A config beat -l info
    volumes:
      - ./backend:/app
    depends_on:
      - db
      - cache
    env_file:
      - .env

  frontend:
    build: ./frontend
    command: npm run dev
    volumes:
      - ./frontend:/app
    ports:
      - "3000:3000"

volumes:
  pgdata:
  valkeydata:
```

If you'd rather not touch the service name `redis` yet (e.g. to keep diffs small while you're mid-feature), just swapping the `image:` line is enough — Valkey is a drop-in replacement and nothing else needs to change.

**Step 2 — Add Celery to the Django project:**

```bash
cd backend
pip install celery
```

```python
# backend/config/celery.py  (new file)
import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('config')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
```

```python
# backend/config/__init__.py
from .celery import app as celery_app
__all__ = ('celery_app',)
```

```python
# backend/config/settings.py — add these
CELERY_BROKER_URL = REDIS_URL          # Valkey, same protocol
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_BEAT_SCHEDULE = {
    'hourly-trend-pipeline': {
        'task': 'tasks.orchestrate.run',
        'schedule': 3600.0,
    },
}
```

**Step 3 — Create the new Django apps** (do this incrementally, only as you start each layer — no need to scaffold all five on day one):

```bash
cd backend
python manage.py startapp collectors
python manage.py startapp pipeline
python manage.py startapp ai
python manage.py startapp tasks
python manage.py startapp core
```

Add whichever you've created to `INSTALLED_APPS` in `settings.py`.

**Step 4 — `.env` additions** as you build each layer:

```bash
# .env.example additions
ANTHROPIC_API_KEY=
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
X_BEARER_TOKEN=
META_ACCESS_TOKEN=
TIKTOK_CLIENT_KEY=
```

Only add the credentials for whichever platform/collector you're actively building — no need to provision all five up front.

### 9.3 Daily dev loop

```bash
# Bring up the full stack
docker compose up -d

# Run migrations after model changes
docker compose exec backend python manage.py makemigrations
docker compose exec backend python manage.py migrate

# Trigger one pipeline run by hand (once tasks/orchestrate.py exists) —
# no need to wait an hour during development
docker compose exec backend python manage.py shell -c "from tasks.orchestrate import run; run()"

# Tail logs for a specific service
docker compose logs -f worker

# Open the dashboard (once frontend exists)
open http://localhost:3000

# Django admin / API root
open http://localhost:8000/admin
```

### 9.4 Suggested build order

Given the current state (Django/Postgres/Redis already running), a reasonable next-step order:

1. **`api` app**: define `Post`, `Trend`, `ScrapeRun` models, run initial migration.
2. **One collector end-to-end**: pick Reddit first (easiest official API, no app-review wait like Meta/TikTok). Get `collectors/reddit.py` writing real `Post` rows.
3. **Celery + Valkey wiring**: get `tasks/orchestrate.py` running that one collector hourly, even before scoring/AI exist.
4. **`pipeline` app**: normalize/score what Reddit gives you.
5. **`ai` app**: wire the Claude client against real scored posts.
6. **Remaining collectors**, one at a time, API-first per §2.2.
7. **Frontend**: once `/api/trends/` and `/api/heatmap/` return real data.
8. **Go middleware**: revisit once a concrete bottleneck or use case appears — don't build it speculatively.

---

## 10. Observability & Operations

- **Metrics**: posts/run/platform, acquisition error rate (API vs scraper), pipeline duration, AI tokens & cost.
- **Logs**: structured JSON, correlated by `run_id`.
- **Alerts**: pipeline failed, a platform's error rate > threshold, run duration > 50 min (risking the next hour), AI cost spike.
- **Runbook** (`docs/runbook.md`): how to backfill a missed hour, rotate keys, disable a failing collector, replay from raw payload store.

---

## 11. Roadmap

| Phase | Scope | Status |
|---|---|---|
| **M0 — Foundation** | Django + DRF + Postgres + Redis/Valkey running | ✅ done |
| **M1 — Skeleton** | `api` app models + migrations, Celery + Valkey wired, one collector (Reddit) | next |
| **M2 — Pipeline** | Normalize + scoring + remaining 4 collectors, API-first per platform | planned |
| **M3 — AI** | Clustering, summarization, sentiment via Claude API | planned |
| **M4 — Heatmap UI** | Next.js dashboard, Mapbox heatmap, topic grid | planned |
| **M5 — Real-time** | Django Channels for live trend push to frontend | planned |
| **M6 — Middleware** | Decide Go middleware's actual job, then build it | planned, placement TBD |
| **M7 — Hardening** | Observability, alerts, compliance review, load testing | planned |

---

*Last updated: 2026-06-18*
