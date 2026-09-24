# Distributed Job Queue & Background Processing System

A backend-heavy async job processing system in the spirit of Sidekiq/Celery/BullMQ —
built from scratch on Node.js, Fastify, Redis, and PostgreSQL, with a live React
dashboard. Implements the full SRS: priority queues, delayed/scheduled jobs,
exponential-backoff retries, a dead-letter queue, crash recovery, and real-time
WebSocket updates.

This was implemented and **smoke-tested end-to-end** against real Redis and
PostgreSQL instances (not mocked) — enqueue → claim → execute → retry-with-backoff
→ complete/DLQ → manual re-queue → graceful shutdown were all exercised live.

## Architecture

```
Producer → API (Fastify) → PostgreSQL (durable record)
                          → Redis (queue/broker)
                                 ↓
                    Worker Pool (N replicas, atomic LMOVE claim)
                                 ↓
                    PostgreSQL (status/history) + Redis pub/sub

Scheduler  → fires recurring (cron) jobs, Redis-lock guarded (single-fire)
Reaper     → scans per-worker processing lists, requeues orphaned jobs
Dashboard  → React SPA, live via WebSocket, REST for everything else
```

Three deployable process types beyond the API: **worker**, **scheduler**, **reaper** —
each horizontally scalable independently (`docker compose up --scale worker=5`).

## Project layout

```
packages/
  shared/      Redis key scheme, backoff math, logger — used by every process
  db/          PostgreSQL schema.sql + migration runner
  server/      Fastify API: routes, auth, rate limiting, WebSocket gateway
  worker/      Claim/execute/retry loop, job handlers, graceful shutdown
  scheduler/   Cron-based recurring job firing (distributed lock)
  reaper/      Crash recovery (orphaned job requeue)
  dashboard/   React + Vite live dashboard
docker/        Dockerfiles + nginx config for the dashboard
tests/         Vitest unit tests (backoff, key scheme, claim priority ordering)
docker-compose.yml
```

## Quick start (Docker Compose — recommended)

```bash
cp .env.example .env   # edit if you want, defaults work out of the box
docker compose up --build
```

This starts Redis, Postgres (+ migration), the API server, 2 worker replicas, the
scheduler, the reaper, and the dashboard.

- API: http://localhost:3000
- Dashboard: http://localhost:8080
- Default API keys: `dev-key-123`, `dev-key-456` (set in docker-compose.yml)

## Quick start (local, no Docker)

Requires Node 20+, a local Redis, and a local PostgreSQL.

```bash
npm install
cp .env.example .env
# create a `jobqueue` Postgres role/db matching DATABASE_URL, then:
npm run db:migrate

# in separate terminals:
npm run dev:server
npm run dev:worker
npm run dev:scheduler
npm run dev:reaper
npm run dev:dashboard   # http://localhost:5173, proxies /api to :3000
```

## Trying it out

```bash
# Enqueue a job
curl -X POST http://localhost:3000/jobs \
  -H "x-api-key: dev-key-123" -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"you@example.com","template":"welcome"},"priority":"high"}'

# Check its status
curl http://localhost:3000/jobs/<id> -H "x-api-key: dev-key-123"

# Queue stats (FR-6.4)
curl http://localhost:3000/queues/stats -H "x-api-key: dev-key-123"

# Create a recurring job (FR-5)
curl -X POST http://localhost:3000/schedules \
  -H "x-api-key: dev-key-123" -H "Content-Type: application/json" \
  -d '{"type":"noop","cronExpression":"*/5 * * * *"}'
```

The bundled `send_email` handler intentionally fails once when
`payload.template === "flaky-demo"`, so you can watch the retry/backoff path live:

```bash
curl -X POST http://localhost:3000/jobs \
  -H "x-api-key: dev-key-123" -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"x@y.com","template":"flaky-demo"},"maxRetries":2}'
```

Open the dashboard and you'll see it go `queued → active → delayed (retry
scheduled) → queued → active → completed` live over the WebSocket feed.

## Testing

```bash
npm test              # Vitest unit tests: backoff math, Redis key scheme,
                       # and claim() priority-ordering logic (against an
                       # in-memory fake Redis — see tests/claim.test.js)
```

The SRS's Testcontainers-based **integration** suite (API → Redis → Worker →
Postgres round trip, concurrent-worker race test, kill-and-recover test) and the
k6/Artillery **load** tests (section 9.3) and chaos tests (9.4) are specified in
the SRS but not included here — they need real ephemeral Redis/Postgres containers
via Testcontainers, which isn't available in this environment. The core logic
they'd exercise (atomic claim, backoff, idempotency, reaper recovery) **was**
manually verified live against real Redis/Postgres during development; see
"What was actually run" below.

## What was actually run (not just written)

During development this system was:
1. Migrated against a real PostgreSQL 16 instance (`npm run db:migrate`).
2. Booted as a real Fastify server against real Redis + Postgres.
3. Exercised via `curl`: job enqueue, idempotency dedup (second call returned the
   same job with `deduped: true`), queue-stats, job detail with full event
   history.
4. A real worker process claimed and completed jobs, including the full
   fail → backoff-delay → auto-promote → retry → succeed path for a job
   designed to fail on its first attempt.
5. A job with `maxRetries: 0` and invalid payload was pushed straight to the
   dead-letter queue (`status: "dead"`), then successfully manually re-queued
   via `POST /jobs/:id/retry`.
6. The scheduler process fired a `* * * * *` recurring job exactly once at the
   minute boundary (no double-fire).
7. `SIGTERM` sent to the worker triggered a clean drain-and-exit per FR-7,
   logging "All in-flight jobs finished cleanly."

## Known gaps / production hardening notes

- **WebSocket auth**: `/ws` doesn't require an API key in v1 (assumed to sit
  behind the same network boundary as the dashboard). Add a signed short-lived
  token or session check before exposing this publicly.
- **Idempotency + Redis push gap**: a job is written to Postgres before being
  pushed onto the Redis queue (2.5 says Postgres is the audit source of truth).
  If the process crashes between those two writes, the job exists in Postgres
  as `queued` but never reaches Redis. There's no automatic reconciler for this
  narrow window in v1 — an operator would need to notice and manually re-fire
  it. A production version should add a periodic reconciliation job that finds
  `status='queued'` rows older than N seconds with no corresponding Redis
  entry and re-pushes them.
- **Redis is single-instance** in this design (per SRS 2.6's stated v1
  assumption) — no Sentinel/Cluster failover. Section 11 lists Redis Cluster
  support as a future enhancement.
- **Worker registry (`workers:active` Redis set)** entries aren't pruned if a
  worker crashes without deregistering; harmless (scanning an empty processing
  list is cheap) but would grow unbounded over a very long-lived deployment.
  A TTL'd worker-liveness heartbeat is a straightforward follow-up.
- **Retention/cleanup job** (NFR-9: purge/archive completed jobs after 30
  days) is not implemented as a running process — add a `cron`-scheduled
  `DELETE FROM jobs WHERE status='completed' AND completed_at < now() -
  interval '30 days'` job (e.g. via the scheduler itself, or a dedicated
  cleanup script) before production use.
- **Testcontainers integration suite / k6 load tests / chaos tests** (SRS 9.2,
  9.3, 9.4) are described above but not implemented as runnable test files in
  this deliverable.

## Configuration reference

See `.env.example` for every environment variable (ports, Redis/Postgres URLs,
worker concurrency, timeouts, backoff base/cap, rate limit, retention days,
etc.) — all match the "Key configs" list in SRS section 10.2.

## API summary

| Method | Endpoint | Maps to |
|---|---|---|
| POST | `/jobs` | FR-1 |
| GET | `/jobs/:id` | FR-6.1 |
| GET | `/jobs` | FR-6.2 |
| POST | `/jobs/:id/retry` | FR-4.3 |
| DELETE | `/jobs/:id` | cancel a queued job |
| GET | `/queues/stats` | FR-6.4 |
| POST | `/schedules` | FR-5.1 |
| GET / PATCH / DELETE | `/schedules[/:id]` | manage recurring definitions |
| WS | `/ws` | FR-6.3 |
| GET | `/health` | container healthcheck |
| GET | `/metrics` | Prometheus format, section 10.4 |

All authenticated routes require `x-api-key` (FR-8.1) and are token-bucket
rate limited per key (FR-8.2, `429` on excess).
