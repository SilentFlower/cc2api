# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Claude Code Gateway is a Rust reverse proxy for the Anthropic API that pools multiple Claude accounts with load balancing, rate limit handling, sticky sessions, TLS fingerprint spoofing, and request/response rewriting. It includes a Vue 3 management dashboard embedded into the single binary via `rust-embed`.

## Build & Run Commands

```bash
# Development (frontend + backend together)
cp .env.example .env
./scripts/dev.sh

# Or run separately:
cd web && npm ci && npm run dev    # Frontend dev server on :3000
cargo run                           # Backend on :5674 (frontend proxies to it)

# Production build (embeds frontend into binary)
./scripts/build.sh                  # Native
./scripts/build.sh linux-amd64     # Cross-compile

# Docker
docker build -f docker/Dockerfile -t claude-code-gateway:latest .
```

No test suite exists. Account validity is tested via the UI's "test" button.

## Architecture

### Request Flow

Auth middleware → API token lookup → account selection (sticky session + load-aware scoring + concurrency) → request rewriting (headers, body, telemetry, identity) → TLS fingerprint spoofing via custom rustls (`craftls/`) → forward to api.anthropic.com → passive usage collection from response headers → response header filtering → return to client.

### Key Modules

- **`src/handler/router.rs`** — All HTTP endpoints: SPA routes, `/admin/*` management API (password-protected), and the catch-all gateway proxy.
- **`src/service/gateway.rs`** — Core forwarding orchestration: account selection, slot acquisition with wait-retry (500ms × 60 = 30s max), upstream request, rate limit detection (429 → quarantine account), passive usage collection from `anthropic-ratelimit-unified-*` response headers.
- **`src/service/rewriter.rs`** — Request/response transformation: header normalization, body patching (session hash, version, telemetry paths like event_logging/GrowthBook), system prompt env var injection, AI Gateway fingerprint header stripping.
- **`src/service/account.rs`** — Account selection logic: sticky sessions (1h TTL, refreshed on hit), load-aware scoring (`5h_utilization * 0.7 + concurrency_load% * 0.3`), OAuth token refresh with locking, concurrency slot management, usage/billing queries, passive usage merge.
- **`src/service/usage_poller.rs`** — Optional background polling for OAuth account usage (controlled per-account via `auto_poll_usage`, default off).
- **`src/tlsfp/tlsfp.rs`** — Custom TLS ClientHello builder that mimics Node.js fingerprint, using the forked rustls in `craftls/`.
- **`src/middleware/auth.rs`** — Extracts API key from `x-api-key` or `Authorization: Bearer` header, validates against token store.
- **`src/store/`** — SQLx-based persistence (SQLite default, PostgreSQL optional) with `CacheStore` trait implemented by `MemoryStore` and `RedisStore`.
- **`src/model/identity.rs`** — Generates canonical device identity (20+ env vars, process fingerprints) for upstream requests.

### Frontend

Vue 3 + Vite + TypeScript in `web/`. Components: Login, Dashboard, Accounts, Tokens. API client in `web/src/api.ts`. Uses shadcn-style UI components in `web/src/components/ui/`.

### Database

SQLite (WAL mode) or PostgreSQL, selected via `DATABASE_DRIVER` env var. Auto-migration on startup in `src/store/db.rs`. Incremental ALTER TABLE migrations for backward compatibility.

### Custom Rustls Fork

`craftls/` contains a patched rustls that exposes low-level TLS ClientHello construction for fingerprint spoofing. This is a workspace dependency, not published.

## Configuration

All via environment variables (see `.env.example`). Key ones: `SERVER_HOST`/`SERVER_PORT` (default 0.0.0.0:5674), `DATABASE_DRIVER`/`DATABASE_DSN`, `REDIS_HOST` (optional, falls back to in-memory), `ADMIN_PASSWORD` (default "admin"), `LOG_LEVEL`.

## Dual Auth Modes

Accounts support two auth types: **SetupToken** (classic API key) and **OAuth** (with automatic access token refresh via stored refresh tokens). Both flows converge in `AccountService::select_account`.

## Usage Tracking

Two complementary mechanisms:

- **Passive collection** (all accounts): Every upstream response's `anthropic-ratelimit-unified-*` headers are parsed and merged into `usage_data`. Zero API cost, works for SetupToken too.
- **Active polling** (opt-in per account): `auto_poll_usage` flag enables background polling via OAuth usage API. Only for OAuth accounts. Default off.

Usage data feeds into both the dashboard display and the load-aware account selection scoring.

## Account Selection

Within the same priority group, accounts are ranked by a composite score:

```
eff_7d = 7d_utilization × (remaining / 7days)
eff_5h = 5h_utilization × (remaining / 5hours)
score  = eff_7d × 0.5 + eff_5h × 0.3 + concurrency_load% × 0.2
```

Utilization is time-decay weighted: accounts closer to reset score lower (about to clear). Missing resets_at uses decay=1.0 (worst case).

Accounts at full concurrency are excluded from selection unless all candidates are full. Concurrency slots that are unavailable trigger a wait-retry loop (500ms interval, 30s max) before returning an error.

## KEY: EVERY TIME YOU WANT TO CHANGE STH, PLEASE REFER cc-gateway
