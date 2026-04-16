# Dog Beach Scout — CLAUDE.md

Project reference for Claude Code. Describes architecture, conventions, decisions, and current state.

---

## What This Is

**Dog Beach Scout** is a mobile-first web app that tells dog owners when and where to take their dogs to the beach. It covers 5 Southern California dog beaches and shows 7-day forecasts, best-window recommendations, crowd levels, and a score for each day.

The project is hosted on GitHub Pages (`beachfranz.github.io`) and backed by Supabase.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Vanilla HTML/CSS/JS (no framework) — `index.html`, `detail.html`, `find.html` |
| Backend | Supabase Edge Functions (Deno/TypeScript) |
| Database | Supabase Postgres |
| AI chat | Anthropic API — `claude-sonnet-4-20250514` via `beach-chat` edge function |
| Data sources | Open-Meteo (weather), NOAA CO-OPS (tides), BestTime.app (crowd forecasts) |
| Hosting | GitHub Pages (static) |

---

## Beaches

Five active locations, all Southern California:

| location_id | Display Name | City |
|---|---|---|
| `huntington-dog-beach` | Huntington Dog Beach | Huntington Beach |
| `coronado-dog-beach` | Coronado Dog Beach | Coronado |
| `del-mar-dog-beach` | Del Mar Dog Beach | Del Mar |
| `ocean-beach-dog-beach` | Ocean Beach Dog Beach | San Diego |
| `rosies-dog-beach` | Rosie's Dog Beach | Long Beach |

---

## Repository Layout

```
dog-beach-claude/
├── index.html                        # Home — 7-day forecast for one beach
├── detail.html                       # Hour-by-hour detail for one beach/day
├── find.html                         # Find Your Beach — search/sort all beaches
├── compare.html                      # Redirects → find.html
├── src/
│   └── avatar.png                    # Scout avatar
├── supabase/
│   ├── functions/
│   │   ├── _shared/cors.ts           # Shared CORS utility (origin whitelist)
│   │   ├── get-beach-summary/        # 7-day forecast for one beach
│   │   ├── get-beach-detail/         # Hourly scores for one beach/day
│   │   ├── get-beach-compare/        # All beaches ranked by score for a date
│   │   ├── beach-chat/               # Scout AI chat (Claude, rate-limited)
│   │   ├── get-calendar-event/       # ICS download for best window
│   │   ├── daily-beach-refresh/      # Data pipeline (weather + tides + crowds + scoring)
│   │   └── send-daily-alerts/        # SMS alerts via Twilio (blocked — see SMS section)
│   ├── migrations/                   # SQL migrations (applied manually via Supabase dashboard)
│   └── backup/
│       ├── schema.sql                # Full CREATE TABLE DDL for all 8 tables
│       ├── seed_beaches.sql          # Beach rows with lat/lng, station IDs, URLs
│       └── seed_scoring_config.sql   # All scoring config values as of 2026-04-15
```

---

## Database Schema (8 tables)

- **`beaches`** — static beach metadata (lat/lng, NOAA station, BestTime venue, hours, amenities)
- **`scoring_config`** — all scoring weights, thresholds, normalization parameters (versioned)
- **`beach_day_hourly_scores`** — per-beach per-hour scores, component scores, explainability JSON, `is_in_best_window` flag
- **`beach_day_recommendations`** — per-beach per-day rollup: `day_status`, `best_window_label`, `go_hours_count`, narrative text, weather summary
- **`subscribers`** — phone numbers for SMS alerts (PII — no anon access)
- **`subscriber_locations`** — which beaches each subscriber follows
- **`notification_log`** — record of sent alerts
- **`refresh_errors`** — pipeline error log
- **`chat_rate_limits`** — IP + hour bucketed counter for Scout chat rate limiting

### RLS Policy
All tables have RLS enabled. Anon role has SELECT-only access on `beaches`, `beach_day_hourly_scores`, and `beach_day_recommendations`. All other tables are fully blocked to anon. Edge functions use the service role key and bypass RLS.

---

## Edge Functions

All browser-facing functions use `--no-verify-jwt` (the `sb_publishable_` key format does not pass JWT verification). Deploy with:

```bash
supabase functions deploy <function-name> --use-api --no-verify-jwt
```

### `get-beach-summary`
- **GET** `?location_id=<id>`
- Returns beach metadata, 7 days of `beach_day_recommendations` (via `SELECT *`), and `composite_score` per day (averaged from best-window `hour_score` values in `beach_day_hourly_scores`), plus all beaches for the location switcher.

### `get-beach-detail`
- **GET** `?location_id=<id>&date=<YYYY-MM-DD>`
- Returns all hourly scores for one beach/day, including `explainability` JSON with per-metric component scores (`tide_score`, `wind_score`, `crowd_score`, `rain_score`, `temp_score`).

### `get-beach-compare`
- **GET** `?date=<YYYY-MM-DD>` (defaults to today)
- Returns all active beaches ranked by `composite_score`, including `latitude` and `longitude` per beach. Score computed from best-window hourly rows.

### `beach-chat`
- **POST** with `{ location_id, question }`
- Calls Anthropic API (`claude-sonnet-4-20250514`) with beach context. Rate-limited to 20 requests/IP/hour via `increment_chat_rate` RPC. Stale rate limit rows pruned probabilistically (10% chance per request, rows older than 24h deleted).

### `get-calendar-event`
- **GET** `?location_id=<id>&date=<YYYY-MM-DD>`
- Returns an ICS file for adding the best window to a calendar.

### `daily-beach-refresh`
- Scheduled pipeline. For each active beach: fetches weather (Open-Meteo), tides (NOAA), crowds (BestTime), runs scoring, writes hourly rows and day recommendation. Also calls `send-daily-alerts`.

### `send-daily-alerts`
- Sends SMS via Twilio. Currently blocked — see SMS section below.

---

## CORS

`_shared/cors.ts` enforces an origin whitelist:
- `https://beachfranz.github.io`
- `null` (file:// for local development)

All edge functions import `corsHeaders(req, methods)` and use it for every response including OPTIONS preflight.

---

## Scoring Model

Each hour is scored 0–100 as a weighted composite:
- **Tide** — lower tide = better; normalized against `norm_tide_max`
- **Wind** — lower wind = better; normalized against `norm_wind_max`
- **Rain** — lower precip chance = better
- **Crowd** — from BestTime busyness score
- **Temp** — optimized around `norm_temp_target` with `norm_temp_range`
- **UV** — lower UV = better (minor weight)

Weights and thresholds are stored in `scoring_config` and versioned. Hours are flagged `go`, `caution`, or `no_go`. The best contiguous window of go/caution hours is selected and flagged `is_in_best_window = true`. Composite score for a day = average `hour_score` across best-window hours.

---

## Frontend Pages

### `index.html` — Home / 7-Day Forecast
- Location switcher (dropdown from `allBeaches` in API response)
- Day cards showing: day abbreviation, composite score (colored by status) with "SCORE" label, weather icon, best window label, metric chips (tide, temp, wind, crowd)
- Scout blurb (AI-generated narrative) with tap-to-chat
- Tapping a day card navigates to `detail.html`

### `detail.html` — Hour-by-Hour Detail
- Hour rows with go/caution/no_go status
- Score tooltip on tap: horizontal component bars (tide, wind, crowd, rain, temp) each colored by status, with score value on non-score status lines
- Calendar download button (ICS)
- Scout chat panel

### `find.html` — Find Your Beach
- Fetches all beaches via `get-beach-compare`
- Sort modes:
  - **Best Conditions** — sort by `composite_score` descending
  - **Closest First** — sort by Haversine distance ascending (requires location)
  - **Best Nearby** — 50/50 blend of normalized score and inverted distance
- Location via browser Geolocation API; reverse geocoded to city name via Nominatim
- ZIP code override: forward geocoded via Nominatim, updates user location
- Distance options disabled until location is available
- Beach cards show distance badge, score, status, window label, weather

### `compare.html`
- Redirects to `find.html` via `<meta http-equiv="refresh">`.

---

## Auth / Keys

- **Anon key** (`sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk`) — used in all three HTML pages for edge function calls. Safe to commit (public, RLS enforced).
- **Service role key** — only in Supabase edge function environment variables, never in frontend code.
- Previous anon key (`sb_publishable_o2d4Kg0xMZoMw92feO2uWQ_PdDfQXKj`) was rotated on 2026-04-15.

---

## Git Workflow

- Branch: **`exploratory`** for all development
- Branch: **`main`** is production (served by GitHub Pages)
- Workflow: develop on exploratory → test locally by opening HTML files in browser → merge to main only after user confirms it works
- Never auto-merge to main without explicit user approval

---

## SMS Alerts (Blocked)

The `send-daily-alerts` edge function and full subscriber pipeline (tables, Twilio integration) are implemented but not operational. Twilio toll-free number verification is backlogged. Will resume once the from-number is approved.

---

## Current State (as of 2026-04-16)

- All 5 beaches have live data
- Full security hardening complete: RLS, CORS origin whitelist, chat rate limiting, anon key rotation
- `find.html` launched as the new search/discovery page
- Index page day cards show composite score
- Schema + seed data backed up to `supabase/backup/`
- SMS pipeline built but blocked on Twilio verification

### Near-term backlog
- Add filters to `find.html` (off-leash zones, amenities, etc.) once `beaches` table is enriched with that metadata
- Consider adding pre-computed `composite_score` column to `beach_day_recommendations` so the pipeline writes it directly rather than computing it at query time
