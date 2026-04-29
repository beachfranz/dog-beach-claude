# Dog Beach Scout — CLAUDE.md

Project reference for Claude Code. Authoritative guide to architecture, data model, scoring, frontend, edge functions, and conventions. Keep this up to date when making significant changes.

---

## What This Is

**Dog Beach Scout** is a mobile-first web app that tells dog owners when and where to take their dogs to the beach. It covers 5 Southern California dog-friendly beaches and provides:

- 7-day forecast cards with composite scores and best-window recommendations
- Hour-by-hour detail with bar charts, a status board, and a score breakdown tooltip
- Multi-beach discovery/comparison page with distance sorting
- AI chat ("Scout") powered by Claude
- Rolling actuals: every hour, the current hour's forecast row is overwritten with live data

Hosted on GitHub Pages (`beachfranz.github.io`), backed by Supabase (Postgres + Edge Functions).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Vanilla HTML/CSS/JS — `index.html`, `detail.html`, `find.html` |
| Backend | Supabase Edge Functions (Deno/TypeScript) |
| Database | Supabase Postgres |
| AI chat | Anthropic API — `claude-sonnet-4-20250514` via `beach-chat` edge function |
| Weather | Open-Meteo (forecast + current conditions) |
| Tides | NOAA CO-OPS API |
| Crowds | BestTime.app (busyness forecasts, 0–100 score) |
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
├── index.html                          # Home — 7-day forecast for one beach
├── detail.html                         # Hour-by-hour detail for one beach/day
├── find.html                           # Find Your Beach — search/sort all beaches
├── compare.html                        # Redirects → find.html
├── src/
│   └── avatar.png                      # Scout avatar image
├── supabase/
│   ├── functions/
│   │   ├── _shared/
│   │   │   ├── cors.ts                 # CORS origin whitelist utility
│   │   │   └── scoring.ts             # Shared scoring engine (single source of truth)
│   │   ├── daily-beach-refresh/        # Daily pipeline: weather+tides+crowds+scoring
│   │   │   ├── index.ts               # Orchestrator
│   │   │   ├── scoring.ts             # Re-export shim → _shared/scoring.ts
│   │   │   ├── openmeteo.ts           # Open-Meteo weather fetch
│   │   │   ├── noaa.ts                # NOAA tides fetch
│   │   │   ├── besttime.ts            # BestTime crowd fetch
│   │   │   └── narrative.ts           # Claude-powered narrative generation
│   │   ├── get-beach-now/             # Live actuals refresh (hourly cron + on-demand)
│   │   │   └── index.ts
│   │   ├── get-beach-summary/         # 7-day forecast for one beach
│   │   │   └── index.ts
│   │   ├── get-beach-detail/          # Hourly scores for one beach/day
│   │   │   └── index.ts
│   │   ├── get-beach-compare/         # All beaches ranked by score for a date
│   │   │   └── index.ts
│   │   ├── beach-chat/                # Scout AI chat (Claude, rate-limited)
│   │   │   └── index.ts
│   │   ├── get-calendar-event/        # ICS download for best window
│   │   │   └── index.ts
│   │   └── send-daily-alerts/         # SMS alerts via Twilio (blocked — see below)
│   │       └── index.ts
│   ├── migrations/                    # SQL migrations (applied manually via Supabase dashboard)
│   └── backup/
│       ├── schema.sql                 # Full CREATE TABLE DDL
│       ├── seed_beaches.sql           # Beach rows with lat/lng, NOAA station IDs
│       └── seed_scoring_config.sql    # Scoring config values as of 2026-04-15
```

---

## Database Schema

Nine tables total. All have RLS enabled.

### `beaches`
Static beach metadata. Key columns: `location_id` (PK, slug), `display_name`, `latitude`, `longitude`, `noaa_station_id`, `besttime_venue_id`, `timezone`, `open_time`, `close_time`, `is_active`, `address`, `website`, `description`, `parking_text`.

Dog policy on this table is split across two parallel sets of columns:
- **Curated** (HTML reads these): `dogs_allowed`, `leash_policy`, `off_leash_flag`, `dogs_prohibited_start/end`, `dogs_allowed_areas`, `access_rule`. Hand-curated; richer enum than the cascade (`yes`/`no`/`mixed`/`seasonal`/`restricted`/...).
- **Cascade-computed** (reference only — added 2026-04-29): `dog_verdict_catalog`, `dog_verdict_catalog_confidence`, `dog_verdict_catalog_computed_at`. Written by the Dagster `consumer_beaches_sync` asset by spatial-joining each beach to the nearest `beach_locations` row within 500m and copying `beach_verdicts.dogs_verdict`. HTML does NOT read these — they're parity references for surfacing catalog vs. curated disagreements via `dbt_dbt.consumer_beach_with_verdict`.

### `scoring_config`
All scoring weights, thresholds, and normalization parameters. Versioned — pipeline always loads the row where `is_active = true`. See **Scoring Model** section for all fields.

### `beach_day_hourly_scores`
One row per beach per hour. The core data table.

Key columns:
- `location_id`, `local_date`, `local_hour`, `forecast_ts` (UTC timestamptz, PK with location_id)
- `hour_label` — "6am", "2pm" etc.
- `is_daylight`, `is_candidate_window`, `is_in_best_window`
- `is_now` — boolean, true for the single row per beach that holds live actuals for the current hour
- Raw inputs: `weather_code`, `temp_air`, `feels_like`, `wind_speed`, `precip_chance`, `uv_index`, `tide_height`, `busyness_score`, `busyness_category`
- Surface temps: `sand_temp`, `asphalt_temp`
- Scores: `hour_score`, `tide_score`, `wind_score`, `crowd_score`, `rain_score`, `temp_score`, `uv_score`
- Statuses (4-tier): `hour_status`, `tide_status`, `wind_status`, `crowd_status`, `rain_status`, `temp_status`, `temp_cold_status`, `temp_hot_status`, `uv_status`, `sand_status`, `asphalt_status`
- `passed_checks`, `failed_checks`, `positive_reason_codes`, `risk_reason_codes`
- `explainability` — JSONB with all component scores
- `hour_text` — short AI-generated hour description
- `scoring_version`, `generated_at`, `timezone`

### `beach_day_recommendations`
One row per beach per day. Day-level rollup.

Key columns: `location_id`, `local_date`, `day_status`, `best_window_label`, `best_window_start`, `best_window_end`, `best_window_status`, `go_hours_count`, `advisory_hours_count`, `caution_hours_count`, `nogo_hours_count`, `avg_temp`, `avg_feels_like`, `avg_wind`, `summary_weather`, `bacteria_risk`, `narrative_text`, `scoring_version`, `generated_at`.

### `subscribers`
Phone numbers for SMS alerts. PII — fully blocked to anon role.

### `subscriber_locations`
Junction table: which beaches each subscriber follows.

### `notification_log`
Record of sent SMS alerts.

### `refresh_errors`
Pipeline error log. Written by `daily-beach-refresh` on failure.

### `chat_rate_limits`
IP + hour bucketed counter for Scout chat rate limiting. Max 20 requests/IP/hour.

### RLS Policy
- Anon role: SELECT-only on `beaches`, `beach_day_hourly_scores`, `beach_day_recommendations`
- All other tables: fully blocked to anon
- Edge functions use the service role key and bypass RLS entirely

---

## Supabase CLI

Available via `supabase` (installed at `~/scoop/shims/supabase`, v2.90.0). The project is pre-linked to **dog-beach-AI** (ref: `ehlzbwtrsxaaukurekau`, East US).

Run a migration against the live DB:
```bash
supabase db query --linked -f supabase/migrations/<file>.sql
```

Verify a value:
```bash
supabase db query --linked "SELECT col FROM table WHERE ..."
```

---

## Edge Functions

Deploy command for all browser-facing functions:
```bash
supabase functions deploy <function-name> --use-api --no-verify-jwt
```
`--no-verify-jwt` is required because the `sb_publishable_` anon key format does not pass Supabase JWT verification.

---

### `get-beach-summary`
**GET** `?location_id=<id>`

Returns:
- `beach` — metadata for the requested beach
- `allBeaches` — all active beaches (for location switcher)
- `days` — 7 rows from `beach_day_recommendations`, each enriched with `composite_score` (averaged from best-window `hour_score` values)

---

### `get-beach-detail`
**GET** `?location_id=<id>&date=<YYYY-MM-DD>`

Returns:
- `beach` — metadata
- `day` — full `beach_day_recommendations` row
- `hours` — all daylight hourly rows for that beach/day, ordered by `local_hour`

Selected columns include all metric statuses and values: `tide_status`, `wind_status`, `crowd_status`, `rain_status`, `temp_status`, `temp_cold_status`, `temp_hot_status`, `uv_status`, `sand_status`, `asphalt_status`, `sand_temp`, `asphalt_temp`, `feels_like`, and all score columns.

---

### `get-beach-now`
**GET** `?location_id=<id>` — refresh a single beach, return its NOW row (used by frontend)
**POST** `{ location_ids?: string[] }` — batch refresh (used by hourly cron)

For each beach:
1. Fetches live weather from Open-Meteo `/current` endpoint (temp, feels-like, wind, weather_code, uv, is_day + hourly precip_probability)
2. Fetches live tide predictions from NOAA CO-OPS for today
3. Borrows `busyness_score` from the existing DB row for the current hour (BestTime is only fetched daily)
4. Builds a `RawHourData` object, runs it through `scoreHours()` from `_shared/scoring.ts`
5. Clears `is_now = true` on any previous NOW row for that beach
6. Upserts the new row with `is_now = true`, overwriting the forecast row for that timestamp
7. Returns the row plus `tide_direction` (rising/falling/steady)

Cron: `pg_cron` fires `hourly-beach-now-refresh` every hour on the hour via `net.http_post`.

---

### `daily-beach-refresh`
The full data pipeline. Triggered by Supabase scheduled function (daily).

For each active beach:
1. Fetches 7-day hourly weather forecast from Open-Meteo
2. Fetches 7-day tide predictions from NOAA CO-OPS
3. Fetches crowd forecast from BestTime.app for each day of the week
4. Builds `RawHourData[]` for all hours across 7 days
5. Runs `scoreHours()` → `selectBestWindows()` → `applyBestWindowFlags()` from `_shared/scoring.ts`
6. Upserts all hourly rows into `beach_day_hourly_scores`
7. Builds daily rollup rows → upserts into `beach_day_recommendations`
8. Generates narrative text via Claude (`narrative.ts`)
9. Calls `send-daily-alerts` for SMS notifications

`daily-beach-refresh/scoring.ts` is a re-export shim: `export * from "../_shared/scoring.ts"`. All actual scoring logic lives in `_shared/scoring.ts`.

---

### `beach-chat`
**POST** `{ location_id, question }`

Calls Anthropic API (`claude-sonnet-4-20250514`) with beach + day context injected into the system prompt. Rate-limited to 20 requests/IP/hour via `increment_chat_rate` RPC. Stale rows pruned probabilistically (10% chance per request, rows older than 24h deleted).

---

### `get-beach-compare`
**GET** `?date=<YYYY-MM-DD>` (defaults to today)

Returns all active beaches ranked by `composite_score` descending, including `latitude`, `longitude`, and beach metadata. Score is computed from best-window hourly rows.

---

### `get-calendar-event`
**GET** `?location_id=<id>&date=<YYYY-MM-DD>`

Returns an `.ics` file for adding the best window to a calendar app.

---

### `send-daily-alerts`
Sends SMS via Twilio to subscribers following a beach when the day status is favorable. **Currently blocked** — Twilio toll-free number verification is backlogged. Will resume once the from-number is approved.

---

## CORS

`_shared/cors.ts` enforces an origin whitelist. Allowed origins:
- `https://beachfranz.github.io` (production)
- `null` (file:// for local development)

All edge functions call `corsHeaders(req, methods)` and include the result on every response, including OPTIONS preflight.

---

## Scoring Model

All scoring logic lives in `supabase/functions/_shared/scoring.ts`. This is the single source of truth — both `daily-beach-refresh` and `get-beach-now` import from it.

### Four-Tier Status System

Every metric produces one of four statuses: `go` / `advisory` / `caution` / `no_go`.

The overall `hour_status` is the worst status across all metrics.

| Status | Meaning | Color |
|---|---|---|
| `go` | No concerns | Green `#4ade80` |
| `advisory` | Minor concern, still worthwhile | Sky blue `#38bdf8` |
| `caution` | Meaningful concern, proceed carefully | Amber `#f59e0b` |
| `no_go` | Not recommended | Red `#ef4444` |

### Metric Status Rules (all thresholds from `scoring_config`)

**Tide** (`tide_height` in ft)
- `no_go`: hardcoded extreme cases only (none currently — tide is soft)
- `caution`: ≥ `caution_tide_height` (default 5.0 ft)
- `advisory`: ≥ `advisory_tide_height` (default 3.0 ft)
- `go`: otherwise

**Wind** (`wind_speed` in mph)
- `no_go`: ≥ `nogo_wind_speed`
- `caution`: ≥ `caution_wind_speed` (default 15 mph)
- `advisory`: ≥ `advisory_wind_speed` (default 10 mph)
- `go`: otherwise

**Rain** (`precip_chance` in %)
- `no_go`: ≥ `nogo_precip_chance` OR severe WMO code
- `caution`: ≥ `caution_precip_chance` (default 50%) OR caution WMO code
- `advisory`: ≥ `advisory_precip_chance` (default 10%)
- `go`: otherwise

**WMO code classification:**
- Severe (no_go): thunderstorm (95–99), heavy rain (63–67), snow/ice (71–77), violent showers (82)
- Caution: drizzle (51–57), slight rain (61), slight/moderate showers (80–81), fog (45, 48)
- Go: everything else (clear 0–3, partly cloudy, overcast) via fallthrough — no explicit list

Note: `SEVERE_WMO_CODES` is hardcoded in `_shared/scoring.ts`. `caution_wmo_codes` lives in the DB. `nogo_wmo_codes` DB column is not read by the engine (dead config).

**Crowd** (`busyness_score` 0–100 from BestTime)
- `no_go`: > `advisory_crowd_max` (default 84) — i.e., above the advisory ceiling
- `caution`: actually maps to score > advisory_crowd_max (too crowded)
- `advisory`: ≥ `advisory_crowd_min` (default 61) and ≤ `advisory_crowd_max` (default 84)
- `go`: < `advisory_crowd_min`

Busyness categories: `quiet` (0–`busy_quiet_max`), `moderate`, `dog_party`, `too_crowded`.

**Temperature** — split hot/cold model using `feels_like` (apparent temperature)
- Cold path: `no_go` if < `caution_temp_cold_min` (default 20°F); `caution` if < 20°F; `advisory` if < `advisory_temp_cold_min` (default 32°F); `go` if ≥ `go_temp_cold_min` (default 50°F)
- Hot path: `no_go` if > `nogo_temp_hot_max` (default 95°F); `caution` if > `caution_temp_hot_max` (default 85°F); `advisory` if > `advisory_temp_hot_max` (default 75°F)
- `temp_status` = worst of cold and hot paths
- `temp_cold_status` and `temp_hot_status` are also written separately

**UV** (`uv_index`)
- `no_go`: ≥ `nogo_uv_index` (default 11)
- `caution`: ≥ `caution_uv_index` (default 8)
- `advisory`: ≥ `advisory_uv_index` (6 — raised from 3 on 2026-04-17; UV 3 fired on every clear SoCal afternoon and became noise)
- `go`: otherwise

**Sand temp** (`sand_temp` in °F — estimated from `temp_air`)
- `no_go`: ≥ `nogo_sand_temp` (default 125)
- `caution`: ≥ `caution_sand_temp` (default 115)
- `advisory`: ≥ `advisory_sand_temp` (default 105)

**Asphalt temp** (`asphalt_temp` in °F — estimated from `temp_air`)
- Same tier structure as sand, using `caution_asphalt_temp` (default 115) / `advisory_asphalt_temp` (default 105)

### Composite Score (0–100)

Scored on `feels_like`, not raw `temp_air`. Weighted sum of six normalized component scores:

| Component | Weight field | Normalization |
|---|---|---|
| Tide | `weight_tide` (22.5%) | 0 → 100, 0 = best, normalized against `norm_tide_max` |
| Rain | `weight_rain` (17.5%) | 0 → 100, lower precip% = better |
| Wind | `weight_wind` (20%) | 0 → 100, lower = better, `norm_wind_max` |
| Crowd | `weight_crowd` (15%) | 0 → 100, lower busyness = better |
| Weather code | `weight_weather_code` (15%) | WMO code → fixed score: clear=1.0, partly cloudy=0.9, overcast=0.75, fog=0.4, drizzle=0.15–0.35, rain=0.3, showers=0.15–0.25, severe=0.0 |
| Temp | `weight_temp` (5%) | bell curve around `norm_temp_target` ± `norm_temp_range` |
| UV | `weight_uv` (5%) | 0 → 100, lower UV = better, `norm_uv_max` |

`hour_score` is null for hours where `is_daylight = false` or beach is closed.

### Best Window Selection

1. Candidate hours: daylight hours where `hour_status` ≠ `no_go` and `hour_score` ≥ `window_score_threshold`
2. Find the longest contiguous block of candidate hours (2–5 hours)
3. Flag those hours with `is_in_best_window = true`
4. Best window label format: "10am–2pm"

### Day Status

- `no_go` if majority of hours are no_go
- `caution` if any caution+ hours, no no_go majority
- `advisory` if only advisory-level hours are flagged
- `go` otherwise

Composite score for the day = average `hour_score` across best-window hours.

---

## Frontend Pages

### `index.html` — Home / 7-Day Forecast

**Data source:** `get-beach-summary`

**Features:**
- Location switcher (dropdown populated from `allBeaches`)
- NOW card: calls `get-beach-now` for the current hour's live actuals, displayed as the first card using the same `buildDayCard()` function as forecast days
- Day cards: day abbreviation, composite score colored by status, weather icon, best window label, metric chips (tide, temp, wind, crowd)
- Scout blurb: AI-generated narrative with tap-to-chat
- Tapping any day card navigates to `detail.html?location_id=<id>&date=<YYYY-MM-DD>`

**Key JS functions:**
- `buildDayCard(day, isNow)` — renders a single card (used for both NOW and forecast days)
- `nowToDay(d)` — adapter that maps a `get-beach-now` response to the day card shape
- `loadNowCard()` — fetches from `get-beach-now` and renders the NOW card
- `chipClass(status)` — returns CSS class for metric chip color

**Status colors (CSS variables):**
- `--advisory: #38bdf8` (sky blue)
- `--amber: #fbbf24` (caution)
- `--red: #ef4444` (no_go)
- `--green: #4ade80` (go)

---

### `detail.html` — Hour-by-Hour Detail

**Data source:** `get-beach-detail`

**Layout (from top to bottom):**
1. Sticky header: beach name + day, best window label (colored by status), Scout blurb
2. Status board + bar charts side by side (`.board-and-charts`)
3. Legend
4. Scout narrative + chat panel

**Status Board** (`.status-board`, left side)
- Shows only metrics with ≥1 non-go hour
- 8 possible metrics: Tide, Wind, Rain, Crowd, Temp, UV, Sand, Asphalt
- Per metric: name (colored by worst status), worst value, time range(s) of firing
- Contiguous block logic: groups consecutive flagged hours into blocks. If 3+ blocks → shows only the 2 largest by duration (sorted back to chronological). 1–2 blocks → shows all.
- Time format: "10am–2pm" or "10am–12pm, 3pm–5pm"

**Bar Charts** (`.charts`, right side)
- Dynamic selection — not all metrics shown always:
  - **Score** — always first
  - **Active metrics** — only those with ≥1 non-go hour, sorted by worst status (no_go first → caution → advisory)
  - **Temp** — always last
- Each bar chart: column of bars (one per hour), height = normalized value, color = metric status (or best-window green for Score)
- Tapping a column opens a tooltip with: hour label, status, reason text, and individual component bar scores

**Bar colors (CSS variables):**
- `--bar-window: #16a34a` (best window hours in Score chart)
- `--bar-go: #4ade80`
- `--bar-advisory: #38bdf8`
- `--bar-caution: #f59e0b`
- `--bar-no_go: #ef4444`

**Key JS functions:**
- `buildStatusBoard(hours)` — generates the status board HTML
- `buildMetricRow(metric, hours, day)` — generates one bar chart row
- `render({ beach, day, hours })` — main render function; selects active metrics dynamically

---

### `find.html` — Find Your Beach

**Data source:** `get-beach-compare`

**Features:**
- Sort modes:
  - **Best Conditions** — `composite_score` descending
  - **Closest First** — Haversine distance ascending (requires location permission)
  - **Best Nearby** — 50/50 blend of normalized score and inverted distance
- Location via browser Geolocation API; reverse geocoded to city name via Nominatim
- ZIP code override: forward geocoded via Nominatim
- Distance-based sort options disabled until location is available
- Beach cards: distance badge, composite score, status, best window label, weather

---

### `compare.html`
Redirects to `find.html` via `<meta http-equiv="refresh">`. Legacy URL compatibility only.

---

## Auth / Keys

- **Anon key** (`sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk`) — used in all HTML pages. Safe to commit — RLS is the security layer, not key secrecy.
- **Service role key** — only in Supabase Edge Function environment variables. Never in frontend code.
- Previous anon key (`sb_publishable_o2d4Kg0xMZoMw92feO2uWQ_PdDfQXKj`) was rotated 2026-04-15.

---

## Rolling Actuals Model

The hourly cron (`hourly-beach-now-refresh`, runs `0 * * * *`) calls `get-beach-now` as a POST with no body, which refreshes all active beaches. Each refresh:

1. Fetches live weather and tides for the current local hour
2. Runs the full scoring engine (same as the daily pipeline)
3. Clears `is_now = true` from the previous NOW row for that beach
4. Upserts the new row with `is_now = true`, overwriting the forecast row for that timestamp

By the end of each day, all past hours have been overwritten with observed actuals. The forecast row for the current hour is always replaced with live data.

Implemented via: `supabase/migrations/20260417_is_now.sql` (column + partial index) and `supabase/migrations/20260417_hourly_now_cron.sql` (pg_cron job using pg_net).

---

## Git Workflow

- All development and production on branch: **`main`**
- GitHub Pages serves `main` directly
- Local testing: open HTML files directly in browser (file:// is whitelisted in CORS)
- Test on exploratory branches for risky changes; only merge to main after confirming it works
- Never auto-merge to main without explicit user approval

---

## Migrations

All SQL migrations in `supabase/migrations/`, applied manually via the Supabase dashboard SQL editor. Filename format: `YYYYMMDD_description.sql`.

Key migrations in order:
| File | Purpose |
|---|---|
| `20260414_add_beach_practical_info.sql` | Address, website, description, parking_text to beaches |
| `20260415_add_component_scores_to_hourly.sql` | Per-metric score columns on hourly table |
| `20260415_add_temp_thresholds_and_metric_statuses.sql` | Temp thresholds + metric status columns |
| `20260415_chat_rate_limits.sql` | chat_rate_limits table |
| `20260415_enable_rls.sql` | RLS on all tables |
| `20260415_window_score_threshold.sql` | window_score_threshold to scoring_config |
| `20260416_bacteria_risk.sql` | bacteria_risk to daily recommendations |
| `20260416_revoke_rpc_anon.sql` | Remove anon access to RPCs |
| `20260417_advisory_tier.sql` | feels_like, sand/asphalt temps, caution WMO codes, crowd/sand/asphalt thresholds |
| `20260417_scoring_thresholds.sql` | Advisory thresholds for all metrics; temp cold/hot split |
| `20260417_advisory_status_values.sql` | Add 'advisory' to CHECK constraints on status columns |
| `20260417_is_now.sql` | is_now column + partial index on hourly scores |
| `20260417_hourly_now_cron.sql` | pg_cron hourly job calling get-beach-now |

---

## Data Ingestion Pipeline — Evidence → Resolve → Promote

The catalog ingest pipeline (separate from the consumer-facing scoring app) follows a layered architecture for collecting and reconciling beach metadata across multiple external sources (CPAD, CCC, TIGER places, NPS, LLM research, park URL scrapes, etc.). All evidence-bearing populators write into `beach_enrichment_provenance` (one row per `(fid, field_group, source, source_url)`), then resolvers pick canonical winners, and promoters write canonical values back to `locations_stage` columns.

### Function family

| Layer | Function pattern | Purpose |
|---|---|---|
| 1. Emit | `_emit_evidence_from_<source>(p_fid)` | Read source data, INSERT/UPSERT evidence rows. No canonical mutation. Idempotent via `ON CONFLICT (fid, field_group, source, source_url)`. |
| 2a. Rank | `_rank_<source>_evidence(p_fid, p_field_group)` | When one source contributes multiple evidence rows per beach (e.g., CPAD candidate fan-out), produce a ranked temp table `_resolver_ranked` per Tier 1 rules. |
| 2b. Resolve | `_resolve_<field_group>(p_fid)` | Apply per-field-group cross-source overrides; set `is_canonical=true` on the winning evidence row. |
| 3. Promote | `_promote_<field_group>_to_stage(p_fid)` | Read canonical evidence's `claimed_values` jsonb, write to `locations_stage` columns. Preserves existing values where claimed_values is null. Validates enums + casts types. |
| 4. Flag | `_compute_review_flags(p_fid)` | Detection-only. Updates `locations_stage.review_status` / `review_notes` for ambiguous cases. |
| Public | `populate_from_<source>(p_fid)` | Orchestrator. Calls emit → resolvers → promoters → flags in order. |

### Tier 1 ranking (in priority order)

When a single source produces multiple evidence rows for one beach (e.g., CPAD candidate fan-out via `beach_cpad_candidates`):

1. **Demote environmental overlays** — Marine Parks / Ecological Reserves / Wildlife Areas lose to non-overlay candidates, BUT keep overlay as fallback when no alternative exists.
2. **Containing CPAD with "Beach" in name wins** — catches the Coronado Municipal Beach pattern.
3. **Trigram similarity to display_name** — picks Mission Beach Park over Mission Bay Park for "Mission Beach".
4. **Smallest CPAD area** — most-specific polygon wins.

Final tiebreaks: confidence desc, id asc.

### Cross-source override patterns (governance field_group)

Three override layers in `_resolve_governance(p_fid)`:

1. **State-park override**: `park_url`/`park_url_buffer_attribution` beats `tiger_places`/`cpad` when source CPAD's `unit_name` matches `\m(state beach|state park|state recreation)\M`. Always beats `name`/`governing_body` (weak inference signals).
2. **Tiger-vs-operator override** (`_resolve_tiger_vs_operator`): when `tiger_places` holds canonical and any operator-source disagrees, tiger loses. Operator candidates ranked by (a) trigram name-similarity, (b) park_url-agreement bonus, (c) hierarchy fallback: `nps_places > csp_parks > tribal_lands > military_bases > park_operators > cpad > park_url`.
3. **Never overridden**: `manual` source (always wins per resolution-rules-design memory).

### Audit trail

Every evidence row carries:
- `cpad_unit_name` — which CPAD unit supplied the data (for park_url-derived sources)
- `extraction_type` — `cpad_source` (URL from CPAD park_url field), `cpad_source_crawl` (URL discovered via CPAD agncy_web sitemap-grep), or `derived_url_crawl` (future: external derivation like place-name + site:search)
- `cpad_role` — `beach_access` or `environmental_overlay`
- `source_url` — the URL the evidence was extracted from
- `source` — which populator emitted it (`park_url`, `cpad`, `tiger_places`, `csp_parks`, `nps_places`, `tribal_lands`, `military_bases`, `park_operators`, `park_url_buffer_attribution`, `name`, `governing_body`, `manual`, etc.)

URL-discovery attempts (sitemap-grep, etc.) log every outcome to `discovery_attempts` with status in `(success, no_sitemap, no_match, agency_skipped, agency_missing, fetch_error)`.

### Review flags

Set on `locations_stage.review_status='needs_review'`, detail in `review_notes`:

| Flag | Trigger |
|---|---|
| `multi_cpad_disagreement` | Beach has ≥2 successful park_url extractions from different CPADs in same field_group |
| `source_governing_mismatch` | Source CPAD ≠ strict-containing CPAD |
| `dogs disagreement (research more permissive)` | LLM research extraction differs from agency-default (pre-existing) |

Flags refresh in place via `regexp_replace` — no append-duplication on re-run.

### Adding a new source

To add a new evidence source (e.g., PAD-US for Oregon, a new research scrape, an external API):

1. Create `_emit_evidence_from_<source>(p_fid)` that inserts evidence rows with all standard audit columns. Use `ON CONFLICT (fid, field_group, source, coalesce(source_url, ''))`.
2. If the new source has its own value in `beach_enrichment_provenance.source` CHECK constraint, extend that constraint.
3. If multiple evidence rows per beach are possible, write `_rank_<source>_evidence` (or reuse `_rank_park_url_evidence` if the same Tier 1 rules apply).
4. Decide cross-source override semantics: does this new source beat existing ones for any field_group? Extend `_resolve_<field_group>` accordingly.
5. The promoter, flag computation, and orchestrator wrap the existing pieces — usually no changes needed unless the new source has unique field_groups.

See `project_pipeline_refactor_trigger.md` memory for guidance on when to refactor the helper-function family vs. extending in place.

---

## SMS Alerts (Blocked)

`send-daily-alerts` and the full subscriber pipeline (tables, Twilio integration) are built but not operational. Twilio toll-free number verification is backlogged. Will resume once the from-number is approved.

---

## Current State (as of 2026-04-17)

- All 5 beaches have live data; daily pipeline and hourly actuals cron both running
- Four-tier status system (go/advisory/caution/no_go) fully implemented end-to-end
- Rolling actuals: hourly cron overwrites forecast rows with live data via `is_now` flag
- `detail.html` has status board + dynamic chart selection (only non-go metrics shown, sorted by severity; Score always first, Temp always last)
- Security hardening complete: RLS, CORS origin whitelist, chat rate limiting, anon key rotation
- Schema + seed data backed up to `supabase/backup/`

### Near-term backlog
- SMS alerts: resume once Twilio toll-free verification completes
- Add filters to `find.html` (off-leash zones, amenities) once `beaches` table is enriched
- Consider pre-computing `composite_score` in `beach_day_recommendations` instead of computing at query time
