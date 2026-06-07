# Dog Beach Scout — CLAUDE.md

Project reference for Claude Code. Authoritative guide to architecture, data model, scoring, frontend, edge functions, and orchestration. Updated post-path-3 (2026-05-02). Memory files (`~/.claude/projects/C--Users-beach/memory/`) carry the moving parts — this file is the stable map.

---

## What This Is

**Dog Beach Scout** is a mobile-first web app that tells dog owners when and where to take their dogs to the beach. Two parallel pipelines:

1. **Catalog ingest** — collects + classifies + dedups beach inventory across CA (and seeded OR), enriches with operator / dog policy / CPAD / CCC overlays. ~764 active beaches in `beaches_gold` as of 2026-05-02.
2. **Consumer scoring** — for the curated subset (`is_scoreable=true`, currently 304 SoCal + 5 OR), runs daily 7-day forecasts (weather + tides + crowds → composite score + best window) and hourly NOW updates.

Hosted on GitHub Pages (`beachfranz.github.io`), backed by Supabase (Postgres + Edge Functions). Orchestrated by Dagster (`scripts/dagster/dog_beach`) with dbt models (`scripts/dbt/dog_beach`) for staging/marts.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Vanilla HTML/CSS/JS — `index.html`, `detail.html`, `find.html`, `paywall.html` (admin gate) |
| Backend | Supabase Edge Functions (Deno/TypeScript) |
| Database | Supabase Postgres + PostGIS |
| AI chat | Anthropic API — `claude-sonnet-4-20250514` via `beach-chat` |
| LLM extraction | Anthropic API — Sonnet for text/JSON, Haiku for enum/bool, with prompt caching |
| Weather | Open-Meteo (forecast + current) |
| Tides | NOAA CO-OPS API |
| Crowds | BestTime.app (busyness 0–100) |
| Maps | Leaflet + `tile.openstreetmap.fr` (per `feedback_map_tile_provider.md`) |
| Orchestration | Dagster (Python 3.11+, `dagster>=1.13`) |
| Transformations | dbt-postgres |
| Hosting | GitHub Pages (`main` branch, repo root) |

---

## Beaches Inventory

The pipeline touches three populations, in increasing curation:

| Layer | Table | Purpose | Approx count |
|---|---|---|---|
| Master inventory | `arena` | Per-state dedup'd beach catalog (POI + OSM + CCC sources) | ~1,250 active CA |
| Cross-state spine | `beaches_gold` | Promoted heads from arena. The post-path-3 spine. | 764 active |
| Scoring set | `beaches_gold WHERE is_scoreable=true` | Beaches the daily pipeline actually scores | 304 SoCal + 5 OR seeds |

`is_scoreable` is the fan-out gate — flipped on by `bulk_promote_socal.py`, `seed_arena_beach.py --score`, or manual curation. Without it, daily-beach-refresh would explode from 309 to 764 calls.

`beaches_gold.location_id` is the human-readable slug (e.g. `coronado-dog-beach`); `beaches_gold.fid` is the canonical numeric ID inherited from arena. HTML pages prefer `?fid=<n>` and fall back to `?location_id=<slug>`.

---

## Repository Layout

```
dog-beach-claude/
├── index.html                       # Home — 7-day forecast for one beach
├── detail.html                      # Hour-by-hour detail
├── find.html                        # Discovery + sort across scored beaches
├── paywall.html                     # Admin gate (obscure-URL + x-admin-secret)
├── compare.html                     # Legacy redirect → find.html
├── src/
│   ├── avatar.png                   # Scout avatar
│   ├── chat.css / chat.js           # Reusable chat library
│   └── ...
├── supabase/
│   ├── functions/
│   │   ├── _shared/                 # cors, scoring, admin-auth, config
│   │   ├── daily-beach-refresh/     # nightly: weather+tides+crowds+score
│   │   ├── get-beach-now/           # hourly: live actuals upsert
│   │   ├── get-beach-summary/       # API: 7-day rollup for one beach
│   │   ├── get-beach-detail/        # API: hour-by-hour for one beach/day
│   │   ├── get-beaches-find/        # API: ranked list for find.html
│   │   ├── beach-chat/              # API: Scout AI chat
│   │   ├── get-calendar-event/      # API: ICS for best window
│   │   ├── send-daily-alerts/       # SMS (Twilio — blocked)
│   │   ├── admin-*/                 # ~25 admin tools (CRUD on beaches/dupes/policy)
│   │   └── v2-*/                    # ~25 catalog ingest steps (classify, dedup, promote)
│   ├── migrations/                  # Numbered SQL migrations (manual apply)
│   └── backup/                      # schema.sql + data.sql snapshots
├── scripts/
│   ├── pipeline/.env                # SUPABASE_URL, SERVICE_KEY, DB_PASSWORD, ADMIN_SECRET, ANTHROPIC_API_KEY
│   ├── seed_arena_beach.py          # One-off beach seeder (arena+gold; --score also fires refresh)
│   ├── bulk_promote_socal.py        # Region promote — backfill metadata + flip is_scoreable + chunked refresh
│   ├── extract_for_orphans.py       # LLM extraction for catalog metadata
│   ├── extract_operator.py          # LLM operator signal
│   ├── extract_*.py                 # CPAD / CDPR / dog-policy extractors
│   ├── load_cpad.py / load_*.py     # External-source loaders
│   ├── one_off/                     # Audits + one-time backfills
│   ├── dagster/dog_beach/           # Dagster project (orchestration)
│   └── dbt/dog_beach/               # dbt project (staging + marts)
└── tmp/                             # Throwaway exploratory artifacts (maps, CSVs)
```

---

## Database Schema

70+ tables in `public`. The ones you'll touch most:

### Consumer surface (read by edge functions, hand-curated content)

- **`beaches_gold`** — the spine. `fid` PK passes through from arena. Columns: identity (name, lat/lon, county, state, geom), scoring metadata (`noaa_station_id`, `besttime_venue_id`, `timezone`, `open_time`, `close_time`, `location_id` slug), promotion audit (`promoted_from`, `promoted_at`), gate (`is_scoreable`), and `display_name_override` for friendlier UI names.
- **`beach_dog_policy`** — curated dog-access overlay, FK→`beaches_gold.fid`. Columns: `dogs_allowed`, `leash_policy`, `off_leash_flag`, `dogs_prohibited_start/end`, `dogs_allowed_areas`, `access_rule`. HTML reads from here (not from cascade verdicts).
- **`beach_day_hourly_scores`** — one row per beach per hour. PK now `(arena_group_id, forecast_ts)` (post path-3b PK swap). Carries `hour_score`, all metric scores + statuses, `is_now`, `explainability` JSONB.
- **`beach_day_recommendations`** — daily rollup, one row per beach per day. PK `(arena_group_id, local_date)`. Carries `day_status`, `best_window_label/start/end`, hour counts, narrative.
- **`scoring_config`** — versioned weights/thresholds. Pipeline always loads `is_active=true`.

### Catalog ingest (the dedup + classify + enrich machinery)

- **`arena`** — master CA beach inventory. POI + OSM + CCC consolidated. `fid` canonical, `group_id` clusters dupes, `nav_lat/lon` is point-on-surface, source_code = `osm`/`poi`/`ccc`/`manual`.
- **`poi_landing`, `osm_landing`, `ccc_landing`** — raw mirrors of the three sources. PK includes `fetched_at` so reloads accumulate history.
- **`us_beach_points`** — legacy 8K-row CSV mirror. No longer consumed by edge functions; kept as Geoapify provenance reference.
- **`cpad_units`** (77 MB) — California Protected Areas Database, 17,239 polygons. FK target is `unit_id`, NOT `objectid` (`project_dog_policy_exceptions_canonical.md`).
- **`ccc_access_points`** — California Coastal Commission access points. Identity = access point, NOT beach (`project_ccc_access_point_semantic.md`).
- **`osm_features`** (66 MB) — OSM beach/park polygons. Operational layer (dog rules, fences, amenities).
- **`jurisdictions`** — TIGER places (CA, 1,618 polygons).
- **`counties`, `states`, `noaa_stations`, `military_bases`, `tribal_lands`, `nps_places`, `csp_parks`, `mpas`, `waterbodies`** — reference layers.
- **`beach_enrichment_provenance`** — evidence layer for the catalog pipeline. One row per `(fid, field_group, source, source_url)`. See "Catalog Ingest Pipeline" below.
- **`operators`, `operator_dogs_policy`, `operator_policy_exceptions`** — operator-keyed dog policy.
- **`cpad_unit_dogs_policy`, `cpad_unit_policy_exceptions`** — unit-keyed dog policy.
- **`beach_verdicts`** — output of `recompute_all_dogs_verdicts_by_origin()` cascade. Per-origin-key dog verdict + confidence + sources JSONB.
- **`beach_policy_extractions`, `beach_policy_gold_set`, `extraction_calibration`, `extraction_prompt_variants`, `policy_research_extractions`, `park_url_extractions`** — LLM extraction stack.

### Operations / audit

- **`subscribers`, `subscriber_locations`, `notification_log`** — SMS pipeline (PII; blocked from anon).
- **`chat_rate_limits`** — IP+hour bucket for Scout chat (max 20/hr).
- **`refresh_errors`** — pipeline error log.
- **`admin_audit`, `admin_rate_limits`** — admin endpoint logging + rate limits.

### Key views

- **`arena_beach_metadata`** — joined arena identity + canonical extractions per field. Read by `get-beach-detail`.
- **`beach_locations`** — legacy UBP+CCC dedup view (~805 rows). Cascade still reads it; HTML does not.
- **`beach_policy_consensus`** — canonical_value per (fid, field_name) over extractions.
- **`truth_comparison_v`** — verdict cascade vs. external truth-set.

### RLS

- Anon: SELECT-only on `beaches_gold`, `beach_dog_policy`, `beach_day_hourly_scores`, `beach_day_recommendations`.
- All other tables blocked to anon.
- Edge functions use the service role key and bypass RLS.

### `operator` vs `operators` — confusingly named, intentionally separate

Two tables. NOT duplicates. Both load-bearing:

| Table | Rows | Role |
|---|---|---|
| `public.operator` (singular) | ~159 | **CANONICAL CONSUMER-SURFACE REGISTRY.** Hand-curated. Includes nonprofits + sub-agencies not in TIGER. Cascade-wired via `entity_operator`, `beach_operator`, `policy_source.issuing_operator_id`. **This is what shows up on `beach.html`.** |
| `public.operators` (plural) | ~9,275 | **JURISDICTION/TIGER EXTRACTION POOL.** Bulk-loaded from TIGER cities, CPAD units, OSM. Used by the operator_llm_extract pipeline (`operator_dogs_policy.operator_id` + `operator_policy_extractions.operator_id` FK here). |

Bridge: `operators.canonical_operator_id BIGINT REFERENCES operator(id)` — formal FK added 2026-05-22 (`20260522_operators_canonical_bridge.sql`). Replaces the brittle `LOWER(name)` joins used previously.

**When writing new code**: prefer the helpers in `scripts/common/operator.py`:
- `get_canonical_operator(name=… or id=…)` → row from `operator` (singular)
- `get_extraction_operator(canonical_name=… or id=…)` → row from `operators` (plural)
- `canonical_for_extraction(extraction_id)` → bridge plural → singular via FK
- `extractions_for_canonical(canonical_id)` → reverse bridge singular → plural

**When adding a NEW operator** (e.g., discovering a nonprofit in a new state):
- Use `add_canonical_operator(name=…, op_type=…, state_code=…, apply=True)` — atomically inserts into BOTH `operator` and `operators` with the FK pre-set.
- **Always populate both tables** (Franz decision 2026-05-22 LATE). Don't repeat the 4-orphan pattern that CA accidentally accumulated. The helper handles this for you.

Table-level `COMMENT ON TABLE` also documents the distinction; visible in `\d operator` / `\d operators`.

See HARD memory pin `operate-two-table-finding`.

---

## Supabase CLI

Installed at `~/scoop/shims/supabase` (v2.90.0+). Project linked to **dog-beach-AI** (ref `ehlzbwtrsxaaukurekau`, East US).

```bash
supabase db query --linked -f supabase/migrations/<file>.sql   # apply migration
supabase db query --linked "SELECT count(*) FROM public.beaches_gold WHERE is_scoreable"   # ad-hoc
scripts/deploy_edge_function.sh <fn-name>                      # canonical deploy (always --no-verify-jwt)
scripts/deploy_edge_function.sh --cron                          # redeploy all cron-targeted functions
scripts/deploy_edge_function.sh --all                           # redeploy every function
```

**Always deploy edge functions through `scripts/deploy_edge_function.sh`.** A bare `supabase functions deploy <name>` will succeed but the function returns 401 `UNAUTHORIZED_INVALID_JWT_FORMAT` on every call — `sb_publishable_` is not a JWT, so the missing `--no-verify-jwt` flag breaks the function silently. This pattern took out `daily-beach-refresh` for 4 days (2026-05-30 → 2026-06-03) before being caught by the new orch response reconciler.

For Python scripts: `scripts/pipeline/.env` carries `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_DB_PASSWORD`, `ADMIN_SECRET`, `ANTHROPIC_API_KEY`. Pooler URL at `supabase/.temp/pooler-url`. **Always check existing stores before asking for a new key** (`feedback_check_secrets_first.md`). When appending to `.env` check for trailing newline first (`feedback_env_append_check_newline.md`).

---

## Edge Functions

### Consumer reads (browser-facing, anon-key)

- **`get-beach-summary`** `?fid=<n>` (or `?location_id=<slug>`) — `{ beach, allBeaches, days[7] }` from `beaches_gold` + `beach_day_recommendations`.
- **`get-beach-detail`** `?fid=<n>&date=<YYYY-MM-DD>` — `{ beach, day, hours[] }` from `beaches_gold` + `beach_day_hourly_scores`.
- **`get-beaches-find`** — calls `find_beaches` RPC; `?lat&lng` for distance sort, `?leash`, `?date`, `?scored_only` (default true), `?limit`. Returns `{ date, is_today, beaches[] }`.
- **`get-beach-compare`** — predecessor of get-beaches-find; still wired but find.html is canonical.
- **`beach-chat`** POST `{ fid|location_id, question }` — Anthropic-backed Scout chat with beach + day context + dog policy injected. Rate-limited 20/IP/hour via `increment_chat_rate` RPC.
- **`get-calendar-event`** — ICS download for the best window.

### Writers (cron + admin)

- **`daily-beach-refresh`** — fans out over `beaches_gold WHERE is_scoreable=true`. For each: 7-day Open-Meteo, NOAA tides, BestTime crowds → `scoreHours` from `_shared/scoring.ts` → upsert `beach_day_hourly_scores` + `beach_day_recommendations` (keyed on `arena_group_id`) + Claude narrative. Admin-gated via `x-admin-secret` header. Accepts `{ location_ids: string[] }` body to scope to a subset.
- **`get-beach-now`** — hourly cron via `pg_cron`/`pg_net.http_post`. For each scoreable beach: live Open-Meteo + NOAA → score → upsert the `is_now=true` row, overwriting forecast for that timestamp.
- **`send-daily-alerts`** — SMS via Twilio. Blocked on toll-free verification.

### Admin tools (`admin-*`, ~25 functions)

CRUD over beaches, dupes, geocoding, off-leash flags, source classification, policy re-extraction. All require `x-admin-secret` header (`_shared/admin-auth.ts`'s `requireAdmin()`). Currently no JWT auth — obscure URL + secret. The repo is public (`project_public_repo.md`), so the obscure URL is **not** a security control. Real auth is a parked decision (`project_admin_access_future.md`, `project_next_session_admin_security.md`).

### Catalog ingest (`v2-*`, ~25 functions)

Step-wise pipeline that classifies + enriches beaches in `beaches_staging_new`. Examples: `v2-county-classify`, `v2-private-land-filter`, `v2-state-classify`, `v2-noaa-station-match`, `v2-promote-to-beaches`. The orchestrator is `v2-run-pipeline`. Architecture documented at the bottom of this file under "Catalog Ingest Pipeline".

### PostgREST 1000-row cap — silent truncation

Supabase applies `db-max-rows=1000` to every PostgREST response (REST `.select()` AND RPC results). Queries matching more rows are silently capped at 1000. PostgREST signals this via HTTP **206 Partial Content** + a `Content-Range: 0-999/<total>` header, and supabase-js surfaces it as `{ count, status }` on the response — but the JSON body (`data`) looks fine. If the function only checks `data` and `error`, the truncation is invisible.

This caused the 2026-06-05 14-day stuck-Luhr-Jensen blackout: `daily-beach-refresh` queried `beaches_gold` (2,889 scoreable beaches), got 1,000 back, and processed only those. The remaining 1,889 — all higher-fid states OR/WA/NH/MI/OH/RI/VA — were invisible to the function for days.

**Two fixes, depending on shape**:

1. **Selection-style queries** (pick N beaches to process): use an aggregating SQL function called via `supabase.rpc(...)` that does the entire selection DB-side and returns ≤ N rows. The result count never exceeds the cap. See `beaches_due_for_refresh(target_fids, target_location_ids, skip_recent_hours, limit)` + `dog_parks_due_for_refresh(...)` as the canonical pattern.

2. **Detail-style queries** (load metadata for a known-bounded set of fids): use `.in("fid", fids)` where `fids.length ≤ 40` or similar, AND wrap with `ensureNotTruncated()` from `_shared/safeSelect.ts` to catch future scope drift.

**`ensureNotTruncated()` usage**:

```ts
import { ensureNotTruncated } from "../_shared/safeSelect.ts";

const result = await supabase
  .from("beach_day_recommendations")
  .select("arena_group_id, generated_at", { count: "exact" })
  .in("arena_group_id", fids);
ensureNotTruncated(result, "beach-recs by fid");
const { data, error } = result;
```

`{ count: 'exact' }` makes PostgREST send back the full count even when truncating. The helper compares `data.length` to `count`; warns on mismatch (or throws with `{ hard: true }`). Skipping `{ count: 'exact' }` makes the helper warn that detection is disabled.

**Audits**:
- `_orch_w_chunked_refresh_freshness_audit` watches per-fid staleness (catches the downstream symptom: beaches stuck stale because the function never sees them).
- `_orch_w_orch_deps_health_audit` watches orch_jobs.depends_on for dead refs (catches the upstream symptom: jobs silently skipped).

Encoded HARD rule: [[paired-functions-port-fixes-both-sides]] — when fixing a query pattern on the beach side, sweep the dog-park sibling in the same change.

### Auth model

Browser-facing reads use the publishable anon key:
```
sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk
```
Safe to commit — RLS is the security layer, not key secrecy. (Previous key rotated 2026-04-15.)

`x-admin-secret` header gates admin + writer endpoints. Service role key is in Supabase env vars only — never in frontend.

---

## CORS

`_shared/cors.ts` enforces an origin allowlist:
- `https://beachfranz.github.io` (prod)
- `null` (file:// for local dev)

Every function calls `corsHeaders(req, methods)` and includes the result on every response, including OPTIONS preflight.

---

## Scoring Model

All scoring lives in `supabase/functions/_shared/scoring.ts`. Both `daily-beach-refresh` and `get-beach-now` import from it.

### Four-tier status system

`go` / `advisory` / `caution` / `no_go`. `hour_status` = worst across all metrics.

| Status | Color CSS var | Meaning |
|---|---|---|
| `go` | `--green: #4ade80` | No concerns |
| `advisory` | `--advisory: #38bdf8` | Minor concern |
| `caution` | `--amber: #fbbf24` | Meaningful concern |
| `no_go` | `--red: #ef4444` | Not recommended |

### Per-metric thresholds (all from `scoring_config`)

- **Tide** (ft): caution ≥ 5.0, advisory ≥ 3.0
- **Wind** (mph): no_go ≥ `nogo_wind_speed`, caution ≥ 15, advisory ≥ 10
- **Rain** (precip% + WMO): see `caution_wmo_codes` in DB; severe codes (95–99, 63–67, 71–77, 82) hardcoded as `no_go`
- **Crowd** (BestTime 0–100): no_go > `advisory_crowd_max` (84), advisory in [61, 84]
- **Temp (`feels_like`)** — split hot/cold: cold no_go < 20°F, hot no_go > 95°F; advisory ranges [32, 50] / [75, 85]
- **UV**: advisory ≥ 6 (raised from 3 on 2026-04-17 — UV 3 fired every clear afternoon and became noise), caution ≥ 8, no_go ≥ 11
- **Sand temp** (estimated from temp_air): advisory ≥ 105°F, caution ≥ 115°F, no_go ≥ 125°F
- **Asphalt temp**: same tier structure as sand

### Composite score (0–100)

Weighted sum of normalized component scores (all from `scoring_config`):

| Component | Weight | Notes |
|---|---|---|
| Tide | 22.5% | normalized vs `norm_tide_max` |
| Wind | 20% | normalized vs `norm_wind_max` |
| Rain | 17.5% | precip% lower = better |
| Crowd | 15% | busyness lower = better |
| Weather code | 15% | WMO → fixed score (clear=1.0, severe=0.0) |
| Temp | 5% | bell curve around `norm_temp_target` ± `norm_temp_range` |
| UV | 5% | UV lower = better |

`hour_score` is null when `is_daylight=false` or beach is closed.

### Best window

Candidate hours = daylight + `hour_status != 'no_go'` + `hour_score >= window_score_threshold`. Pick longest contiguous block (2–5 hours). Label format: `"10am–2pm"`.

### Day status

`day_status` derives from majority of `hour_status` across the day's daylight hours. Composite for the day = avg `hour_score` across best-window hours.

---

## Frontend Pages

### `index.html` — Home / 7-Day Forecast
Reads `get-beach-summary?fid=...`. Renders NOW card (live `get-beach-now`) + 7 day cards. Tapping a card → `detail.html?fid=...&date=...`. Location switcher dropdown from `allBeaches`.

### `beach.html` — Beach Page (dog-policy focused)
Accepts `?fid=<n>` or `?location_id=<slug>` (resolves slug → fid via PostgREST `beaches_gold`). The dog-policy-focused beach view — reads from `beach_dog_policy` (curated overlay) for the leashy verdict, plus the photo block + zone-rules block. This is the page to verify after cascade work: tonight's `compute_beach_field_consensus` + `promote_canonical_dogs_to_beach_dog_policy` propagation flows directly to what this page renders.

### `detail.html` — Hour-by-Hour Detail
Reads `get-beach-detail?fid=...&date=...`. Sticky header with best window + Scout blurb. Status board (left) lists only metrics with ≥1 non-go hour, with time-range blocks. Bar charts (right) dynamically picked: Score first, active metrics by severity, Temp last. Tapping a bar opens tooltip with reason + component scores. Scout chat panel at the bottom.

### `find.html` — Discovery
Reads `get-beaches-find` (calls `find_beaches` RPC). Three sort modes: Best Conditions / Closest First / Best Nearby. Geolocation via browser API + Nominatim reverse geocode; ZIP override via Nominatim forward geocode. Toggle: Scored only (default) vs Full catalog (UI exists; RPC `scored_only=false` currently has a default-LIMIT bug).

### `compare.html`
Legacy redirect to `find.html` via `<meta http-equiv="refresh">`.

### `paywall.html`
Admin gate. `x-admin-secret` collection + localStorage stash, used by curator/admin tooling.

### Status colors
CSS variables shared across pages: `--green #4ade80`, `--advisory #38bdf8`, `--amber #fbbf24`, `--red #ef4444`. Best-window highlight `--bar-window #16a34a`.

### Maps
Leaflet + `tile.openstreetmap.fr` (OSM Carto). Voyager / Stadia / OpenFreeMap as fallbacks. Public OSMF tile server is blocked from Franz's network — don't use it. (`feedback_map_tile_provider.md`)

---

## Rolling Actuals (NOW)

Hourly cron `hourly-beach-now-refresh` (`pg_cron`) fires `get-beach-now` POST with no body → refreshes every scoreable beach. Each refresh:
1. **Weather: reads from `weather_grid_hourly` via `weather_for_point()` RPC** (Phase W2 cutover, 2026-05-25). Falls back to live Open-Meteo if grid cell unloaded.
2. Live NOAA tide for current hour
3. ~~Borrows busyness_score from existing DB row~~ — BestTime soft-removed; crowd score = 0.5 neutral
4. Runs `scoreHours()` from `_shared/scoring.ts`
5. Clears previous `is_now=true` row for the beach
6. Upserts new row with `is_now=true`, overwriting the forecast for that timestamp

By end of day all past hours have been overwritten with observed actuals.

Migrations: `20260417_is_now.sql` + `20260417_hourly_now_cron.sql`.

---

## Weather as Reference Layer (Phase W1+W2, 2026-05-25)

Weather is a **reference layer** in the same shape as `jurisdictions` / `cpad_units` / `osm_features` — peer to other spatial reference tables. Single loader populates a gridded forecast/observed table; beach + dog-park + future-trail consumers JOIN against it. **No per-entity Open-Meteo fetches** anywhere in the consumer path.

### Tables + helpers

- **`weather_grid`** — materialized inventory of 0.025° (~3km) cells with ≥1 consumer entity. ~4,000 cells across CA + OR + WA + early eastern states. Auto-populated by triggers on entity tables.
- **`weather_grid_hourly`** — 168-hour rolling forecast per cell + past 3 days observed. PK `(grid_lat, grid_lon, forecast_ts)`. `is_observed=true` for past hours (bacteria-grade), `false` for forecast.
- **`weather_for_point(lat, lng, start_ts, end_ts)`** — primary consumer read entry point. Returns hourly weather rows for the cell containing the point within a time window.
- **`weather_grid_bin_lat(lat)` / `weather_grid_bin_lon(lng)`** — bin-floor helpers used by the entity-table triggers + consumers.
- **`precip_72h_for_point(lat, lng, anchor_date)`** — convenience for bacteria-risk.
- **`precipitation_history`** (view) — daily SUM(precip_mm) over observed hours per cell × date. Replaces inline 72h precip computation in daily-beach-refresh.

### Materialization

Cached `weather_grid_lat` / `weather_grid_lon` columns on `beaches_gold` + `dog_parks_gold`. Trigger `tg_compute_weather_grid_cell` computes them on lat/lon insert/update AND auto-registers the cell in `weather_grid`. Same pattern as `beach_polygon_membership`. Backfilled for existing rows.

Consumer reads are pure JOIN — no function calls at query time:
```sql
SELECT wgh.* FROM weather_grid_hourly wgh
JOIN beaches_gold b ON b.weather_grid_lat = wgh.grid_lat
                  AND b.weather_grid_lon = wgh.grid_lon
WHERE b.fid = $1 AND wgh.forecast_ts BETWEEN $2 AND $3;
```

### Loader

`scripts/dagster/dog_beach/dog_beach/assets/weather_grid.py` (Python on Dagster — NOT edge function, sidesteps WORKER_RESOURCE_LIMIT). Per-cell tier-based stale thresholds:
- Cells with ≥1 `scoring_tier='hourly'` beach → 1h fresh threshold
- Cells with active `daily`-tier beach or scoreable dog park → 6h
- Cells with no active consumers → 24h

Multi-location batched Open-Meteo fetch (~100 cells per API call). Schedule: hourly (`hourly_weather_grid_schedule`, default STOPPED — toggle ON in Dagster UI). Daily backstop rebuild (`daily_weather_grid_inventory_schedule`) re-UNIONs from entity tables in case triggers drift.

### Consumer cutover (Phase W2)

Four edge functions read from grid instead of fetching Open-Meteo:
- `daily-beach-refresh` (W2.1) — `fetchWeatherFromGrid()` replaces `fetchWeather()`. NOAA also gated to cache-only (only weekly cron fetches).
- `daily-dog-park-refresh` (W2.2) — same swap.
- `get-beach-now` (W2.3) — `fetchCurrentWeatherFromGrid()` replaces `fetchCurrentWeather()`.
- `get-dog-park-now` (W2.4) — same swap.

### Cost / call volume

Pre-cutover: ~16,500 Open-Meteo calls/day across all consumers. Post-cutover: <100/day (mostly the grid loader + occasional NOW-path fallback). ~165× reduction.

### Per-cell warmth gate — the canonical pattern (2026-06-06)

Every consumer that reads from `weather_grid_hourly` (or the analogous `marine_grid_hourly`) MUST gate on per-cell coverage. Falling back to per-entity direct Open-Meteo fetch on partial cells is the **anti-pattern** — it re-introduces N calls every fresh-state launch and erodes the 165× cost-collapse. Pin: `[[grid-consumers-require-cell-warmth-gate]]`.

The loader fans out per-cell fetches across batches over minutes-to-hours (per-tier schedule + retry-with-backoff). If a consumer races the loader, it sees partial coverage. Without a gate, it writes partial output silently — the canonical incident was HI fid 13882 on 2026-06-07: daily-beach-refresh fired at UTC 00:21 when t1 hadn't yet covered UTC 06-08 in the cell. The function wrote 213 of 240 expected hourly rows; bar charts on detail.html ended at HI 1PM.

**Two gate shapes:**

1. **Picker SQL fn** (`beaches_due_for_refresh`, `dog_parks_due_for_refresh`): CTE filters entities whose cell has < 22 forecast rows in `[now, now+24h]`. Pattern in `20260606ze_picker_weather_grid_warm_gate.sql`; threshold tuned in `20260607c_grid_warm_threshold_realism.sql`.
2. **Direct SQL fn** (`refresh_beach_day_hourly_scores_bulk`, `refresh_marine_advisories`): same filter embedded in the `_scope` CTE so cold-cell entities never reach the JOIN. Don't add a fallback — let the entity drop out; next loader-tier tick + next consumer-tick auto-recovers.

**Orchestrator gate** (state-launch): `weather_grid_warm` phase in `run_state_pipeline.py` polls cell coverage, fires `refresh-weather-grid {tier:'t1', state_filter}` if cold, criterion ≥95% of state cells warm. Belt-and-suspenders so a fresh-state launch completes without operator intervention.

**Threshold = ≥22 of next-24h** because t1 fetches Open-Meteo with `forecast_days=2` → hours from `today 00:00` to `tomorrow 23:00` UTC. As the day progresses, the forward-looking subset (rows > now()) erodes from 48 → ~24 by end-of-day. The original ≥45/48h threshold was satisfiable only in a narrow window around UTC midnight — surfaced as the ME launch (16:30 UTC) gate-pass of 1/201 cells. ≥22/24 anchors to the window t1 reliably populates at any UTC hour while preserving the gate's intent (cell has contiguous near-term forecast — prevents partial-with-gaps writes like HI fid 13882's 1PM cutoff).

### Direct-fetch fallback — when it's OK

ONE remaining surface still uses direct-fetch fallback: `get-beach-now` / `get-dog-park-now`. They fetch a single hour per beach per tick (hourly cron), so worst-case fallback cost = ~hundreds of calls in the first hour after a state launch, not thousands sustained. Acceptable. The gate pattern is still preferred if you're refactoring, but lower priority.

### Caveats

- **Dagster schedules default STOPPED** per project convention. Toggle ON via Dagster UI (`hourly_weather_grid_schedule` + `daily_weather_grid_inventory_schedule`) or grid goes stale within 1-3 days as forecasts age out. Picker gate then keeps consumers from racing the cold grid.
- **WORKER_RESOURCE_LIMIT still possible** on `daily-beach-refresh` at >50 beaches/call. Mitigated via 2-min chunked cron (`beach_refresh_chunked`) + 22h stale filter. Real fix is Phase W4 (Dagster port of daily refreshes).

Migrations: `20260525_weather_grid_schema.sql` + `_helpers.sql` + `_materialization.sql` + `20260606ze_picker_weather_grid_warm_gate.sql` + `20260607c_grid_warm_threshold_realism.sql`. Pins: `[[weather-grid-reference-layer]]`, `[[grid-consumers-require-cell-warmth-gate]]`.

---

## Dog Park Coverage Sub-Pipeline (9-op state launcher)

`scripts/dog_park_pipeline/` is the canonical per-state coverage pipeline. Brings dog parks from `osm_default` stub policy to `operator_posted_v2` with verbatim cites + source URLs. Codified after CA proof point 0% → 79.5% (2026-05-25 LATE).

### Run signatures

```bash
# Via Dagster (canonical, observable via dog_park_coverage_runs metrics table):
dagster asset materialize -m dog_beach \
  --select "dp_preflight,dp_pip_address_city,dp_reclassify_junk,dp_generic_display_names,dp_seed_operators,dp_walk_catalogs,dp_ingest_queue,dp_run_extractor,dp_retry_no_match" \
  --partition <CA|OR|WA|MD>

# Or single-state standalone (no Dagster needed):
python -m scripts.dog_park_pipeline.run --state OR
```

### 9-op sequence (strict order)

1. **preflight** — Playwright + env vars + operator-coverage check (warns if state has <3 city operators)
2. **pip_address_city** — `UPDATE dog_parks_gold.address_city = j.name FROM jurisdictions j WHERE ST_Contains(j.geom, dpg.geom)`
3. **reclassify_junk** — flip tiny + private parks `is_active=false`; PROTECTED list keeps rest stops + truck-stop pet zones
4. **generic_display_names** — `display_name_override = city + name` for "Dog Park" / "Bark Park" / etc.
5. **seed_operators** — auto-create city operators from `state_operator_seeds.json` + PIP-backfill `inferred_operator_id`
6. **walk_catalogs** — top-N city operators → web_search for catalog → enumerate parks → fuzzy+spatial match → queue unmatched
7. **ingest_queue** — geocode pending queue rows via Google Places + 150m dup check → INSERT new gold rows with `website=catalog_url`
8. **run_extractor** — process all default-source parks (existing + newly ingested). Routes: OSM-website / URL-slug fallback / `NO_PER_PARK_URL_HOSTS` web_search / `HOST_SECTION_EXTRACTORS` shared-page slicer / no-website web_search
9. **retry_no_match** — query-variant rescue with web_search max_uses=5 + display_name_override-aware names

Each op writes per-run metrics to `public.dog_park_coverage_runs` (state, op, started_at, ended_at, metrics jsonb).

### Adding a new state

1. Add city list to `scripts/dog_park_pipeline/state_operator_seeds.json` under the state's 2-letter code (cities with ≥3 active dog parks)
2. Add state code to `DOG_PARK_STATES` `StaticPartitionsDefinition` in `assets/dog_park_coverage.py`
3. Run the pipeline. Cost ~$5-15 + ~10-15 min runtime per state.

### Adding a per-host shared-page slicer

Seattle pattern: `seattle.gov/parks/recreation/dog-off-leash-areas` is one shared page with all OLAs as accordion sections. To support a similar host (e.g., Portland, Baltimore), add a `_handler(url, park_name) -> section_text | None` function and one entry to `HOST_SECTION_EXTRACTORS` in `scripts/extract_dog_park_amenities.py`.

### Key tables

| Table | Role |
|---|---|
| `dog_parks_gold` | Identity (1 row per park). `inferred_operator_id` drives walker. |
| `dog_park_dog_policy` | Curated overlay (consumer reads). `source='operator_posted_v2'` marks extracted. |
| `dog_park_enrichment_provenance` | Per-source claims with verbatim cite quotes. Promoted via `promote_canonical_dog_park_policy()`. |
| `dog_park_discovery_queue` | Walker-found parks not yet in gold; ingest target. Statuses: pending / ingested / duplicate / needs_review / rejected. |
| `dog_park_coverage_runs` | Per-op run metrics for observability + freshness guards. |

State proof points (2026-05-25 LATE):
- CA: 322/405 = 79.5%
- OR: 73/103 = 70.9%
- WA: 92/142 = 64.8%

Pin: `[[dog-park-coverage-playbook]]` — full sequencing rationale, what helped + what to skip, road-tripper exception for rest stops + truck-stop pet zones.

---

## Per-Beach Off-Leash Extraction (Codify-Pattern Port)

`scripts/extract_per_beach_offleash_v2.py` is the canonical script for extracting per-beach off-leash policy with cite-required verbatim quotes. Built 2026-05-25 LATE as a port of the codify pipeline's URL-resolution + LLM-extraction substrate from `scripts/derive_policy_source_for_jurisdiction.py`.

### Why a separate script

`extract_research_v2.py` finds candidate URLs from a static pool (`park_url_extractions` + operators + cpad_units + city_policy_sources). That pool misses authoritative deep URLs for many famous beaches because the surrounding pipeline focuses on jurisdiction-level metadata, not per-beach pages. Result: v1 of this extractor got 0/25 yes-with-cite because every candidate was an agency catalog root.

### What v2 ports from codify

1. **Park-platform candidate builders** (`candidates_for_beach`) — analog of codify's `_candidates_municode`/`_codepublishing`/`_amlegal`. Switches on `cpad_units.mng_agncy` + city to emit URLs for CDPR, sandiego.gov bchdog, beaches.lacounty.gov, ocparks.com, plus city-specific deep pages (Coronado, Capitola, Newport Beach, Santa Cruz, etc.).
2. **Smart-fetch routing** (`smart_fetch`) — Playwright for known JS-rendered/blocked hosts (parks.ca.gov, newportbeachca.gov, sanjoseca.gov, cityofdavis.org); urllib otherwise. Auto-escalates to Playwright on 403/503 per `feedback_403_means_playwright_skip_ua_tricks.md`.
3. **AUTH_DOMAINS scoring** — richer than `extract_research_v2`'s table (adds beaches.lacounty.gov, cityofcapitola.org, malibucity.org, etc.); demotes aggregators (bringfido, yelp, tripadvisor).
4. **Deep-link gate** (`is_url_deep_enough` + `DEEPLINK_MARKERS`) — backstop rejecting bare catalog roots before LLM cost.
5. **Anthropic web_search_20250305 escalation** — when ALL deterministic candidates fail OR a candidate returns thin/blocked content, escalate Sonnet with `web_search_20250305` tool enabled. Sonnet routes around Cloudflare/404s by searching the web for authoritative .gov / park-system sources. Cost ~$0.01-0.03 per beach. Patterned after codify Step 6.8.
6. **Sentinel-row writes** — on no-result paths, write a BEP row with `source='per_beach_offleash_v2_no_result'`, `is_canonical=false` so freshness guards skip re-extracting.

### Cite-required contract

LLM prompt enforces: `answer ∈ {yes, no, unknown, no_match}` + verbatim `quote` 50-300 chars MUST contain the beach name + `time_window` if yes + `confidence ∈ {high, medium, low}`. Yes-with-cite passes name_match() on the quote before writing canonical.

### Output

- **yes-with-cite** → writes BEP row (`source='per_beach_offleash_v2'`, `is_canonical=true`), then fires `compute_beach_field_consensus(fid)` + `promote_canonical_dogs_to_beach_dog_policy(fid)` per fid to propagate to consumer surface.
- **no / unknown / no_match** → writes sentinel row, skips cascade.

### Usage

```bash
python scripts/extract_per_beach_offleash_v2.py --apply --limit 5
python scripts/extract_per_beach_offleash_v2.py --apply --fids 8472,8333
python scripts/extract_per_beach_offleash_v2.py --apply              # default 25-beach famous-CA set
python scripts/extract_per_beach_offleash_v2.py --apply --no-web-search   # cheaper, accuracy drops
```

Default candidate list is `DEFAULT_CANDIDATES` at the top of the script (25 famous CA beaches stuck on `scoring_tier='none'`). Override with `--fids`.

Pin: `[[codify-patterns-beyond-statutes]]`.

---

## Catalog Ingest Pipeline — Evidence → Resolve → Promote

Separate from consumer scoring. Collects + reconciles beach metadata from CPAD, CCC, TIGER, NPS, OSM, LLM research, park-URL scrapes, etc. All evidence-bearing populators write into `beach_enrichment_provenance` (one row per `(fid, field_group, source, source_url)`); resolvers pick canonical winners; promoters write canonical values back to `locations_stage`.

### Function family

| Layer | Pattern | Purpose |
|---|---|---|
| Emit | `_emit_evidence_from_<source>(p_fid)` | Insert evidence rows. Idempotent via ON CONFLICT. No canonical mutation. |
| Rank | `_rank_<source>_evidence(p_fid, p_field_group)` | When a source has multiple candidates per beach, rank per Tier 1 rules. |
| Resolve | `_resolve_<field_group>(p_fid)` | Cross-source overrides; set `is_canonical=true` on winning row. |
| Promote | `_promote_<field_group>_to_stage(p_fid)` | Write canonical evidence's `claimed_values` jsonb to staging columns. |
| Flag | `_compute_review_flags(p_fid)` | Detection-only review_status updates. |
| Public | `populate_from_<source>(p_fid)` | Orchestrator: emit → resolve → promote → flag. |

### Tier 1 ranking (priority order)

1. Demote environmental overlays (Marine Parks / Eco Reserves / Wildlife Areas)
2. Containing CPAD with "Beach" in name wins (Coronado Municipal Beach pattern)
3. Trigram similarity to display_name (Mission Beach Park over Mission Bay Park)
4. Smallest CPAD area (most specific polygon)

Final tiebreaks: confidence desc, id asc.

### Cross-source override patterns (governance)

1. **State-park override**: `park_url` beats `tiger_places`/`cpad` when CPAD `unit_name` matches `\m(state beach|state park|state recreation)\M`. Always beats `name`/`governing_body`.
2. **Tiger-vs-operator override** (`_resolve_tiger_vs_operator`): when `tiger_places` holds canonical and any operator-source disagrees, tiger loses. Operator candidates ranked by trigram + park_url-agreement bonus + hierarchy fallback (`nps_places > csp_parks > tribal_lands > military_bases > park_operators > cpad > park_url`).
3. **Never overridden**: `manual` source.

### Audit trail

Every evidence row carries: `cpad_unit_name`, `extraction_type` (`cpad_source` / `cpad_source_crawl` / `derived_url_crawl`), `cpad_role` (`beach_access` / `environmental_overlay`), `source_url`, `source`. URL-discovery attempts log to `discovery_attempts` (`success`/`no_sitemap`/`no_match`/`agency_skipped`/`agency_missing`/`fetch_error`).

### Adding a new source

1. Create `_emit_evidence_from_<source>(p_fid)` with all standard audit columns. ON CONFLICT `(fid, field_group, source, coalesce(source_url, ''))`.
2. Extend `beach_enrichment_provenance.source` CHECK constraint if needed.
3. Write `_rank_<source>_evidence` if multiple per-beach evidence rows are possible (or reuse `_rank_park_url_evidence`).
4. Decide cross-source override semantics; extend `_resolve_<field_group>`.
5. Promoter + flags + orchestrator usually wrap unchanged.

`project_pipeline_refactor_trigger.md` — when to refactor vs. extend in place.

---

## Verdict Cascade

`recompute_all_dogs_verdicts_by_origin()` walks `beach_locations` + `osm_features` (beach polys) + active CCC access points and writes `beach_verdicts` (per-origin-key dog verdict + confidence + sources). Cascade has 6 passes (Pass 1–6) with cumulative invariants tracked in `project_verdict_cascade_invariants.md`.

`beach_dog_policy` is the curated overlay HTML reads. The cascade is reference / parity-check only — `dbt_dbt.consumer_beach_with_verdict` was the parity report (currently broken — it joined `public.beaches` which was dropped 2026-05-02; pending repoint or retire decision).

---

## Orchestration

### Dagster (`scripts/dagster/dog_beach`)

Asset modules under `dog_beach/assets/`:
- `arena.py` — landing tables, master arena, group/nav populators, audit.
- `gold.py` — `beaches_gold`, `beach_dog_policy`, `is_scoreable_gate`, `arena_orphans` (added 2026-05-02).
- `consumer_pipeline.py` — `beach_day_hourly_scores`, `beach_day_recommendations`, `daily_beach_refresh_run`, `get_beach_now_run`.
- `verdicts.py` — cascade asset (`beach_verdicts`). Note: the `beaches` write-back asset is STALE pending retire-vs-repoint decision (`public.beaches` dropped 2026-05-02).
- `frontend.py` — lineage-only AssetSpecs for edge functions + HTML pages.
- `dbt_assets.py` — wraps dbt models as Dagster assets.
- `ingest.py`, `external_sources.py` — catalog ingest assets.

Run: `dagster dev` from `scripts/dagster/dog_beach/` (after `pip install -e .`).

### dbt (`scripts/dbt/dog_beach`)

- `models/sources.yml` — `public.beaches_gold`, `beach_dog_policy`, `beach_locations`, etc.
- `models/staging/` — passthroughs + light cleaning per source.
- `models/marts/consumer_beach_with_verdict.sql` — parity report (currently broken; pending fix).
- `models/marts/truth_comparison.sql` — cascade vs external truth-set.

---

## Migrations

All SQL migrations in `supabase/migrations/`, applied manually via `supabase db query --linked -f <file>`. Filename format `YYYYMMDD_description.sql`. Numbering order matters when path-3-style sequences depend on prior steps.

Recent path-3 migrations (2026-05-01 → 2026-05-02):

| File | Purpose |
|---|---|
| `20260501_beaches_gold.sql` | Created `beaches_gold` cross-state spine |
| `20260501_path3a_beach_dog_policy.sql` | Curated overlay table, FK→beaches_gold.fid |
| `20260501_path3a_beaches_gold_scoring_columns.sql` | Added NOAA/timezone/open/close to gold |
| `20260501_path3a_or_arena_seeds.sql` | 5 manual OR seeds |
| `20260501_path3a_scoring_tables_dual_key.sql` | Dual-key window: scoring tables accept location_id OR arena_group_id |
| `20260502_path3b_scoring_pk_swap.sql` | Final scoring PK swap to arena_group_id |
| `20260502_path3b_slug_to_gold.sql` | location_id slug → beaches_gold |
| `20260502_path3b_is_scoreable.sql` | `is_scoreable` gate |
| `20260502_path3b_marketing_text_to_gold.sql` | address/website/description to gold |
| `20260502_path3b_find_beaches_rpc_swap.sql` | find_beaches RPC reads beaches_gold |
| `20260502_path3b_drop_public_beaches.sql` | Drop `public.beaches` (the spine swap completes) |

---

## Conventions

- **CRS**: All geometry stored as `EPSG:4326`. Reproject at ingest. Cast to `::geography` at query time for meters. No mixed SRIDs in columns. (`project_crs_convention.md`)
- **Buffers**: Polygons-beach-sits-IN use 100m. Lakes (points inland of shoreline) use 1km. Point-to-point matching (CCC) 200m. NOAA nearest-station unbounded KNN. (`project_buffer_convention.md`)
- **Dedupe scope**: CA-only mode until another state explicitly activated. (`project_dedupe_scope_ca_only.md`)
- **CPAD vs PAD-US**: CPAD for CA (more curated), PAD-US for other states. Same target columns. (`project_pad_us_for_other_states.md`)
- **Operator key**: `mng_agncy` (manager) is the operational entity; `agncy_name` (owner) is what gets paired with `agncy_web` URLs. (`project_cpad_agncy_vs_mng.md`)
- **CPAD FK**: `cpad_unit_id` joins on `cpad_units.unit_id`, **not** `objectid`. (`project_dog_policy_exceptions_canonical.md`)
- **OSM vs Google**: OSM = operational layer (rules, fences, amenities). Google Places = on-demand UGC (photos, reviews). (`project_osm_vs_google_data_sources.md`)
- **CCC = access points, not beaches**: CCC's lat/lng is parking/trailhead, not the beach. Pair via proximity, never identity. (`project_ccc_access_point_semantic.md`)
- **MPAs are footnotes**: marine-take regulation, not beach access. (`project_mpas_are_footnotes.md`)
- **HTML page links**: when discussing/editing any HTML page, include a `file://` link so Franz can open without copy-pasting paths. (`feedback_html_page_links.md`)

---

## Git Workflow

- Branch: **`main`** (all dev + prod). GitHub Pages serves `main` directly.
- Local testing: open HTML files in browser (file:// is in CORS allowlist).
- Risky changes on exploratory branches; merge only after end-to-end acceptance.
- Never auto-merge to main without explicit approval. (`feedback_merge_workflow.md`)

---

## SMS Alerts (Blocked)

`send-daily-alerts` + subscriber pipeline (tables, Twilio integration) built but not operational. Twilio toll-free number verification is backlogged. Resume once from-number is approved. (`project_sms_mess.md`)

---

## Current State (2026-05-02)

- **`beaches_gold` cross-state spine** is the canonical surface; `public.beaches` dropped 2026-05-02 (path 3b).
- **304 SoCal scoreable beaches** scored daily (Santa Barbara → Mexico). 5 OR seeds in gold but not yet scored. Fan-out gated by `is_scoreable`.
- **Operator + dog policy LLM extractions** running via `extract_for_orphans.py` / `extract_operator.py` against the calibration framework (`extraction_prompt_variants` + `beach_policy_extractions`).
- **Cascade verdict pipeline (6 passes)** writes `beach_verdicts`; HTML reads curated `beach_dog_policy` overlay instead.
- **Hourly NOW refresh + 4-tier status system + best-window selection + status board / dynamic charts** all running end-to-end on the gold spine.
- **Catalog ingest (~25 v2-* edge functions + Evidence → Resolve → Promote)** keeps adding inventory; promotions to `beaches_gold` are batched.
- **Dagster + dbt orchestration** wraps both pipelines; gold + dog-policy assets added in this session.

### Near-term parking lot (see session notes)

- Verdict write-back asset (`verdicts.beaches`) stale — retire vs repoint to `beach_dog_policy`
- `dbt build` currently breaks: `stg_beaches.sql` + `consumer_beach_with_verdict.sql` reference dropped `public.beaches`
- `get-beaches-find` `scored_only=false` returns same 309 as `true` — RPC default LIMIT
- `get-beach-detail` response body doesn't echo `fid` (accepts `?fid=` but doesn't return it)
- Real admin auth (currently obscure URL + `x-admin-secret`; repo is public)
- Orphan-grouping pass over manually-promoted SoCal beaches (likely dupes against existing arena groups)
- Dagster step 3: actually repoint `consumer_pipeline.py` + `frontend.py` AssetKey deps to `beaches_gold`
- SMS pipeline unblock when Twilio toll-free verification lands
