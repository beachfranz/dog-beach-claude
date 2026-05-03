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

---

## Supabase CLI

Installed at `~/scoop/shims/supabase` (v2.90.0+). Project linked to **dog-beach-AI** (ref `ehlzbwtrsxaaukurekau`, East US).

```bash
supabase db query --linked -f supabase/migrations/<file>.sql   # apply migration
supabase db query --linked "SELECT count(*) FROM public.beaches_gold WHERE is_scoreable"   # ad-hoc
supabase functions deploy <fn-name> --no-verify-jwt           # deploy edge function
```

`--no-verify-jwt` is required because the `sb_publishable_` anon key format does not pass Supabase JWT verification.

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
1. Live Open-Meteo `/current` + hourly precip
2. Live NOAA tide for current hour
3. Borrows `busyness_score` from existing DB row (BestTime fetched daily only)
4. Runs `scoreHours()` from `_shared/scoring.ts`
5. Clears previous `is_now=true` row for the beach
6. Upserts new row with `is_now=true`, overwriting the forecast for that timestamp

By end of day all past hours have been overwritten with observed actuals.

Migrations: `20260417_is_now.sql` + `20260417_hourly_now_cron.sql`.

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
