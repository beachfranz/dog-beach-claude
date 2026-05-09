# Dog Beach Scout — End-to-End Pipeline Overview

**Last verified:** 2026-05-08. Grounded in source code analysis (file:line refs throughout). This is the five-page overview; deeper per-phase docs live alongside.

The pipeline ingests raw geographic + policy data, dedupes it into a canonical beach inventory, attaches dog-policy and operational evidence, and serves daily scored recommendations to the UI. There are **ten distinct phases** in two halves: a **catalog half** (Phases 0–7) that builds the static beach record, and a **scoring half** (Phases 8–9) that runs daily over the curated subset. Phase 10 wraps both as orchestrators.

---

## 1. The Spine and the Ten Phases

```
Phase 0 — External Data Ingestion          (loaders → landing tables)
Phase 1 — Landing-Stage Attribution        (county/cpad/place PIP on landings)
Phase 2 — Arena Clustering                 (dedup landings → arena spine)
Phase 3 — Promotion to Gold                (arena → beaches_gold, fires populators+resolvers)
Phase 4 — BEP Cascade & Resolvers          (evidence ledger → canonical columns)
Phase 5 — Consumer Table Promotion         (canonical → beach_dog_policy, beach_amenities)
Phase 6 — Operator-Level LLM Enrichment    (Tavily + Sonnet → operator_dogs_policy)
Phase 7 — Descriptions & Photos            (LLM prose, photo aggregation)
Phase 8 — Daily Scoring                    (weather/tide/crowd → day status)
Phase 9 — UI Consumer Feeds                (Edge Functions render the find feed, detail page)
Phase 10 — Orchestrators                   (run_pipeline_for_state, daily-beach-refresh, triggers)
```

The **canonical spine** is `public.beaches_gold` (PK `fid`). Everything before Phase 3 builds the row; everything after consumes it. Two parallel keys are used downstream:

- `beaches_gold.fid` (bigint) — internal canonical ID, inherited from `arena.fid`
- `beaches_gold.location_id` (text slug, e.g. `coronado-dog-beach`) — public-facing key used by Edge Functions and the URL space

A `(fid, location_id)` row participates in many overlay tables (`beach_dog_policy`, `beach_amenities`, `beach_descriptions`, `beach_photos`, `beach_day_recommendations`, `beach_day_hourly_scores`).

---

## 2. Phase 0 — External Data Ingestion

**Purpose:** pull authoritative external geographic + policy data into raw landing tables. Source-of-truth for everything downstream.

**Sources & target tables:**

| Source | Table | Loader | Endpoint kind |
|---|---|---|---|
| USGS PAD-US (per-state) | `pad_us_units` | `scripts/external_sources.py:332-346` | ArcGIS REST |
| OSM amenities (per-state) | `osm_amenities` | `external_sources.py:320-364` + Overpass QL | Overpass |
| OSM `natural=beach` | `osm_landing` | `scripts/load_state.py:91-200` | Overpass |
| Google Places POIs | `poi_landing` | seeded separately | manual / one-off |
| California Coastal Commission | `ccc_landing` / `ccc_access_points` | one-off CSV | manual |
| NOAA tide stations | `noaa_tide_stations` | admin Edge Function | NOAA CO-OPS |
| TIGER counties | `counties` | `scripts/load_counties_shapefile.py` | shapefile |
| TIGER places (cities/CDPs) | `jurisdictions` | `scripts/load_places_shapefile.py` | shapefile |
| OSM dog features | `osm_features` | `scripts/one_off/bulk_load_dog_features.py` | Overpass area |
| Closure polygons | `dog_policy_zones` | manual | shapefile |

**Idempotency:** every loader checks `last_loaded_at()` (`external_sources.py:373-383`) and skips if within TTL (PAD-US 90d, OSM amenities 180d). Inserts use `ON CONFLICT (id_columns) DO UPDATE`.

**Freshness tracker (added 2026-05-08):** `public.external_source_status(source, state, last_loaded_at, row_count, status)` is written by every bulk loader and gates the pipeline via the Phase-10 precheck.

**Nomenclature:** `Source` dataclass (`external_sources.py:88`) holds `name`, `fetcher_kind ∈ {arcgis_rest, overpass}`, `target_table`, `id_columns`, `mapper`, `ttl_days`, `state_bbox`. Loader auto-dispatches on `fetcher_kind`.

---

## 3. Phase 1 — Landing-Stage Attribution

**Purpose:** annotate landing rows with derived attributes (county FIPS, CPAD unit, place name, governing level) **before** they enter `arena`. The handoff doc and the 2026-05-08 audit confirmed this stage *had* a CA-only filter; the fix path now derives state from `STATEFP` via `state_from_fips()` (`20260508_jurisdictions_state_from_fips.sql`).

**Columns added per landing table:**

- `osm_landing` / `poi_landing` / `ccc_landing` get: `county_geoid`, `county_name`, `cpad_unit_id`, `place_name`, `governing_level`, `name_source`
- `poi_landing` additionally gets `parsed_address` JSONB (street/city/zip), `has_parking`, `is_dog_beach_signal`

**Triggers / migrations:**
- `20260429_osm_landing_enrich_trigger.sql` — auto-PIP on insert
- `20260429_poi_landing_county_cpad_enrich.sql` — county PIP + smallest-containing CPAD
- `20260430_landing_governing_level.sql` — classifies into {federal, state, county, city, tribal, military}
- `20260427_osm_features_name_source.sql` — name provenance ('osm_tag' / 'derived_from_poi/N' / 'ccc' / 'manual_seed')

**Dependencies:** Phase 0 (counties, cpad_units, jurisdictions, nps_places, military_bases, tribal_lands).

---

## 4. Phase 2 — Arena Clustering

**Purpose:** dedupe landing rows representing the same real-world beach into a single canonical record. This is **the spine table for the catalog half.**

**Table:** `public.arena`. Columns of note:
- `fid` (PK), `name`, `lat`/`lon`, `county_fips`, `source_code` ∈ {`osm`, `poi`, `ccc`, `manual`, `cpad`}, `source_id`, `cpad_unit_id`
- `group_id` — clustering output. All duplicates of the same beach point to the same canonical fid (or self if singleton).
- `nav_lat`/`nav_lon`, `nav_source` — preferred navigation centroid (OSM-preferred over POI)
- `name_source`, `park_name` — provenance + park-unit attribution

**Functions (called by Phase 10's `run_pipeline_for_state`):**
1. `promote_*_landing_to_arena()` — moves landing rows into arena (idempotent on `(source_code, fid)`)
2. `populate_arena_group_id()` — three clustering strategies: spatial-100m + name similarity, county-scoped name match, CPAD relation-skip
3. `populate_arena_extras()` — cross-source extras: intra-OSM trigram, intra-POI spatial + name, cross-OSM/POI pairs

**Safe-mode flag:** `app.arena_clustering_active` GUC prevents the auto-promote trigger from firing during bulk inserts.

**Sequencing:** poi_landing → osm_landing → name refresh → group_id → extras. All idempotent.

---

## 5. Phase 3 — Promotion to Gold

**Purpose:** the master orchestrator that produces `beaches_gold` rows and runs the full populator/resolver/consumer-promote chain. Single SQL function, one entry point, runs per-fid in a loop.

**`promote_to_gold(p_fids bigint[], p_score boolean)`** at `20260508_fix_gold_inheritance_gaps.sql:47-156`. Returns row counts for each phase.

**3.1 — INSERT** (`beaches_gold` columns): fid, location_id (via `_make_location_slug`), name, lat/lon, county_name, **county_fips** (added 2026-05-08), source_code, source_id, group_id, nav_lat/lon, nav_source, name_source, **park_name** (added 2026-05-08), state (via `_infer_state_from_county`), promoted_from='promote_to_gold_v3', is_active=true, noaa_station_id (via `_nearest_noaa_station`), timezone='America/Los_Angeles', open/close_time, is_scoreable=p_score, geom.

**3.2 — Per-fid populator chain** (each emits BEP rows; see Phase 4):
```
populate_polygon_containment_gold(fid)     -- cpad/jurisdictions/counties/military/tribal PIP
populate_from_cpad_gold(fid)                -- CA park-unit names, governance
populate_from_pad_us_gold(fid)              -- non-CA equivalent (added 2026-05-08)
populate_from_park_operators_gold(fid)      -- agency-level dog policy from operators
populate_from_research_gold(fid)            -- manual curated research evidence
populate_from_park_url_gold(fid)            -- LLM extraction from operator websites
populate_from_park_url_governance_gold(fid) -- domain → agency inference
populate_from_unified_v1_gold(fid)          -- aggregated LLM extractions
populate_from_city_dog_policy_gold(fid)     -- city-ordinance evidence
populate_from_county_dog_policy_gold(fid)   -- county-ordinance evidence
_emit_evidence_from_osm_amenities(fid)      -- ★ wired 2026-05-08; OSM amenities → practical BEP
```

**3.3 — Per-fid resolvers** (consume BEP, write canonical columns):
```
_resolve_polygon_containment(fid)    -- → state, county_fips, poly_contained_*
_resolve_governance_gold(fid)        -- → governing_jurisdiction, governing_body, governing_level
_resolve_dogs_gold(fid)              -- → dogs_allowed, leash_policy, off_leash_flag
_resolve_practical_gold(fid)         -- → has_parking, has_restrooms, has_showers, has_lifeguards
_resolve_field_group_gold('access', fid) -- → access_restricted, access_restriction_*
compute_beach_field_consensus(fid)   -- Layer-2 cross-source voting
```

**3.4 — Consumer-table promotion**: `promote_canonical_to_consumer_tables(fid)` writes `beach_dog_policy` and `beach_amenities` overlays from the canonical columns just resolved.

**Dependencies:** Phase 2 arena rows (`is_active=true`), Phase 0 reference tables. The entire function is idempotent (re-running for same fids skips already-promoted rows but re-runs populators+resolvers).

---

## 6. Phase 4 — BEP & Resolvers (Evidence Layer)

**Purpose:** every populator emits structured evidence into `beach_enrichment_provenance` (BEP). Resolvers consume the ledger via consensus voting and write canonical values back.

**`beach_enrichment_provenance` columns:** `gold_fid`, `field_group ∈ {dogs, practical, access, governance, polygon_containment}`, `field_name`, `source` (∈ ~24 values: `manual`, `cpad`, `pad_us`, `park_url`, `unified_v1`, `osm_amenities_v1`, `research`, etc.), `claimed_values` JSONB, `evidence_quote`, `confidence`, `field_source_calibration_weight`.

**Source registry:** `bep_source_catalog` (`20260508_bep_source_catalog.sql`) — declarative table listing each source, its kind (`raw_spatial`, `derived`, `extraction`, `manual`), allowed field_groups, and emitter function name.

**Consensus mechanics:** Wilson 90% lower bound (`_wilson_lower_bound(accuracy, n, z=1.645)`) to penalize small samples. Source calibration weights (per-field, per-source) seeded from a 2026-05-03 ground-truth audit:

| Field | Top sources (calibration weight) |
|---|---|
| `dogs_allowed` | research 0.91, park_url 0.86, governing_body 0.75, unified_v1 0.70, llm 0.69 |
| `leash_policy` | research 0.83, old_school_llm 0.77, park_url 0.75, unified_v1 0.70 |
| `has_parking` | park_url 0.90, old_school_llm 0.85 |
| `has_lifeguards` | park_url 0.90, old_school_llm 0.82 |

URL-level voting within `unified_v1`: authority_score weighted by calibration weight; tie-break on `disagreement_flag`. Manual votes get full weight (1.0).

---

## 7. Phase 5 — Consumer Tables

**Two overlays** built from the canonical beaches_gold columns:

- **`beach_dog_policy`** (one per fid): `arena_group_id` (FK), `dogs_allowed ∈ {yes,no,mixed,seasonal,unknown}`, `leash_policy`, `has_off_leash`, `has_on_leash`, `dogs_prohibited_start`/`_end` (seasonal MM-DD), `zone_rules` JSONB, `source ∈ {auto_promoted_from_consensus, manual, public.beaches}`.

- **`beach_amenities`** (one per fid per amenity_type): `arena_group_id`, `amenity_type ∈ {parking, restrooms, showers, lifeguards, food, picnic_area, fire_pits, drinking_water, disabled_access}`, `has_amenity`, `source`.

**`zone_rules` JSONB shape** (canonical; CA fully populated, OR/WA single-region only as of 2026-05-08):
```jsonc
{
  "regions": [{
    "name": "Main Beach",
    "sections": {
      "sand":   { "rule": "off_leash" },
      "water":  { "rule": "swim" },
      "trails": { "rule": "on_leash" }
    }
  }],
  "seasons": [...]   // optional, MM-DD ranges
}
```

`beach_dog_policy_zones` is a **VIEW** derived from spatial overlap of `dog_policy_zones` polygons with `beaches_gold.geom` — not directly insertable. Surfaces snowy plover / shorebird closures once the polygons are loaded.

---

## 8. Phase 6 — Operator-Level LLM Enrichment

**Purpose:** research dog policy at the *governing body* level (state agency, county, city) so multiple beaches inherit a single curated policy. CA has a complete operator graph (~1,200 operators with researched policies); OR/WA were seeded 2026-05-08 (`populate_operators_for_state` in `20260508_pipeline_includes_operators.sql`) and policy research is in flight.

**Tables:**
- `operators` — master list. Cols: `id`, `slug`, `canonical_name`, `level ∈ {federal,state,tribal,county,city,special-district,private,joint}`, `subtype`, `jurisdiction_id`, `county_geoid`, `state_code`, `cpad_agncy_name`, `osm_operator_strings`, `website`, `dog_policy_url`.
- `operator_policy_extractions` — raw Tavily+LLM rows (one per operator × URL × pass)
- `operator_dogs_policy` — merged canonical per-operator policy (curator-reviewed)
- `operator_policy_exceptions` — carve-outs (e.g., "off-leash allowed at Magnuson Park only")

**Scripts:**
- `scripts/extract_operator_dogs_policy.py` — Tavily site:domain + general web search; Sonnet 3-pass extraction (structure → extract → verify); upserts to `operator_policy_extractions`. CLI: `--ids`, `--counties`, `--limit`, `--dry-run`.
- `scripts/one_off/merge_operator_dogs_policy.py` — auto-merges extractions into `operator_dogs_policy` (idempotent on `operator_id`)
- `supabase/functions/v2-enrich-operational/index.ts` — Edge Function alternative; takes `tier ∈ {state,city,county,federal}`, body_filter; same Tavily+Claude flow.

**Cost:** ~$0.30/operator (6 LLM calls × $0.05) + Tavily search.

---

### Phase 6b — Per-Section Zone-Rule Extraction (added 2026-05-08)

**Purpose:** map operator-level dog policy summaries onto per-section beach rules (`sand`, `water`, `trails`, `picnic_area`) so OR/WA beaches get the same multi-section `zone_rules` JSON shape that CA produces. Bridges the gap between flat operator policy text and the structured `regions[].sections` shape the find feed and detail page consume.

**Data flow:**
1. **`scripts/extract_beach_section_rules.py`** — for each tier-1/1b/1c beach with a matchable operator (city via TIGER places PIP → county via county_geoid → state via PAD-US containment, in priority order), pull `operator_dogs_policy.summary`, send to Haiku with a section-mapping prompt, parse JSON output.
2. **Writes BEP** — one row per beach with `field_group='dogs'`, `source='section_research_v1'`, `claimed_values={"sections": {...}}`.
3. **Re-fires `_promote_zone_rules_for_fid(fid)`** — the new injector `_zr_inject_sections_from_bep` reads section_research_v1 evidence and merges it into `regions[0].sections`, overlaying the perimeter and sand-baseline injectors.

**Schema add:** `_zr_inject_sections_from_bep(p_zr jsonb, p_fid bigint)` in `20260508_section_evidence_to_zone_rules.sql`. Wired into `_promote_zone_rules_for_fid` after the existing `_zr_inject_perimeter` and `_zr_inject_sand_from_policy` calls.

**Cost:** ~$0.005/beach (Haiku, no Tavily — reuses already-extracted operator policy summaries). 544 OR/WA tier-1 beaches → ~$3.

**Match coverage:** roughly 20 OR/WA operators have `policy_found=true`, so the population of beaches whose city/county/state-agency was successfully researched is the upper bound on multi-section coverage. Beaches outside any researched operator's jurisdiction stay single-section (sand only, derived from `beach_dog_policy.dogs_allowed`).

**Why BEP-direct (not via `v2-enrich-operational`):** OR/WA were ingested through `arena → beaches_gold` directly, bypassing `beaches_staging_new`. The `v2-enrich-operational` Edge Function operates on `beaches_staging_new` only, so OR has 478 staging rows and WA has zero — calling that function for OR/WA would do nothing for WA and only partially for OR. This BEP-direct path matches the actual ingest architecture.

---

## 9. Phase 7 — Descriptions & Photos

**`beach_descriptions`** (one per fid): `arena_group_id`, `description` (2-3 sentences, 2nd-person imperative), `model='claude-sonnet-4-5'`, `input_hash` (SHA256 of bundle for caching).

**Generator** (`scripts/generate_beach_descriptions.py`): pulls `zone_rules` + CPAD parent + Overpass features within 300m (cliffs, piers, jetties, breakwaters, marinas, streams), hashes the bundle, skips if cached, calls Sonnet, upserts. Cost ~$0.003/beach. Prompt structure:
1. Lead with activities derived from `zone_rules.sections`
2. Weave in verified physical features ("backed by coastal bluffs", "at the mouth of {name}")
3. Mention seasonal restrictions concretely
4. Mention parking once with type+cost

**`beach_photos`** (one per fid per photo): `arena_group_id`, `source ∈ {mapillary, flickr, pixabay, pexels, unsplash, ccc, nps, manual}`, `external_id`, `image_url`, `thumb_url`, `attribution`, `license`, `lat`/`lng`, `distance_m`, `sort_order`.

**Loaders:** `scripts/load_{mapillary,flickr,pixabay,pexels,unsplash}_photos.py` — query API near beach centroid, filter via `_photo_filters.py` (license check, orientation, quality), upsert.

---

## 10. Phase 8 — Daily Scoring

**Purpose:** for each `is_scoreable=true` beach, compute hour-by-hour go/advisory/caution/no_go status for the next 7 days. Runs nightly via cron + on-demand via the per-fid trigger.

**Edge Function:** `supabase/functions/daily-beach-refresh/index.ts`. Inputs:
- Open-Meteo (forecast: temp, wind, precip, UV, WMO weather code)
- NOAA CO-OPS (tide predictions for `noaa_station_id`)
- BestTime.app (crowd predictions for `besttime_venue_id`; skipped per current direction)

**Output tables:**
- `beach_day_recommendations` — per-day rollup. Cols: `local_date`, `day_status ∈ {go, advisory, caution, no_go}`, `summary_weather ∈ {sunny, partly_cloudy, cloudy, foggy, rainy, windy}`, `best_window_label` (e.g. "10am–2pm"), `best_window_status`, `bacteria_risk ∈ {none, low, moderate, high}`, `avg_temp`, `avg_wind`, `avg_tide_height`, `lowest_tide_height`, `avg_busyness_score`.
- `beach_day_hourly_scores` — per-hour 0-23 PDT. Cols: `hour`, `hour_status`, `hour_score` (0-100), `temp_f`, `wind_mph`, `precip_chance`, `tide_height_m`, `uv_index`, `crowd_level`, `crowd_category`, `is_daylight`, `is_open`.

**Scoring config** (`scoring_config` table, version-pinned via `scoring_version`):
- Weights: tide 22.5%, wind 20%, rain 17.5%, crowd 15%, weather code 15%, temp 5%, UV 5%
- Thresholds: see CLAUDE.md per-metric. Worst-of-all metrics determines `hour_status`.

**Best-window selection** (`scoring.ts:selectBestWindows`): contiguous daylight hours with `hour_status != 'no_go'` AND `hour_score >= window_score_threshold`. Pick longest 2-5 hour block.

**Trigger plumbing:**
- `tg_beaches_gold_score_on_flip` (UPDATE on `is_scoreable`) → `_fire_daily_beach_refresh_for_fid(fid)` → HTTP POST to refresh function. *Note:* the secret embedded here is rotated; use the global cron or a manual call until that's fixed.
- Cron: `daily_beach_refresh_nightly` at 09:00 UTC (`20260506_daily_beach_refresh_cron.sql`).

**Scoring scope (set 2026-05-08, vocabulary updated 2026-05-09):** only beaches with Location Tier `1_off-leash` or `2_on-leash` get scored. Tiers `3_limited_access` and `4_no_dogs` are excluded. Enforced via `is_scoreable` gate. Tier classifier is the single SQL function `public.beach_location_tier(dogs_allowed, has_off_leash, has_on_leash, dogs_prohibited_start)` — callers should use it rather than duplicating the inline CASE.

---

## 11. Phase 9 — UI Consumer Feeds

| Edge Function | Endpoint | Purpose |
|---|---|---|
| `get-beaches-find` | `GET /get-beaches-find` | Find feed: nearby beaches + day status + scores. Fields: arena_group_id, location_id, display_name, lat/lon, access_rule, has_on_leash/off_leash, distance_m, day_status, best_window_label, bacteria_risk, summary_weather, weather_code, avg/lowest tide, avg_temp, avg_wind, busyness_category, composite_score + per-metric scores. Backed by `find_beaches()` RPC (PostGIS KNN + day_recs join). |
| `get-beach-summary` | `GET /get-beach-summary?fid=N` | 7-day rollup card. Backed by `get_beach_info()` JSONB aggregator. |
| `get-beach-detail` | `GET /get-beach-detail?fid=N&date=YYYY-MM-DD` | Hour-by-hour scoring + amenities + map + photos + description + zone_rules. |
| `get-beach-now` | `GET /get-beach-now?fid=N` | Current hour + next 3 hours + crowd trend. |
| `beach-chat` | `POST /beach-chat` | Scout AI Q&A per beach (Sonnet with prompt-cached beach context). |

**Filters in find feed:** `leash ∈ {any, off_leash, on_leash, mixed}`, `scored ∈ {true, false}`, `lat`/`lng` for KNN, optional `limit`. Default returns 50 nearest scored beaches.

**Auth:** all admin-* and write functions require `x-admin-secret`; read functions are public via CORS allowlist (`https://beachfranz.github.io`, `null` for file://).

---

## 12. Phase 10 — Orchestrators

**`run_pipeline_for_state(p_state, p_fids, p_skip_precheck)`** — the integrated pipeline (`20260509_propagate_poi_address_to_gold.sql` is the latest definition; supersedes `20260508_pipeline_includes_operators.sql`). Order:

1. **`assert_state_upstream_loaded(state)`** — raises if any of `pad_us`, `osm_landing`, `osm_amenities`, `tiger_places` is not `status='ok'` in `external_source_status` for the state. Skippable for ad-hoc reruns.
2. **`populate_operators_for_state(state)`** — TIGER cities + counties → `operators` (added 2026-05-08).
3. **`populate_arena_group_id()`** — Phase 2 clustering.
4. **`populate_arena_extras()`** — cross-source dedup.
5. **`promote_to_gold(fids, false)`** — Phase 3 (full populator + resolver chain).
6. **`_enrich_address_city_for_state(state)`** — TIGER places PIP for `address_city` (fallback when POI doesn't supply one).
7. **`_enrich_address_from_poi_for_state(state)`** — propagate `address` / `address_street` / `address_city` / `address_state` / `address_zip` from `poi_landing` (Google Places-derived, structured) into `beaches_gold`. Added 2026-05-09 after WA audit found 388/388 WA POIs had `address_full` populated but 0/404 `beaches_gold` rows had `address` set — pure propagation gap that pre-dated the WA launch.
8. **`_enrich_name_source_for_state(state)`** — backfill `arena.name_source` then propagate.
9. **`run_late_stage_dedup()`** — kill-and-merge late dupes.
10. **`process_geom_change_queue(100)`** — drain pending geom-driven cascade.

Daily-beach-refresh is **not** triggered by `run_pipeline_for_state` (the secret-rotation issue forced separation). Either: (a) cron handles it nightly; (b) call manually after a state launch.

**`v2-run-pipeline`** — separate Edge Function orchestrating the 18-stage v2 jurisdiction-classification flow (legacy, still operates on staging).

**Triggers active on `beaches_gold`:**
- `tg_after_insert_gold_promote_chain` — fires populator chain on row insert
- `tg_beaches_gold_geom_change` — invalidates dog policy on geom change
- `tg_beaches_gold_score_on_flip` / `_on_insert` — fires daily-refresh enqueue
- `tg_before_insert_auto_scoreable_socal` — auto-flip is_scoreable for SoCal seeds
- `trg_beaches_gold_set_geom` — derive `geom` from lat/lon

---

## 13. Dependency Graph

```
External APIs (USGS, OSM, NOAA, Census, Open-Meteo, BestTime, Tavily, Anthropic)
  ↓
Phase 0  raw landings (pad_us_units, osm_landing, osm_amenities, counties, jurisdictions, ...)
  ↓
Phase 1  landing-stage attribution (county_fips, place_name, governing_level)
  ↓
Phase 2  arena clustering (dedup → arena.fid + group_id)
  ↓
Phase 3  promote_to_gold ──┬──> populators emit BEP rows ──┐
  ↓                        │                                │
                           ↓                                ↓
                    Phase 4  beach_enrichment_provenance + resolvers
                           ↓
                    Phase 5  beach_dog_policy + beach_amenities
                           ↓                                ↑
                    Phase 6  operator_dogs_policy ──────────┘ (feeds back via populate_from_park_operators_gold)
                           ↓
                    Phase 7  beach_descriptions, beach_photos
                           ↓
                    Phase 8  daily-beach-refresh → beach_day_recommendations + beach_day_hourly_scores
                           ↓
                    Phase 9  UI Edge Functions
```

**Sequencing constraints:**
- Phase 0 must complete for a state before Phase 1 can attach references.
- Phase 1 → 2 → 3 is strict: arena needs landing attribution; gold needs arena clusters.
- Phase 4/5 are *inside* Phase 3 (each fid runs the populator → resolver → consumer-promote loop).
- Phase 6 (operator LLM) is independent of 3 chronologically but feeds Phase 3's populators on **next** run (closing the loop).
- Phase 8 only fires for `is_scoreable=true` rows. Tonight's directive: only tier 1 / 1b / 1c.
- Phase 9 reads from Phase 3, 5, 7, 8 — degrades gracefully on missing data.

---

## 14. Nomenclature Reference

| Concept | Vocabulary | Where defined |
|---|---|---|
| Source codes (arena) | `osm`, `poi`, `ccc`, `manual`, `cpad`, `pad_us` | `arena.source_code` |
| Operator levels | `federal`, `state`, `tribal`, `county`, `city`, `special-district`, `private`, `joint`, `unknown` | `operators.level` |
| Governing tiers (enrichment) | `state`, `city`, `county`, `federal` | `v2-enrich-operational/index.ts:35` |
| Location Tiers (dog access) | `1_off-leash`, `2_on-leash`, `3_limited_access`, `4_no_dogs`, `unknown` | `public.beach_location_tier(dogs_allowed, has_off_leash, has_on_leash, dogs_prohibited_start)`; canonical SQL function as of 2026-05-09 |
| BEP field groups | `dogs`, `practical`, `access`, `governance`, `polygon_containment` | `beach_enrichment_provenance.field_group` |
| BEP sources (~24) | `manual, plz, cpad, ccc, llm, research, park_url, unified_v1, park_operators, nps_places, tribal_lands, military_bases, pad_us, jurisdictions, counties, json_explode, old_school_llm, osm_amenities_v1, ...` | `bep_source_catalog` |
| Hour status | `go`, `advisory`, `caution`, `no_go` | `_shared/scoring.ts` |
| Busyness category | `quiet`, `moderate`, `dog_party`, `too_crowded` | `_shared/scoring.ts` |
| Bacteria risk | `none`, `low`, `moderate`, `high` | `_shared/scoring.ts` |
| Summary weather | `sunny`, `partly_cloudy`, `cloudy`, `foggy`, `rainy`, `windy` | `daily-beach-refresh/openmeteo.ts` |
| Zone rules | `regions[].sections.{sand|water|trails|picnic_area|asphalt}` | `beach_dog_policy.zone_rules` JSONB |
| Quality anchor tiers (test fixtures only) | Tier 1 marquee, Tier 2 regional, Tier 3 long-tail | `tests/quality_anchors/*.yaml` |

---

## 15. Recent Pipeline Changes (2026-05-08)

Five migrations shipped this date that changed the integrated pipeline:

1. **`20260508_external_source_status.sql`** — adds the freshness tracker table.
2. **`20260508_jurisdictions_state_from_fips.sql`** — fixes `load_jurisdictions_batch` hardcoding `state='CA'` regardless of input STATEFP. Also adds `state_from_fips()` helper used by the rest of the pipeline.
3. **`20260508_wire_osm_amenities_emitter.sql`** — wires `_emit_evidence_from_osm_amenities` into both `promote_to_gold` and `refire_bep_cascade`. Fixes the gap where OSM amenity evidence never landed in BEP for any state.
4. **`20260508_fix_gold_inheritance_gaps.sql`** — adds `county_fips` and `park_name` to the `promote_to_gold` INSERT (they were dropped on the floor); backfills `arena.park_name` from `pad_us_units` spatial join; backfills existing `beaches_gold` rows from `arena`.
5. **`20260508_integrated_pipeline_spec.sql`** + **`20260508_pipeline_includes_operators.sql`** — adds the upstream-data precheck and post-promote enrichment helpers (`_enrich_address_city_for_state`, `_enrich_name_source_for_state`); inserts `populate_operators_for_state` as the first step after the precheck so future state launches build the operators graph automatically.

Plus `20260508_seed_plover_closures.sql` which seeds Mar 15 – Sep 15 plover season on any matched named beach (interim until polygon shapefile load lands).

Plus `20260508_populate_from_operators_generic.sql` + `20260508_wire_operators_populator.sql` — adds `populate_from_operators_gold(fid)` (state-agnostic operator-via-PAD-US/county/jurisdictions BEP emitter) and wires it into both `promote_to_gold` and `refire_bep_cascade`. This is the path that brought WA from 0 to 1 marquee beach.

Plus `20260508_section_evidence_to_zone_rules.sql` — adds `_zr_inject_sections_from_bep` and rewrites `_promote_zone_rules_for_fid` to call it. Companion script `scripts/extract_beach_section_rules.py` is Phase 6b (above).

Plus `20260509_propagate_poi_address_to_gold.sql` — adds `_enrich_address_from_poi_for_state(state)` and wires it into `run_pipeline_for_state` step 7. Closes the propagation gap where `poi_landing` had `address_full` populated for 100% of OR/WA POIs but `beaches_gold.address` was 0% across the board. After backfill: OR address 0% → 57%, WA 0% → 66%.

Plus state-aware patches to `scripts/extract_operator_dogs_policy.py` (2026-05-09): replaced "California" hardcoding throughout the picker + extraction prompts with state-parameterized text; added `_DOMAIN_STATE_HINTS` heuristic to drop cross-state candidates pre-picker; added `b2_query` iterative re-query (alternative search with explicit state code) when initial picker rejects all. Audit found 9/20 of the original OR/WA "found=true" extractions were CA URLs accepted by a CA-prompt-bound picker; flipped to `pass_a_status='cross_state_purged'` and re-running with patched script.

### State-aware operator matching (Phase 6, refined 2026-05-09)

The full operator-matching chain for non-CA states now reads:

1. **Tavily site-search** with `site:<operator-domain>` (typically returns 0 hits — operators rarely have responsive dog-policy pages on their own sites).
2. **Tavily web search** with state name embedded: `"<operator name> <state name> dogs allowed beach official rules ordinance"`.
3. **Pre-picker domain filter** — `_DOMAIN_STATE_HINTS` table maps known wrong-state domains (`.ca.gov`, `coronado.ca.us`, `cityofmyrtlebeach.com`, etc.) to the state they belong to; any candidate whose hint doesn't match the operator's state is dropped before the LLM picker sees it.
4. **State-aware picker prompt** — Sonnet picker is told the operator's state explicitly ("Operator: City of Carbonado (city, manages **Washington** beaches)") and instructed to reject any URL that's clearly for a different state's same-named jurisdiction.
5. **Iterative re-query (`b2_query`)** — if the picker rejects all candidates from the first general-web search, fire a second query with municipal-code-specific phrasing: `<operator name> <state-code> municipal code dogs leash beach park`. This catches operators whose default Tavily search returns lifestyle-blog SEO content but whose municipal codes are indexable.
6. **Multi-pass extraction (Pass A / B / C)** — same as before, but each pass's prompt now references the operator's state, so the LLM can reject pages that are actually for a different jurisdiction even if the URL passed the picker.
7. **Heuristic purge SQL** — runs across `operator_policy_extractions`, flips `pass_a_policy_found=false` and sets `pass_a_status='cross_state_purged'` on any row whose `source_url` matches a wrong-state domain hint. Cascade: re-run `merge_operator_dogs_policy.py` → delete affected BEP rows → `refire_bep_cascade(fids)` → re-fire `extract_beach_section_rules.py`.

Companion clean-up after Google reverse-geocode: a Plus-code-stripping pass (`^[2-9CFGHJMPQRVWX]{4,}\+[2-9CFGHJMPQRVWX]+\s+`) runs across `beaches_gold.address` to convert `XV44+XJ Florence, OR, USA` → `Florence, OR, USA`. About 8% of OR/WA reverse-geocode results were Plus-code-only (beaches at unpaved access points without street addresses) — strip the grid prefix so the UI shows clean city-level fallback text instead of grid coordinates.

---

*This document is the overview. Per-phase drilldowns (BEP source registry, scoring config, populator internals, calibration audit) belong in their own docs/ files. File:line refs throughout this doc are accurate as of 2026-05-08.*
