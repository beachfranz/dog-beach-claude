# State Launch Runbook

**For:** the engineer (human or AI) launching a new state in dog-beach-scout. **Last verified:** 2026-05-09 against OR + WA tier-1+2 launches. **Companion to** `docs/pipeline-overview.md` (architectural map). This doc is procedural — it captures the *operational truth* including every gotcha we hit tonight.

---

## TL;DR — single-command launch

```bash
python scripts/run_state_pipeline.py --state RI
```

That's it. The Python orchestrator at `scripts/run_state_pipeline.py` runs **20 gated phases**, each with a success criterion. First failure halts; resumable by `run_id`. After it completes, the state's beaches are scored, populated, photographed, described, and live in the find feed.

Wall clock: **~45-90 min for structural phases, +1-3 hr for LLM phases**. Cost: **~$25-55 per state** (LLM phases). DB growth: ~50-200 MB depending on state size.

---

## Pre-flight checklist

Before kicking off `run_state_pipeline.py --state XX`:

1. **Upstream data must be loaded for the state.** Phase `precheck` (the first phase) calls `public.assert_state_upstream_loaded(state)` which checks four sources: `pad_us`, `osm_landing`, `osm_amenities`, `tiger_places`. All four must show `status='ok'` in `public.external_source_status`. If not:

   ```bash
   # Bulk-load any missing source for the state:
   python scripts/one_off/bulk_load_pad_us.py --states XX
   python scripts/one_off/bulk_load_overpass.py --states XX
   python scripts/one_off/bulk_load_amenities.py --states XX
   python scripts/one_off/bulk_load_tiger_places.py --states XX
   ```

2. **API keys present in `scripts/pipeline/.env`:**
   - `ANTHROPIC_API_KEY` (descriptions, operator/section extractors, daily refresh's narrative LLM)
   - `TAVILY_API_KEY` (operator-policy URL discovery)
   - `MAPILLARY_TOKEN` (photos)
   - `GOOGLE_MAPS_API_KEY` (reverse-geocode for missing addresses)
   - `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `ADMIN_SECRET`, `SUPABASE_DB_PASSWORD`

3. **DB headroom** (Nano-tier cap is 6.5 GB):
   ```sql
   select pg_size_pretty(pg_database_size(current_database()));
   ```
   Each state adds 50-200 MB depending on size. Halt if approaching cap.

4. **No conflicting jobs running.** Check the task list. The pipeline does not coordinate with concurrent extractors; double-fire is wasteful but not destructive.

---

## The 20 phases

| # | Phase | Kind | Action | Success criterion | Typical wall clock |
|---|---|---|---|---|---|
| 1 | `precheck` | SQL | `assert_state_upstream_loaded(state)` | All 4 required sources ok in `external_source_status` | <1s |
| 2 | `operators` | SQL | `populate_operators_for_state(state)` | `operators` has rows for state | 1-3s |
| 3 | `cluster_group` | SQL | `populate_arena_group_id()` | non-error | 30-180s (global) |
| 4 | `cluster_extras` | SQL | `populate_arena_extras()` | non-error | 1-10s |
| 5 | `promote` | SQL | `promote_to_gold(<state's fids>, false, true)` | every active gold row has `county_fips` | 10-60s |
| 6 | `address_poi` | SQL | `_enrich_address_from_poi_for_state(state)` | non-error | <2s |
| 7 | `address_city` | SQL | `_enrich_address_city_for_state(state)` | non-error | <5s |
| 8 | `name_source` | SQL | `_enrich_name_source_for_state(state)` | every active gold row has `name_source` | <2s |
| 9 | `strip_plus_codes` | SQL | `strip_plus_codes_from_addresses(state)` | no plus-code-prefixed `address` remains | <2s |
| 10 | `align_scoreable` | SQL | `align_is_scoreable_to_tier(state)` | no Tier-3/4 beach is scoreable | <2s |
| 11 | `purge_pollution` | SQL | `purge_cross_state_extractions(state)` | non-error (idempotent) | <2s |
| 12 | `dedup` | SQL | `run_late_stage_dedup()` | non-error | 5-30s |
| 13 | `geom_queue` | SQL | `process_geom_change_queue(100)` | non-error | <5s |
| 14 | `operator_llm_extract` | Python | `extract_operator_dogs_policy.py --ids <state ops>` | fresh extractions exist (last 7d) | 20-50 min, $5-25 |
| 15 | `operator_merge` | Python | `merge_operator_dogs_policy.py` | merged operator policies for state | 1-3 min |
| 16 | `bep_refire` | Python | `refire_bep_cascade(<tier-1+2 fids>)` | non-error | 1-5 min |
| 17 | `section_extract` | Python | `extract_beach_section_rules.py --states X` | non-error (capped by upstream) | 1-5 min, ~$0.25 |
| 18 | `descriptions` | Python | `generate_beach_descriptions.py --fids <tier-1+2>` | ≥50% of tier-1+2 have description | 15-45 min, $0.50-3 |
| 19 | `photos_mapillary` | Python | `load_mapillary_photos.py --fids <tier-1+2>` | non-error (rate-limited) | 10-30 min, $0 |
| 20 | `daily_refresh_fire` | Python | HTTP fire to `daily-beach-refresh` per scoreable | today's rec ≥95% of scoreable | 5-20 min |

**Resumability:** every phase records `(run_id, state, phase, status)` in `public.pipeline_phase_status`. Re-run with `--run-id <id> --resume` skips already-`ok` phases. Use `--phase-from <key>` to start at a specific phase. Use `--force` to re-run all phases regardless.

**Investigation when a phase fails:**

```sql
select * from public.pipeline_phase_status
 where run_id = <id> order by phase;
```

Look for `status='failed'`, the `error_message`, and the `criterion_text` for context. The `rows_affected` column tells you whether the action did work (rows>0 + criterion failed = action ran but didn't satisfy the bar).

---

## Decisions baked in (the "why")

These choices were made on 2026-05-08/09 during the OR + WA launches and are now canon. Don't second-guess without revisiting the rationale.

### Scoring scope = Location Tier 1 + 2 only

Only beaches with `dogs_allowed='yes'` (tier `1_off-leash` or `2_on-leash`) get scored daily. Tier 3 (`3_limited_access`) and Tier 4 (`4_no_dogs`) are excluded. **Why:** scoring a no-dogs beach wastes weather/tide/LLM API spend; limited-access beaches don't make a clean "should I take my dog" recommendation.

Enforced via `is_scoreable` gate on `beaches_gold`. Phase 10 (`align_scoreable`) flips the gate to match tier classification on every run. The canonical classifier is `public.beach_location_tier(dogs_allowed, has_off_leash, has_on_leash, dogs_prohibited_start)`.

### Location Tier vocabulary (renamed 2026-05-09)

Old set: `1_marquee`, `1b_offleash_caveat`, `1c_onleash`, `3_no_dogs`, `4_limited_access`.

New set: `1_off-leash` (collapsed old marquee + 1b), `2_on-leash`, `3_limited_access` (was tier 4), `4_no_dogs` (was tier 3). **Note the tier 3 ↔ 4 number swap.** Lower number = better-for-dogs, ordering is preserved.

### State-aware operator extraction

`extract_operator_dogs_policy.py` was originally CA-hardcoded. Auditing OR/WA's first run found ~50% of "policy_found=true" extractions were cross-state pollution: Tavily returned similarly-named jurisdictions from CA (e.g. WA's "City of Carbonado" got Coronado, CA's dog-policy URL accepted by a CA-prompt-bound picker). The patch:

1. Replaced "California" hardcoding with state-parameterized prompts throughout picker + Pass-A/B/C extraction.
2. Added `_DOMAIN_STATE_HINTS` heuristic — drops candidates whose URL domain is from a known wrong state *before* the picker sees them.
3. Added `b2_query` iterative re-query — when the initial picker rejects all candidates, fires a second Tavily search with explicit state code: `<operator name> <state code> municipal code dogs leash beach park`. This catches operators whose first search returns SEO blogs but whose municipal codes are indexable.

When you suspect new pollution, run `select * from public.purge_cross_state_extractions('XX');` — heuristically flips `pass_a_policy_found=false` on rows whose URL domain is from a different state.

### POI address propagation must run

`poi_landing` ingests Google Places addresses (street/city/state/zip) for ~100% of POIs. `promote_to_gold` did not propagate them to `beaches_gold`; for any state until 2026-05-09 the `address` column was 0% populated despite the upstream data being there. Phase 6 (`address_poi`) is the fix — `_enrich_address_from_poi_for_state(state)` copies POI addresses to gold per-fid.

### Plus-code stripping (Google reverse-geocode artifact)

`scripts/one_off/geocode_or_wa_missing_address.py` (Google reverse-geocode for OR/WA gold rows missing a POI address) sometimes returns Plus codes (`XV44+XJ Florence, OR, USA`) for remote beaches without street addresses. Phase 9 (`strip_plus_codes`) regex-strips the leading `XV44+XJ ` to yield clean `Florence, OR, USA`. **Net result:** ~92% of OR/WA addresses are real street addresses, ~8% are city-only, 0% are raw Plus codes.

### Plover season seed: source='manual'

`20260508_seed_plover_closures.sql` set `dogs_prohibited_start='03-15'` and `dogs_prohibited_end='09-15'` on matched named plover beaches (Long Beach WA was the only tier-1+2 match). The `promote_canonical_to_consumer_tables(fid)` resolver overwrote those values back to NULL on the next refire because `source='auto_promoted_from_consensus'`. **Fix:** `20260509_plover_seed_source_manual.sql` re-applies the seed with `source='manual'` so the resolver leaves it alone.

### BEP-direct section extraction (not via beaches_staging_new)

The Edge Function `v2-enrich-operational` was the original CA pattern for per-tier dog-policy research, but it writes to `beaches_staging_new` — a table OR/WA never went through (they used the `arena → beaches_gold` direct path). Running `v2-enrich-operational` for OR/WA finds 0 staging rows and does nothing.

**Resolution:** `scripts/extract_beach_section_rules.py` reads from `operator_dogs_policy` and writes per-section evidence directly to BEP with `source='section_research_v1'`. The new injector `_zr_inject_sections_from_bep(zr, fid)` folds those sections into `beach_dog_policy.zone_rules`. Bypasses staging entirely.

### Two parallel ingest paths still coexist

- **`arena → beaches_gold`** (tonight's canon path) — used by OR/WA.
- **`beaches_staging_new → beaches_gold` via `v2-run-pipeline`** (legacy path, partial CA) — 18-stage classifier; 992 CA rows + 478 OR rows in staging, 0 WA rows.

Long-term decision: **deprecate `beaches_staging_new`** in favor of the canon path. Staging adds confusion and parallel logic. Open architectural debt.

---

## Inline actions tonight, codified into the code

Every ad-hoc thing we ran tonight at the SQL prompt or as a `python -c "..."` snippet is now a callable artifact. Reference list:

| Action we ran inline | Now lives in |
|---|---|
| `update beach_enrichment_provenance set pass_a_policy_found=false where ... cross-state ...` | `public.purge_cross_state_extractions(state)` SQL fn |
| Plus-code regex strip on `beaches_gold.address` | `public.strip_plus_codes_from_addresses(state)` SQL fn |
| `update beaches_gold set is_scoreable = ... case tier ...` | `public.align_is_scoreable_to_tier(state)` SQL fn |
| `delete from beach_enrichment_provenance where source in ('operator_*', 'section_research_v1')` | `public.purge_bogus_operator_bep(state)` SQL fn |
| Inline CASE deriving policy tier (used in 6+ scripts) | `public.beach_location_tier(...)` canonical SQL fn |
| `update beach_dog_policy set dogs_prohibited_start='03-15', source='manual' where ... plover beaches` | `20260509_plover_seed_source_manual.sql` migration |
| Backfill `external_source_status` from existing pad_us / osm_amenities / etc rows | `scripts/one_off/_backfill_external_source_status.py` |
| Cross-state pollution domain heuristic | `_DOMAIN_STATE_HINTS` table in `extract_operator_dogs_policy.py` + `domain_state_hint(url)` helper |
| Per-fid `_emit_evidence_from_osm_amenities` invocation | wired into `promote_to_gold` + `refire_bep_cascade` |
| Per-fid POI address propagation | `_enrich_address_from_poi_for_state(state)` SQL fn |
| Per-fid `populate_from_operators_gold` (state-agnostic) | wired into `promote_to_gold` + `refire_bep_cascade` |

Anything still ad-hoc and not in this list is a bug in the runbook. Open an issue.

---

## Known traps

### Mapillary rate limits

`load_mapillary_photos.py` can hit `HTTP 500: Please reduce the amount of data you're asking for` after a few hundred consecutive queries. The loader now has **adaptive backoff** (5s → 15s → 45s → 120s on 429 / 5xx) and **skip-existing by default** (a beach with photos already is skipped unless `--refresh` is passed), so a rate-limited run is resumable: re-run the same command and it picks up where it stopped without re-fetching. Realistic coverage: 30-70% (limited by Mapillary's actual streetview density near beaches, not by our retry logic).

### Daily-refresh per-fid trigger has rotated secret

`tg_beaches_gold_score_on_flip` calls `_fire_daily_beach_refresh_for_fid(fid)` which HTTP-POSTs to the `daily-beach-refresh` Edge Function with an admin secret hardcoded in the function definition. The secret has rotated; current calls return HTTP 401. **Workaround in canon:** Phase 20 (`daily_refresh_fire`) bypasses the trigger and fires manually using the env var `ADMIN_SECRET`. **Real fix:** move the secret to a config table and have the trigger read from it (TODO; flagged in the harness as a security concern when I tried to embed a working secret in pg_proc).

### USFWS Snowy Plover polygons need manual shapefile download

The aggregated `USFWS_Critical_Habitat/FeatureServer/0` REST endpoint returns a placeholder ("Please check current species specific shapefile") for `comname='Western Snowy Plover'`. The CA-hosted `gis.cnra.ca.gov/.../CSMW_Western_Snowy_Plover_Critical_Habitat` has 108 polygons but only `STATE='CA'`. Conservation_Inputs_Snowy_Plover service is a hex grid for modeling, not the actual habitat polygons.

**To unstick:** manually download from `https://gis-fws.opendata.arcgis.com/datasets/fws::snowy-plover-critical-habitat-5` (click → Download → Shapefile), drop the zip in `supabase/.temp/usfws/`, then write a loader modeled on `load_places_shapefile.py` that inserts polygons into `public.dog_policy_zones`. After polygons land, spatial-overlap function fills `public.beach_dog_policy_zones` view.

Memory: `~/.claude/projects/.../memory/project_plover_polygon_load.md`.

### AK PAD-US silent failure

`bulk_load_pad_us.py --priority 1` succeeded for 26 of 27 states; AK exit-coded rc=1 with empty stderr. Pinned for follow-up. Suspect: AK ArcGIS REST endpoint may need pagination or have unusual timeout behavior; or the date-line bbox crossing breaks the query. Re-run individually:
```bash
python scripts/external_sources.py load pad_us --state AK
```
and watch stderr.

### `promote_to_gold` and `run_pipeline_for_state` overloads

Both functions had stale 2-arg signatures alongside the canonical 3-arg ones. **Resolved 2026-05-09**: `20260509_drop_legacy_overloads.sql` dropped both stale overloads. Only the canonical signatures remain:
```
public.promote_to_gold(p_fids bigint[], p_score boolean DEFAULT false, p_publish boolean DEFAULT true)
public.run_pipeline_for_state(p_state text DEFAULT 'CA', p_fids bigint[] DEFAULT NULL, p_skip_precheck boolean DEFAULT false)
```
`scripts/promote_to_gold.py` updated to call the 3-arg form explicitly.

### psycopg2 cursor in failed-transaction state

After `RAISE EXCEPTION` from a SQL function, the psycopg2 cursor remains in `InFailedSqlTransaction` state until a `ROLLBACK` is issued or a new connection opens. If you're orchestrating phases in Python, **open a fresh connection per phase**. The `run_state_pipeline.py` script does this correctly (`open_conn()` returns a fresh connection per phase) — copy that pattern in any new orchestrator.

### Pexels filter rejects all stock results

`load_pexels_photos.py` ran across 1,077 scoreable beaches and saved 0 rows. Pexels' generic stock photos don't match dog-beach specificity; the filter in `_photo_filters.py` correctly rejects them. **Don't expect coverage from Pexels.** Mapillary is the workhorse (CA 68%, OR 33%, WA 24% coverage after one full run).

### v2-enrich-operational doesn't help OR/WA

It writes to `beaches_staging_new`, which OR/WA never populated. Use `extract_beach_section_rules.py` instead (BEP-direct path).

### BEACON not built

`bacteria_risk` is a column on `beach_day_recommendations` consumed by the find feed but no loader exists for the EPA BEACON 2.0 data that would populate it. Field stays null for all states until built. ~1-2 hour build, no API cost (free EPA REST endpoint). Pinned for follow-up.

---

## Runbook for common scenarios

### "Fresh state launch — first time"

```bash
# 1. Pre-load upstream data (one-time per state):
python scripts/one_off/bulk_load_pad_us.py --states XX
python scripts/one_off/bulk_load_overpass.py --states XX
python scripts/one_off/bulk_load_amenities.py --states XX
python scripts/one_off/bulk_load_tiger_places.py --states XX

# 2. Run the orchestrator:
python scripts/run_state_pipeline.py --state XX

# 3. Inspect results:
psql ... -c "select * from public.pipeline_phase_status where state_code='XX' order by run_id, phase;"
```

### "Phase failed, want to resume"

```bash
# Look up the run_id, then:
python scripts/run_state_pipeline.py --state XX --run-id <id> --resume
```

`--resume` skips phases already `status='ok'` for that run_id.

### "I changed an upstream source, want to refresh just that downstream"

```bash
# Force re-run from a specific phase:
python scripts/run_state_pipeline.py --state XX --run-id <id> --resume --phase-from address_poi
```

### "I want a dry-run before I commit"

```bash
python scripts/run_state_pipeline.py --state XX --dry-run
```

Prints the 20-phase plan; no DB writes; no API calls.

### "I just changed `extract_operator_dogs_policy.py` and want to re-extract"

```bash
# Re-run only the LLM half (start at phase 14):
python scripts/run_state_pipeline.py --state XX --run-id <new-id> --phase-from operator_llm_extract
```

### "I think there's cross-state pollution"

```sql
-- Audit:
select op.canonical_name, ope.source_url
  from public.operator_policy_extractions ope
  join public.operators op on op.id = ope.operator_id
 where op.state_code = 'XX' and ope.pass_a_policy_found = true
 order by op.canonical_name;

-- Manually purge by URL pattern:
select * from public.purge_cross_state_extractions('XX');

-- Or re-run the LLM phases (state-aware patches will catch it):
python scripts/run_state_pipeline.py --state XX --run-id <id> --resume --phase-from operator_llm_extract
```

### "Population rates aren't where I want them"

The runbook for each field's coverage:

| Low coverage | Why | What to do |
|---|---|---|
| `address` low | POI didn't have it; Google reverse-geocode gives Plus codes | run `geocode_or_wa_missing_address.py` for state, accept ~8% city-only |
| `addr_city` low | beach outside any TIGER place polygon | data-ceiling: unincorporated coast |
| `c1_jurisdiction_id` low | same as `addr_city` | data-ceiling |
| `park_name` low | beach centroid not inside any PAD-US polygon | data-ceiling for ocean beaches outside protected areas |
| `multi_section zone_rules` low | few operators have `policy_found=true` | re-run `extract_operator_dogs_policy.py` with broader operator set |
| `photos` low | Mapillary rate-limited | spread runs over time; or accept 30-50% coverage |
| `descriptions` low | LLM job hadn't run on tier-1+2 fids yet | re-run Phase 18 |
| `bacteria_risk` always null | BEACON loader not built | build the BEACON loader (TODO) |

---

## What's still TODO (open issues at end of 2026-05-09)

These are real gaps documented but not yet shipped. Each is a known surface for the next session.

1. **BEACON loader** — populate `bacteria_risk` from EPA BEACON 2.0. ~1-2 hr build, free API. **Highest user-facing value remaining.**
2. **AK PAD-US retry** — silent rc=1 failure; investigate.
3. **PAD-US `mng_name` ↔ `operators.canonical_name` alias matching** — currently strict string-equal, so `populate_from_operators_gold` finds 0 PAD-US matches. Add fuzzy matching or alias normalization.
4. **WDFW shorebird closures** — separate per-agency recon for WA. Plover season seed is interim.
5. **USFWS plover polygons** — pinned, manual shapefile download required.
6. **Multi-region zone_rules** — current section extractor produces single-region. To support "north end is off-leash, south end is on-leash" for a single beach, need richer LLM prompt + schema. ~4-8 hr engineering.
7. **Daily-refresh trigger secret rotation** — move secret to config table, update trigger.
8. ~~**Photo loaders not idempotent on rate-limit retry**~~ — fixed 2026-05-09. Loader now has adaptive backoff (5s/15s/45s/120s) on 429/5xx and skip-existing by default; re-runs pick up where they stopped.
9. **`v2-run-pipeline` deprecation** — kill the staging path or fully merge into the canon. Architectural debt.
10. **Dagster integration** — every phase as an asset, lineage in UI. The strategic destination per `docs/pipeline-overview.md` Phase 10 discussion.
11. ~~**Drop legacy 2-arg `promote_to_gold` overload**~~ — fixed 2026-05-09. `20260509_drop_legacy_overloads.sql` dropped both 2-arg `promote_to_gold` and 2-arg `run_pipeline_for_state`. Only canonical 3-arg signatures remain.
12. **Photos source coverage** — Pexels filter rejects all; Pixabay/Unsplash/Flickr keys not loaded; only Mapillary works. Investigate Wikipedia Commons or NPS public photos.

---

## Per-state status

| State | Path | Last launched | Coverage | Notes |
|---|---|---|---|---|
| CA | legacy + partial canon | pre-2026-05-08 | tier-1 mature; staging+gold mix | partial v2-run-pipeline history; do not re-run canon without scoping |
| OR | full canon | 2026-05-08 | 150 tier-1+2; ~50% multi-section, 51% descriptions, 33% photos | clean canon test passed (run_id=3, all 13 SQL phases ok) |
| WA | full canon (via SQL `run_pipeline_for_state`) | 2026-05-08 | 393 tier-1+2; 51% descriptions, 24% photos | bypassed `beaches_staging_new` entirely |
| RI | not yet | — | — | **recommended first all-canon test** (manageable size, all-coastal, 70 OSM beaches) |
| Other priority-1 | upstream loaded, pipeline not yet run | — | — | ready to launch via `run_state_pipeline.py --state XX` |

---

## Memory index

These memory files complement this runbook:

- `~/.claude/projects/C--Program-Files-Git/memory/project_scoring_scope.md` — Tier 1+2 scoring rule, `beach_location_tier()` canonical classifier
- `~/.claude/projects/C--Program-Files-Git/memory/project_plover_polygon_load.md` — pinned USFWS shapefile manual download
- `~/.claude/projects/C--Program-Files-Git/memory/feedback_afk_autonomy.md` — execution-mode discipline when Franz is AFK
- `~/.claude/projects/C--Program-Files-Git/memory/project_tab_merge.md` — UI-side follow-up (not pipeline)

---

## When this runbook is wrong

This document is the operational truth as of 2026-05-09. When you hit something the runbook doesn't predict, **update the runbook before moving on**. Specifically:

- New corrective action you ran inline → codify as SQL function + add row to "Inline actions codified" table
- New trap you hit → add to "Known traps" with the workaround
- New decision you made → add to "Decisions baked in" with the rationale
- New phase needed → add to "The 20 phases" table (or mark as inserted between existing phases)

The runbook + the canonical orchestrator + the migrations together are the codebase's operational memory. Every gap that's only in conversation context is a future bug.
