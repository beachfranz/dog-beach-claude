---
name: dog-park-state-launch
description: Use this skill when launching the dog-park coverage pipeline for a new US state, or re-running it after stops/regressions. Triggers include "launch dog parks for <STATE>", "run dog park pipeline on <STATE>", "OR dog parks", "WA dog park coverage", or asking how to get a new state's dog parks off `osm_default` policy and onto `operator_posted_v2`. DO NOT use for beach codify (use codify-state), beach per-policy extraction (use phase-10b-extract), or photo loaders (use add-state-park-loader). The pipeline drives 12 ops via scripts/dog_park_pipeline/run.py in strict order; deviating from the order skips load-bearing unblocks per [[dog-park-coverage-playbook]].
---

# dog-park-state-launch — 12-op coverage pipeline for one state

Bring a state's dog parks from `osm_default` stub policy to `operator_posted_v2` curated policy + amenities + photos. CA proof point: 0% → 79.5% in one session. OR: 65.9% in one Dagster run. WA: similar.

The pipeline encodes lessons from CA's wandering 3-hour discovery into a strict-order sequence. Each step's value depends on the prior step's output — don't reorder unless you understand why.

## Prerequisites (state-specific, mostly one-time per state)

1. **State has dog parks in `dog_parks_gold`** — usually from OSM bulk-load. Check: `SELECT count(*) FROM dog_parks_gold WHERE state='<STATE>' AND is_active`. Need ≥20 to be worth running.
2. **City operators seeded for big cities** — most states already have these from prior runs. Check: `SELECT count(*) FROM operator WHERE state_code='<STATE>'`. Need ≥3-5 city operators for walker to do anything useful.
3. **`scripts/dog_park_pipeline/state_operator_seeds.json` has the state's city list** for `seed_operators` step (only matters if seeding new states).
4. **Playwright installed** — `.venv-pipeline/Scripts/playwright install chromium`. Preflight checks this.

If 1+2 are sparse for a new state (e.g., FL with no operators seeded), the pipeline still runs but `walk_catalogs` and `retry_no_match` are no-ops — expected coverage ceiling drops to ~60-70% on the extractor + no-website routes alone.

## The 12-op sequence (DO NOT REORDER)

| # | Op | What it does | Why before next |
|---|---|---|---|
| 1 | `preflight` | Browser + env vars + operator-coverage check | Catch missing infra before LLM cost |
| 2 | `pip_address_city` | `UPDATE address_city` via ST_Contains on jurisdictions | Steps 4/7/8 need address_city for web_search queries |
| 3 | `reclassify_junk` | Flip tiny/HOA/commercial parks inactive | Don't waste LLM on parks we'll deactivate |
| 4 | `generic_display_names` | "Lake Elsinore Small Dog Park" not "Small Dog Park" | LLM web_search much better with city prefix |
| 5 | `seed_operators` (Dagster only) | Auto-create city operators from state_operator_seeds.json | Walker needs operator pool |
| 6 | `walk_catalogs` | Top-N city operators → web_search → discovery queue | Discovers NEW parks not in OSM |
| 7 | `ingest_queue` | Geocode pending + 150m dup check → INSERT new gold | Creates gold rows with website tag |
| 8 | `run_extractor` | Extract amenities for ALL default-source parks | Routes: OSM-website / URL-slug / web_search |
| 9 | `retry_no_match` | Query-variant retry for first-pass misses | Uses prefixed display names from step 4 |
| 10 | `dp_photos_load_flickr` | Flickr photo loader, biased to dog terms | Per [[dp-loader-dog-bias]] |
| 11 | `dp_photos_load_wikimedia` | Wikimedia loader | Some parks only on commons |
| 12 | `dp_photos_load_websearch` | Tavily web_search loader | Fills gaps |
| 13 | `dp_photos_curate` | Top-3 by sort_order | Per [[dp-photos-are-dog-only]] |

## Run signatures

```bash
# Standalone (preferred while Dagster's weather_grid is broken):
.venv-pipeline/Scripts/python.exe -m scripts.dog_park_pipeline.run --state OR

# Re-run a subset
.venv-pipeline/Scripts/python.exe -m scripts.dog_park_pipeline.run --state WA --only run_extractor,retry_no_match

# Resume from an op (skip everything before it)
.venv-pipeline/Scripts/python.exe -m scripts.dog_park_pipeline.run --state CA --start-at run_extractor

# Dagster (once weather_grid Config is fixed):
dagster asset materialize -m dog_beach \
  --select "dp_preflight,dp_pip_address_city,dp_reclassify_junk,dp_generic_display_names,dp_seed_operators,dp_walk_catalogs,dp_ingest_queue,dp_run_extractor,dp_retry_no_match" \
  --partition <STATE>
```

Wall time per state: ~10-15 min CA-size, ~5-8 min smaller states. Cost: ~$5-15 LLM per state. The pipeline writes metrics to `public.dog_park_coverage_runs` (state, op, started_at, ended_at, metrics jsonb) for observability.

## What to do FIRST for a new state

1. Check prerequisites SQL above
2. If city operator pool is thin (<3-5), add seed cities to `scripts/dog_park_pipeline/state_operator_seeds.json` under the 2-letter code. Cities with ≥3 active dog parks.
3. Add state code to `DOG_PARK_STATES` `StaticPartitionsDefinition` in `scripts/dagster/dog_beach/dog_beach/assets/dog_park_coverage.py` (Dagster only).
4. Run `--state <NEW>` standalone first to validate before scheduling.

## Reclassify guard — DO NOT flip these inactive (per Franz 2026-05-25 LATE)

Tempting to flip "Pet Area" / "Pet Exercise Area" / "Love's Dog Park" / "LaPine State Park Pet Exercise Area" as junk. **DON'T.** These are the road-tripper use case — someone driving I-5 from SF to LA wants to know about every rest-stop pet zone on the way.

**ONLY flip inactive:**
- area_m2 < 200 (mis-tagged park-entrance pins / apartment dog runs)
- name contains HOA / apartment / condo / villa / residents / complex
- Explicitly commercial-indoor (Camp Canines, Fido's Indoor Dog Park — paywalled training, not public)

**Don't flip:**
- "Pet Area" / "Pet Exercise Area" (rest stops)
- "Love's Dog Park" (truck-stop pet zones)
- State park rest-area facilities
- Any park that's outdoor + public-access even if no operator website

Already encoded in `scripts/dog_park_pipeline/reclassify.py` PROTECTED list. Don't loosen it.

## Honest coverage ceiling per state

| Park type | Web_search extracts? | Realistic share |
|---|---|---|
| Big-city parks with operator site | yes (~85%) | ~40-60% of state |
| Rural / unincorporated dog parks | maybe | ~10-20% |
| Rest-stop / truck-stop pet zones | no (no operator site) | ~5-10% (KEEP) |
| Tiny / commercial / private HOA | n/a (flip inactive) | ~3-8% |

**Practical ceiling: 75-85% operator-posted-v2.** Stop chasing % past that — diminishing returns.

## After-run verification

1. Check `dog_park_coverage_runs` for failed ops:
```sql
SELECT state, op, ended_at-started_at AS dur, metrics->>'error' AS err
FROM public.dog_park_coverage_runs
WHERE state='<STATE>' AND started_at > now() - interval '2 hours'
ORDER BY started_at;
```

2. Coverage SQL:
```sql
SELECT
  count(*) FILTER (WHERE is_active) AS active,
  count(*) FILTER (WHERE is_active AND policy_source = 'operator_posted_v2') AS posted_v2,
  round(100.0 * count(*) FILTER (WHERE is_active AND policy_source = 'operator_posted_v2')
        / NULLIF(count(*) FILTER (WHERE is_active), 0), 1) AS pct
FROM public.dog_park_dog_policy ddp
JOIN public.dog_parks_gold dpg ON dpg.fid = ddp.dog_park_fid
WHERE dpg.state = '<STATE>';
```

3. **`verify-sweep` skill** — click-through audit of dog-park.html across 5-8 representative parks: big-city, small-city, rural, rest-stop, no-operator. Catches 500s + missing operator names + Scout blurb issues.

## Anti-patterns

- **Don't hand-code operator catalog URLs** — all 14 CA URLs hand-coded in early attempts were stale. Web_search escalation finds the real ones every time.
- **Don't add a 12s Playwright wait** — caused regression on CA. 4-6s + URL-slug fallback is better.
- **Don't run `extract_per_beach_offleash_v2` or per-beach policy** — wrong scope. Dog parks live in different schema.
- **Don't try operator codify for dog parks** — per [[dogpark-rules-are-operator-posted]], city-wide leash codify doesn't help (dog parks are definitionally OK + off-leash). Operator-posted policy + amenities is the value.
- **Don't try "City of <name>" matching for orphan attribution** — singular `operator` table is sparse for non-CA states. PIP-fill `address_city` instead via step 2.

## Per Franz preferences

- [[use-pipeline-infrastructure]] — `run.py` is the orchestrator. Don't write one-off chunked drivers.
- [[chunked-subprocess]] — pipeline ops are already chunked (workers=4 walker, workers=6 extractor) under [[supabase-pool-cap-vs-dagster-concurrency]] cap.
- [[claim-tested-without-end-state-verification]] — coverage SQL only proves rows exist. `verify-sweep` is mandatory.
- [[never-solve-same-problem-twice]] — the playbook is in the pipeline. Don't re-discover the order by zig-zagging.
