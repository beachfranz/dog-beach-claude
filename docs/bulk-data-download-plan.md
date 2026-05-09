# Bulk geographic data pre-download — plan

Status: planned 2026-05-08, ready for next session to execute autonomously.

## Constraints

**Supabase compute: Nano tier** (smallest dedicated). Upgrade is currently
blocked (Franz posted to Supabase support). This means:

- Statement timeouts hit faster than on Micro/Small — already requires
  the chunking pattern shipped in `20260508_chunked_promote_in_pipeline.sql`
- Spatial joins on a global 500K-row `pad_us_units` will be meaningfully
  slower than today's 50K (CA+OR+WA). Plan for ~10× the wall clock on
  any per-fid spatial query.
- Storage is finite — Nano's typical limit is 8 GB on Pro. Current DB
  size is ~1.5 GB. Headroom for full-US load is real but not generous.
  See "Storage budget" below.
- No read replicas (Nano can't host them).

**Mitigation strategy**: load **coastal states first** (where actual
beach data lives), gate on storage thresholds, defer non-coastal interior
states until upgrade unblocks.

## Goal

Pre-load all the US-wide geographic data the pipeline needs for any future
state launch. Once shipped, every state launch becomes a thin in-DB
operation: no Overpass wait, no PAD-US fetch, no rate-limit anxiety.
State-launch wall clock drops from ~15 min to ~30 sec.

## Why now

OR + WA launches surfaced repeated friction:
- Overpass per-state queries take 30–60s and are subject to rate limits
- PAD-US per-state ArcGIS REST queries take 3–5 min each
- A botched run leaves partial data and re-fetches
- Franz is AFK a lot; manual-each-state is fragile

Pre-loading is **idempotent and resumable** — a daemon can chip through
the list overnight without supervision.

## What to load

### 1. PAD-US for all 50 states + DC + territories (highest priority)

Existing tool: `scripts/external_sources.py load pad_us --state XX`
Already loaded: CA (30,990), OR (9,597), WA (10,390)

Remaining ~47 states. Estimated: ~500K total rows, ~500 MB–1 GB storage.
Sequential per-state run via the existing loader.

**Provider behavior**: USGS PAD-US Living Atlas REST endpoint is public
and explicitly designed for bulk consumption. Rate-limit friendly when
sequential. Loading all 50 sequentially: ~30–40 min wall clock total.

**Cost**: $0 (free public API).

### 2. Overpass `natural=beach` for all 50 states

Existing tool: `scripts/load_state.py --state XX --skip-pad-us --skip-noaa`
runs the Overpass+osm_landing portion only.

Already loaded: CA (sparse), OR (479 fetched), WA (1,482 fetched).
Estimate for full US: ~30–50K elements total, ~50 MB.

**Provider behavior**: Overpass tolerates state-by-state queries. Best
practice is **one query per state**, sequential, with 5–10s rest between
states. Loading all 50: ~40 min wall clock.

**Cost**: $0.

### 3. NOAA tide-prediction stations

Already loaded nationwide (3,449 stations across all states). ✓ done.

### 4. us_beach_points POI catalog

Already loaded (8,041 rows nationwide). ✓ done. Next step is
reactivating per-state via `reactivate_poi_landing_for_state(state)` —
that's now part of `run_pipeline_for_state` so it happens automatically.

### 5. Counties (TIGER)

Already loaded nationwide (~3,000 counties). ✓ done.

### 6. OSM amenities (`amenity=parking|toilets|drinking_water|...`)

Existing migration loaded CA OSM amenities (~329K). For other states,
similar Overpass query per state. Per-state ~30–80K amenities. Lower
priority — only matters for `beach_amenities` enrichment, which is
already a known weak spot (4/65 OR coverage today).

## Execution

### Phase 1 — PAD-US, coastal states first

Build a one-off script `scripts/one_off/bulk_load_pad_us.py` with a
priority-ordered list and a storage gate.

**Priority 1 — coastal + Great Lakes states** (where beach data lives):
```
AK ME NH MA RI CT NY NJ DE MD VA NC SC GA FL AL MS LA TX
HI MI MN WI IL IN OH PA   (Great Lakes)
```

**Priority 2 — major-river states** (eventually):
```
ID NV AZ UT NM CO WY MT ND SD NE KS OK AR MO IA TN KY WV VT
```

**Priority 3 — defer until upgrade**:
```
DC AL territories
```

Loader pseudocode:
```python
STORAGE_LIMIT_MB = 6000   # Nano-safe ceiling; pause if approaching
for st in priority_1_states:
    if pad_us_units_count(st) > 100:
        log(f'{st} already loaded, skipping')
        continue
    if total_db_size_mb() > STORAGE_LIMIT_MB:
        log(f'STORAGE GATE: {total_db_size_mb()}MB approaching {STORAGE_LIMIT_MB}MB; stopping')
        break
    subprocess.run(['python', 'scripts/external_sources.py',
                    'load', 'pad_us', '--state', st])
    time.sleep(5)
```

Wall clock estimate (priority 1 only, ~28 states): ~60–80 min on Nano.
Fully autonomous; resumable across crashes (per-state idempotent).

### Phase 2 — Overpass per state

Build `scripts/one_off/bulk_load_overpass.py` modeled on `load_state.py`'s
Phase A.2 only:

```python
for st in states:
    osm_count = osm_landing_count_for_state(st)
    if osm_count > 50:
        print(f'{st} already has {osm_count} osm_landing rows, skipping')
        continue
    fetch_overpass_for_state(st)  # state-clipped, idempotent
    time.sleep(10)  # be polite to Overpass
```

Wall clock: ~40 min.

### Phase 3 — Refresh / staleness tracking

Add `external_source_status(source, state, last_loaded_at, row_count)`
table. Each loader writes a row on completion. Quarterly cron checks
rows older than 90 days and queues them for re-fetch.

Out of scope tonight; pin for follow-up.

## Risk + mitigation

| Risk | Mitigation |
|---|---|
| Storage exceeds 8 GB Pro tier | Monitor `pg_total_relation_size('public.pad_us_units')` after each batch; stop if approaching limit |
| Overpass rate-limit / 429 | Sequential with sleep; loader retries with backoff |
| State-clip rejects valid bleed-overs | The clip is by intent (CA→OR launch had 3 ID + 1 CA bleed-over); accept |
| Bad data corrupts existing CA/OR/WA rows | Loader is idempotent ON CONFLICT DO UPDATE; only writes per-state slice |
| Long-running script fails halfway | Each per-state load is its own transaction; resume by checking `pad_us_units_count` per state |

## Success criteria

- All 50 states have `pad_us_units` rows (count > 100 per state, exception for HI/AK)
- All 50 coastal states have `osm_landing` rows (`natural=beach` count > 50)
- New state launch via admin UI: Phase A.0 (NOAA already loaded) + Phase A.1 (PAD-US already loaded) + Phase A.2 (Overpass already loaded) → all skip; Phase B promotes immediately
- Wall clock per launch drops from 15 min → 30 sec

## Autonomous runner

Designed to work without Franz present:

1. Both bulk loaders are **idempotent** — re-running picks up where it stopped
2. They **log progress** in a known file (e.g., `tmp/bulk_pad_us.log`)
3. They **fail soft** — one bad state doesn't block the rest
4. Total ~80 min of wall clock, can chunk into background tasks

Run sequence for new session:
```
python scripts/one_off/bulk_load_pad_us.py 2>&1 | tee tmp/bulk_pad_us.log
python scripts/one_off/bulk_load_overpass.py 2>&1 | tee tmp/bulk_overpass.log
```

Or fire-and-forget as Bash background tasks if working through the
Claude Code interface.

## Open questions for user

1. **Storage budget** — current Supabase tier and remaining headroom. If
   ~1 GB grows the project past the tier, pre-decide whether to upgrade
   or trim (e.g., skip non-coastal states). Loaded sizes per state in
   the existing 3:
   - CA: ~412 MB
   - OR: ~52 MB
   - WA: ~57 MB
   So full-US PAD-US is more like 600 MB–1 GB depending on state mix.

2. **States to skip** — if storage is tight, skip non-coastal/non-water states:
   keep coastal + Great Lakes + major-river-beach states; drop AZ/CO/UT/etc.

3. **Refresh cadence** — PAD-US is updated quarterly by USGS; OSM is
   continuous. Next-session pin: build the staleness tracker.
