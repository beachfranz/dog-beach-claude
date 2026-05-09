# Next-session handoff (2026-05-08 → next)

Franz is AFK; new session should pick up and execute autonomously.

## Context (90-second read)

OR + WA shipped end-to-end tonight. CA stays as-is (752 active scoreable).
Two new states are in `beaches_gold`:

- **OR**: 152 active, 111 Tier 1 marquee (off-leash, Beach Bill), 39 Tier 1c, 2 Tier 4. Gated `is_scoreable=false` — not in find feed.
- **WA**: 405 active, 0 Tier 1 (correct — Public Trust doesn't grant blanket off-leash), 394 Tier 1c, 11 Tier 4. Also gated.

Pipeline hardening shipped this session:
- FIPS-based state inference (no more 15 misclassifications per launch)
- POI-landing reactivator wired into run_pipeline_for_state (Phase 0)
- Tier 3 auto-inactivate trigger
- zone_rules sand baseline (every beach has at least the headline rule)
- promote_to_gold chunked into batches of 50 (timeout-safe)
- Per-row trigger respects `app.promote_to_gold_active` flag
- search_path locked on 106 SECURITY DEFINER functions
- state_dogs_policy entity (new — first-class state-level dogs policy)
- OPRD ocean-shore proxy + WA tideland proxy polygons in pad_us_units
- state_dogs_policy county-scoping (OR coastal vs inland)

## Constraints to respect

- **Supabase Nano tier**, upgrade currently blocked. Storage limit ~8 GB,
  current size ~1.5 GB. Statement timeouts hit fast on big spatial joins.
- **Franz is AFK** — no synchronous decisions. If you hit a real fork,
  pin and stop rather than guess.
- **East US region** — every query has ~70ms RTT to CA. Per-fid loops
  are network-bound. Use SQL set-ops where possible; chunk per-fid.

## Next-session priority queue

### Priority 1 — autonomous, ship-ready: bulk PAD-US load

```
python scripts/one_off/bulk_load_pad_us.py --priority 1 2>&1 | tee tmp/bulk_pad_us.log
```

This loads PAD-US for the 27 priority-1 coastal + Great Lakes states.
Idempotent: skips any state already with >100 rows. Storage-gated:
pauses if DB approaches 6.5 GB. Expected wall clock: 60–90 min.

After it finishes, `python scripts/one_off/bulk_load_pad_us.py --priority 1 --dry-run`
should show all states "SKIP (already loaded)".

### Priority 2 — autonomous, ship-ready: bulk Overpass per state

```
python scripts/one_off/bulk_load_overpass.py --priority 1 2>&1 | tee tmp/bulk_overpass.log
```

Loads `natural=beach` Overpass per state into `osm_landing`. State-clipped
via the load_state.py logic. Idempotent. Wall clock: ~40 min.

### Priority 3 — pin-only

These need Franz's input or careful review; **don't auto-run**:

- **RLS audit** on 64 RLS-disabled public tables (project_supabase_advisor_cleanup.md)
  — half-day of per-table policy decisions. Includes PII tables.
- **3 missing FK indexes** — quick win once decided which to ship
- **Region migration** (East→West) — not until upgrade unblocks
- **OPRD canonical ocean-shore polygon** — manual download from
  spatialdata.oregonexplorer.info would replace the synthesized proxy
- **OR seeds got demoted from marquee** — Cannon Beach et al. are
  correctly Tier 1 marquee now. The 5 OR seeds (fids 9711–9715) had
  per-beach extracted evidence pulling them to 1c; the new state
  coastal row at 0.65 should override. Actually verified post-fix:
  Cannon Beach is correctly Tier 1 now.
- **Daily-refresh gating** — OR's 152 active beaches have `is_scoreable=true`
  from before tonight's gating fix; they're still being scored daily and
  costing $1–5/day in narrative LLM. If you want them gated retroactively:
  `update beaches_gold set is_scoreable=false where state='OR' and is_active`
  — but loses 7 days of forecast data. Better: leave as-is; they're paid for.

### Priority 4 — open data quality

- **WA Tier 1 designated off-leash beaches** — Magnuson Park (King),
  Double Bluff (Island), Westhaven SP (Grays Harbor), Edmonds Marina
  (Snohomish), Howarth Park (Snohomish). Need per-beach evidence
  curation. Manual admin curator work.
- **Snowy plover seasonal closures (Mar 15–Sep 15)** — 6 OR beaches
  affected; need per-beach evidence to drop them to Tier 1b. Specific
  beaches: Sutton (Lane), Siltcoos River Mouth (Lane), Tahkenitch Creek
  (Douglas), Tenmile Creek (Coos), New River (Coos), Floras Lake (Curry).

## How to verify the bulk loads worked

After Priority 1 + 2 finish:

```sql
-- Count states with PAD-US loaded
select state, count(*) c from public.pad_us_units
 where state is not null group by 1 order by c desc;

-- Count states with osm_landing data (rough: bbox-based)
select count(distinct round(lat::numeric, 0)) lat_buckets,
       count(*) total_osm_landing,
       max(fetched_at) latest
  from public.osm_landing where tags->>'natural' = 'beach';

-- Db size sanity check
select pg_size_pretty(pg_database_size(current_database()));
```

## What ships AFTER bulk download

Once data is loaded, future state launches via `admin/state-launcher.html`
become instant Phase B — no Overpass wait, no PAD-US fetch. State launcher
becomes one-click for any state.

## Files to consult

- `docs/bulk-data-download-plan.md` — full plan with constraints
- `scripts/one_off/bulk_load_pad_us.py` — runner
- `scripts/one_off/bulk_load_overpass.py` — runner
- `~/.claude/projects/.../memory/MEMORY.md` — index of pinned followups
- `CLAUDE.md` — stable map of architecture
