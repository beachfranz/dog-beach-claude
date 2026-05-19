# PIP-prelaunch spec

**Status:** v1 2026-05-19
**Author:** Claude (Franz directive 2026-05-19)
**Related:** `[[never-solve-same-problem-twice]]`, `[[promote-ad-hoc-tools-to-process]]`, `supabase/migrations/20260514_beach_polygon_membership_table.sql`

## Why this exists

A canonical `refresh_beach_polygon_membership('WA')` benchmark on 2026-05-19 showed **13.7s per fid** — 100 min full-state populate. Profiling found two structural bugs:

1. **No state filter on `counties` join.** 3,235 US counties scanned per beach → 9.4s alone.
2. **`pu.geom::geography` cast prevented use of `pad_us_units_geom_geog_idx`.** 308k rows scanned without a geog index → 13s+.

Both fixes brought per-fid time to ~1.2s. But the failure mode that produced this — operating on national polygon tables without state pre-filter and without verifying index use — is repeatable across other tables (`military_bases`, `tribal_lands`) and future polygon kinds.

**This spec codifies the PIP-prelaunch discipline:**

> **Every polygon kind joined in PIP MUST be narrowed to in-state rows before the spatial test.** Every polygon table MUST have a GIST index on the geometry used by the join. Both invariants are asserted at PIP-prelaunch.

## The rule

For any polygon table `T` joined in `refresh_beach_polygon_membership(p_state, p_fid)`:

1. **Narrow to in-state rows first.** In order of preference:
   - (a) Direct state column equality: `T.state = p_state` (pad_us_units, jurisdictions)
   - (b) State FIPS lookup: `T.state_fp = <derived FIPS>` (counties)
   - (c) State postal lookup: `T.state_postal = p_state` (military_bases)
   - (d) Spatial intersect with state polygon: `ST_Intersects(T.geom, states.geom WHERE state_code = p_state)` (tribal_lands, any future table without a state column)
2. **Use the geography GIST index when distance test is in meters.** If table has `geom_geog`, prefer it directly. Don't cast `geom::geography` per row when a geog column exists.
3. **Assert geom GIST index exists at PIP-prelaunch.** If missing, halt with actionable error — don't silently scan-the-world.

## PIP-prelaunch function

`public.assert_pip_indices(p_state text) returns table(table_name text, status text, note text)`

For each polygon table participating in PIP:
- Verifies a GIST index exists on the join geometry column
- Verifies a usable state filter is in place (state column with btree index OR geom-poly prefilter scoped to state)
- Returns one row per table with status='ok'|'warn'|'fail' and a note

Called by `refresh_beach_polygon_membership` as the FIRST step before any spatial join. Halts on `fail`. Logs `warn` rows but continues.

This is the `[[promote-ad-hoc-tools-to-process]]` move for the today's 13s/fid finding — no future polygon kind ships without the pre-check verifying its index + state filter are in place.

## Per-table requirements (canonical list)

| Table              | State filter pattern                       | Geom index for join                  | Notes                                |
|--------------------|--------------------------------------------|---------------------------------------|--------------------------------------|
| pad_us_units       | `pu.state = p_state`                       | `pad_us_units_geom_geog_idx`         | Use `pu.geom_geog` directly          |
| cpad_units         | (CA only — table is CA-scoped)             | gist on geom                          | OK as-is                             |
| jurisdictions      | `j.state = p_state`                        | `jurisdictions_geom_gix`             | OK as-is                             |
| counties           | `c.state_fp = <derived>`                   | `counties_geom_gix`                  | Fixed 2026-05-19 (v2 migration)      |
| military_bases     | `mb.state_postal = p_state`                | `military_bases_geom_gix`            | **Add state filter** (today)         |
| tribal_lands       | `ST_Intersects(tl.geom, state_geom)`       | `tribal_lands_geom_gix`              | **Add state-poly prefilter** (today) |

## Pipeline integration

PIP-prelaunch is the FIRST step inside `refresh_beach_polygon_membership`. Future per-state pipeline phases that call this function inherit the assertion automatically.

For ad-hoc PIP work outside the function, call `assert_pip_indices(state)` directly first. Standalone scripts that do their own spatial joins (rare; should be replaced by the central function) need the same pre-check.

## What this DOESN'T solve

- New polygon kinds added later still require an explicit state filter pattern decision (the spec lists the 4 patterns; pick the one that fits the table's columns).
- Materialized views or cached polygon-sets are out of scope for v1 — the in-line `ST_Intersects(state_geom, T.geom)` prefilter is fast enough at v1 row counts.
- This doesn't address other slow phases (governance resolve, BEP refire). Those are separate `[[never-solve-same-problem-twice]]` candidates.

## Benchmark trail

| Date       | Per-fid (WA) | Notes                                                          |
|------------|--------------|----------------------------------------------------------------|
| 2026-05-19 | 13.74s       | Original — pu.geom::geography cast + no counties state filter |
| 2026-05-19 | 10.68s       | After geom_geog patch on pad_us_units                          |
| 2026-05-19 | 1.24s        | After counties.state_fp filter (v2)                            |
| target     | <0.8s        | After military.state_postal + tribal state-poly prefilter (v3) |

429 WA scoreable beaches × 0.8s ≈ 6 min total — down from the 100 min original.
