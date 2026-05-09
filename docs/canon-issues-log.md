# Canon Issues Log

Every issue surfaced by the gated state-launch canon, with root cause and resolution. The discipline of canon ("every phase must pass its criterion before the next runs") forces issues to surface visibly instead of silently corrupting downstream data. Each row is a paid lesson — preserving them so future state launches don't repeat them and so the pipeline's collective immune system grows.

**Format:** chronological. Each entry: ID, date, surfaced-by, phase/area, symptom, root cause, fix (commit + migration), verification.

**How to add a new entry:** when a phase fails its criterion or an unexpected behavior is discovered, add a row to the latest table below before resuming. Don't ship a fix without logging it.

---

## 2026-05-08 — OR/WA launches (pre-canon era)

These were surfaced by manual investigation during OR/WA pipeline work, before the formal phase-gating canon shipped. Each is now codified as durable code.

| # | Phase / area | Symptom | Root cause | Fix |
|---|---|---|---|---|
| 1 | Phase 0 — TIGER places ingest | OR/WA places loaded into `jurisdictions` but `state` column shows 'CA' | `load_jurisdictions_batch(p_batch)` hardcoded `state := 'CA'` regardless of STATEFP in input | `20260508_jurisdictions_state_from_fips.sql` — `state_from_fips()` helper + RPC rewrite |
| 2 | Phase 3 — promote_to_gold | OR/WA `beaches_gold.county_fips`/`park_name` always NULL despite arena having values | `promote_to_gold` INSERT column list omitted both fields; values dropped on the floor | `20260508_fix_gold_inheritance_gaps.sql` — added to INSERT, backfilled existing rows from arena |
| 3 | Phase 3 — populator chain | OSM amenity evidence never landed in BEP for any state | `_emit_evidence_from_osm_amenities(fid)` was defined but never invoked from `promote_to_gold` or `refire_bep_cascade` | `20260508_wire_osm_amenities_emitter.sql` — wired into both orchestrators |
| 4 | Phase 3 — operator policy flow | OR/WA beaches inherit nothing from operators; `populate_from_park_operators_gold` is CA-only (joins `csp_parks`) | Function depended on CA-specific source tables; no analog for non-CA states | `20260508_populate_from_operators_generic.sql` — state-agnostic `populate_from_operators_gold` via PAD-US/county/jurisdictions; `20260508_wire_operators_populator.sql` wires it into orchestrators |
| 5 | Phase 2 — operators table | OR/WA had 0 rows in `operators` (CA-only seed) | No state-parameterized seed function existed | `20260508_populate_operators_for_state.sql` + wired into `run_pipeline_for_state` |
| 6 | Phase 14 — operator LLM extraction | ~50% cross-state pollution: e.g. WA "City of Carbonado" → coronado.ca.us URL accepted | `extract_operator_dogs_policy.py` had "California" hardcoded throughout picker + Pass A/B/C prompts | Patches to script: state-parameterized prompts, `_DOMAIN_STATE_HINTS` heuristic pre-filter, `b2_query` iterative re-query when initial picker rejects all. Plus `20260509_codify_corrective_actions.sql` ships `purge_cross_state_extractions(state)` for cleanup |
| 7 | Phase 5/6 — POI address propagation | OR/WA `beaches_gold.address` 0% populated despite `poi_landing.address_full` 100% populated | `promote_to_gold` did not copy structured address fields from `poi_landing` to gold | `20260509_propagate_poi_address_to_gold.sql` — `_enrich_address_from_poi_for_state(state)` + wired into `run_pipeline_for_state` |
| 8 | Phase 9 — Plus-code address artifact | Google reverse-geocode returns `XV44+XJ Florence, OR, USA` for remote beaches with no street address | Google falls back to Plus-code grid when no street address exists; ~8% of OR/WA results | `20260509_codify_corrective_actions.sql` — `strip_plus_codes_from_addresses(state)` regex-strips the prefix to leave `Florence, OR, USA` |
| 9 | Phase 10 — `is_scoreable` gate | OR/WA tier classification didn't match `is_scoreable` flag (OR had Tier-4 scoreable; WA had Tier-1+2 not scoreable) | Manual flips earlier; no canonical alignment function | `20260509_codify_corrective_actions.sql` — `align_is_scoreable_to_tier(state)` flips per `beach_location_tier()` classifier |
| 10 | Plover closure seed | Long Beach WA had Mar 15-Sep 15 season set then resolver overwrote to NULL on next refire | Seed UPDATE didn't set `source='manual'`; consensus resolver reverted | `20260509_plover_seed_source_manual.sql` — re-applies seed with `source='manual'` |
| 11 | Phase 3 — operator_pad_us BEP | 0 rows emitted from `populate_from_operators_gold` PAD-US arm despite state operators with `policy_found=true` | Join condition was `op.canonical_name = pu.mng_name` but PAD-US `mng_name` for OR/WA is a 3-4 letter agency CODE ('SPR', 'BLM', 'CITY'), not the full name | `20260509_populate_from_operators_pad_us_match.sql` — match against `raw_attrs->>'Loc_Mang'` and `Loc_Own` (full names). Added DISTINCT ON for multi-polygon containment. Effect: 7 OR beaches shifted Tier 2 → Tier 1. |
| 12 | Photos — Mapillary loader | Single rate-limit hit (HTTP 500 "reduce data") killed the run mid-state | No backoff/retry; throw-away on first 5xx | Adaptive backoff (5s/15s/45s/120s) added to `scripts/load_mapillary_photos.py`. Combined with existing skip-existing-by-default → resumable. |
| 13 | SQL — promote_to_gold ambiguity | `promote_to_gold(fids, false)` returned `is not unique` error | Two overloads (2-arg legacy + 3-arg current with `p_publish DEFAULT true`); call ambiguous | `20260509_drop_legacy_overloads.sql` — dropped 2-arg `promote_to_gold` and 2-arg `run_pipeline_for_state`. `scripts/promote_to_gold.py` updated to call 3-arg form explicitly. |
| 14 | Operations — `tee` log buffering | Log file 0 bytes for 80 min during inland operator extract; thought job was hung | `python ... 2>&1 \| tee tmp/foo.log` in this Bash-on-Windows shell buffers tee output for hours | Documented in runbook "Known traps" with cross-checks: process CPU time, DB activity timestamp, harness task status. **No code change.** |

---

## 2026-05-09 — RI launch (canon-era; gating did its job)

The first end-to-end run of the canonical orchestrator (`run_state_pipeline.py --state RI`) on a fresh state. The gate halted at every real issue, forced a fix, allowed resume.

| # | Phase | Symptom | Root cause | Fix |
|---|---|---|---|---|
| 15 | Phase 1 — `precheck` | RI failed precheck despite all 4 sources loaded | `assert_state_upstream_loaded(state)` required `status='ok'`; RI's `osm_landing` was `status='skipped'` (the bulk loader's marker for "data already loaded; declined to re-fetch") | `20260509_relax_precheck_status.sql` — accept both `'ok'` and `'skipped'` |
| 16 | Phase 3 — arena population | RI had 0 arena rows; `promote` would find 0 fids and silently "succeed" with empty state | Canon assumed `arena` was already populated for the state. OR/WA had been hand-prepopulated by `load_state.py` before canon shipped; fresh state launch hits empty arena | New phase `arena_seed` between `operators` and `cluster_group`; runs `promote_poi_landing_to_arena()`, `promote_osm_landing_to_arena()`, `refresh_arena_names_from_osm_landing()`. Criterion: state has ≥1 arena row whose county_fips maps to it. **Updated phase count 20 → 21.** |
| 17 | Phase 4 — `cluster_group` | Timed out at 121s with `statement_timeout` error | Default Postgres `statement_timeout` (60-120s on the pooler) too short for global trigram clustering on 5000+ arena rows (`populate_arena_group_id` is O(N²)) | `open_conn()` in `run_state_pipeline.py` now sets `statement_timeout='600s'` per connection |
| 18 | Phase 6 — `promote` | All 47 RI fids passed promote logic but `beaches_gold` rows had `county_fips=NULL` → criterion failed | A later `create or replace function public.promote_to_gold(...)` migration dropped `county_fips` from the INSERT column list, even though `t.county_fips` was still being read for state inference. Regression from issue #2 — same column, dropped again | `20260509_promote_to_gold_county_fips.sql` — restored `county_fips` to INSERT column list and select clause. Backfill UPDATE for any rows promoted earlier today without it. |

---

## Patterns observed

- **CA-hardcoded assumptions** kept surfacing: load_jurisdictions_batch (issue #1), populate_from_park_operators_gold (#4), extract_operator_dogs_policy.py (#6), `_infer_state_from_county` (related to #18). Each was an architectural assumption that "all states look like CA." Wherever a function takes per-state data, audit it for hardcoded CA logic.
- **Multi-version function drift** happened twice: promote_to_gold had county_fips added (#2), then dropped (#18) by a later migration. Same with run_pipeline_for_state (4 versions, two signatures, dropped in #13). When a function is redefined across migrations, the latest definition wins — easy to silently lose columns. **Mitigation:** before any new migration that touches `promote_to_gold` or `run_pipeline_for_state`, diff against the canonical version in this repo and run the full canon on a test state.
- **Function-with-default-arg overloads** create call ambiguity (#13). Default-arg overloads should be replacements, not additions; the legacy form must be DROPed.
- **Silent gaps** that the gate caught: arena empty (#16), county_fips dropped (#18), cross-state pollution (#6). Each would have produced "successful" pipeline runs that quietly delivered broken data. Canon makes them loud.

## How to extend this log

Add a new row in the latest dated section when:
- A phase criterion fails for a reason other than upstream data not being loaded
- A pipeline run completes but post-hoc data inspection reveals incorrect output
- A migration is shipped to fix a regression introduced by an earlier migration
- A workaround / runbook entry is added for a known trap

For each row, name the **phase**, **symptom** (what was visible to the operator), **root cause** (what the code was actually doing wrong), and **fix** (commit hash or migration filename plus a one-line description of the change). The combination of all three is what makes this log useful in 6 months when none of us remember the details.
