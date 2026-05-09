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
| 19 | Phase 17/18/19 — BEP refire / sections / descriptions | RI all 21 phases passed; `beaches_gold` has 46 active rows; but `beach_dog_policy` has 0 rows for RI; downstream phases all return rows=0 | RI has no `state_dogs_policy` row (only CA, OR, WA are seeded). With no operator-level policy researched yet AND no state-level fallback, the BEP consensus had no evidence to resolve, so `promote_canonical_to_consumer_tables` wrote no `beach_dog_policy` rows. **Not a code bug** — expected outcome for a state launch where neither state_dogs_policy nor operator policies have been curated yet. Same fresh-state-quiet-zero pattern OR/WA had before their state_dogs_policy seed. **Mitigation:** seed RI's state_dogs_policy row before relying on the canon to populate dog-policy fields for new states. Codified as new pipeline phase `state_policy_seed` (see issue #20 below + `20260509_policy_seed_phase_helpers.sql`). |
| 20 | populator chain — `populate_from_state_default_gold` | RI launch passed all 21 phases AND had a `state_dogs_policy` row seeded for RI, but BEP showed 0 evidence with `source='state_dogs_policy_v1'`. Manual call to `_emit_evidence_from_state_dogs_policy(fid)` worked fine — emitter was healthy, just never fired during refire | The wrapper `populate_from_state_default_gold(fid)` was added to `promote_to_gold`'s populator loop in 20260508_state_dogs_policy_entity.sql, but **subsequent re-creates of `promote_to_gold` (and `refire_bep_cascade` which never had it) silently dropped the call from the populator chain**. Same multi-version drift pattern as issue #18 (county_fips). Symptom: state-default policy never reached the BEP for any state launched after the drop, including RI. | `20260509_restore_state_default_in_populator_chains.sql` — restored `populate_from_state_default_gold(fid)` to BOTH `promote_to_gold` and `refire_bep_cascade`. After fix: 46/46 RI beaches resolved to Tier `2_on-leash` on the next BEP refire. |
| 21 | Phase 24 — `daily_refresh_fire` | RI's `daily_refresh_fire` phase exited with `rows_affected=24` (all HTTP calls succeeded with 200) but the criterion `today rec exists for >= 95% of scoreable beaches` failed because `beach_day_recommendations` had 0 rows for any of RI's 24 scoreable beaches. The Edge Function silently returns 200 for locations it can't process | The `noaa_stations` table held only **400 Pacific Coast stations** (longitude range -124.7 to -117.1, all CA/OR/WA). Atlantic, Gulf, AK, HI, Great Lakes were absent. `_nearest_noaa_station(geom, p_max_km=100)` returned NULL for every RI beach (~4500km from the nearest CA station, well outside the 100km cutoff). All 24 RI scoreable beaches got `noaa_station_id=NULL` during `promote_to_gold`. The daily-beach-refresh Edge Function silently skips locations without a NOAA station (they need tide data to score). Same family as issue #1 (TIGER places hardcoded to CA): the upstream loader was CA-only and not nationalized | One-off load (2026-05-09 03:35): fetched the full NOAA CO-OPS station list (3,449 stations) from `api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions` and called `load_noaa_stations_batch()` in 200-row batches. Total 3,449 stations now seeded (FL=550, AK=509, SC=247, NJ=195, CA=192, RI=33, etc.). Backfilled `noaa_station_id` on 115 previously-NULL active beaches (RI=46, CA=48, WA=21) via `update beaches_gold set noaa_station_id = _nearest_noaa_station(geom, 100) where noaa_station_id is null`. RI's nearest stations are 1-15km away. **Follow-up:** make the NOAA station load a first-class entry in `external_source_status` (it's currently a one-off, not gated by precheck). The pipeline should validate that `noaa_station_id` is set on every scoreable beach in `align_scoreable` or as a new criterion in `promote`. |

**RI launch completion** (run_id=4, all 21/21 phases ok at 02:49:03 UTC 2026-05-09): first end-to-end clean canon run on a fresh state. Issues #15-18 each surfaced at a phase criterion failure, halted the orchestrator, got fixed via migration, allowed resume. The discipline worked. 46 RI beaches are in `beaches_gold` with structural fields populated.

**Post-RI seed pass (2026-05-09 03:15)**: surfaced issues #19 and #20. Seed migration `20260509_seed_ri_state_dogs_policy.sql` added a RI row to `state_dogs_policy`; populator-chain fix `20260509_restore_state_default_in_populator_chains.sql` re-wired `populate_from_state_default_gold` into both `promote_to_gold` and `refire_bep_cascade`. After both shipped, RI's tier distribution went from `(empty)` → `2_on-leash: 46`.

**Architectural follow-up (2026-05-09 03:30)**: shipped `20260509_policy_seed_phase_helpers.sql` + three new pipeline phases (`state_policy_seed`, `federal_policy_seed`, `seasonal_closure_seed`) in `scripts/run_state_pipeline.py`. They sit between `precheck` and `operators` and assert the per-state policy seed tables are populated before the data-emission phases run. Future state launches halt at `state_policy_seed` with a template INSERT instead of producing a quiet-zero canon (issue #19's symptom). Phase count went 21 → 24.

---

**Durability hardening pass (2026-05-09 03:45)** — after RI surfaced the NOAA gap, audited the entire issues list and asked: *do we have automated guardrails to prevent each issue from recurring?* Closing 3 remaining gaps:

| Family | Issues covered | Mitigation |
|---|---|---|
| Populator-chain regression | #2, #3, #18, #20 — function gets re-created in a later migration and silently drops a column from INSERT or a populator call from the foreach loop | New SQL function `assert_populator_chains_intact()` asserts every required populator/column is present in `promote_to_gold` and `refire_bep_cascade`. New canon phase `chain_integrity_check` runs it right after `precheck` (cheap idempotent check; halts canon BEFORE any data emission). |
| NOAA station coverage | #21 — Pacific-only data caused all RI scoreable beaches to get NULL station_id and silently fail at `daily_refresh_fire` | (a) Registered `noaa_stations` in `external_source_status` with `state='*'` (global marker). (b) `assert_state_upstream_loaded(state)` now also requires global `noaa_stations` ≥ 3000 rows. (c) New canon phase `noaa_station_check` runs `assert_scoreable_have_noaa_for_state(state)` after `align_scoreable` — halts if any scoreable beach has NULL station. (d) Updated `align_is_scoreable_to_tier(state)` to demote any beach with NULL `noaa_station_id` regardless of tier (inland lake beaches → catalog only, not scoreable). |
| Promote completeness | #2, #18 — promote dropped county_fips silently | New SQL function `assert_promote_complete_for_state(state)` requires every active beach in state to have BOTH `county_fips` and `state` set. Promote phase criterion now calls this function (was just a county_fips count before). |

**Side-effect from durability hardening:** the new `align_is_scoreable_to_tier` strict-tier-1+2-only-with-station rule corrected pre-existing CA scope drift: 373 CA Tier-3/4/unknown beaches that had `is_scoreable=true` (they predate the canonical Tier 1+2 scoring scope) were demoted to `is_scoreable=false`. CA scoreable count: 577 → 204. This is canonical-correct per `project_scoring_scope.md` ("only Tier 1 + Tier 2 get scored"); the prior 577 included 373 out-of-scope beaches that should never have been scored.

**Pipeline now 26 phases** (was 25). Two new SQL phases added: `chain_integrity_check` (after precheck) and `noaa_station_check` (after align_scoreable). Plus the `precheck` and `promote` phase criteria are tighter. Migration file: `20260509_canon_durability_fixes.sql` + `20260509_canon_durability_v2.sql`.

---

---

## 2026-05-09 (afternoon) — DE launch + architectural cleanups

The DE launch surfaced six more durability gaps and three architectural shortcomings (CA-hardcoded data, federal-as-separate-pipeline, free-text agency matching). Each is now codified.

| # | Phase / area | Symptom | Root cause | Fix |
|---|---|---|---|---|
| 22 | poi_landing → arena | DE launch promoted only 6 OSM beaches; 49 POI candidates (Bethany, Rehoboth, Cape Henlopen, etc.) sat in `poi_landing` invisible to `promote_poi_landing_to_arena()`. Same pattern affected 24 priority-1 states (FL=708, MA=685, MI=662, etc.) | `poi_landing.state` was only populated for CA/OR/WA. The promote function filters by state; rows with `state=NULL` were skipped silently | `20260509_poi_landing_state_from_fips.sql` — backfilled `state` from `state_from_fips(county_geoid)` (7,484 rows). Added trigger `poi_landing_state_sync` to keep state synced from county_geoid on every INSERT/UPDATE. **Spec:** `poi_landing.state` is derived from FIPS, not from Google's text `address_state` |
| 23 | poi_landing | Despite #22 fix, 0 DE POIs promoted. Investigation: all 49 had `is_active=false, inactive_reason='not_ca_county'` | A one-time CA-scope filter soft-deleted every poi_landing row outside CA counties. Stale CA-hardcoded artifact from before priority-1 expansion | `20260509_poi_landing_activate_non_ca.sql` — re-activated 5,327 rows across 24 priority-1 states (FL=708, MA=685, MI=662, HI=538, NY=469, NJ=269, ME=192, etc.) |
| 24 | promote_poi_landing_to_arena | Despite #22+#23, still 0 DE POIs promoted | `promote_poi_landing_to_arena()` requires BOTH `is_active=true` AND `is_dog_beach_signal=true`. The signal flag was false for all non-Pacific POIs | Inline UPDATE flipped `is_dog_beach_signal=true` for 6,169 priority-1 active POIs. *No migration file (data fix only); should add to a future schema-curation migration if we re-load these tables* |
| 25 | promote_poi_landing_to_arena | DE arena_seed phase failed: `duplicate key value violates unique constraint "arena_pkey"` | Function's `WHERE NOT EXISTS` collision check filtered to `source_code='poi'` only, but `arena.fid` is the PK regardless of source. POI fids that collided with existing OSM fids broke the INSERT | `20260509_promote_poi_arena_conflict_nothing.sql` — `ON CONFLICT (fid) DO NOTHING`. Silent skip is correct: the OSM row already represents the same beach |
| 26 | cluster_group | DE v5/v5b hit 600s timeout / connection drop | `populate_arena_group_id()` ran globally on the entire arena (~12k rows after activation pass). For DE's 55 candidates, we were O(N²)-clustering 12k rows | `20260509_cluster_group_state_scoped.sql` — added `p_state` parameter; all CTE filters scope to state's counties. DE clustering went 600s+ → 8s |
| 27 | tg_inactivate_on_no_dogs trigger | Tier 4 beaches (`dogs_allowed='no'`) were entirely removed from catalog | Trigger flipped `is_active=false` whenever `beach_dog_policy.dogs_allowed='no'` was written. Wrong gate: display should be tier-based; cost gate is `is_scoreable` | `20260509_tier4_visible_not_scoreable.sql` — dropped trigger; re-activated 130 clean trigger casualties (CA=87, WA=29, OR=14). Dedup victims (86 with `inactive_reason='dupe_of_*'`) correctly stayed inactive |
| 28 | scoring scope | NOAA-station-required gate kept inland beaches (Lake Chelan WA, Yakima River, etc.) out of scoreable. 19 WA beaches affected | `align_is_scoreable_to_tier` required `noaa_station_id IS NOT NULL`; daily-beach-refresh threw on null station | `20260509_inland_beaches_scoreable.sql` — dropped NOAA gate from align; relaxed `noaa_station_check` to advisory. `daily-beach-refresh/index.ts` skips fetchTides on null station and proceeds with empty tideMap (mirror of the crowd-failure pattern). Scoring uses `tideHeight=null → score=0.5` neutral fallback — same pattern as null `busynessScore` |
| 29 | find page display | "Scored" toggle conflated cost gate (LLM/refresh) with display gate (catalog visibility). Tier 4 beaches were invisible | Display should be tier-based, not based on whether scoring data exists | `20260509_find_beaches_returns_tier.sql` — RPC returns `location_tier`. `find.html` adds CSS `tier-${tier}` class; T1+T2 normal, T3 muted, T4 grey + 🚫 prefix. Edge Function default flipped to `scored=false` (full catalog) |
| 30 | operator_llm_extract | Phase 20 ran extraction for ALL 51 DE city operators (~13min, $3) — most are inland with no beach in their footprint | Smart filter missing | `20260509_operators_with_beaches_filter.sql` — `state_operator_ids_with_beaches(state)` filters to operators whose footprint contains a beach. Savings: DE 90% (51→5), RI 55%, OR 84% (269→42), WA 78% (295→64), CA 73% (611→165) |
| 31 | federal-policy curation | Original design had a separate `federal_policy_seed` phase + `pad_us_unit_dogs_policy` table. Cape Cod NS, Monomoy NWR, etc. needed manual curation | We already have `level='federal'` in operators (CA had 19) and an extraction pipeline that works for any operator with a website (NPS / FWS unit pages are findable). Separate path was redundant | `20260509_federal_operators_per_state.sql` — `populate_operators_for_state` now seeds federal coastal-rec units (NPS NS / Lakeshore / NRA / Park / Monument + USFWS NWR) as level='federal' operators with PAD-US polygon as geom. Dropped `federal_policy_seed` phase from canon. Filter tightened from "any FED" to canonical name patterns. Result: MA=12, RI=6, DE=3, OR=43, WA=46, CA=89 federal operators |
| 32 | populate_from_operators_gold PAD-US join | Text-match `op.canonical_name = pu.raw_attrs->>'Loc_Mang'` was case/whitespace-sensitive. Missing 30-50% of legitimate matches across PAD-US name variations ("DELAWARE STATE PARKS" vs "Delaware State Parks") | No canonical agency-name dictionary | `20260509_agency_aliases_schema.sql` + `20260509_agency_resolver_and_integration.sql` + `20260509_agency_dictionary_complete.sql` — new `agency_aliases` table (alias + alias_normalized + operator_id), `_normalize_agency_text()` (lowercase + punctuation collapse + whitespace; **does NOT strip** "City of"/"County of" to avoid cross-state collisions per Franz's review), `resolve_agency()` with 4-step strategy (exact / normalized_exact / stripped fallback / fuzzy via pg_trgm GIN). `populate_from_operators_gold` PAD-US arm now uses `resolve_agency_id` instead of textual match. Auto-self-alias trigger keeps dictionary in sync with operators table. Backfill: 6,234 self+legacy aliases. Bulk PAD-US/OSM backfill runs out-of-band via `scripts/one_off/backfill_agency_aliases.py` |

**Architectural decisions captured this round:**

- **Display vs. cost gates separated** (issues #27, #29): `is_active` controls catalog presence (all tiers visible); `is_scoreable` controls downstream LLM/refresh spend (Tier 1+2 only). Tier 4 beaches stay in catalog with muted UI.
- **NOAA-station optional for scoreability** (issue #28): inland beaches scored without tide data; tide axis becomes neutral 0.5 (same as null crowd). Allows Lake Chelan, Yakima River, etc. to be scored.
- **Federal collapsed into operators** (issue #31): instead of two parallel curation pipelines, NPS/USFWS units are first-class operators. Existing `extract_operator_dogs_policy.py` researches them like any other operator. `pad_us_unit_dogs_policy` retained as a manual-override layer for high-confidence curated rules but no longer the primary federal path.
- **Canonical agency dictionary** (issue #32): `agency_aliases` table is the single source of truth for agency-name reconciliation across PAD-US, OSM, operator inputs, and free text. Normalization preserves boilerplate ("City of") to avoid collisions; resolver fallback strips it for novel references.

**Pipeline now 26 phases** (was 24 before this session). Phase order with new additions in **bold**:

1. precheck (now also requires global `noaa_stations` ≥ 3000)
2. **chain_integrity_check** (NEW; catches populator regression family #2/#18/#20)
3. state_policy_seed
4. ~~federal_policy_seed~~ REMOVED — collapsed into Pass 3 of `populate_operators_for_state`
5. seasonal_closure_seed
6. operators (now seeds cities + counties + **federal**)
7. arena_seed
8. cluster_group (now **state-scoped** — was global O(N²))
9. cluster_extras
10. promote (criterion now uses `assert_promote_complete_for_state`)
11. address_poi
12. address_city
13. name_source
14. strip_plus_codes
15. align_scoreable (no NOAA gate; Tier 1+2 only)
16. **noaa_station_check** (NEW; advisory)
17. purge_pollution
18. dedup
19. geom_queue
20. operator_llm_extract (uses **smart filter**)
21. operator_merge
22. bep_refire
23. section_extract
24. descriptions
25. photos_mapillary
26. daily_refresh_fire
27. **field_population_check** (NEW; per-state audit at end)

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
