"""run_state_pipeline.py — canonical state-launch orchestrator (2026-05-09 canon).

Each phase: action SQL + success criterion. Status recorded in
public.pipeline_phase_status. First failure halts. Resumable by run_id.

Phases (in order):
   chain_integrity_check  — assert_populator_chains_intact
   state_policy_seed      — research + seed state_dogs_policy
   seasonal_closure_seed  — research + seed seasonal_closures
   ensure_tiger_places    — bulk_load_tiger_places (per-state, idempotent)
   ensure_pad_us          — bulk_load_pad_us       (per-state, idempotent)
   ensure_overpass        — bulk_load_overpass     (osm_landing, per-state)
   ensure_amenities       — bulk_load_amenities    (osm_amenities, per-state)
   ensure_dog_features    — bulk_load_dog_features (osm_dog_features, per-state)
   precheck               — assert_state_upstream_loaded (sanity-check)
   operators              — populate_operators_for_state
   arena_seed             — promote_*_landing_to_arena
   cluster_group          — populate_arena_group_id (state-scoped)
   cluster_extras         — populate_arena_extras
   promote                — promote_to_gold for the state's fids
   address_poi            — _enrich_address_from_poi_for_state
   address_city           — _enrich_address_city_for_state
   name_source            — _enrich_name_source_for_state
   strip_plus_codes       — strip_plus_codes_from_addresses
   catchment_refresh      — refresh_catchment_cascade (catchment_score → state_pct → scoring_tier)
   purge_pollution        — purge_cross_state_extractions
   dedup                  — run_late_stage_dedup (arena-cluster based)
   dedup_distance_name    — run_distance_name_dedup (same-name + same-county + 1km)
   geom_queue             — process_geom_change_queue
   <LLM/external phases — operator_llm_extract, operator_merge, bep_refire,
                          section_extract, descriptions, photos_wikimedia,
                          daily_refresh_fire, field_population_check>

Usage:
   python scripts/run_state_pipeline.py --state OR
   python scripts/run_state_pipeline.py --state OR --run-id 42 --resume
   python scripts/run_state_pipeline.py --state OR --force      (ignore prior status)
   python scripts/run_state_pipeline.py --state OR --skip-precheck
   python scripts/run_state_pipeline.py --state OR --dry-run    (print plan only)
"""
from __future__ import annotations
import argparse, ast, json, os, re, subprocess, sys, threading, time, urllib.parse
from pathlib import Path
import httpx
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

# Windows: stdout defaults to cp1252 when piped (e.g. through tee), which
# can't encode unicode chars like → ≤ used in our log lines. Reconfigure
# to utf-8 with replace fallback so logging never crashes a phase.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except (AttributeError, OSError):
        pass

HEARTBEAT_INTERVAL_S = 15

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')


# Phase definitions. Each has:
#   key            — short identifier stored in pipeline_phase_status.phase
#   action         — SQL returning a single int (rows_affected)
#   criterion      — SQL returning a boolean (true = pass)
#                    Use $STATE as a placeholder; substituted at runtime.
#   criterion_text — human-readable description of the criterion
PHASES = [
    # Cheap idempotent check — runs early so any populator-chain drift
    # halts the canon BEFORE we run promote/refire and write bad data.
    # Catches the regression family of issues #2, #18, #20. Has no upstream
    # data dependency, so it goes first.
    {
        'key': 'chain_integrity_check',
        'action': "select case when public.assert_populator_chains_intact() then 1 else 0 end::int",
        'criterion': "select public.assert_populator_chains_intact()",
        'criterion_text':
            'promote_to_gold + refire_bep_cascade contain all expected populator '
            'calls AND promote INSERT contains all expected columns',
    },
    # ─── Policy-seed phases (Franz, 2026-05-09) ─────────────────────────
    # These three phases assert the per-state / per-unit / per-species
    # policy seed tables are populated BEFORE the canon's data-emission
    # phase (`promote`) runs. Without seeded policies, downstream phases
    # produce 0 beach_dog_policy rows (the RI fresh-state-quiet-zero
    # symptom). See docs/canon-issues-log.md issue #19 + #20.
    #
    # Each phase has:
    #   - action: a python action that auto-attempts to seed via LLM
    #     research script (or no-op if data is already present).
    #   - criterion: SQL that returns true iff the seed is sufficient.
    {
        'key': 'state_policy_seed',
        'kind': 'python',
        'action': 'state_policy_seed',
        'criterion':
            "select (public.unseeded_state_policy_for_state($STATE) = 0)::boolean",
        'criterion_text': 'state_dogs_policy has at least 1 row for state',
    },
    # federal_policy_seed phase REMOVED 2026-05-09: collapsed into the
    # operator-extraction pipeline. populate_operators_for_state now seeds
    # federal coastal units (NPS NS / USFWS NWR / etc.) as level='federal'
    # operators. extract_operator_dogs_policy.py researches them like any
    # other operator. populate_from_operators_gold emits BEP via PAD-US
    # polygon containment. See 20260509_federal_operators_per_state.sql.
    {
        'key': 'seasonal_closure_seed',
        'kind': 'python',
        'action': 'seasonal_closure_seed',
        'criterion':
            "select (public.unseeded_seasonal_closures_for_state($STATE) = 0)::boolean",
        'criterion_text':
            'no pending seasonal closures (status=pending) for state',
    },
    # ─── Upstream bulk-loader phases (Franz, 2026-05-09) ────────────────
    # These wrap the per-state bulk loaders that USED to live in the
    # state-launch runbook as manual commands. With these in canon, a
    # state launch is truly single-command. Each ensure_X phase:
    #   - Reads public.external_source_status for (source, $STATE).
    #   - If status in ('ok','skipped') AND last_loaded_at is recent, skip.
    #   - Otherwise invoke the loader subprocess; loader updates the status
    #     row on success/failure.
    # Order: tiger_places before operators (operators depends on
    # jurisdictions); the rest before arena_seed (which depends on OSM
    # landings + PAD-US/amenities for downstream populators).
    {
        'key': 'ensure_tiger_places',
        'kind': 'python',
        'action': 'ensure_tiger_places',
        'criterion':
            "select coalesce((select status in ('ok','skipped') "
            "                 from public.external_source_status "
            "                 where source='tiger_places' and state=$STATE), false)",
        'criterion_text': "external_source_status['tiger_places', state] in (ok, skipped)",
    },
    {
        'key': 'ensure_pad_us',
        'kind': 'python',
        'action': 'ensure_pad_us',
        'criterion':
            "select coalesce((select status in ('ok','skipped') "
            "                 from public.external_source_status "
            "                 where source='pad_us' and state=$STATE), false)",
        'criterion_text': "external_source_status['pad_us', state] in (ok, skipped)",
    },
    {
        'key': 'ensure_overpass',
        'kind': 'python',
        'action': 'ensure_overpass',
        'criterion':
            "select coalesce((select status in ('ok','skipped') "
            "                 from public.external_source_status "
            "                 where source='osm_landing' and state=$STATE), false)",
        'criterion_text': "external_source_status['osm_landing', state] in (ok, skipped)",
    },
    {
        'key': 'ensure_amenities',
        'kind': 'python',
        'action': 'ensure_amenities',
        'criterion':
            "select coalesce((select status in ('ok','skipped') "
            "                 from public.external_source_status "
            "                 where source='osm_amenities' and state=$STATE), false)",
        'criterion_text': "external_source_status['osm_amenities', state] in (ok, skipped)",
    },
    {
        'key': 'ensure_dog_features',
        'kind': 'python',
        'action': 'ensure_dog_features',
        'criterion':
            "select coalesce((select status in ('ok','skipped') "
            "                 from public.external_source_status "
            "                 where source='osm_dog_features' and state=$STATE), false)",
        'criterion_text': "external_source_status['osm_dog_features', state] in (ok, skipped)",
    },
    # PIP-prelaunch — populate beach_polygon_membership for the state.
    # Per docs/pip_prelaunch_spec.md. Refactored RPC asserts indices
    # exist + state-filters every polygon kind; per-fid ~1.1s on WA.
    # Must run AFTER ensure_pad_us (needs polygons loaded) and BEFORE
    # codify_cascade (which reads PIP via codify_federal_coverage).
    {
        'key': 'ensure_pip_membership',
        'kind': 'python',
        'action': 'ensure_pip_membership',
        'criterion':
            # Every active beach in the state has ≥1 polygon_membership row.
            # (PAD-US has near-universal coverage in MVP+ states.)
            "select coalesce("
            "  (select count(*) filter (where exists ("
            "         select 1 from public.beach_polygon_membership m "
            "          where m.gold_fid = g.fid)) "
            "        = count(*) "
            "    from public.beaches_gold g "
            "   where g.state = $STATE and g.is_active and g.geom is not null), "
            "  false)",
        'criterion_text':
            'every active beach in state has ≥1 beach_polygon_membership row',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active and geom is not null), "
            "     d as (select count(*)::int n from public.beaches_gold g "
            "             where g.state=$STATE and g.is_active and g.geom is not null "
            "               and exists (select 1 from public.beach_polygon_membership m "
            "                             where m.gold_fid = g.fid)) "
            "select d.n done, t.n total from d, t",
    },
    # precheck moved here from phase #1: now serves as the final sanity
    # check that all loaders above succeeded. With the ensure_* phases
    # auto-running missing loaders, precheck is now belt-and-suspenders
    # rather than a hard gate at the start.
    {
        'key': 'precheck',
        'action': "select count(*)::int from public.assert_state_upstream_loaded($STATE)",
        'criterion':
            "select (count(*) >= 4)::boolean from public.external_source_status "
            "where state = $STATE and source in ('pad_us','osm_landing','osm_amenities','tiger_places') "
            "  and status in ('ok','skipped')",
        'criterion_text': 'all 4 required sources status in (ok, skipped) + noaa_stations(global) loaded',
    },
    {
        # Codify-cascade phase — per docs/codify_cascade_phase_spec.md
        # (Franz 2026-05-19). Enumerates federal / county / city
        # codify universe for the state, pre-checks coverage, emits
        # a JSON manifest of any units needing agent dispatch. v1
        # is REPORT-ONLY (doesn't auto-dispatch). Gate: ≤5% of
        # scoreable beaches may lack a beach_policy_source link.
        # On new state launches (MA/FL/MI) this will surface real
        # codify gaps and halt until codify is run. For existing
        # MVP+ states (CA/OR/WA) the gate passes immediately.
        'key': 'codify_cascade',
        'kind': 'python',
        'action': 'codify_cascade',
        'criterion':
            "select (public.count_uncovered_scoreable_beaches($STATE) "
            "        <= greatest(1, (select count(*) from public.beaches_gold "
            "                          where state=$STATE and is_active "
            "                            and scoring_tier in ('daily','hourly')) "
            "                         / $UNCOVER_DIVISOR))::boolean",
        'criterion_text':
            'coverage ≥ launch-mode gate (state-launch 80% / steady-state 95%)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active and scoring_tier in ('daily','hourly')), "
            "     d as (select t.n - public.count_uncovered_scoreable_beaches($STATE) as n from t) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-13 split: each pass runs in its own autocommitted
        # transaction to bound memory. The monolithic SQL FUNCTION used
        # to OOM Supabase on states with heavy PAD-US (OR's max 915K-
        # vertex polygons). See 20260513_split_operator_seeding.sql.
        'key': 'operators',
        'kind': 'python',
        'action': 'operators_chunked',
        'criterion':
            "select (count(*) > 0)::boolean from public.operators "
            "where state_code = $STATE and is_active",
        'criterion_text': 'operators table has rows for state',
    },
    {
        'key': 'arena_seed',
        'action':
            # Three landing→arena promotions are global (touch all rows). Run all
            # three; total rows_affected = poi promotes + osm promotes + name refreshes.
            "select coalesce((select promoted from public.promote_poi_landing_to_arena()), 0)::int "
            "     + coalesce((select promoted from public.promote_osm_landing_to_arena()), 0)::int "
            "     + coalesce((select arena_rows_updated from public.refresh_arena_names_from_osm_landing()), 0)::int",
        'criterion':
            # State has at least one arena row whose county_fips maps to it.
            # State FIPS is derived inline because we don't have a SQL helper that takes a 2-letter state.
            "select (count(*) > 0)::boolean from public.arena a "
            "join public.counties c on c.geoid = a.county_fips "
            "where a.is_active and c.state_fp = (select case $STATE "
            "  when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05' "
            "  when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10' "
            "  when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16' "
            "  when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20' "
            "  when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24' "
            "  when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28' "
            "  when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32' "
            "  when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36' "
            "  when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40' "
            "  when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45' "
            "  when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49' "
            "  when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54' "
            "  when 'WI' then '55' when 'WY' then '56' end)",
        'criterion_text': 'at least one arena row with county_fips in this state',
    },
    {
        'key': 'cluster_group',
        'action':
            # State-scoped: only re-clusters arena rows in this state's
            # counties. Global O(N²) was hitting 600s timeout once arena
            # exceeded ~10k rows. State-scope keeps it fast (DE: 55 rows).
            "select coalesce((select relation_grouped from public.populate_arena_group_id($STATE)), 0)::int",
        'criterion': "select true",
        'criterion_text': 'no exception',
    },
    {
        'key': 'cluster_extras',
        'action':
            "select coalesce((select intra_osm_trigram from public.populate_arena_extras()), 0)::int",
        'criterion': "select true",
        'criterion_text': 'no exception',
    },
    {
        'key': 'promote',
        'action':
            "with f as (select array_agg(a.fid) fids from public.arena a "
            "  join public.counties c on c.geoid = a.county_fips "
            "  where a.is_active and c.state_fp = (select case $STATE "
            "    when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05' "
            "    when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10' "
            "    when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16' "
            "    when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20' "
            "    when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24' "
            "    when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28' "
            "    when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32' "
            "    when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36' "
            "    when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40' "
            "    when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45' "
            "    when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49' "
            "    when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54' "
            "    when 'WI' then '55' when 'WY' then '56' end) ) "
            "select coalesce((select rows_promoted + rows_already_in_gold "
            "                   from public.promote_to_gold((select fids from f)::bigint[], false::boolean, true::boolean)), 0)::int",
        'criterion':
            "select public.assert_promote_complete_for_state($STATE)",
        'criterion_text':
            'every active beach has county_fips and state set (#2/#18 guard)',
    },
    {
        'key': 'address_poi',
        'action':
            "select public._enrich_address_from_poi_for_state($STATE)",
        'criterion': "select true",
        'criterion_text': 'no exception (POI propagation)',
    },
    {
        'key': 'address_city',
        'action':
            "select public._enrich_address_city_for_state($STATE)",
        'criterion': "select true",
        'criterion_text': 'no exception (city PIP)',
    },
    {
        'key': 'name_source',
        'action':
            "select public._enrich_name_source_for_state($STATE)",
        'criterion':
            "select (count(*) filter (where name_source is null) = 0)::boolean "
            "from public.beaches_gold where state = $STATE and is_active",
        'criterion_text': 'every active beach in state has name_source set',
    },
    {
        'key': 'strip_plus_codes',
        'action': "select public.strip_plus_codes_from_addresses($STATE)",
        'criterion':
            "select (count(*) = 0)::boolean from public.beaches_gold "
            "where state = $STATE and is_active "
            "  and address ~* '^[2-9CFGHJMPQRVWX]{4,}\\+[2-9CFGHJMPQRVWX]+\\s+'",
        'criterion_text': 'no plus-code-prefixed addresses remain',
    },
    # ── align_scoreable + noaa_station_check phases retired 2026-05-13 ──
    # is_scoreable column is gone. scoring_tier (Matrix C') is the only
    # gate now. NOAA absence is a data-quality flag (noaa_station_id IS
    # NULL → tide score null), not a hard exclusion. Catchment phase
    # below produces the canonical tier directly.
    # Refresh catchment → state_pct → scoring_tier. Chains:
    # recompute_beach_catchment(state) → refresh_catchment_state_pct(state)
    # → refresh_scoring_tier(null). Idempotent.
    {
        'key': 'catchment_refresh',
        'action':
            "select coalesce((select catchment_recomputed "
            "                   from public.refresh_catchment_cascade($STATE)), 0)::int",
        'criterion':
            "select (count(*) filter (where scoring_tier is null) = 0)::boolean "
            "from public.beaches_gold where state = $STATE and is_active",
        'criterion_text': 'every active beach in state has scoring_tier set',
    },
    {
        # 2026-05-21: refresh beaches_gold.nearest_dog_park_* materialized
        # cols per state. Powers the cliff/no-sand-access overlay
        # (project_deferred_beach_dogpark_cross_reference). Idempotent
        # per-fid UPDATE; self-chunks at 250/commit. Runs after
        # catchment_refresh (both maintain derived columns on beaches_gold).
        'key': 'refresh_nearest_dog_park',
        'kind': 'python',
        'action': 'refresh_nearest_dog_park',
        'criterion':
            "select (count(*) filter (where nearest_dog_park_fid is not null "
            "                            or nearest_dog_park_distance_m is not null) "
            "        >= floor(count(*) * $NEAREST_DP_PCT))::boolean "
            "from public.beaches_gold "
            "where state = $STATE and is_active",
        'criterion_text': 'at least 90% (steady-state) / 80% (state-launch) of active beaches '
                          'have nearest_dog_park_* populated',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active), "
            "     d as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active "
            "               and (nearest_dog_park_fid is not null "
            "                    or nearest_dog_park_distance_m is not null)) "
            "select d.n done, t.n total from d, t",
    },
    {
        'key': 'purge_pollution',
        'action':
            "select rows_purged from public.purge_cross_state_extractions($STATE)",
        'criterion': "select true",
        'criterion_text': 'cross-state pollution flipped (idempotent)',
    },
    {
        'key': 'dedup',
        # Pass the current state so dedup runs per-state. Original function was
        # hardcoded to CA; refined 2026-05-12 with state param + pair-safety +
        # name-group sub-clustering. See project_beaches_gold_dedup_backlog.md.
        'action': "select coalesce((select kills from public.run_late_stage_dedup($STATE)), 0)::int",
        'criterion': "select true",
        'criterion_text': 'no exception',
    },
    {
        'key': 'dedup_distance_name',
        # Complementary pass: catches same-normalized-name + same-county pairs
        # within 1km that the arena-cluster-based dedup misses (OSM+POI cross-
        # source-cluster dups). Added 2026-05-12 after spotting Ocean Shores
        # Bulkhead (1115m) + Picnic Beach OC (290m) outside the auditor reach.
        # 1000m threshold is the safe sweet spot; >1km may merge legit
        # opposite-endpoint cases on long beaches.
        'action': "select coalesce((select kills from public.run_distance_name_dedup($STATE, 1000)), 0)::int",
        'criterion': "select true",
        'criterion_text': 'no exception',
    },
    {
        'key': 'geom_queue',
        'action': "select coalesce((select fids_processed from public.process_geom_change_queue(100)), 0)::int",
        'criterion': "select true",
        'criterion_text': 'no exception',
    },
    # ─── LLM / external phases ─────────────────────────────────────
    # These use kind='python' — they shell out to scripts and validate
    # via SQL criterion afterward.
    {
        'key': 'operator_llm_extract',
        'kind': 'python',
        'action': 'operator_llm_extract',  # python function name below
        # Criterion accepts the no-op case (every candidate was skipped-recent
        # or picker-rejected → 0 new rows, but that's correct work). Passes if:
        #   (a) any extractions exist for this state, OR
        #   (b) no active operators in this state still need extraction
        #       (every candidate either already-extracted OR not-applicable).
        # MD 2026-05-22: 3 candidates, all skipped/rejected → 0 fresh rows; the
        # original "fresh in last 7d" check failed that legitimate no-op case.
        'criterion':
            "select (exists ("
            "    select 1 from public.operator_policy_extractions ope "
            "    join public.operators op on op.id = ope.operator_id "
            "    where op.state_code = $STATE) "
            "  or not exists ("
            "    select 1 from public.operators o "
            "    where o.state_code = $STATE and o.is_active "
            "      and o.level in ('city','county','state') "
            "      and not exists (select 1 from public.operator_policy_extractions ope "
            "                       where ope.operator_id = o.id)))::boolean",
        'criterion_text': 'extractions exist for state OR all active operators tried at least once',
        'progress_sql':
            "with t as (select count(*)::int n from public.operators "
            "             where state_code = $STATE and is_active "
            "               and level in ('city','county','state')), "
            "     d as (select count(distinct ope.operator_id)::int n "
            "             from public.operator_policy_extractions ope "
            "             join public.operators op on op.id = ope.operator_id "
            "            where op.state_code = $STATE "
            "              and ope.extracted_at > now() - interval '4 hours') "
            "select d.n done, t.n total from d, t",
    },
    {
        'key': 'operator_merge',
        'kind': 'python',
        'action': 'operator_merge',
        'criterion':
            "select (count(*) > 0)::boolean from public.operator_dogs_policy odp "
            "join public.operators op on op.id = odp.operator_id "
            "where op.state_code = $STATE",
        'criterion_text': 'merged operator policies exist for state',
    },
    {
        'key': 'bep_refire',
        'kind': 'python',
        'action': 'bep_refire',
        'criterion':
            "select true",  # idempotent — non-error == success
        'criterion_text': 'refire ran without error',
    },
    {
        'key': 'section_extract',
        'kind': 'python',
        'action': 'section_extract',
        'criterion':
            "select true",  # match coverage capped by operator policy_found set; 0 acceptable
        'criterion_text': 'section extractor ran (coverage capped by upstream)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct gold_fid)::int n "
            "             from public.beach_enrichment_provenance "
            "            where source = 'section_research_v1' "
            "              and gold_fid in (select g.fid from public.beaches_gold g where g.state=$STATE)) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: v2 area-first re-extract (Franz). Supersedes
        # section_extract's operator-summary path. Reads policy_source
        # full_text directly with area-first prompt + canonical_area_description
        # hint; rewrites bps/temporals per beach. Multi-ps beaches re-run
        # each ps. Skips manual_curator. Cascade picks up changes via
        # existing _zr_inject_from_policy_sources injector — no extra
        # wiring downstream.
        #
        # section_extract is retained as a no-op-on-clean path (idempotent
        # if its operator_summary input is unchanged), but v2 is now the
        # source of truth for sections/regions/windows.
        'key': 'zone_rules_v2_refresh',
        'kind': 'python',
        'action': 'zone_rules_v2_refresh',
        'criterion':
            # Pass: at least 80% of MVP+ beaches have a bps row produced
            # in the last 7 days. Uses COALESCE(extracted_at, created_at)
            # because agent-authored INSERTs typically leave extracted_at
            # NULL (only the auto-populated created_at is set). Both signal
            # "row is recent"; either-or treatment is correct.
            "select (count(*) filter (where exists ("
            "          select 1 from public.beach_policy_source bps "
            "           where bps.beach_fid = g.fid "
            "             and COALESCE(bps.extracted_at, bps.created_at) >= now() - interval '7 days')) "
            "        >= floor(count(*) * 0.8))::boolean "
            "from public.beaches_gold g "
            "join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "where g.state=$STATE and g.is_active "
            "  and bdp.source <> 'manual_curator' "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash')",
        'criterion_text': 'at least 80% of MVP+ beaches have a fresh-within-7d bps row '
                          '(created_at or extracted_at)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and bdp.source <> 'manual_curator' "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct g.fid)::int n "
            "             from public.beaches_gold g "
            "             join public.beach_policy_source bps on bps.beach_fid=g.fid "
            "            where g.state=$STATE and g.is_active "
            "              and COALESCE(bps.extracted_at, bps.created_at) >= now() - interval '7 days') "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: materialize beach_section_hour_status. Walks
        # zone_rules per-minute (most-restrictive-wins) with seasonal
        # filter + operating-hours clip, sampling at :30 of each hour.
        # Powers scoring + rendered chips/tiles without re-running that
        # logic at request time. Idempotent: deletes + re-inserts
        # (beach_fid, valid_date) slice per fid.
        #
        # Must run AFTER zone_rules_v2_refresh (depends on freshly
        # extracted zone_rules) and BEFORE descriptions (so generated
        # copy can reference the rendered chip state).
        'key': 'hourly_status_refresh',
        'kind': 'python',
        'action': 'hourly_status_refresh',
        'criterion':
            # Pass: at least 80% of MVP+ beaches have a row in
            # beach_section_hour_status for today's date.
            "select (count(*) filter (where exists ("
            "          select 1 from public.beach_section_hour_status h "
            "           where h.beach_fid = g.fid "
            "             and h.valid_date = current_date)) "
            "        >= floor(count(*) * 0.8))::boolean "
            "from public.beaches_gold g "
            "join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "where g.state=$STATE and g.is_active "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash')",
        'criterion_text': 'at least 80% of MVP+ beaches have today\'s hourly status materialized',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct h.beach_fid)::int n "
            "             from public.beach_section_hour_status h "
            "             join public.beaches_gold g on g.fid=h.beach_fid "
            "            where g.state=$STATE and g.is_active "
            "              and h.valid_date = current_date) "
            "select d.n done, t.n total from d, t",
    },
    {
        # Phase 24 swapped from Mapillary → Wikimedia Commons (2026-05-09).
        # Wikimedia produces ~3× higher-quality photos (CC-licensed real
        # photos vs Mapillary's street-view shots) and the loader has
        # keyword bias + photographer auto-blocklist. Mapillary loader
        # still exists at scripts/load_mapillary_photos.py for ad-hoc
        # use but is no longer in the canon.
        #
        # 2026-05-21: moved AHEAD of descriptions so the description
        # generator can leverage tagged + curated photos. Order is now
        # photos_wikimedia → photos_tag → photos_curate → descriptions.
        'key': 'photos_wikimedia',
        'kind': 'python',
        'action': 'photos_wikimedia',
        'criterion': "select true",  # coverage varies by region; 0 acceptable
        'criterion_text':
            'wikimedia loader ran (Commons coverage varies; rural beaches may have 0)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct bp.arena_group_id)::int n "
            "             from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "            where g.state=$STATE and g.is_active and bp.source='wikimedia') "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: Flickr photos (alongside Commons). Both feed the
        # downstream photos_tag → photos_curate selector. ON CONFLICT
        # DO NOTHING in the loader makes re-runs safe (HARD pin
        # feedback_photo_loaders_not_idempotent).
        'key': 'photos_flickr',
        'kind': 'python',
        'action': 'photos_flickr',
        'criterion': "select true",  # coverage varies (API rate, keyword bias)
        'criterion_text': 'flickr loader ran (coverage varies)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct bp.arena_group_id)::int n "
            "             from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "            where g.state=$STATE and g.is_active and bp.source='flickr') "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: state-conditional agency photo loaders.
        #   NPS (always, --state filter)
        #   CDPR + CCC (CA only)
        #   WSPRC (WA only)
        #   OPRD (OR only — NOT BUILT, flagged in log)
        # Agency-driven (not beach-fid driven) so single-shot, not chunked.
        'key': 'state_photo_galleries',
        'kind': 'python',
        'action': 'state_photo_galleries',
        'criterion': "select true",  # coverage varies by state; NPS-only states may be light
        'criterion_text': 'agency photo loaders ran (NPS always; CDPR+CCC for CA; WSPRC for WA)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct bp.arena_group_id)::int n "
            "             from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "            where g.state=$STATE and g.is_active "
            "              and bp.source in ('nps','cdpr','ccc','wsprc','oprd')) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-22: populate distance_m for agency-gallery fanout photos
        # via park-centroid backfill (polygon-via-membership). Runs before
        # photos_curate so get_beach_photos_diverse can use the new
        # distance_m signal. Catalog-wide, idempotent.
        'key': 'photo_centroid_backfill',
        'kind': 'python',
        'action': 'photo_centroid_backfill',
        'criterion':
            # Pass: at least 90% of MVP+ beaches' agency photos (CDPR/WSPRC/
            # NPS/OPRD) have distance_m populated.
            "select coalesce("
            "  (select count(*) filter (where distance_m is not null)::float / "
            "          nullif(count(*), 0) >= 0.9 "
            "     from public.beach_photos bp "
            "     join public.beaches_gold g on g.fid=bp.arena_group_id "
            "    where g.state=$STATE and g.is_active "
            "      and bp.source in ('cdpr','wsprc','nps','oprd')), true)::boolean",
        'criterion_text': 'at least 90% of MVP+ state agency photos have distance_m populated',
        'progress_sql':
            "with t as (select count(*)::int n from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "             where g.state=$STATE and g.is_active "
            "               and bp.source in ('cdpr','wsprc','nps','oprd')), "
            "     d as (select count(*)::int n from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "            where g.state=$STATE and g.is_active "
            "              and bp.source in ('cdpr','wsprc','nps','oprd') "
            "              and bp.distance_m is not null) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: vision-tag photos so the description generator and
        # the auto-curator can use scene/subject/landscape tags. Tagger
        # is idempotent (skips photos already at current model id).
        'key': 'photos_tag',
        'kind': 'python',
        'action': 'photos_tag',
        'criterion':
            # Pass: at least 80% of MVP+ beaches have at least one tagged
            # photo (source_meta->'vision' present).
            "select (count(*) filter (where exists ("
            "          select 1 from public.beach_photos bp "
            "           where bp.arena_group_id = g.fid "
            "             and (bp.source_meta ? 'vision' "
            "                  or bp.source_meta->'vision' is not null))) "
            "        >= floor(count(*) * 0.8))::boolean "
            "from public.beaches_gold g "
            "join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "where g.state=$STATE and g.is_active "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash')",
        'criterion_text': 'at least 80% of MVP+ beaches have at least one vision-tagged photo',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct g.fid)::int n "
            "             from public.beaches_gold g "
            "             join public.beach_photos bp on bp.arena_group_id=g.fid "
            "            where g.state=$STATE and g.is_active "
            "              and bp.source_meta ? 'vision') "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: auto-curate N best+diverse photos per beach using
        # the vision-tag-aware diverse selector. HUMAN ALWAYS WINS: skips
        # any beach where a manual curator already picked photos.
        'key': 'photos_curate',
        'kind': 'python',
        'action': 'photos_curate',
        'criterion':
            # Pass: at least 80% of MVP+ beaches have at least one
            # curated photo (curated_at NOT NULL).
            "select (count(*) filter (where exists ("
            "          select 1 from public.beach_photos bp "
            "           where bp.arena_group_id = g.fid "
            "             and bp.curated_at is not null)) "
            "        >= floor(count(*) * 0.8))::boolean "
            "from public.beaches_gold g "
            "join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "where g.state=$STATE and g.is_active "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash')",
        'criterion_text': 'at least 80% of MVP+ beaches have at least one curated photo',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct g.fid)::int n "
            "             from public.beaches_gold g "
            "             join public.beach_photos bp on bp.arena_group_id=g.fid "
            "            where g.state=$STATE and g.is_active "
            "              and bp.curated_at is not null) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-21: descriptions moved AFTER photos_tag + photos_curate
        # so the generator can reference the tagged + curated gallery.
        'key': 'descriptions',
        'kind': 'python',
        'action': 'descriptions',
        'criterion':
            "select (count(*) filter (where bd.arena_group_id is not null) >= "
            "        floor(count(*) * 0.5))::boolean "
            "from public.beaches_gold g "
            "left join public.beach_descriptions bd on bd.arena_group_id = g.fid "
            "join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid "
            "where g.state = $STATE and g.is_active "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash')",
        'criterion_text': 'at least 50% of tier-1+2 beaches have descriptions',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct bd.arena_group_id)::int n "
            "             from public.beach_descriptions bd "
            "             join public.beaches_gold g on g.fid=bd.arena_group_id "
            "            where g.state=$STATE and g.is_active) "
            "select d.n done, t.n total from d, t",
    },
    {
        # 2026-05-22: soft regression check on freshly-generated descriptions
        # (per Franz). Runs the leak-pattern audit (amenities/facilities
        # filler, lifeguards-on-duty, crowd-timing fluff, safety hedging,
        # architecture-explicit leaks) + multi-region surfacing rate on
        # descriptions updated in the last 2 hours. Prints the report;
        # phase always passes (audit is informational, not gating).
        'key': 'descriptions_audit',
        'kind': 'python',
        'action': 'descriptions_audit',
        'criterion': "select true",  # informational; never gates
        'criterion_text': 'description leak-pattern audit (informational; warnings printed)',
    },
    {
        'key': 'daily_refresh_fire',
        'kind': 'python',
        'action': 'daily_refresh_fire',
        'criterion':
            "with sc as (select count(*) c from public.beaches_gold "
            "             where state = $STATE and is_active and scoring_tier in ('daily','hourly')), "
            "     rec as (select count(distinct r.location_id) c "
            "               from public.beach_day_recommendations r "
            "               join public.beaches_gold g on g.location_id = r.location_id "
            "              where g.state = $STATE and r.local_date = current_date) "
            "select (rec.c::float >= sc.c::float * 0.95)::boolean from sc, rec",
        'criterion_text': 'today rec exists for >= 95% of scoreable beaches',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active and scoring_tier in ('daily','hourly')), "
            "     d as (select count(distinct r.location_id)::int n "
            "             from public.beach_day_recommendations r "
            "             join public.beaches_gold g on g.location_id=r.location_id "
            "            where g.state=$STATE and r.local_date=current_date) "
            "select d.n done, t.n total from d, t",
    },
    {
        # End-of-pipeline drift/coverage check. Runs the per-state audit
        # in --check mode; non-zero exit if any of the hard thresholds
        # fail (county_fips < 100%, name_source < 100%, today_rec < 95%,
        # state_dogs_policy_v1 missing from BEP, tier-1+2 = 0). Prints
        # the full report regardless.
        'key': 'field_population_check',
        'kind': 'python',
        'action': 'field_population_check',
        'criterion': "select true",  # validation is in the action exit code
        'criterion_text':
            'population audit thresholds met (county_fips=100%, name_source=100%, '
            'today_rec>=95%, BEP has state_dogs_policy_v1, tier-1+2 > 0)',
    },
]


# ─── Python phase actions ──────────────────────────────────────────
# Each takes a state code, returns int rows_affected (or raises).

def _state_operator_ids(state: str) -> list[int]:
    """Operators worth researching for this state's dog policy.

    Filters to operators whose footprint contains (or is within 1km of)
    at least one active gold beach in the state. Avoids the 80%+ of
    inland city/county operators that have no chance of contributing
    coastal dog policy. Falls back to ALL active operators if the
    filtered set is empty (first-launch before promote runs, OR a state
    where polygon containment hasn't resolved yet).
    """
    with open_conn() as c, c.cursor() as cur:
        # 2026-05-13: switched from state_operator_ids_with_beaches
        # (114 OR ops, includes spatial 1km proximity to ANY beach) to
        # state_operator_ids_for_scoreable_beaches (60 OR ops, strict
        # BEP attribution to daily/hourly-tier beaches only). The cost
        # gate Franz wanted.
        cur.execute(
            'select operator_id from public.state_operator_ids_for_scoreable_beaches(%s) '
            'order by operator_id',
            (state,)
        )
        ids = [r[0] for r in cur.fetchall()]
        if ids:
            cur.execute(
                "select count(*) from public.operators "
                " where state_code = %s and is_active and level in ('city','county','state')",
                (state,)
            )
            total = cur.fetchone()[0]
            log(f'    operator filter: {len(ids)}/{total} ops have beaches '
                f'(saving {100 - len(ids)*100//total if total else 0}%)')
            return ids
        # Fallback for first-launch state where containment hasn't run yet
        log(f'    operator filter empty for {state} — falling back to all ops')
        cur.execute(
            "select id from public.operators "
            " where state_code = %s and is_active and level in ('city','county','state') "
            " order by id",
            (state,)
        )
        return [r[0] for r in cur.fetchall()]


# Optional county filter for "test 3 counties" runs. CSV of county_fips
# strings (e.g. ['41041','41015','41007']). When set, every beach-scoped
# phase is restricted to beaches in these counties. Operator-scoped
# phases (operator_llm_extract / operator_merge) still run at full state
# scope — operator-level filtering is harder and the LLM cost is bounded.
COUNTIES_FILTER: list[str] | None = None
LIMIT_N: int | None = None  # if set, beach-fid-scoped phases cap to first N fids
CANONICAL_N: int | None = None  # if set, beach-fid-scoped phases use the class-diverse picker
LAUNCH_MODE: str = 'steady-state'  # 'state-launch' | 'steady-state'
# Coverage gate divisor used in the codify_cascade criterion SQL.
# steady-state = 5% allowance (divisor 20); state-launch = 20% allowance (divisor 5).
GATE_DIVISOR_BY_MODE = {'state-launch': 5, 'steady-state': 20}
GATE_PCT_BY_MODE      = {'state-launch': 80.0, 'steady-state': 95.0}
# nearest_dog_park gate: CA at MVP+ is 86.7%; remote beaches legitimately
# have no DP within the 25mi cap. 95% is unrealistic everywhere. Lowered
# to a realistic 90% for steady-state; 80% for state-launch.
# Structural fix (task #158): add a sentinel for "no DP within cap" so
# those beaches count as covered.
NEAREST_DP_PCT_BY_MODE = {'state-launch': 0.80, 'steady-state': 0.90}


def _canonical_fids(state: str, n: int) -> list[int]:
    """Pick up to N beaches representing distinct jurisdictional classes.

    For state-launch walkthrough: one representative per class so the full
    pipeline exercises every cascade path (state-park codify, federal
    codify, city codify, county codify, private/HOA fallback) before
    committing the broad wave.

    Classes (mutually exclusive, evaluated in order):
      1. state_park    — beach inside a PAD-US unit with mng_type='STAT'
      2. federal       — beach inside a PAD-US unit with mng_type='FED'
      3. city_municipal— beach inside a c1_city (TIGER incorporated place)
      4. public_other  — beach in some other public polygon (county, cpad)
      5. residual      — beach in NO government polygon (private / HOA / unincorporated)

    Allocation: ceil(N/5) per class, ordered by fid for determinism.
    If a class has fewer than ceil(N/5) beaches, the remainder isn't
    backfilled from other classes — keeps the diversity intent.
    """
    per_class = max(1, (n + 4) // 5)
    sql = """
      WITH state_fids AS (
        SELECT g.fid FROM public.beaches_gold g
         WHERE g.is_active AND g.state = %s
           AND g.scoring_tier IN ('daily','hourly')
      ),
      classes AS (
        SELECT s.fid,
          COALESCE(bool_or(m.polygon_kind = 'c1_city'),         false) AS in_city,
          COALESCE(bool_or(m.polygon_kind IN ('pad_us_unit','cpad_unit','military_base','county')), false) AS in_any_public
        FROM state_fids s
        LEFT JOIN public.beach_polygon_membership m ON m.gold_fid = s.fid
        GROUP BY s.fid
      ),
      fed_lookup AS (
        SELECT DISTINCT m.gold_fid AS fid
        FROM public.beach_polygon_membership m
        JOIN public.pad_us_units p ON p.unit_id::text = m.polygon_id::text
        WHERE m.polygon_kind = 'pad_us_unit' AND p.mng_type = 'FED'
      ),
      state_park_lookup AS (
        SELECT DISTINCT m.gold_fid AS fid
        FROM public.beach_polygon_membership m
        JOIN public.pad_us_units p ON p.unit_id::text = m.polygon_id::text
        WHERE m.polygon_kind = 'pad_us_unit' AND p.mng_type = 'STAT'
      ),
      typed AS (
        SELECT c.fid,
          CASE
            WHEN sp.fid IS NOT NULL              THEN 'state_park'
            WHEN fl.fid IS NOT NULL              THEN 'federal'
            WHEN c.in_city                       THEN 'city_municipal'
            WHEN c.in_any_public                 THEN 'public_other'
            ELSE                                       'residual'
          END AS class
        FROM classes c
        LEFT JOIN state_park_lookup sp ON sp.fid = c.fid
        LEFT JOIN fed_lookup fl ON fl.fid = c.fid
      ),
      ranked AS (
        SELECT fid, class,
          ROW_NUMBER() OVER (PARTITION BY class ORDER BY fid) AS rn
        FROM typed
      )
      SELECT fid, class
        FROM ranked
       WHERE rn <= %s
       ORDER BY
         CASE class
           WHEN 'state_park'     THEN 1
           WHEN 'federal'        THEN 2
           WHEN 'city_municipal' THEN 3
           WHEN 'public_other'   THEN 4
           WHEN 'residual'       THEN 5
         END,
         fid
       LIMIT %s
    """
    with open_conn() as c, c.cursor() as cur:
        cur.execute(sql, (state, per_class, n))
        rows = cur.fetchall()
    if rows:
        log(f'    canonical picker: {len(rows)} beaches across '
            f'{len(set(r[1] for r in rows))} class(es): '
            f'{", ".join(f"{r[1]}#{r[0]}" for r in rows)}')
    return [r[0] for r in rows]


def _state_tier12_fids(state: str) -> list[int]:
    """Tier 1_off-leash + 2_on-leash active fids in state.

    When COUNTIES_FILTER is set, additionally restricts to beaches whose
    county_fips is in that list. Used by bep_refire / section_extract /
    descriptions / photos_wikimedia phases.

    Bootstrap: if state has zero beach_dog_policy rows (fresh state or
    post-wipe), falls back to all active+scoreable beaches so bep_refire
    can populate beach_dog_policy on its first pass. Subsequent calls
    once beach_dog_policy is populated will correctly narrow to tier-1+2.
    """
    # Detect bootstrap state: no beach_dog_policy rows for this state yet.
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select exists(select 1 from public.beach_dog_policy bdp "
            "  join public.beaches_gold g on g.fid = bdp.arena_group_id "
            "  where g.state = %s)",
            (state,)
        )
        has_dog_policy = cur.fetchone()[0]

    args: tuple
    if has_dog_policy:
        sql = ("select g.fid from public.beaches_gold g "
               "join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid "
               "where g.is_active and g.state = %s "
               "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
               "      in ('1_off-leash','2_on-leash') ")
        args = (state,)
    else:
        # Bootstrap: refire on all active+scoreable beaches to populate beach_dog_policy.
        log(f'    bootstrap: no beach_dog_policy for {state}, using all active+scoreable beaches')
        sql = ("select g.fid from public.beaches_gold g "
               "where g.is_active and g.scoring_tier in ('daily','hourly') and g.state = %s ")
        args = (state,)

    if COUNTIES_FILTER:
        sql += "  and g.county_fips = any(%s) "
        args = args + (COUNTIES_FILTER,)
    sql += "order by g.fid"
    if LIMIT_N is not None and LIMIT_N > 0:
        sql += f" limit {int(LIMIT_N)}"

    with open_conn() as c, c.cursor() as cur:
        cur.execute(sql, args)
        fids = [r[0] for r in cur.fetchall()]

    # CANONICAL mode: intersect with the class-diverse picker so we run
    # representatives of each jurisdictional class instead of the first N
    # by fid. Applied AFTER the tier filter so canonical fids still respect
    # the tier-1+2 universe (or the bootstrap "all scoreable" fallback).
    if CANONICAL_N is not None and CANONICAL_N > 0:
        canonical = _canonical_fids(state, CANONICAL_N)
        in_tier = set(fids)
        # Keep canonical fids that are in the tier set; if too few, top up
        # with non-canonical first-by-fid up to CANONICAL_N. Preserves
        # diversity but doesn't strand the pipeline on a too-small set.
        picked = [f for f in canonical if f in in_tier]
        if len(picked) < CANONICAL_N:
            extras = [f for f in fids if f not in set(picked)]
            picked.extend(extras[:CANONICAL_N - len(picked)])
        return picked[:CANONICAL_N]

    return fids


def _run_subprocess(cmd: list[str], timeout: int = 14400) -> tuple[int, str, str]:
    """Stream subprocess output line-by-line to our stdout while also
    collecting it for parse_fn callers. capture_output=True buffered
    the entire run, giving zero progress visibility on long LLM/loader
    phases. Diagnosed during 2026-05-19 rescue work (Popen+tee pattern
    surfaces real-time progress through the BG-task harness pipe)."""
    deadline = time.time() + timeout
    proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            encoding='utf-8', errors='replace', bufsize=1)
    out_lines: list[str] = []
    err_lines: list[str] = []
    # Drain stdout on main thread (tee); pull stderr from a worker.
    def _drain_stderr():
        assert proc.stderr is not None
        for line in proc.stderr:
            err_lines.append(line)
    t_err = threading.Thread(target=_drain_stderr, daemon=True)
    t_err.start()
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f'      | {line.rstrip()}', flush=True)
        out_lines.append(line)
        if time.time() > deadline:
            proc.kill()
            raise subprocess.TimeoutExpired(cmd, timeout)
    rc = proc.wait(timeout=max(1.0, deadline - time.time()))
    t_err.join(timeout=2.0)
    return rc, ''.join(out_lines), ''.join(err_lines)


def _ping_or_reconnect(conn):
    """Return a live conn. Long SQL passes (30-min statement_timeout) sit
    behind pgbouncer's idle timeout; the held psycopg2 conn dies silently.
    Ping SELECT 1; if broken, close + reopen. Cheap and idempotent.
    Diagnosed in rescue 2026-05-19 (feedback_pgbouncer_kills_held_conn)."""
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1'); cur.fetchone()
        return conn
    except Exception:
        try: conn.close()
        except Exception: pass
        return open_conn()


def _ensure_loader(state: str, source: str, script_name: str,
                    max_age_days: int = 30, timeout: int = 2400) -> int:
    """Generic upstream-loader gate: skip if external_source_status for
    (source, state) is 'ok' or 'skipped' and recently loaded; otherwise
    invoke the bulk loader subprocess. Used by phases ensure_pad_us,
    ensure_overpass, ensure_amenities, ensure_tiger_places,
    ensure_dog_features.

    Pre-flight bulk loaders that USED to live in the runbook as manual
    commands. Now per-state launch is truly single-command.
    """
    from datetime import datetime, timezone, timedelta
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select status, last_loaded_at from public.external_source_status "
            " where source=%s and state=%s",
            (source, state),
        )
        r = cur.fetchone()
    if r:
        status, last = r
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        if status in ('ok', 'skipped') and last and last > cutoff:
            log(f'    {source} already loaded for {state} ({status}, '
                f'age <{max_age_days}d); skip')
            return 0
    log(f'    invoking scripts/one_off/{script_name} --states {state}')
    rc, out, err = _run_subprocess(
        [sys.executable, f'scripts/one_off/{script_name}', '--states', state],
        timeout=timeout,
    )
    if rc != 0:
        raise RuntimeError(f'{script_name} exit {rc}: {err[-500:]}')
    return 1


def action_operators_chunked(state: str) -> int:
    """Phase 10: seed operators for a state via 14 per-pass calls.

    2026-05-13 split (post-OOM): the monolithic SQL function used to OOM
    Supabase for states with heavy PAD-US (OR's max 915K-vertex polygons).
    Each pass now runs as a separate function call in its own auto-
    committed transaction, bounding memory to a single pass's working set.

    Skip-CA: passes 4-11 are no-op for CA (legacy CPAD seeding covers).
    Returns the total rows added/changed across all passes (sums everything
    that contributes to the criterion 'operators has rows for state')."""
    passes = [
        # (label, fn_name, skip_for_ca)
        ('cities',        '_op_pass1_cities',        False),
        ('counties',      '_op_pass2_counties',      False),
        ('federal',       '_op_pass3_federal',       False),
        ('state',         '_op_pass4_state',          True),
        ('district',      '_op_pass5_district',       True),
        ('ngo',           '_op_pass6a_ngo',           True),
        ('pvt',           '_op_pass6b_pvt',           True),
        ('tribal',        '_op_pass7_tribal',         True),
        ('joint',         '_op_pass8_joint',          True),
        ('bia',           '_op_pass9_bia',            True),
        ('osm',           '_op_pass10_osm',           True),
        ('bep',           '_op_pass11_bep',           True),
        ('linkage',       '_op_linkage',             False),
        ('consolidation', '_op_pass12_consolidation', False),
    ]
    is_ca = (state == 'CA')
    total = 0

    def _fresh_conn():
        # Dedicated autocommit conn; each pass commits on its own. 30 min
        # statement_timeout bounds a single pass; pgbouncer may still cut
        # the conn during multi-pass runs, so we ping+reconnect per pass.
        cc = psycopg2.connect(**PG)
        cc.set_session(autocommit=True)
        with cc.cursor() as cu:
            cu.execute("SET statement_timeout = '1800s'")
        return cc

    c = _fresh_conn()
    try:
        for label, fn_name, skip_for_ca in passes:
            if skip_for_ca and is_ca:
                log(f'    {label}: SKIP (CA)')
                continue
            t0 = time.time()
            # Ping before each pass; reconnect if pgbouncer dropped us
            # between long-running predecessor passes. Diagnosed in
            # 2026-05-19 rescue (feedback_pgbouncer_kills_held_conn).
            try:
                with c.cursor() as cur:
                    cur.execute('SELECT 1'); cur.fetchone()
            except Exception:
                log(f'    {label}: conn dropped; reconnecting')
                try: c.close()
                except Exception: pass
                c = _fresh_conn()
            with c.cursor() as cur:
                cur.execute(f"SELECT public.{fn_name}(%s)", (state,))
                n = int(cur.fetchone()[0])
            dt = time.time() - t0
            total += n
            log(f'    {label}: +{n} rows in {dt:.1f}s')
    finally:
        try: c.close()
        except Exception: pass
    return total


def action_ensure_pad_us(state: str) -> int:
    """Load PAD-US for state via the canonical loader scripts/load_pad_us_state.py.

    Policy 2026-05-12 (Franz): do NOT use scripts/one_off/bulk_load_pad_us.py
    or scripts/external_sources.py for PAD-US — both write `geom` only and
    miss `geom_geog`, which downstream Method A + Method B containment
    populators spatial-join on. load_pad_us_state.py is patched to write
    geom_geog correctly on INSERT + ON CONFLICT UPDATE.

    Honors external_source_status skip-logic (skip if status=ok and <30d).
    Writes the success row itself since load_pad_us_state.py doesn't track
    external_source_status today.
    """
    from datetime import datetime, timezone, timedelta
    source = 'pad_us'
    max_age_days = 30

    # Skip if already loaded recently
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select status, last_loaded_at from public.external_source_status "
            " where source=%s and state=%s",
            (source, state),
        )
        r = cur.fetchone()
    if r:
        status, last = r
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        if status in ('ok', 'skipped') and last and last > cutoff:
            log(f'    {source} already loaded for {state} ({status}, '
                f'age <{max_age_days}d); skip')
            return 0

    # Invoke canonical loader
    log(f'    invoking scripts/load_pad_us_state.py {state}  (canonical PAD-US loader)')
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/load_pad_us_state.py', state],
        timeout=2400,
    )
    if rc != 0:
        raise RuntimeError(f'load_pad_us_state.py exit {rc}: {err[-500:]}')

    # Write success row to external_source_status (loader doesn't do this itself)
    with open_conn() as c, c.cursor() as cur:
        cur.execute("select count(*) from public.pad_us_units where state=%s", (state,))
        row_count = cur.fetchone()[0]
        cur.execute(
            "insert into public.external_source_status "
            " (source, state, status, last_loaded_at, row_count) "
            "values (%s, %s, 'ok', now(), %s) "
            "on conflict (source, state) do update set "
            " status='ok', last_loaded_at=now(), row_count=excluded.row_count",
            (source, state, row_count),
        )
    return 1

def action_ensure_overpass(state: str) -> int:
    return _ensure_loader(state, 'osm_landing', 'bulk_load_overpass.py', timeout=1200)

def action_ensure_amenities(state: str) -> int:
    return _ensure_loader(state, 'osm_amenities', 'bulk_load_amenities.py', timeout=1200)

def action_ensure_tiger_places(state: str) -> int:
    return _ensure_loader(state, 'tiger_places', 'bulk_load_tiger_places.py', timeout=600)

def action_ensure_dog_features(state: str) -> int:
    return _ensure_loader(state, 'osm_dog_features', 'bulk_load_dog_features.py', timeout=600)


def _chunked_subprocess(script_path: str, items: list, *,
                        flag_name: str = '--fids',
                        chunk_size: int = 30,
                        per_chunk_timeout: int = 600,
                        extra_args: list | None = None,
                        parse_fn=None,
                        retry: int = 1) -> int:
    """Run a Python script in chunks; one subprocess per chunk.

    Each chunk's subprocess timeout is bounded (default 10 min) instead
    of hours. Failures retry once with backoff; if still fail, log and
    continue (don't halt the whole run). Underlying script must be
    idempotent (e.g. --skip-recent N) so failed chunks self-recover on
    the next phase run.

    See feedback_chunked_subprocess.md.

    Args:
      script_path: Python script under repo root
      items: list of strings/ints to chunk
      flag_name: CLI flag the script accepts for the chunk's csv list
      chunk_size: items per subprocess invocation
      per_chunk_timeout: seconds per subprocess
      extra_args: list of additional CLI args (e.g. ['--states', 'MA'])
      parse_fn: callable(stdout) -> int rows; default returns 0
      retry: how many additional attempts on rc!=0 (default 1)

    Returns total rows summed from parse_fn across successful chunks.
    """
    extra_args = list(extra_args or [])
    if not items:
        log('    no items; skip')
        return 0
    total_chunks = (len(items) + chunk_size - 1) // chunk_size
    log(f'    chunking {len(items)} items → {total_chunks} chunks of ≤{chunk_size} '
        f'(per-chunk timeout {per_chunk_timeout}s)')
    total_rows = 0
    failed_chunks = 0
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        chunk_idx = i // chunk_size + 1
        chunk_args = extra_args + [flag_name, ','.join(map(str, chunk))]
        cmd = [sys.executable, script_path] + chunk_args
        t0 = time.time()
        attempts = retry + 1
        rc, out, err = -1, '', ''
        for attempt in range(1, attempts + 1):
            try:
                rc, out, err = _run_subprocess(cmd, timeout=per_chunk_timeout)
            except subprocess.TimeoutExpired:
                rc, out, err = -2, '', f'subprocess timeout after {per_chunk_timeout}s'
            if rc == 0:
                break
            if attempt < attempts:
                log(f'      chunk {chunk_idx} attempt {attempt}/{attempts} '
                    f'rc={rc}; retry in 30s')
                time.sleep(30)
        elapsed = time.time() - t0
        if rc != 0:
            failed_chunks += 1
            log(f'    chunk {chunk_idx}/{total_chunks} FAILED rc={rc} '
                f'({elapsed:.0f}s): {err[-200:]}')
            continue
        rows = parse_fn(out) if parse_fn else 0
        total_rows += rows
        log(f'    chunk {chunk_idx}/{total_chunks} ok ({elapsed:.0f}s; '
            f'rows={rows}; cumulative={total_rows})')
    if failed_chunks:
        log(f'    {failed_chunks}/{total_chunks} chunk(s) failed; '
            f'they will retry on next phase run via --skip-recent self-resume')
    return total_rows


def _parse_op_extract(out: str) -> int:
    m = re.search(r"\{'src_a':[^\}]+\}", out)
    if m:
        d = ast.literal_eval(m.group(0))
        return int(d.get('src_a', 0)) + int(d.get('src_b', 0))
    return 0


def _parse_section_ok(out: str) -> int:
    m = re.search(r'Done\.\s+ok=(\d+)', out)
    return int(m.group(1)) if m else 0


def _parse_descriptions_generated(out: str) -> int:
    m = re.search(r'generated:\s+(\d+)', out)
    return int(m.group(1)) if m else 0


def _parse_photos_saved(out: str) -> int:
    m = re.search(r'(\d+)\s+photos saved', out)
    return int(m.group(1)) if m else 0


def action_operator_llm_extract(state: str) -> int:
    """Invoke extract_operator_dogs_policy.py for state's operator IDs.
    Chunked into groups of 5 ops (~3min/chunk) to bound subprocess timeout
    and enable graceful recovery on transient failures.

    Phase E cost gate (2026-05-17, docs/wave3_pipeline_integration_design.md
    Q4 rec 4b): skip operators whose attributed scoreable beaches are ALL
    already covered by a tier-1 (statute / municipal_code / federal_regulation
    / state_regulation / state_statute / special_district_ordinance /
    tribal_code) or tier-2 (mou / lease_agreement / operating_agreement /
    concession_lease) beach_policy_source row. When tier-1/2 evidence exists
    the canonical rule is locked by source authority, so LLM-extracted
    operator-page nuance can't move it — running Sonnet+Tavily is wasted
    spend. Operators with any uncovered beach still run; per-operator
    granularity matches the iteration unit (one operator → N beaches)."""
    ids = _state_operator_ids(state)
    if not ids:
        log(f'    no operators for {state}; skip')
        return 0
    pre_count = len(ids)
    ids = _filter_operators_lacking_tier12_coverage(state, ids)
    skipped = pre_count - len(ids)
    if skipped:
        log(f'    Phase E cost gate: skipping {skipped}/{pre_count} operators '
            f'whose beaches are fully covered by tier-1/2 policy_source '
            f'(estimated savings ~${skipped*0.05:.2f})')
    if not ids:
        log(f'    all {pre_count} operators fully covered by tier-1/2; nothing to extract')
        return 0
    log(f'    extracting for {len(ids)} operators (smart filter; estimated cost ~${len(ids)*0.05:.0f})')
    return _chunked_subprocess(
        'scripts/extract_operator_dogs_policy.py', ids,
        flag_name='--ids', chunk_size=5, per_chunk_timeout=600,
        extra_args=['--skip-recent', '24'],
        parse_fn=_parse_op_extract,
    )


def _filter_operators_lacking_tier12_coverage(state: str, ids: list[int]) -> list[int]:
    """Phase E (2026-05-17) — return the subset of ``ids`` that still have
    at least one attributed scoreable beach NOT yet covered by a tier-1 or
    tier-2 policy_source row.

    An operator is kept when there exists any (operator_id, gold_fid) pair
    in beach_enrichment_provenance — restricted to the state's
    active+scoring_tier in ('daily','hourly') beaches — for which no
    beach_policy_source row joins to a policy_source with
    public.policy_source_authority(subtype) <= 2. If every attributed
    beach already has tier-1/2 coverage the operator is dropped from the
    LLM-extract target list.

    Implementation note: we mirror the operator→beach attribution used by
    public.state_operator_ids_for_scoreable_beaches(state) so the cost
    gate matches the candidate set produced upstream. Operators with zero
    attributed scoreable beaches (defensive: should not happen given the
    upstream helper, but possible during state-bootstrap edge cases) are
    kept — we'd rather pay for the extraction than silently drop work.
    """
    if not ids:
        return ids
    # Operators kept = those with >= 1 uncovered beach (LEFT JOIN form).
    # Plus operators that have NO attributed scoreable beach at all
    # (defensive fallback — keep them rather than silently drop).
    sql = (
        "WITH op_beach AS ( "
        "  SELECT DISTINCT bep.operator_id, bep.gold_fid "
        "    FROM public.beach_enrichment_provenance bep "
        "    JOIN public.beaches_gold g ON g.fid = bep.gold_fid "
        "   WHERE g.state = %s "
        "     AND g.is_active "
        "     AND g.scoring_tier IN ('daily','hourly') "
        "     AND bep.operator_id = ANY(%s) "
        "), "
        "covered AS ( "
        "  SELECT DISTINCT bps.beach_fid "
        "    FROM public.beach_policy_source bps "
        "    JOIN public.policy_source ps ON ps.id = bps.policy_source_id "
        "   WHERE public.policy_source_authority(ps.subtype) <= 2 "
        "), "
        "op_uncovered AS ( "
        "  SELECT ob.operator_id "
        "    FROM op_beach ob "
        "    LEFT JOIN covered c ON c.beach_fid = ob.gold_fid "
        "   GROUP BY ob.operator_id "
        "  HAVING bool_or(c.beach_fid IS NULL) "
        "), "
        "op_no_attrib AS ( "
        "  SELECT unnest(%s::bigint[]) AS operator_id "
        "  EXCEPT "
        "  SELECT operator_id FROM op_beach "
        ") "
        "SELECT operator_id FROM op_uncovered "
        "UNION "
        "SELECT operator_id FROM op_no_attrib "
        "ORDER BY operator_id"
    )
    with open_conn() as c, c.cursor() as cur:
        cur.execute(sql, (state, ids, ids))
        return [r[0] for r in cur.fetchall()]


def action_operator_merge(state: str) -> int:
    """merge_operator_dogs_policy.py is global (no state filter)."""
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/one_off/merge_operator_dogs_policy.py'],
        timeout=600,
    )
    if rc != 0:
        raise RuntimeError(f"merge exit {rc}: {err[-500:]}")
    m = re.search(r'upserted (\d+)/\d+ operator rows', out)
    return int(m.group(1)) if m else 0


def action_refresh_nearest_dog_park(state: str) -> int:
    """Refresh beaches_gold.nearest_dog_park_* materialized columns for
    state's active beaches.

    Per Franz 2026-05-21. Single-shot subprocess (script self-chunks at
    250 beaches/commit per its --chunk default). Drives the cliff/no-
    sand-access "nearby dog park" overlay feature. Idempotent: each
    UPDATE just rewrites the materialized values.

    Runs AFTER ensure_dog_features (depends on dog-park geometry being
    loaded) and AFTER promote (depends on beaches_gold having
    state-tagged rows). Cheap pure-DB work.
    """
    cmd = [sys.executable, 'scripts/refresh_nearest_dog_park.py',
           '--state', state]
    log(f'    refresh_nearest_dog_park --state {state}')
    rc, out, err = _run_subprocess(cmd, timeout=1800)
    if rc != 0:
        raise RuntimeError(f'refresh_nearest_dog_park exit {rc}: {err[-500:]}')
    # Parse "✔ done — N beaches refreshed" if present
    m = re.search(r'(\d+)\s+beaches refreshed', out or '')
    return int(m.group(1)) if m else 0


def action_ensure_pip_membership(state: str) -> int:
    """Per-state PIP populate via refresh_beach_polygon_membership.

    Iterates active beach fids and calls the RPC once per fid (bulk call
    times out at the pooler statement_timeout; per-fid is sub-second per
    the post-perf-fix benchmarks in docs/pip_prelaunch_spec.md).

    Idempotent — the RPC DELETEs scope first then re-inserts, so re-runs
    overwrite cleanly. Returns the total polygon-membership rows written
    (sum of n_rows across all kinds for all fids).
    """
    log(f'    PIP populate: scanning beaches_gold for state={state}')
    total_rows = 0
    n_fids = n_err = 0
    with open_conn() as c:
        c.autocommit = True
        cur = c.cursor()
        cur.execute(
            "select fid from public.beaches_gold "
            " where state = %s and is_active and geom is not null "
            " order by scoring_tier in ('daily','hourly') desc, fid",
            (state,),
        )
        fids = [r[0] for r in cur.fetchall()]
        log(f'    {len(fids)} fids to PIP')
        for i, fid in enumerate(fids, 1):
            try:
                cur.execute(
                    "select kind, n_rows from public.refresh_beach_polygon_membership(%s, %s)",
                    (state, fid),
                )
                for _kind, n in cur.fetchall():
                    total_rows += (n or 0)
                n_fids += 1
            except Exception as e:
                n_err += 1
                if n_err <= 5:
                    log(f'    fid={fid} ERR: {e}')
            if i % 100 == 0:
                log(f'    {i}/{len(fids)} done  membership_rows={total_rows} err={n_err}')
        log(f'    done. fids={n_fids} err={n_err} membership_rows={total_rows}')
    return total_rows


def action_codify_cascade(state: str) -> int:
    """Per-state codify pre-check + manifest emit.

    Wraps scripts/codify_cascade_phase.py. v1 is REPORT-ONLY — it enumerates
    the federal / county / city codify universe for the state, dry-runs the
    derive_policy_source script for each jurisdiction, and emits a JSON
    manifest under tmp/. Does NOT auto-dispatch agents.

    Coverage gate (separate from this action) is the criterion SQL using
    public.count_uncovered_scoreable_beaches(state).

    See docs/codify_cascade_phase_spec.md.
    """
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/codify_cascade_phase.py', '--state', state,
         '--gate-pct', str(GATE_PCT_BY_MODE[LAUNCH_MODE])],
        timeout=900,  # OR full run was ~3 min; WA federal-only sub-second; 15min cap
    )
    # rc=0 = gate passes; rc=1 = gate fails (informational here — the phase
    # criterion does the real gate check). Both are "ran successfully";
    # anything else is a script error.
    if rc not in (0, 1):
        raise RuntimeError(f'codify_cascade_phase exit {rc}: {err[-500:]}')
    # Parse manifest path from output ("Manifest: <path>") for the log
    manifest = None
    for line in (out or '').splitlines():
        line = line.strip()
        if line.startswith('Manifest:'):
            manifest = line.split(':', 1)[1].strip()
            break
    log(f'    codify_cascade manifest: {manifest or "<not found>"}')
    log(f'    codify_cascade gate (script): {"PASS" if rc == 0 else "FAIL (see manifest)"}')
    return 1


def action_bep_refire(state: str) -> int:
    """Refire BEP cascade for state's tier-1+2 fids."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    with open_conn() as c, c.cursor() as cur:
        cur.execute('select * from public.refire_bep_cascade(%s)', (fids,))
        r = cur.fetchone()
    return int(r[0]) if r else 0


def action_section_extract(state: str) -> int:
    """Per-beach section rules. Chunked into groups of 40 fids (~2-3min
    each, batched 8 beaches/Haiku call inside the script)."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/extract_beach_section_rules.py', fids,
        flag_name='--fids', chunk_size=40, per_chunk_timeout=600,
        parse_fn=_parse_section_ok,
    )


def action_zone_rules_v2_refresh(state: str) -> int:
    """v2 area-first re-extract of ALL policy_sources per MVP+ beach.

    Per Franz 2026-05-21. Supersedes the operator-summary path
    (action_section_extract) which derived section rules from
    operator_dogs_policy.summary. v2 reads beach_policy_source.full_text
    directly with an area-first prompt + canonical_area_description
    hint, and rewrites bps/temporals per beach. Multi-ps beaches get
    each source re-run (helper iterates).

    Downstream: bps changes fire the existing _zr_inject_from_policy_sources
    cascade (no extra wiring needed). hourly_status_refresh phase runs
    AFTER this to materialize the new minute-by-minute rule layer.

    Skips beaches whose beach_dog_policy.source = 'manual_curator'
    (curator overrides are preserved). Chunked at 30 fids/subprocess
    (each fid runs ~1 LLM call per ps × ~1-3 ps, ~$0.03/beach).
    """
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/reextract_beach_all_ps.py', fids,
        flag_name='--fids', chunk_size=30, per_chunk_timeout=900,
        extra_args=['--skip-if-fresh-within', '7'],
        parse_fn=None,
    )


def action_hourly_status_refresh(state: str) -> int:
    """Materialize beach_section_hour_status for MVP+ beaches.

    Walks zone_rules per-minute (most-restrictive-wins) with seasonal
    filter + operating-hours clip, sampling at :30 of each hour. Mirrors
    beach.html _regionTimeBucketTiles logic in Python. The hourly_status
    table powers scoring + the rendered chip/tile state without re-running
    that logic at request time.

    Runs AFTER action_zone_rules_v2_refresh so the materialization
    reflects the freshly-extracted zone_rules. Idempotent: deletes +
    re-inserts (beach_fid, valid_date) slice per fid.

    Chunked at 100 fids/subprocess (~50ms/beach Python-side, no LLM cost).
    """
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/refresh_beach_section_hour_status.py', fids,
        flag_name='--fids', chunk_size=100, per_chunk_timeout=600,
        parse_fn=_parse_section_ok,
    )


def action_descriptions(state: str) -> int:
    """Generate descriptions for state's tier-1+2 fids. Chunked into
    groups of 30 fids (~5min each).

    --no-photos: omits the experimental photo_descriptions input from
    the description prompt (per Franz 2026-05-22 — photo-text pipeline
    works but needs more catalog-wide audit before defaulting on).
    Flip back when ready.

    The prompt-aware input_hash in generate_beach_descriptions.py
    means any prompt or MODEL change auto-invalidates the cache; this
    phase will regen all stored descriptions where the hash differs."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/generate_beach_descriptions.py', fids,
        flag_name='--fids', chunk_size=30, per_chunk_timeout=900,
        extra_args=['--no-photos'],
        parse_fn=_parse_descriptions_generated,
    )


def action_photos_wikimedia(state: str) -> int:
    """Wikimedia Commons photos for state's tier-1+2 fids. Chunked into
    groups of 100 fids (~3min each — Commons API is fast, throttle is
    politeness rather than rate limiting).

    Replaced Mapillary (2026-05-09) — Commons photos are CC-licensed,
    higher quality (real photos vs street-view), keyword-biased, and
    photographer-auto-blocklisted. The Mapillary loader still exists
    at scripts/load_mapillary_photos.py for ad-hoc use."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/load_wikimedia_commons_photos.py', fids,
        flag_name='--fids', chunk_size=100, per_chunk_timeout=600,
        parse_fn=_parse_photos_saved,
    )


def action_photos_flickr(state: str) -> int:
    """Flickr photos for state's tier-1+2 fids. Chunked at 50 fids
    (Flickr API has stricter pacing than Commons).

    Runs alongside photos_wikimedia — both produce candidate photos
    that downstream photos_tag will vision-tag and photos_curate will
    rank + select from. ON CONFLICT DO NOTHING in the loader makes
    re-runs safe (HARD pin feedback_photo_loaders_not_idempotent).
    """
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/load_flickr_photos.py', fids,
        flag_name='--fids', chunk_size=50, per_chunk_timeout=900,
        parse_fn=_parse_photos_saved,
    )


def action_state_photo_galleries(state: str) -> int:
    """State-conditional agency photo loaders.

    These loaders are agency-driven (not beach-fid driven) so they don't
    chunk via _chunked_subprocess. Each is a single-shot subprocess
    with a long timeout. Runs:
      - NPS (always, --state filter) — national parks coastal beaches
      - CDPR (CA only) — California State Parks gallery
      - CCC (CA only) — California Coastal Commission
      - WSPRC (WA only) — Washington State Parks
      - OPRD (OR only) — Oregon Parks and Recreation Department
    """
    runs: list[tuple[str, list[str], int]] = [
        ('NPS', ['scripts/load_nps_photos.py', '--state', state, '--full'], 1800),
    ]
    if state == 'CA':
        runs.append(('CDPR', ['scripts/load_cdpr_park_gallery.py', '--full'], 1800))
        runs.append(('CCC',  ['scripts/load_ccc_photos.py', '--full'], 1800))
    elif state == 'WA':
        runs.append(('WSPRC', ['scripts/load_wsprc_photos.py', '--full'], 1800))
    elif state == 'OR':
        runs.append(('OPRD', ['scripts/load_oprd_photos.py', '--full'], 1800))

    total = 0
    for name, cmd, timeout in runs:
        log(f'    state_photo_galleries → {name} ...')
        full_cmd = [sys.executable] + cmd
        t0 = time.time()
        try:
            rc, out, err = _run_subprocess(full_cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            log(f'      {name}: TIMEOUT after {timeout}s — skipping')
            continue
        elapsed = time.time() - t0
        if rc != 0:
            log(f'      {name}: FAIL rc={rc} ({elapsed:.0f}s): {err[-200:] if err else ""}')
            continue
        rows = _parse_photos_saved(out) if out else 0
        total += rows
        log(f'      {name}: ok ({elapsed:.0f}s; saved={rows})')
    return total


def action_photo_centroid_backfill(state: str) -> int:
    """Populate beach_photos.distance_m for agency-gallery fanout photos.

    Per Franz 2026-05-22. Agency loaders (WSPRC/CDPR/NPS/OPRD) set
    distance_m=NULL because gallery photos lack per-photo coords; the
    fanout-via-PIP gives the same photo to every beach in the park
    polygon. This phase computes each park's centroid (via the
    polygon-via-membership lookup — the polygon that contains ALL
    fanout beach_fids = the park) and sets distance_m per beach.

    Bonus: region-name match. If photo title (filename + vision-desc
    heading) contains any name from beach's zone_rules.regions[*], the
    name is stamped into source_meta.matched_region.

    Catalog-wide (state-agnostic): runs once per pipeline invocation,
    not per-state. Idempotent: re-running just rewrites distance_m to
    the same value when nothing has changed.

    Runs AFTER state_photo_galleries (photos loaded) and BEFORE
    photos_curate (so get_beach_photos_diverse can use the new
    distance_m signal). Single-shot subprocess, ~30-60 min for full
    backfill across ~4700 NULL-distance agency photos."""
    cmd = [sys.executable, 'scripts/backfill_agency_photo_centroid.py',
           '--apply']
    log(f'    photo_centroid_backfill (catalog-wide, --apply)')
    try:
        rc, out, err = _run_subprocess(cmd, timeout=5400)  # 90 min cap
    except subprocess.TimeoutExpired:
        log(f'    backfill TIMEOUT after 5400s')
        return 0
    if rc != 0:
        log(f'    backfill exit {rc}: {err[-300:] if err else ""}')
        return 0
    # Parse "distance_m updates: N" line
    m = re.search(r'distance_m updates:\s+(\d+)', out or '')
    return int(m.group(1)) if m else 0


def action_photos_tag(state: str) -> int:
    """Vision-tag MVP+ beach photos via Claude Haiku.

    Per Franz 2026-05-21. Runs AFTER photos_wikimedia so newly-loaded
    photos get tagged; runs BEFORE descriptions so the description
    generator can leverage scene/subject/landscape tags. Idempotent:
    tagger skips any photo where source_meta.vision.model == current
    model id.

    Chunked at 30 fids/subprocess (~5-10 photos/fid × Haiku vision call
    ≈ $0.001/photo). Per-chunk timeout 30min."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/load_photo_vision_tags.py', fids,
        flag_name='--fids', chunk_size=30, per_chunk_timeout=1800,
        parse_fn=None,
    )


def action_photos_curate(state: str) -> int:
    """Auto-curate (pick N best+diverse photos per beach).

    Per Franz 2026-05-21 photo curation v3. Runs AFTER photos_tag so
    the diverse-photo selector can use vision tags as ranking inputs;
    runs BEFORE descriptions so the description generator sees the
    curated gallery. Per HARD pin (feedback_auto_curate_default_broad)
    auto-curate is safe broad: skipped_human guard prevents stomping
    manual selections.

    Chunked at 100 fids/subprocess (DB-only, no LLM cost)."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    return _chunked_subprocess(
        'scripts/auto_curate.py', fids,
        flag_name='--fids', chunk_size=100, per_chunk_timeout=600,
        extra_args=['--skip-if-fresh-within', '7'],
        parse_fn=None,
    )


def action_descriptions_audit(state: str) -> int:
    """Soft regression check on freshly-generated descriptions.

    Per Franz 2026-05-22. Runs after action_descriptions and checks the
    leak patterns (amenities/facilities filler, lifeguards-on-duty,
    crowd-timing fluff, safety hedging, architecture-explicit leaks,
    multi-region surfacing rate). Soft: prints a report; doesn't fail
    the phase (no --strict).

    Scope: descriptions updated in the last 2 hours for the state.
    Returns the number of FAILED thresholds (informational only)."""
    cmd = [sys.executable, 'scripts/audit_beach_descriptions.py',
           '--hours', '2', '--states', state]
    log(f'    descriptions_audit --hours 2 --states {state}')
    rc, out, err = _run_subprocess(cmd, timeout=300)
    # Print the audit output to the pipeline log (it's already terse)
    for line in (out or '').splitlines():
        log(f'      {line}')
    # Parse "FAIL" occurrences from output as the "rows_affected" count
    import re as _re
    n_failures = len(_re.findall(r'\bFAIL\b', out or ''))
    return n_failures


def action_daily_refresh_fire(state: str) -> int:
    """Fire daily-beach-refresh with state's scoreable location_ids in batches.
    Honors COUNTIES_FILTER if set."""
    sql = ("select location_id from public.beaches_gold "
           "where state = %s and is_active and scoring_tier in ('daily','hourly')")
    args: tuple = (state,)
    if COUNTIES_FILTER:
        sql += " and county_fips = any(%s)"
        args = (state, COUNTIES_FILTER)
    with open_conn() as c, c.cursor() as cur:
        cur.execute(sql, args)
        ids = [r[0] for r in cur.fetchall()]
    if not ids:
        return 0
    url = os.environ['SUPABASE_URL'].rstrip('/') + '/functions/v1/daily-beach-refresh'
    headers = {
        'Authorization': f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        'apikey':         os.environ['SUPABASE_SERVICE_KEY'],
        'x-admin-secret': os.environ['ADMIN_SECRET'],
        'Content-Type':   'application/json',
    }
    BATCH = 25
    ok = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        try:
            r = httpx.post(url, headers=headers,
                           json={'location_ids': batch, 'tide_window_days': 7,
                                 'skip_recent_hours': 24},
                           timeout=300.0)
            if r.is_success:
                ok += len(batch)
            else:
                log(f'    batch {i//BATCH+1} HTTP {r.status_code}: {r.text[:200]}')
        except Exception as e:
            log(f'    batch {i//BATCH+1} EXC: {e}')
        time.sleep(2)
    return ok


_STATE_SEED_TEMPLATE = (
    "  INSERT INTO public.state_dogs_policy\n"
    "    (state_code, state_name, county_fips_filter,\n"
    "     dogs_allowed, default_rule, has_on_leash, has_off_leash,\n"
    "     source_quote, source_url, ordinance_ref, scope_notes, confidence)\n"
    "  VALUES\n"
    "    ('{state}', '<full state name>', NULL,\n"
    "     'yes'|'no'|'mixed', 'yes'|'no'|'mixed', true|false, true|false,\n"
    "     '<state public-trust statute or beach-access law verbatim>',\n"
    "     '<source URL — state parks dog-policy page or statute>',\n"
    "     '<statute citation — e.g. RI Gen. Laws §4-13-15>',\n"
    "     '<scope: where this default applies — coastal-only? statewide?>',\n"
    "     0.40);\n"
)


def action_state_policy_seed(state: str) -> int:
    """Ensure state_dogs_policy has a row for state.

    If unseeded, fail loudly with a template INSERT the operator can
    fill out. Once filled and applied, re-run the canon.
    """
    with open_conn() as c, c.cursor() as cur:
        cur.execute('select public.unseeded_state_policy_for_state(%s)', (state,))
        unseeded = cur.fetchone()[0]
    if unseeded == 0:
        log(f'    state_dogs_policy already seeded for {state}; skip')
        return 0
    raise RuntimeError(
        f'state_dogs_policy is missing a row for {state}. The canon halts here\n'
        f'because every active beach in {state} would otherwise resolve to\n'
        f'dogs_allowed=unknown and produce 0 beach_dog_policy rows (see\n'
        f'docs/canon-issues-log.md issue #19).\n\n'
        f'Remediation: INSERT a row using the template below, then re-run\n'
        f'this phase via:\n'
        f'  python scripts/run_state_pipeline.py --state {state} --resume\n\n'
        f'Template:\n\n{_STATE_SEED_TEMPLATE.format(state=state)}'
    )


def action_seasonal_closure_seed(state: str) -> int:
    """Pending seasonal-closure seed rows for state.

    If any rows are status='pending', either the seed table has new
    species/sites that haven't been matched to gold beaches yet, OR
    the seed table is unpopulated for this state and needs species-
    level curation. Fails loudly if pending > 0.
    """
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select count(*) from public.seasonal_closure_seed "
            " where state_code=%s and status='pending'",
            (state,)
        )
        pending = cur.fetchone()[0]
    if pending == 0:
        log(f'    no pending seasonal_closure_seed rows for {state}; skip')
        return 0
    raise RuntimeError(
        f'{pending} pending seasonal_closure_seed rows for {state}.\n'
        f'These need to be matched to a beaches_gold fid OR marked status=\'no_match\'.\n'
        f'Inspect with:\n'
        f'  SELECT * FROM public.seasonal_closure_seed\n'
        f'   WHERE state_code = \'{state}\' AND status = \'pending\';\n'
        f'Then UPDATE matched_fid + status=\'applied\' (or status=\'no_match\').'
    )


def action_field_population_check(state: str) -> int:
    """End-of-pipeline drift check: run the per-state audit in --check
    mode. Prints the full report; raises if any hard threshold fails."""
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/audit/state_population_audit.py',
         '--state', state, '--check'],
        timeout=300,
    )
    # Always print the audit output to the orchestrator log
    print(out)
    if err.strip():
        print(err, file=sys.stderr)
    if rc != 0:
        raise RuntimeError(
            f'field_population_check FAIL for {state} (exit {rc}). See report '
            f'output above for which criteria violated.'
        )
    return 0


PYTHON_ACTIONS = {
    'state_policy_seed':       action_state_policy_seed,
    'seasonal_closure_seed':   action_seasonal_closure_seed,
    'ensure_tiger_places':     action_ensure_tiger_places,
    'ensure_pad_us':           action_ensure_pad_us,
    'ensure_overpass':         action_ensure_overpass,
    'ensure_amenities':        action_ensure_amenities,
    'ensure_dog_features':     action_ensure_dog_features,
    'refresh_nearest_dog_park': action_refresh_nearest_dog_park,
    'ensure_pip_membership':   action_ensure_pip_membership,
    'codify_cascade':          action_codify_cascade,
    'operator_llm_extract':    action_operator_llm_extract,
    'operator_merge':          action_operator_merge,
    'operators_chunked':       action_operators_chunked,
    'bep_refire':              action_bep_refire,
    'section_extract':         action_section_extract,
    'zone_rules_v2_refresh':   action_zone_rules_v2_refresh,
    'hourly_status_refresh':   action_hourly_status_refresh,
    'photos_wikimedia':        action_photos_wikimedia,
    'photos_flickr':           action_photos_flickr,
    'state_photo_galleries':   action_state_photo_galleries,
    'photo_centroid_backfill': action_photo_centroid_backfill,
    'photos_tag':              action_photos_tag,
    'photos_curate':           action_photos_curate,
    'descriptions':            action_descriptions,
    'descriptions_audit':      action_descriptions_audit,
    'daily_refresh_fire':      action_daily_refresh_fire,
    'field_population_check':  action_field_population_check,
}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def open_conn():
    c = psycopg2.connect(**PG)
    c.autocommit = True  # critical: each phase commits independently for status persistence
    # Some phases run global clustering / dedup over the whole arena table
    # (populate_arena_group_id, run_late_stage_dedup) and exceed the default
    # 60s statement_timeout once arena grows past a few thousand rows.
    # Bump to 10 minutes for the orchestrator's connections.
    with c.cursor() as cur:
        cur.execute("set statement_timeout = '600s'")
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--state', required=True)
    ap.add_argument('--run-id', type=int, help='Resume an existing run_id (else allocates new)')
    ap.add_argument('--resume', action='store_true', help='Skip phases already ok for this run_id')
    ap.add_argument('--force', action='store_true', help='Ignore prior status, re-run all phases')
    ap.add_argument('--skip-precheck', action='store_true', help='Skip precheck phase only')
    ap.add_argument('--dry-run', action='store_true', help='Print phase plan; do not execute')
    ap.add_argument('--phase-from', help='Start at a specific phase (skip prior)')
    ap.add_argument('--counties', help='CSV of county_fips to scope beach phases (test mode). '
                                       'State-wide criteria are downgraded to warnings.')
    ap.add_argument('--limit', type=int, help='Cap beach-fid-scoped phases to first N fids '
                                              '(pilot mode; state-wide loaders still run full)')
    ap.add_argument('--canonical', type=int, metavar='N',
                    help='Walkthrough mode: pick N beaches representing distinct '
                         'jurisdictional classes (state_park / federal / city / public_other / '
                         'residual) for full-pipeline validation before broad sweep. '
                         'Default N=5. Surfaces cascade gaps and migration-template issues '
                         'on a small canonical set before committing the broad wave.')
    ap.add_argument('--launch-mode', choices=['state-launch','steady-state'],
                    default='steady-state',
                    help='Codify coverage gate. state-launch=80%% (allows long-tail '
                         'jurisdictions to land across multiple passes; right for new '
                         'state launches like MD/NJ/FL); steady-state=95%% (right for '
                         'MVP+-ready states like CA/OR/WA). Default steady-state.')
    args = ap.parse_args()

    state = args.state.upper()

    # Apply county filter mode if requested
    global COUNTIES_FILTER, LIMIT_N, CANONICAL_N, LAUNCH_MODE
    LAUNCH_MODE = args.launch_mode
    if LAUNCH_MODE != 'steady-state':
        log(f'LAUNCH MODE: {LAUNCH_MODE} '
            f'(codify gate = {GATE_PCT_BY_MODE[LAUNCH_MODE]}%)')
    if args.counties:
        COUNTIES_FILTER = [c.strip() for c in args.counties.split(',') if c.strip()]
        log(f'COUNTY-FILTERED MODE: restricting beach phases to {len(COUNTIES_FILTER)} counties: '
            f'{",".join(COUNTIES_FILTER)}')
        log('  state-wide criteria will be downgraded to warnings (no halt on failure)')
    if args.limit and args.limit > 0 and args.canonical and args.canonical > 0:
        raise SystemExit('--limit and --canonical are mutually exclusive')
    if args.limit and args.limit > 0:
        LIMIT_N = args.limit
        log(f'LIMIT MODE: beach-fid-scoped phases capped to first {LIMIT_N} fids')
    if args.canonical and args.canonical > 0:
        CANONICAL_N = args.canonical
        log(f'CANONICAL MODE: beach-fid-scoped phases scoped to {CANONICAL_N} '
            f'class-diverse representatives (state_park / federal / city / public_other / residual)')
        log('  surfaces cascade gaps + migration-template issues before broad wave;')
        log('  remove --canonical and re-run for the full state.')

    if args.dry_run:
        print(f'Plan for state={state}, {len(PHASES)} phases:')
        for p in PHASES: print(f'  - {p["key"]:<22} criterion: {p["criterion_text"]}')
        return

    # Allocate run_id
    with open_conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if args.run_id:
            run_id = args.run_id
        else:
            cur.execute('select public.next_pipeline_run_id() id')
            run_id = cur.fetchone()['id']
    log(f'state={state} run_id={run_id}  phases={len(PHASES)}')

    skip_until_phase = args.phase_from
    started = (skip_until_phase is None)

    for ph in PHASES:
        if not started:
            if ph['key'] == skip_until_phase:
                started = True
            else:
                log(f'  SKIP {ph["key"]} (--phase-from)')
                continue

        if args.skip_precheck and ph['key'] == 'precheck':
            log(f'  SKIP {ph["key"]} (--skip-precheck)')
            continue

        # Resumability
        if args.resume and not args.force:
            with open_conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute('''select status from public.pipeline_phase_status
                                where run_id=%s and state_code=%s and phase=%s''',
                            (run_id, state, ph['key']))
                r = cur.fetchone()
                if r and r['status'] == 'ok':
                    log(f'  SKIP {ph["key"]} (already ok for run_id={run_id})')
                    continue

        criterion = (ph['criterion']
                     .replace('$STATE', f"'{state}'")
                     .replace('$UNCOVER_DIVISOR', str(GATE_DIVISOR_BY_MODE[LAUNCH_MODE]))
                     .replace('$NEAREST_DP_PCT', str(NEAREST_DP_PCT_BY_MODE[LAUNCH_MODE])))
        kind = ph.get('kind', 'sql')
        progress_sql = ph.get('progress_sql')
        if progress_sql:
            progress_sql = (progress_sql
                            .replace('$STATE', f"'{state}'")
                            .replace('$UNCOVER_DIVISOR', str(GATE_DIVISOR_BY_MODE[LAUNCH_MODE]))
                            .replace('$NEAREST_DP_PCT', str(NEAREST_DP_PCT_BY_MODE[LAUNCH_MODE])))
        phase_num = PHASES.index(ph) + 1
        log(f'  RUN  [{phase_num}/{len(PHASES)}] {ph["key"]:<22} ...')
        t0 = time.time()

        # Heartbeat thread: every HEARTBEAT_INTERVAL_S seconds, print phase
        # number/name + elapsed + (if progress_sql is set) done/total counts.
        stop_heartbeat = threading.Event()
        def _heartbeat():
            while not stop_heartbeat.wait(HEARTBEAT_INTERVAL_S):
                elapsed = int(time.time() - t0)
                msg = f'  ··   [{phase_num}/{len(PHASES)}] {ph["key"]:<22} elapsed={elapsed}s'
                if progress_sql:
                    try:
                        with open_conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                            cur.execute(progress_sql)
                            r = cur.fetchone() or {}
                            done = r.get('done', 0) or 0
                            total = r.get('total', 0) or 0
                            pct = (done / total * 100) if total else 0
                            msg += f'  rows={done}/{total} ({pct:.0f}%)'
                    except Exception as e:
                        msg += f'  (progress query err: {str(e)[:50]})'
                print(msg, flush=True)
        hb = threading.Thread(target=_heartbeat, daemon=True)
        hb.start()

        try:
            if kind == 'sql':
                action = ph['action'].replace('$STATE', f"'{state}'")
                with open_conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        'select * from public.run_pipeline_phase(%s, %s, %s, %s, %s, %s)',
                        (run_id, state, ph['key'], action, criterion, ph['criterion_text'])
                    )
                    r = cur.fetchone()
                rows = r['rows_affected']
            else:  # python phase
                # Mark in_progress in tracker
                with open_conn() as c, c.cursor() as cur:
                    cur.execute("""
                      insert into public.pipeline_phase_status
                        (run_id, state_code, phase, status, started_at)
                      values (%s, %s, %s, 'in_progress', now())
                      on conflict (run_id, state_code, phase) do update set
                        status='in_progress', started_at=now(), finished_at=null,
                        rows_affected=null, criterion_met=null, error_message=null
                    """, (run_id, state, ph['key']))
                # Run python action
                fn = PYTHON_ACTIONS[ph['action']]
                rows = fn(state)
                # Validate criterion
                with open_conn() as c, c.cursor() as cur:
                    cur.execute(criterion)
                    passed = cur.fetchone()[0]
                if passed:
                    with open_conn() as c, c.cursor() as cur:
                        cur.execute("""
                          update public.pipeline_phase_status
                             set status='ok', finished_at=now(),
                                 rows_affected=%s, criterion_met=true, criterion_text=%s
                           where run_id=%s and state_code=%s and phase=%s
                        """, (rows, ph['criterion_text'], run_id, state, ph['key']))
                else:
                    err = f"criterion failed: {ph['criterion_text']}"
                    # County-filtered mode: criteria are state-wide thresholds that
                    # won't be met when we only ran 3 counties. Record as 'skipped'
                    # (criterion_met=false) and continue rather than halting.
                    if COUNTIES_FILTER is not None:
                        with open_conn() as c, c.cursor() as cur:
                            cur.execute("""
                              update public.pipeline_phase_status
                                 set status='skipped', finished_at=now(),
                                     rows_affected=%s, criterion_met=false,
                                     criterion_text=%s,
                                     error_message=%s
                               where run_id=%s and state_code=%s and phase=%s
                            """, (rows, ph['criterion_text'],
                                  f'county-mode soft-warn: {err}',
                                  run_id, state, ph['key']))
                        log(f'    WARN [{phase_num}/{len(PHASES)}] {ph["key"]:<22} '
                            f'state-wide criterion failed but county-mode continues')
                    else:
                        with open_conn() as c, c.cursor() as cur:
                            cur.execute("""
                              update public.pipeline_phase_status
                                 set status='failed', finished_at=now(),
                                     rows_affected=%s, criterion_met=false,
                                     criterion_text=%s, error_message=%s
                               where run_id=%s and state_code=%s and phase=%s
                            """, (rows, ph['criterion_text'], err, run_id, state, ph['key']))
                        raise RuntimeError(err)

            stop_heartbeat.set()
            elapsed = time.time() - t0
            log(f'    OK   [{phase_num}/{len(PHASES)}] {ph["key"]:<22} rows={rows:<6} ({elapsed:.0f}s)')

        except psycopg2.errors.RaiseException as e:
            stop_heartbeat.set()
            elapsed = time.time() - t0
            log(f'    FAIL [{phase_num}/{len(PHASES)}] {ph["key"]:<22} ({elapsed:.0f}s)')
            log(f'    {str(e).splitlines()[0]}')
            log(f'\nHALTED at phase={ph["key"]} run_id={run_id}. Inspect:')
            log(f"  select * from public.pipeline_phase_status where run_id={run_id} order by phase;")
            sys.exit(1)
        except RuntimeError as e:
            stop_heartbeat.set()
            elapsed = time.time() - t0
            log(f'    FAIL [{phase_num}/{len(PHASES)}] {ph["key"]:<22} ({elapsed:.0f}s) — {e}')
            log(f'\nHALTED at phase={ph["key"]} run_id={run_id}.')
            sys.exit(1)
        except Exception as e:
            stop_heartbeat.set()
            elapsed = time.time() - t0
            log(f'    ERR  [{phase_num}/{len(PHASES)}] {ph["key"]:<22} ({elapsed:.0f}s) — unexpected: {e}')
            # Record error
            try:
                with open_conn() as c, c.cursor() as cur:
                    cur.execute("""
                      update public.pipeline_phase_status
                         set status='failed', finished_at=now(), error_message=%s
                       where run_id=%s and state_code=%s and phase=%s
                    """, (str(e)[:1000], run_id, state, ph['key']))
            except Exception:
                pass
            sys.exit(2)

    log(f'\nAll {len(PHASES)} phases ok for state={state} run_id={run_id}.')
    log('Next: run LLM/external scripts (extract_operator_dogs_policy.py, '
        'extract_beach_section_rules.py, generate_beach_descriptions.py, '
        'load_mapillary_photos.py) and then trigger daily-beach-refresh for the state.')


if __name__ == '__main__':
    main()
