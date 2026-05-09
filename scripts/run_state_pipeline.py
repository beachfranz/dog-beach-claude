"""run_state_pipeline.py — canonical state-launch orchestrator (2026-05-09 canon).

Each phase: action SQL + success criterion. Status recorded in
public.pipeline_phase_status. First failure halts. Resumable by run_id.

Phases (in order):
   precheck            — assert_state_upstream_loaded
   operators           — populate_operators_for_state
   cluster             — populate_arena_group_id + populate_arena_extras
   promote             — promote_to_gold for the state's fids
   address_poi         — _enrich_address_from_poi_for_state
   address_city        — _enrich_address_city_for_state
   name_source         — _enrich_name_source_for_state
   strip_plus_codes    — strip_plus_codes_from_addresses
   align_scoreable     — align_is_scoreable_to_tier
   dedup               — run_late_stage_dedup
   geom_queue          — process_geom_change_queue
   purge_pollution     — purge_cross_state_extractions (post-LLM, idempotent)
   <LLM/external phases — run separately or in their own scripts>

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
    {
        'key': 'precheck',
        'action': "select count(*)::int from public.assert_state_upstream_loaded($STATE)",
        'criterion':
            "select (count(*) >= 4)::boolean from public.external_source_status "
            "where state = $STATE and source in ('pad_us','osm_landing','osm_amenities','tiger_places') "
            "  and status in ('ok','skipped')",
        'criterion_text': 'all 4 required sources status in (ok, skipped)',
    },
    {
        'key': 'operators',
        'action':
            "select cities_added + counties_added from public.populate_operators_for_state($STATE)",
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
            "select coalesce((select relation_grouped from public.populate_arena_group_id()), 0)::int",
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
            "select (count(*) filter (where county_fips is null) = 0)::boolean "
            "from public.beaches_gold where state = $STATE and is_active",
        'criterion_text': 'every active beach in state has county_fips set',
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
    {
        'key': 'align_scoreable',
        'action':
            "select promoted + demoted from public.align_is_scoreable_to_tier($STATE)",
        'criterion':
            "select (count(*) filter (where g.is_scoreable and "
            "    public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "    not in ('1_off-leash','2_on-leash')) = 0)::boolean "
            "from public.beaches_gold g join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "where g.state = $STATE and g.is_active",
        'criterion_text': 'no Tier 3/4 beach is scoreable',
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
        'action': "select coalesce((select kills from public.run_late_stage_dedup()), 0)::int",
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
        'criterion':
            "select (count(*) > 0)::boolean from public.operator_policy_extractions ope "
            "join public.operators op on op.id = ope.operator_id "
            "where op.state_code = $STATE and ope.extracted_at > now() - interval '7 days'",
        'criterion_text': 'fresh extractions exist for state',
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
        'key': 'photos_mapillary',
        'kind': 'python',
        'action': 'photos_mapillary',
        'criterion': "select true",  # rate-limited; 0 acceptable
        'criterion_text': 'mapillary loader ran (Mapillary rate limits may cap coverage)',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold g "
            "             join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid "
            "             where g.state=$STATE and g.is_active "
            "               and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "                   in ('1_off-leash','2_on-leash')), "
            "     d as (select count(distinct bp.arena_group_id)::int n "
            "             from public.beach_photos bp "
            "             join public.beaches_gold g on g.fid=bp.arena_group_id "
            "            where g.state=$STATE and g.is_active and bp.source='mapillary') "
            "select d.n done, t.n total from d, t",
    },
    {
        'key': 'daily_refresh_fire',
        'kind': 'python',
        'action': 'daily_refresh_fire',
        'criterion':
            "with sc as (select count(*) c from public.beaches_gold "
            "             where state = $STATE and is_active and is_scoreable), "
            "     rec as (select count(distinct r.location_id) c "
            "               from public.beach_day_recommendations r "
            "               join public.beaches_gold g on g.location_id = r.location_id "
            "              where g.state = $STATE and r.local_date = current_date) "
            "select (rec.c::float >= sc.c::float * 0.95)::boolean from sc, rec",
        'criterion_text': 'today rec exists for >= 95% of scoreable beaches',
        'progress_sql':
            "with t as (select count(*)::int n from public.beaches_gold "
            "             where state=$STATE and is_active and is_scoreable), "
            "     d as (select count(distinct r.location_id)::int n "
            "             from public.beach_day_recommendations r "
            "             join public.beaches_gold g on g.location_id=r.location_id "
            "            where g.state=$STATE and r.local_date=current_date) "
            "select d.n done, t.n total from d, t",
    },
]


# ─── Python phase actions ──────────────────────────────────────────
# Each takes a state code, returns int rows_affected (or raises).

def _state_operator_ids(state: str) -> list[int]:
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select id from public.operators "
            "where state_code = %s and is_active and level in ('city','county','state') "
            "order by id",
            (state,)
        )
        return [r[0] for r in cur.fetchall()]


def _state_tier12_fids(state: str) -> list[int]:
    """Tier 1_off-leash + 2_on-leash active fids in state."""
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select g.fid from public.beaches_gold g "
            "join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid "
            "where g.is_active and g.state = %s "
            "  and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
            "      in ('1_off-leash','2_on-leash') "
            "order by g.fid",
            (state,)
        )
        return [r[0] for r in cur.fetchall()]


def _run_subprocess(cmd: list[str], timeout: int = 14400) -> tuple[int, str, str]:
    rc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout)
    return rc.returncode, rc.stdout, rc.stderr


def action_operator_llm_extract(state: str) -> int:
    """Invoke extract_operator_dogs_policy.py for state's operator IDs."""
    ids = _state_operator_ids(state)
    if not ids:
        log(f'    no operators for {state}; skip')
        return 0
    log(f'    extracting for {len(ids)} operators (cost ~${len(ids)*0.05:.0f})')
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/extract_operator_dogs_policy.py', '--ids', ','.join(map(str, ids))]
    )
    if rc != 0:
        raise RuntimeError(f"extractor exit {rc}: {err[-500:]}")
    m = re.search(r"\{'src_a':[^\}]+\}", out)
    if m:
        d = ast.literal_eval(m.group(0))
        return int(d.get('src_a', 0)) + int(d.get('src_b', 0))
    return 0


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
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/extract_beach_section_rules.py', '--states', state],
        timeout=3600,
    )
    if rc != 0:
        raise RuntimeError(f"section_extract exit {rc}: {err[-500:]}")
    m = re.search(r'Done\.\s+ok=(\d+)', out)
    return int(m.group(1)) if m else 0


def action_descriptions(state: str) -> int:
    """Generate descriptions for state's tier-1+2 fids (passes --fids)."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/generate_beach_descriptions.py',
         '--fids', ','.join(map(str, fids))],
        timeout=7200,
    )
    if rc != 0:
        raise RuntimeError(f"descriptions exit {rc}: {err[-500:]}")
    m = re.search(r'generated:\s+(\d+)', out)
    return int(m.group(1)) if m else 0


def action_photos_mapillary(state: str) -> int:
    """Mapillary photos for state's tier-1+2 fids."""
    fids = _state_tier12_fids(state)
    if not fids:
        return 0
    rc, out, err = _run_subprocess(
        [sys.executable, 'scripts/load_mapillary_photos.py',
         '--fids', ','.join(map(str, fids))],
        timeout=7200,
    )
    if rc != 0:
        raise RuntimeError(f"photos exit {rc}: {err[-500:]}")
    m = re.search(r'photos saved:\s+(\d+)', out)
    return int(m.group(1)) if m else 0


def action_daily_refresh_fire(state: str) -> int:
    """Fire daily-beach-refresh with state's scoreable location_ids in batches."""
    with open_conn() as c, c.cursor() as cur:
        cur.execute(
            "select location_id from public.beaches_gold "
            "where state = %s and is_active and is_scoreable",
            (state,)
        )
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
            r = httpx.post(url, headers=headers, json={'location_ids': batch, 'tide_window_days': 7},
                           timeout=300.0)
            if r.is_success:
                ok += len(batch)
            else:
                log(f'    batch {i//BATCH+1} HTTP {r.status_code}: {r.text[:200]}')
        except Exception as e:
            log(f'    batch {i//BATCH+1} EXC: {e}')
        time.sleep(2)
    return ok


PYTHON_ACTIONS = {
    'operator_llm_extract': action_operator_llm_extract,
    'operator_merge':       action_operator_merge,
    'bep_refire':           action_bep_refire,
    'section_extract':      action_section_extract,
    'descriptions':         action_descriptions,
    'photos_mapillary':     action_photos_mapillary,
    'daily_refresh_fire':   action_daily_refresh_fire,
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
    args = ap.parse_args()

    state = args.state.upper()

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

        criterion = ph['criterion'].replace('$STATE', f"'{state}'")
        kind = ph.get('kind', 'sql')
        progress_sql = ph.get('progress_sql')
        if progress_sql:
            progress_sql = progress_sql.replace('$STATE', f"'{state}'")
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
