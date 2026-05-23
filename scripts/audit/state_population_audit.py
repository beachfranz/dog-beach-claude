"""state_population_audit.py — drift-detection audit per state.

Reports field population for a target state's active beaches, scoped
to Location Tier 1+2 (the scoring scope per docs/state-launch-runbook.md).
Sections: external data status, catalog counts, tier distribution,
structural enrichment, zone_rules section depth, daily-scoring
coverage, operators, BEP sources, closure overlays, unnamed-pipeline
filter sanity, DB size.

Two modes:
  - Default: print human-readable report.
  - --check (used by the pipeline's `field_population_check` phase):
    print the report AND return exit code based on hard thresholds.
    Exit 0 = pass, exit 1 = fail (criterion-blocking).

Usage:
  python scripts/audit/state_population_audit.py                # all 27 priority-1
  python scripts/audit/state_population_audit.py --state OR     # OR only
  python scripts/audit/state_population_audit.py --state RI --check
"""
import argparse, os, sys, urllib.parse
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')


def fetch(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []


TIER = ("public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, "
        "bdp.has_on_leash, bdp.dogs_prohibited_start::text)")


def hdr(s):
    print('\n' + '=' * 70)
    print(s)
    print('=' * 70)


def states_clause(states: list[str]) -> str:
    return ','.join(f"'{s}'" for s in states)


def run(states: list[str], check_mode: bool = False,
        launch_mode: str = 'steady-state') -> int:
    """Run the audit. Returns exit code (0=pass, 1=fail). Failure modes
    only fire in --check mode and require all criteria to pass.

    launch_mode (gap #37/#38, 2026-05-23 LATE): 'state-launch' uses 85%
    daily-refresh threshold (fresh-state edge-function long-tail expected);
    'steady-state' uses 95% (catalog should be settled). Mirrors the
    pipeline's DAILY_REFRESH_PCT_BY_MODE in run_state_pipeline.py — keep
    in sync."""

    daily_refresh_threshold = 0.85 if launch_mode == 'state-launch' else 0.95

    s_clause = states_clause(states)
    failures: list[str] = []

    hdr('A. EXTERNAL DATA — external_source_status by source/status')
    for r in fetch("""select source, status, count(*) c
                      from public.external_source_status group by 1,2 order by 1,2"""):
        print(f"  {r['source']:<16} {r['status']:<10} states={r['c']}")
    # Per-state precheck-source presence
    if check_mode:
        for st in states:
            r = fetch("""select count(*) c from public.external_source_status
                          where state=%s and source in ('pad_us','osm_landing','osm_amenities','tiger_places')
                            and status in ('ok','skipped')""", (st,))
            if not r or r[0]['c'] < 4:
                failures.append(
                    f'{st}: not all 4 required external sources are loaded '
                    f'(have {r[0]["c"] if r else 0}/4)')

    hdr(f'B. CATALOG — beaches_gold ({", ".join(states)})')
    for r in fetch(f"""select state, count(*) tot,
                              count(*) filter (where is_active) act,
                              count(*) filter (where scoring_tier IN ('daily','hourly') and is_active) score_act
                         from public.beaches_gold where state in ({s_clause})
                         group by 1 order by 1"""):
        print(f"  {r['state']}: total={r['tot']:>5} active={r['act']:>5} scoreable_active={r['score_act']:>5}")
        if check_mode and r['act'] == 0:
            failures.append(f'{r["state"]}: 0 active beaches in beaches_gold')

    hdr(f'C. POLICY TIER DISTRIBUTION — {", ".join(states)} active')
    tier_count_by_state: dict[str, dict[str, int]] = {st: {} for st in states}
    for r in fetch(f"""select g.state, ({TIER}) tier, count(*) c
                        from public.beaches_gold g
                        join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                       where g.is_active and g.state in ({s_clause})
                       group by 1,2 order by 1,2"""):
        print(f"  {r['state']} {r['tier']:<22} {r['c']}")
        tier_count_by_state.setdefault(r['state'], {})[r['tier']] = r['c']
    if check_mode:
        for st in states:
            tier12 = (tier_count_by_state.get(st, {}).get('1_off-leash', 0)
                      + tier_count_by_state.get(st, {}).get('2_on-leash', 0))
            if tier12 == 0:
                failures.append(
                    f'{st}: 0 beaches in scoring scope (Tier 1+2). Likely '
                    f'missing state_dogs_policy seed or BEP drift (see issue #19/#20).')

    hdr(f'D. STRUCTURAL ENRICHMENT — {", ".join(states)} tier-1+2')
    print('  state  tot county park   c1  addr  nsrc amen  desc')
    for r in fetch(f"""select g.state, count(*) tot,
                              count(*) filter (where g.county_fips is not null) cfips,
                              count(*) filter (where g.park_name is not null) park,
                              count(*) filter (where g.c1_jurisdiction_id is not null) c1,
                              count(*) filter (where g.address_city is not null) addr,
                              count(*) filter (where g.name_source is not null) nsrc,
                              count(*) filter (where ba.arena_group_id is not null) amen,
                              count(*) filter (where bd.arena_group_id is not null) desc_filled
                         from public.beaches_gold g
                         join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                         left join public.beach_amenities ba on ba.arena_group_id=g.fid
                         left join public.beach_descriptions bd on bd.arena_group_id=g.fid
                        where g.is_active and g.state in ({s_clause})
                          and ({TIER}) in ('1_off-leash','2_on-leash')
                        group by 1 order by 1"""):
        print(f"  {r['state']:<5} {r['tot']:>4} {r['cfips']:>6} {r['park']:>4} {r['c1']:>4} {r['addr']:>4} {r['nsrc']:>5} {r['amen']:>4} {r['desc_filled']:>5}")
        if check_mode and r['tot']:
            if r['cfips'] / r['tot'] < 1.0:
                failures.append(f'{r["state"]}: county_fips coverage {r["cfips"]}/{r["tot"]} (must be 100%)')
            if r['nsrc'] / r['tot'] < 1.0:
                failures.append(f'{r["state"]}: name_source coverage {r["nsrc"]}/{r["tot"]} (must be 100%)')

    hdr(f'E. ZONE_RULES SECTION DEPTH — {", ".join(states)} active')
    for r in fetch(f"""with z as (
                        select g.state,
                               (select count(*)::int from jsonb_array_elements(coalesce(bdp.zone_rules->'regions','[]'::jsonb)) r,
                                                        jsonb_object_keys(r->'sections')) n_sections
                          from public.beaches_gold g
                          join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                         where g.is_active and g.state in ({s_clause})
                      )
                      select state, count(*) tot,
                             count(*) filter (where n_sections >= 2) ge2,
                             count(*) filter (where n_sections >= 4) ge4,
                             max(n_sections) maxs
                        from z group by 1 order by 1"""):
        print(f"  {r['state']:<5} active={r['tot']:>4} >=2={r['ge2']:>4} >=4={r['ge4']:>4} max={r['maxs']}")

    hdr(f'F. SCORING — daily-beach-refresh today ({", ".join(states)})')
    for r in fetch(f"""select g.state, count(distinct g.fid) scoreable,
                              count(distinct r.location_id) with_today
                         from public.beaches_gold g
                         left join public.beach_day_recommendations r
                           on r.location_id=g.location_id and r.local_date = current_date
                        where g.scoring_tier IN ('daily','hourly') and g.is_active and g.state in ({s_clause})
                        group by 1 order by 1"""):
        print(f"  {r['state']}: scoreable={r['scoreable']:>4} with_today_rec={r['with_today']:>4}")
        if check_mode and r['scoreable']:
            pct = r['with_today'] / r['scoreable']
            if pct < daily_refresh_threshold:
                failures.append(
                    f'{r["state"]}: today rec coverage {r["with_today"]}/{r["scoreable"]} '
                    f'({pct*100:.0f}%) - must be >= {daily_refresh_threshold*100:.0f}%. '
                    f'Run daily-beach-refresh.')

    hdr(f'G. OPERATORS — {", ".join(states)} by level + policy coverage')
    for r in fetch(f"""select state_code, level, count(*) ops,
                              count(odp.operator_id) with_pol,
                              count(*) filter (where odp.policy_found = true) found
                         from public.operators op
                         left join public.operator_dogs_policy odp on odp.operator_id=op.id
                        where state_code in ({s_clause}) and is_active
                        group by 1,2 order by 1,2"""):
        print(f"  {r['state_code']} {r['level']:<8} ops={r['ops']:<5} with_policy={r['with_pol']:<3} found={r['found']}")

    hdr(f'H. BEP EVIDENCE — sources for {", ".join(states)} tier-1+2 fids')
    bep_sources_by_state: dict[str, set[str]] = {st: set() for st in states}
    for r in fetch(f"""select g.state, bep.source, count(distinct bep.gold_fid) fids
                        from public.beach_enrichment_provenance bep
                        join public.beaches_gold g on g.fid=bep.gold_fid
                        join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                       where g.is_active and g.state in ({s_clause})
                         and ({TIER}) in ('1_off-leash','2_on-leash')
                       group by 1,2 order by g.state, fids desc limit 50"""):
        print(f"  {r['state']:<3} {r['source']:<32} {r['fids']}")
        bep_sources_by_state.setdefault(r['state'], set()).add(r['source'])
    if check_mode:
        for st in states:
            srcs = bep_sources_by_state.get(st, set())
            tier12 = (tier_count_by_state.get(st, {}).get('1_off-leash', 0)
                      + tier_count_by_state.get(st, {}).get('2_on-leash', 0))
            if tier12 > 0 and 'state_dogs_policy_v1' not in srcs:
                failures.append(
                    f'{st}: state_dogs_policy_v1 evidence missing from BEP for tier-1+2 '
                    f'fids — populator drift (see issue #20). Re-run BEP refire.')

    hdr('I. CLOSURE OVERLAYS — beach_dog_policy_zones (view)')
    for r in fetch(f"""select g.state, count(distinct z.fid) beaches_with_overlay
                        from public.beaches_gold g
                        left join public.beach_dog_policy_zones z on z.fid = g.fid
                       where g.is_active and g.state in ({s_clause})
                       group by 1 order by 1"""):
        print(f"  {r['state']}: with_overlay={r['beaches_with_overlay']}")

    hdr('J. SEASONAL CLOSURE SEED PENDING')
    for r in fetch(f"""select state_code, status, count(*) c
                        from public.seasonal_closure_seed
                       where state_code in ({s_clause})
                       group by 1,2 order by 1,2"""):
        print(f"  {r['state_code']} {r['status']:<12} {r['c']}")
    if check_mode:
        for st in states:
            r = fetch("""select count(*) c from public.seasonal_closure_seed
                          where state_code=%s and status='pending'""", (st,))
            if r and r[0]['c'] > 0:
                failures.append(
                    f'{st}: {r[0]["c"]} seasonal_closure_seed rows still pending')

    hdr('K. DB SIZE')
    for r in fetch("select pg_size_pretty(pg_database_size(current_database())) size"):
        print(f"  {r['size']}")

    if check_mode:
        hdr('CHECK SUMMARY')
        if failures:
            print(f'  FAIL - {len(failures)} criterion(s) violated:')
            for f in failures: print(f'    [X] {f}')
            return 1
        print('  PASS - all criteria met for: ' + ', '.join(states))
        return 0
    return 0


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--state', help='2-letter state code; default = OR,WA,CA,RI')
    ap.add_argument('--check', action='store_true',
                    help='Return non-zero exit code if criterion violations found')
    ap.add_argument('--launch-mode', default='steady-state',
                    choices=['state-launch', 'steady-state'],
                    help='Gates daily-refresh threshold (85%% launch / 95%% steady). '
                         'Mirror of run_state_pipeline.py')
    args = ap.parse_args()
    states = [args.state.upper()] if args.state else ['OR','WA','CA','RI']
    sys.exit(run(states, check_mode=args.check, launch_mode=args.launch_mode))
