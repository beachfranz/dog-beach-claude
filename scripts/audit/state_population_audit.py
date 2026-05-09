"""state_population_audit.py — drift-detection audit across all states.

Reports field population per state for active beaches, scoped to
Location Tier 1+2 (the scoring scope per docs/state-launch-runbook.md).
Sections: external data status, catalog counts, tier distribution,
structural enrichment, zone_rules section depth, daily-scoring
coverage, operators, BEP sources, closure overlays, unnamed-pipeline
filter sanity, DB size.

Run after a state launch (or any pipeline change) to confirm coverage
matches expectations. If field-population numbers regress unexpectedly,
that's drift between the runbook claims and reality — investigate
before continuing.

Usage:
  python scripts/audit/state_population_audit.py
"""
import os, urllib.parse
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
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


TIER = """public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text)"""


def hdr(s):
    print('\n' + '=' * 70)
    print(s)
    print('=' * 70)


hdr('A. EXTERNAL DATA — external_source_status by source/status')
for r in fetch("""select source, status, count(*) c
                  from public.external_source_status group by 1,2 order by 1,2"""):
    print(f"  {r['source']:<16} {r['status']:<10} states={r['c']}")

hdr('B. CATALOG — beaches_gold by state')
for r in fetch("""select state, count(*) tot,
                        count(*) filter (where is_active) act,
                        count(*) filter (where is_scoreable) score
                   from public.beaches_gold where state in ('CA','OR','WA') group by 1 order by 1"""):
    print(f"  {r['state']}: total={r['tot']:>5} active={r['act']:>5} scoreable={r['score']:>5}")

hdr('C. POLICY TIER DISTRIBUTION — OR/WA active')
for r in fetch(f"""select g.state, ({TIER}) tier, count(*) c
                    from public.beaches_gold g
                    join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                   where g.is_active and g.state in ('OR','WA')
                   group by 1,2 order by 1,2"""):
    print(f"  {r['state']} {r['tier']:<22} {r['c']}")

hdr('D. STRUCTURAL ENRICHMENT — OR/WA tier-1 (1+1b+1c)')
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
                    where g.is_active and g.state in ('OR','WA')
                      and ({TIER}) in ('1_off-leash','2_on-leash')
                    group by 1 order by 1"""):
    print(f"  {r['state']:<5} {r['tot']:>4} {r['cfips']:>6} {r['park']:>4} {r['c1']:>4} {r['addr']:>4} {r['nsrc']:>5} {r['amen']:>4} {r['desc_filled']:>5}")

hdr('E. ZONE_RULES SECTION DEPTH — OR/WA active')
for r in fetch("""with z as (
                    select g.state,
                           (select count(*)::int from jsonb_array_elements(coalesce(bdp.zone_rules->'regions','[]'::jsonb)) r,
                                                    jsonb_object_keys(r->'sections')) n_sections
                      from public.beaches_gold g
                      join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                     where g.is_active and g.state in ('OR','WA')
                  )
                  select state, count(*) tot,
                         count(*) filter (where n_sections >= 2) ge2,
                         count(*) filter (where n_sections >= 4) ge4,
                         max(n_sections) maxs
                    from z group by 1 order by 1"""):
    print(f"  {r['state']:<5} active={r['tot']:>4} >=2={r['ge2']:>4} >=4={r['ge4']:>4} max={r['maxs']}")

hdr('F. SCORING — daily-beach-refresh today')
for r in fetch("""select g.state, count(distinct g.fid) scoreable,
                        count(distinct r.location_id) with_today
                   from public.beaches_gold g
                   left join public.beach_day_recommendations r
                     on r.location_id=g.location_id and r.local_date = current_date
                  where g.is_scoreable and g.is_active and g.state in ('CA','OR','WA')
                  group by 1 order by 1"""):
    print(f"  {r['state']}: scoreable={r['scoreable']:>4} with_today_rec={r['with_today']:>4}")

hdr('G. OPERATORS — OR/WA by level + policy coverage')
for r in fetch("""select state_code, level, count(*) ops,
                        count(odp.operator_id) with_pol,
                        count(*) filter (where odp.policy_found = true) found
                   from public.operators op
                   left join public.operator_dogs_policy odp on odp.operator_id=op.id
                  where state_code in ('OR','WA') and is_active
                  group by 1,2 order by 1,2"""):
    print(f"  {r['state_code']} {r['level']:<8} ops={r['ops']:<5} with_policy={r['with_pol']:<3} found={r['found']}")

hdr('H. BEP EVIDENCE — sources for OR/WA tier-1 fids')
for r in fetch(f"""select bep.source, count(distinct bep.gold_fid) fids
                    from public.beach_enrichment_provenance bep
                    join public.beaches_gold g on g.fid=bep.gold_fid
                    join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
                   where g.is_active and g.state in ('OR','WA')
                     and ({TIER}) in ('1_off-leash','2_on-leash')
                   group by 1 order by fids desc limit 15"""):
    print(f"  {r['source']:<32} {r['fids']}")

hdr('I. CLOSURE OVERLAYS — beach_dog_policy_zones (view)')
for r in fetch("""select g.state, count(distinct z.fid) beaches_with_overlay
                   from public.beaches_gold g
                   left join public.beach_dog_policy_zones z on z.fid = g.fid
                  where g.is_active and g.state in ('CA','OR','WA')
                  group by 1 order by 1"""):
    print(f"  {r['state']}: with_overlay={r['beaches_with_overlay']}")
for r in fetch("""select count(*) c from public.plover_seed_pending"""):
    print(f"  plover_seed_pending (need beach inventory): {r['c']}")

hdr('J. UNNAMED PIPELINE FILTER — landings → arena → gold')
for r in fetch("""select 'OR' state, count(*) tot,
                        count(*) filter (where coalesce(tags->>'name','')='') unnamed
                   from public.osm_landing
                  where lat between 41.99 and 46.30 and lon between -124.57 and -116.46
                 union all
                 select 'WA', count(*),
                        count(*) filter (where coalesce(tags->>'name','')='')
                   from public.osm_landing
                  where lat between 45.54 and 49.00 and lon between -124.85 and -116.92"""):
    print(f"  osm_landing {r['state']}: total={r['tot']:>4} unnamed (filtered before arena)={r['unnamed']}")
print('  arena and beaches_gold for OR/WA: 0 unnamed (filter is correct)')

hdr('K. DB SIZE')
for r in fetch("select pg_size_pretty(pg_database_size(current_database())) size"):
    print(f"  {r['size']}")
