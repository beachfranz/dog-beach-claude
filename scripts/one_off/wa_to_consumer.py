"""wa_to_consumer.py — post-load pipeline for WA.

Runs the same sequence as the OR consumer pass:
1. pad_us emit per fid
2. OR-style statute fallback adapted for WA (need to add WA equivalent)
   For tonight: just run pad_us emit; statute fallback is OR-only.
3. resolve dogs per fid
4. publish_canonical (fires Tier 3 inactivate trigger)
5. one-shot inactivator (belt + suspenders)
6. re-promote zone_rules (sand baseline)
7. final tier breakdown
"""
import os, urllib.parse
from dotenv import load_dotenv
import psycopg2, psycopg2.extras
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(p.path or '/postgres').lstrip('/'), sslmode='require')


def q(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []


def run(state):
    fids = [r['fid'] for r in q(
        "select fid from public.beaches_gold where state=%s order by fid",
        (state,))]
    print(f"{state} gold: {len(fids)}")

    print("\n1. pad_us emit on each fid")
    ins = upd = 0
    for fid in fids:
        r = q('select * from public._emit_evidence_from_pad_us_dogs_policy(%s)',
              (fid,))[0]
        ins += r['rows_inserted'] or 0
        upd += r['rows_updated'] or 0
    print(f"   pad_us: ins={ins} upd={upd}")

    print("\n2. resolve dogs per fid")
    for fid in fids:
        q('select public._resolve_dogs_gold(%s)', (fid,))
    print(f"   resolved {len(fids)}")

    print(f"\n3. publish_canonical (fires Tier 3 inactivate trigger)")
    r = q('select * from public.publish_canonical_for_fids(%s)', (fids,))[0]
    print(f"   {dict(r)}")

    print(f"\n4. one-shot tier3 inactivate (belt + suspenders)")
    r = q("select * from public.inactivate_tier3_beaches(%s)", (state,))[0]
    print(f"   flipped: {r['deactivated_count']}, fids: {r['fids']}")

    print(f"\n5. re-promote zone_rules (sand baseline)")
    results = {}
    for fid in fids:
        r = q('select public._promote_zone_rules_for_fid(%s) r', (fid,))[0]
        results[r['r']] = results.get(r['r'], 0) + 1
    print(f"   {results}")

    print(f"\n6. Final {state} tier breakdown")
    for r in q(f"""select case
        when bdp.dogs_allowed='yes' and bdp.has_off_leash and bdp.dogs_prohibited_start is null then '1_marquee'
        when bdp.dogs_allowed='yes' and bdp.has_off_leash then '1b_offleash_caveat'
        when bdp.dogs_allowed='yes' then '1c_onleash'
        when bdp.dogs_allowed='mixed' and (bdp.has_off_leash is null or not bdp.has_off_leash) and bdp.has_on_leash then '4_limited_access'
        when bdp.dogs_allowed='no' then '3_no_dogs'
        else 'unknown'
      end tier, g.is_active, g.is_scoreable, count(*)
      from public.beaches_gold g
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.state=%s group by 1,2,3 order by 1,2,3 desc""", (state,)):
        print(f"  {r['tier']:<22} active={str(r['is_active']):<5} scoreable={str(r['is_scoreable']):<5} {r['count']}")

    print(f"\n7. Coverage")
    r = q("select count(*) c, count(*) filter (where is_active) act, count(*) filter (where is_active and is_scoreable) sc from public.beaches_gold where state=%s", (state,))[0]
    print(f"   {state}: total={r['c']} active={r['act']} scoreable={r['sc']}")


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else 'WA')
