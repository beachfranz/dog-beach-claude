"""bringfido_dogtrekker_calibration.py — diff our resolved beach_dog_policy
against the BringFido + DogTrekker truth set in `truth_external`.

Purpose: tell us where the existing pipeline disagrees with curated
external dog-beach databases, WITHOUT changing anything. If the diff
is small (~5%), we know we're solid. If large (>20%), we have a
calibration problem worth investigating before adding BringFido as
a real evidence source.

Method:
  1. Load all bringfido + dogtrekker rows from truth_external.
  2. For each, find the corresponding gold beach:
     - First via matched_origin_key (already populated for 179 of 266)
     - Fall back to name+state+lat/lng spatial proximity
  3. Compare truth.dogs_rule to gold's beach_dog_policy fields.
  4. Categorize: AGREE / DISAGREE / NO_GOLD_MATCH / NO_GOLD_POLICY
  5. Output CSV + summary.

Mapping:
  truth.dogs_rule='off_leash' → expect bdp.dogs_allowed='yes' + has_off_leash=true
  truth.dogs_rule='leash'     → expect bdp.dogs_allowed='yes' + has_on_leash=true
                                AND has_off_leash IS NULL OR FALSE
  truth.dogs_rule='yes'       → expect bdp.dogs_allowed='yes' (any leash form)
  truth.dogs_rule='unknown'   → skip (no comparison)

Usage:
  python scripts/audit/bringfido_dogtrekker_calibration.py
  python scripts/audit/bringfido_dogtrekker_calibration.py --csv /tmp/calibration.csv
"""
import argparse, csv, os, sys, urllib.parse
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


SQL = """
with truth as (
  select source, source_id, name, city, state, lat, lng, dogs_rule, raw_dog_text,
         matched_origin_key, source_url
    from public.truth_external
   where source in ('bringfido','dogtrekker')
),
matched_via_key as (
  -- Matched directly via matched_origin_key (179 / 266 rows)
  select t.*, g.fid as gold_fid, g.name as gold_name, g.state as gold_state,
         0.0::float8 as match_distance_m, 'origin_key' as match_via
    from truth t
    join public.beaches_gold g on g.location_id = t.matched_origin_key
   where t.matched_origin_key is not null
),
matched_via_spatial as (
  -- Fallback: nearest beach within 500m + same state, with name overlap
  select distinct on (t.source, t.source_id)
         t.*, g.fid as gold_fid, g.name as gold_name, g.state as gold_state,
         st_distance(g.geom::geography,
                     st_setsrid(st_makepoint(t.lng, t.lat),4326)::geography) as match_distance_m,
         'spatial' as match_via
    from truth t
    left join public.beaches_gold g
      on g.is_active and g.state = t.state
     and g.geom is not null
     and st_dwithin(g.geom::geography,
                    st_setsrid(st_makepoint(t.lng, t.lat),4326)::geography, 500)
   where t.matched_origin_key is null
   order by t.source, t.source_id, match_distance_m
),
all_matched as (
  select * from matched_via_key
  union all
  select * from matched_via_spatial
)
select m.source, m.source_id, m.name as truth_name, m.city as truth_city,
       m.state as truth_state, m.dogs_rule as truth_rule,
       m.raw_dog_text, m.source_url,
       m.gold_fid, m.gold_name, m.gold_state,
       m.match_distance_m, m.match_via,
       bdp.dogs_allowed as gold_dogs_allowed,
       bdp.has_off_leash as gold_off,
       bdp.has_on_leash as gold_on,
       public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash,
         bdp.has_on_leash, bdp.dogs_prohibited_start::text) as gold_tier
  from all_matched m
  left join public.beach_dog_policy bdp on bdp.arena_group_id = m.gold_fid
 order by m.source, m.gold_fid;
"""


def verdict(truth_rule: str, gold_dogs: str | None,
            gold_off: bool | None, gold_on: bool | None,
            gold_fid: int | None) -> tuple[str, str]:
    """Return (verdict, reason)."""
    if gold_fid is None:
        return ("NO_GOLD_MATCH",
                "truth row has no spatial/key match in beaches_gold")
    if gold_dogs is None:
        return ("NO_GOLD_POLICY",
                "matched gold beach has no beach_dog_policy row")
    if truth_rule == "unknown":
        return ("SKIP", "truth dogs_rule is unknown")

    if truth_rule == "off_leash":
        if gold_dogs == "yes" and gold_off is True:
            return ("AGREE", "gold confirms off-leash allowed")
        return ("DISAGREE",
                f"truth=off_leash but gold={gold_dogs}/off={gold_off}/on={gold_on}")

    if truth_rule == "leash":
        if gold_dogs == "yes" and gold_on is True and not gold_off:
            return ("AGREE", "gold confirms leashed only")
        if gold_dogs == "yes" and gold_off is True:
            return ("DISAGREE",
                    "truth=leash-only but gold says off-leash allowed")
        if gold_dogs == "no":
            return ("DISAGREE", "truth=leash but gold says no dogs")
        return ("DISAGREE",
                f"truth=leash but gold={gold_dogs}/off={gold_off}/on={gold_on}")

    if truth_rule == "yes":
        # 'yes' is the listing-level "dog-friendly" — any positive policy agrees
        if gold_dogs == "yes":
            return ("AGREE", "gold confirms dogs allowed (any leash form)")
        if gold_dogs == "no":
            return ("DISAGREE", "truth=dog-friendly but gold says no dogs")
        if gold_dogs == "mixed":
            return ("PARTIAL_AGREE",
                    "truth=dog-friendly; gold=mixed (zone-dependent)")
        if gold_dogs == "unknown":
            return ("DISAGREE", "truth=dog-friendly but gold has unknown")
        return ("DISAGREE",
                f"truth=yes but gold dogs_allowed={gold_dogs}")

    return ("UNKNOWN_RULE", f"truth dogs_rule={truth_rule!r} not handled")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', default='tmp/bringfido_dogtrekker_calibration.csv')
    args = ap.parse_args()

    csv_path = ROOT / args.csv if not args.csv.startswith(('/', 'C:')) else Path(args.csv)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("set statement_timeout = '120s'")
            cur.execute(SQL)
            rows = cur.fetchall()

    summary = {'AGREE': 0, 'DISAGREE': 0, 'PARTIAL_AGREE': 0,
               'NO_GOLD_MATCH': 0, 'NO_GOLD_POLICY': 0, 'SKIP': 0}
    by_source = {}

    out_rows = []
    for r in rows:
        v, why = verdict(r['truth_rule'], r['gold_dogs_allowed'],
                         r['gold_off'], r['gold_on'], r['gold_fid'])
        summary[v] = summary.get(v, 0) + 1
        by_source.setdefault(r['source'], {})
        by_source[r['source']][v] = by_source[r['source']].get(v, 0) + 1
        out_rows.append({
            'source': r['source'],
            'truth_name': r['truth_name'],
            'truth_state': r['truth_state'],
            'truth_rule': r['truth_rule'],
            'gold_fid': r['gold_fid'],
            'gold_name': r['gold_name'],
            'gold_dogs_allowed': r['gold_dogs_allowed'],
            'gold_off': r['gold_off'],
            'gold_on': r['gold_on'],
            'gold_tier': r['gold_tier'],
            'match_distance_m': round(r['match_distance_m'], 1) if r['match_distance_m'] else None,
            'match_via': r['match_via'],
            'verdict': v,
            'reason': why,
            'source_url': r['source_url'],
        })

    with csv_path.open('w', newline='', encoding='utf-8') as f:
        if out_rows:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)

    total = len(rows)
    print(f'\n=== BringFido / DogTrekker Calibration ({total} truth rows) ===\n')
    print('Overall:')
    for v, n in sorted(summary.items(), key=lambda x: -x[1]):
        pct = 100 * n / total if total else 0
        print(f'  {v:<18} {n:>4}  ({pct:.0f}%)')

    print('\nBy source:')
    for src in sorted(by_source.keys()):
        sub = by_source[src]
        sub_total = sum(sub.values())
        agree = sub.get('AGREE', 0) + sub.get('PARTIAL_AGREE', 0)
        disagree = sub.get('DISAGREE', 0)
        comparable = agree + disagree
        agree_pct = (100 * agree / comparable) if comparable else 0
        print(f'  {src} (n={sub_total}, comparable={comparable}, '
              f'agree={agree_pct:.0f}%):')
        for v, n in sorted(sub.items(), key=lambda x: -x[1]):
            pct = 100 * n / sub_total if sub_total else 0
            print(f'    {v:<18} {n:>4}  ({pct:.0f}%)')

    print(f'\nFull report: {csv_path}')

    if summary.get('DISAGREE', 0):
        print(f'\nFirst 10 disagreements (highest signal for calibration work):')
        ds = [r for r in out_rows if r['verdict'] == 'DISAGREE'][:10]
        for r in ds:
            print(f'  {r["source"]:<11} {r["truth_name"]!r:<35} '
                  f'truth={r["truth_rule"]!r:<10} '
                  f'gold={r["gold_dogs_allowed"]!r}/off={r["gold_off"]}/on={r["gold_on"]} '
                  f'| {r["reason"]}')


if __name__ == '__main__':
    main()
