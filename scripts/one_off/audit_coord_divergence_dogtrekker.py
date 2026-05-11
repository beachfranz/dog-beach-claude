"""audit_coord_divergence_dogtrekker.py

For each DogTrekker beach with a CA spatial match within 2km, compare
their (lat, lng) against ours (beaches_gold). Use osm_landing beach
polygons as ground-truth reference — whichever of (ours, theirs) is
closer to the nearest OSM beach polygon is "probably at the sand."

Outputs a side-by-side audit table sorted by likely-wrong-first, with
Google Maps URLs for visual verification + the fid for direct
beach-editor-gold navigation.

Read-only — does NOT modify any beaches_gold coordinates. Output is a
shortlist for curation.

Usage:
  python scripts/one_off/audit_coord_divergence_dogtrekker.py
  python scripts/one_off/audit_coord_divergence_dogtrekker.py --min-divergence-m 200
"""
from __future__ import annotations
import argparse, os, urllib.parse
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--min-divergence-m', type=int, default=100,
                    help='Skip pairs where ours and theirs are within N meters')
    ap.add_argument('--max-coord-search-radius-m', type=int, default=2000,
                    help='Match radius for finding the candidate pair')
    ap.add_argument('--beach-poly-search-radius-m', type=int, default=5000,
                    help='Look for OSM beach polygon within this radius of each coord')
    ap.add_argument('--limit', type=int, default=200)
    args = ap.parse_args()

    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("set statement_timeout = '300s'")
        # Step 1: get the divergent pairs only (fast, GIST-index friendly)
        cur.execute("""
          select distinct on (t.source_id)
                 t.source_id, t.name as dt_name, t.lat as dt_lat, t.lng as dt_lng,
                 b.fid, coalesce(b.display_name_override, b.name) as bg_name,
                 b.county_name, b.is_active, b.is_scoreable,
                 b.lat as bg_lat, b.lon as bg_lon,
                 st_distance(b.geom::geography,
                             st_setsrid(st_makepoint(t.lng, t.lat), 4326)::geography)::int
                 as divergence_m
            from public.truth_external t
            join public.beaches_gold b
              on b.state='CA'
             and st_dwithin(b.geom::geography,
                            st_setsrid(st_makepoint(t.lng, t.lat), 4326)::geography, %s)
           where t.source='dogtrekker' and t.lat is not null
           order by t.source_id,
                    st_distance(b.geom::geography,
                                st_setsrid(st_makepoint(t.lng, t.lat), 4326)::geography)
        """, (args.max_coord_search_radius_m,))
        all_pairs = [r for r in cur.fetchall() if r[11] > args.min_divergence_m]

        # Step 2: for each diverged pair, compute OSM-beach-polygon distance
        # for each candidate coord (one row at a time — fast with index).
        rows = []
        for r in all_pairs:
            (sid, dt_name, dt_lat, dt_lng, fid, bg_name, county,
             active, score, bg_lat, bg_lon, div_m) = r
            cur.execute("""
              select min(st_distance(ol.geom_full::geography,
                                     st_setsrid(st_makepoint(%s,%s),4326)::geography))::int
                from public.osm_landing ol
               where ol.is_active and ol.geom_full is not null
                 and st_dwithin(ol.geom_full::geography,
                                st_setsrid(st_makepoint(%s,%s),4326)::geography, %s)
            """, (bg_lon, bg_lat, bg_lon, bg_lat, args.beach_poly_search_radius_m))
            ours_ref = cur.fetchone()[0]
            cur.execute("""
              select min(st_distance(ol.geom_full::geography,
                                     st_setsrid(st_makepoint(%s,%s),4326)::geography))::int
                from public.osm_landing ol
               where ol.is_active and ol.geom_full is not null
                 and st_dwithin(ol.geom_full::geography,
                                st_setsrid(st_makepoint(%s,%s),4326)::geography, %s)
            """, (dt_lng, dt_lat, dt_lng, dt_lat, args.beach_poly_search_radius_m))
            theirs_ref = cur.fetchone()[0]

            if theirs_ref is None and ours_ref is None: verdict = 'no_ref'
            elif theirs_ref is None: verdict = 'ours_only_has_ref'
            elif ours_ref is None: verdict = 'theirs_only_has_ref'
            elif ours_ref <= theirs_ref: verdict = 'ours_closer'
            else: verdict = 'theirs_closer'

            rows.append((fid, bg_name, county, active, score,
                         bg_lat, bg_lon, dt_lat, dt_lng,
                         div_m, ours_ref, theirs_ref, verdict))

        # Sort: theirs_closer first (highest priority for fix), by improvement margin
        def sort_key(r):
            verdict = r[12]
            ours_ref, theirs_ref = r[10], r[11]
            if verdict == 'theirs_closer':
                return (-1, -(ours_ref - theirs_ref))
            if verdict == 'theirs_only_has_ref':
                return (0, 0)
            return (1, 0)
        rows.sort(key=sort_key)
        rows = rows[:args.limit]

    print(f'Coord divergence audit — {len(rows)} candidates (min divergence {args.min_divergence_m}m)\n')
    moves = 0
    for r in rows:
        (fid, name, county, active, score, blat, blon, dlat, dlon,
         div_m, ours_ref, theirs_ref, verdict) = r
        if verdict == 'theirs_closer':
            moves += 1
        # Use unicode-safe printing
        try:
            line = (f'fid={fid:<7} {(name or "")[:32]:<32}  div={div_m:>5}m  '
                    f'ours_ref={str(ours_ref):>5}  theirs_ref={str(theirs_ref):>5}  '
                    f'verdict={verdict}')
            print(line)
            if verdict == 'theirs_closer':
                print(f'    ours:   https://www.google.com/maps/@{blat},{blon},19z  '
                      f'beach-editor-gold.html?fid={fid}')
                print(f'    theirs: https://www.google.com/maps/@{dlat},{dlon},19z')
        except UnicodeEncodeError:
            pass

    print()
    print(f'SUMMARY: {len(rows)} divergent pairs')
    print(f'  theirs_closer to OSM beach polygon (probably needs move): {moves}')
    print(f'  ours_closer to OSM beach polygon (we look fine):          {sum(1 for r in rows if r[12]=="ours_closer")}')
    print(f'  no OSM beach polygon nearby either coord:                 {sum(1 for r in rows if r[12]=="no_ref")}')


if __name__ == '__main__':
    main()
