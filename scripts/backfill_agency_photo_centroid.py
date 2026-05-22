"""backfill_agency_photo_centroid.py — populate beach_photos.distance_m
for agency-gallery photos (WSPRC/CDPR/NPS/OPRD) that currently have
distance_m=NULL because they were attributed via PIP-fanout, plus
region-name match the title against beach's zone_rules.regions.

Approach (source-agnostic):
  1. For each fanout image_url (≥2 beach_fids), find the polygon in
     beach_polygon_membership that contains ALL of those fids → the
     park. Use the polygon's centroid. Per-beach distance_m =
     beach.geom <-> centroid.
  2. Title-name match: build "title text" from filename + first 250
     chars of vision.description. For each beach attached to the URL,
     check the beach's zone_rules.regions[*].name. If any region name
     appears in the title, set source_meta.matched_region = {name, fid}.
  3. Single-beach URLs: skip distance_m (already attributed) but still
     do region-match.

Skips CCC (per-access-point, already has distance_m populated).

Usage:
  python scripts/backfill_agency_photo_centroid.py                   # dry-run (default), all sources
  python scripts/backfill_agency_photo_centroid.py --apply           # write
  python scripts/backfill_agency_photo_centroid.py --sources wsprc   # one source
"""
from __future__ import annotations
import sys, os, re, urllib.parse, argparse, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)  # type: ignore[attr-defined]
from pathlib import Path
from collections import defaultdict
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')

AGENCY_SOURCES = ['wsprc', 'cdpr', 'nps', 'oprd']


def _connect():
    pp = urllib.parse.urlparse((ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip())
    return psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')


def normalize(s: str) -> str:
    s = urllib.parse.unquote(s or '').lower()
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='write changes; default is dry-run')
    ap.add_argument('--sources', default=','.join(AGENCY_SOURCES))
    args = ap.parse_args()
    sources = [s.strip() for s in args.sources.split(',') if s.strip()]
    print(f'Sources: {sources}  mode: {"APPLY" if args.apply else "DRY-RUN"}')

    conn = _connect()
    cur = conn.cursor()

    # Total scope = NULL-distance agency photos
    cur.execute("""
      SELECT source, COUNT(*) FROM beach_photos
       WHERE source = ANY(%s) AND distance_m IS NULL
       GROUP BY source ORDER BY 2 DESC
    """, (sources,))
    print(f'\\n=== NULL-distance scope ===')
    for r in cur.fetchall(): print(f'  {r[0]:<8} {r[1]} photos')

    total_dist_updates = 0
    total_region_updates = 0

    for src in sources:
        print(f'\\n===== Source: {src} =====')
        # Per unique image_url, get all (beach_fid, beach_lat, beach_lon, filename, vision)
        cur.execute("""
          SELECT bp.image_url,
                 bp.source_meta->>'filename' AS filename,
                 bp.external_id AS ext_id,
                 LEFT(bp.source_meta->'vision'->>'description', 250) AS vd,
                 bp.arena_group_id AS fid,
                 ST_Y(g.geom::geometry) AS lat,
                 ST_X(g.geom::geometry) AS lon
            FROM beach_photos bp
            JOIN beaches_gold g ON g.fid = bp.arena_group_id
           WHERE bp.source = %s AND bp.image_url IS NOT NULL
           ORDER BY bp.image_url
        """, (src,))
        by_url: dict[str, list[dict]] = defaultdict(list)
        for url, fname, ext_id, vd, fid, lat, lon in cur.fetchall():
            by_url[url].append({
                'filename': fname or ext_id or '', 'vd': vd or '',
                'fid': fid, 'lat': lat, 'lon': lon,
            })

        urls = list(by_url.items())
        urls_single = [u for u, b in urls if len(b) == 1]
        urls_fanout = [u for u, b in urls if len(b) >= 2]
        print(f'  unique URLs: {len(urls)}  ({len(urls_single)} single-beach + {len(urls_fanout)} fanout)')

        n_centroid_hit = 0
        n_centroid_miss = 0
        n_region_match = 0

        import time as _t
        _t0 = _t.time()
        for _i, (url, beaches) in enumerate(urls, 1):
            if _i % 50 == 0:
                print(f'  ... {_i}/{len(urls)} URLs processed ({_t.time()-_t0:.0f}s)', flush=True)
            title = normalize(beaches[0]['filename'] + ' ' + beaches[0]['vd'])

            # ----- Centroid distance (fanout only) -----
            if len(beaches) >= 2:
                fid_list = [b['fid'] for b in beaches]
                # Find polygon(s) in beach_polygon_membership that contain ALL these fids
                cur.execute("""
                  SELECT m.polygon_kind, m.polygon_id, m.polygon_name, m.area_m2
                    FROM beach_polygon_membership m
                   WHERE m.gold_fid = ANY(%s)
                   GROUP BY m.polygon_kind, m.polygon_id, m.polygon_name, m.area_m2
                  HAVING COUNT(DISTINCT m.gold_fid) = %s
                   ORDER BY area_m2 ASC NULLS LAST  -- smallest polygon wins (most specific)
                   LIMIT 1
                """, (fid_list, len(fid_list)))
                poly = cur.fetchone()
                if poly:
                    pkind, pid, pname, _area = poly
                    # Get the polygon centroid (from pad_us_units if pad_us, else compute)
                    cur.execute("""
                      SELECT ST_Y(ST_Centroid(geom)::geometry) AS lat,
                             ST_X(ST_Centroid(geom)::geometry) AS lon
                        FROM pad_us_units WHERE unit_id::text = %s
                    """, (str(pid),))
                    pc = cur.fetchone()
                    if pc and pc[0] is not None:
                        c_lat, c_lon = float(pc[0]), float(pc[1])
                        import math
                        for b in beaches:
                            dlat = math.radians(b['lat'] - c_lat); dlon = math.radians(b['lon'] - c_lon)
                            h = math.sin(dlat/2)**2 + math.cos(math.radians(c_lat))*math.cos(math.radians(b['lat']))*math.sin(dlon/2)**2
                            dist = 2 * 6371000.0 * math.asin(math.sqrt(h))
                            b['dist_m'] = round(dist, 0)
                        n_centroid_hit += 1
                        if args.apply:
                            for b in beaches:
                                cur.execute("""
                                  UPDATE beach_photos SET distance_m = %s
                                   WHERE source = %s AND image_url = %s AND arena_group_id = %s
                                """, (b['dist_m'], src, url, b['fid']))
                                total_dist_updates += 1
                    else:
                        n_centroid_miss += 1
                else:
                    n_centroid_miss += 1

            # ----- Region-name match (all URLs) -----
            for b in beaches:
                cur.execute("""
                  SELECT jsonb_path_query_array(zone_rules, '$.regions[*].name')
                    FROM beach_dog_policy WHERE arena_group_id = %s
                """, (b['fid'],))
                rn_row = cur.fetchone()
                region_names = [n for n in (rn_row[0] if rn_row else []) if n] if rn_row else []
                for rn in region_names:
                    rn_norm = normalize(rn)
                    if not rn_norm or len(rn_norm) < 6: continue
                    distinct_toks = [t for t in rn_norm.split() if len(t) >= 5]
                    if not distinct_toks: continue
                    if all(re.search(r'\b' + re.escape(t) + r'\b', title) for t in distinct_toks):
                        n_region_match += 1
                        if args.apply:
                            cur.execute("""
                              UPDATE beach_photos
                                 SET source_meta = jsonb_set(coalesce(source_meta, '{}'::jsonb),
                                                              '{matched_region}', to_jsonb(%s::text))
                               WHERE source = %s AND image_url = %s AND arena_group_id = %s
                            """, (rn, src, url, b['fid']))
                            total_region_updates += 1
                        break

        if args.apply:
            conn.commit()
        print(f'  centroid distance hit: {n_centroid_hit} fanout URLs (miss: {n_centroid_miss})')
        print(f'  region-name matches:   {n_region_match}')

    print()
    print('=== TOTALS ===')
    print(f'  distance_m updates:    {total_dist_updates} (mode: {"applied" if args.apply else "would apply"})')
    print(f'  region-name updates:   {total_region_updates}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
