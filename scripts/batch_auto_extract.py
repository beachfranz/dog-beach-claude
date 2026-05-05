"""batch_auto_extract.py — run insert_auto_extracted_polygon for beaches that
have NO OSM polygon and NO approved auto-extracted row yet. Logs progress
and skips on failure.

Usage:
  python scripts/batch_auto_extract.py --limit 25
  python scripts/batch_auto_extract.py --limit 25 --scoreable-only
  python scripts/batch_auto_extract.py            # all 135-ish; takes ~1-2 hours
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

import insert_auto_extracted_polygon as ip


def supabase_query(sql: str) -> dict:
    out = subprocess.check_output(
        ['supabase', 'db', 'query', '--linked', sql], text=True,
    )
    idx = out.find('{')
    end = out.rfind('}')
    return json.loads(out[idx:end+1])


def candidate_fids(scoreable_only: bool, limit: int | None) -> list[dict]:
    pred_score = 'AND bg.is_scoreable' if scoreable_only else ''
    pred_limit = f'LIMIT {limit}' if limit else ''
    rows = supabase_query(f"""
        SELECT bg.fid, bg.name, bg.county_name, bg.is_scoreable
          FROM public.beaches_gold bg
         WHERE bg.is_active
           {pred_score}
           AND NOT EXISTS (
             SELECT 1 FROM public.osm_beach_polys op
              WHERE ST_Intersects(op.geom, bg.geom)
           )
           AND NOT EXISTS (
             SELECT 1 FROM public.beach_polygons_auto_extracted ae
              WHERE ae.fid = bg.fid AND ae.review_status = 'approved'
           )
         ORDER BY bg.county_name, bg.fid
         {pred_limit}
    """)['rows']
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None,
                    help='Max number of beaches to process this run')
    ap.add_argument('--scoreable-only', action='store_true',
                    help='Only run on is_scoreable=true beaches')
    ap.add_argument('--fids', type=str, default=None,
                    help='Comma-separated explicit list of fids to re-run '
                         '(bypasses the no-OSM/no-approval filter)')
    args = ap.parse_args()

    if args.fids:
        ids = [int(x) for x in args.fids.split(',') if x.strip()]
        candidates = supabase_query(f"""
            SELECT fid, name, county_name, is_scoreable
              FROM public.beaches_gold
             WHERE fid = ANY(ARRAY[{','.join(map(str, ids))}])
               AND is_active
        """)['rows']
        # Preserve user-supplied order
        order = {fid: i for i, fid in enumerate(ids)}
        candidates.sort(key=lambda r: order.get(r['fid'], 999))
    else:
        candidates = candidate_fids(args.scoreable_only, args.limit)
    print(f'\n=== Batch auto-extraction ===')
    print(f'Candidates: {len(candidates)}  (scoreable_only={args.scoreable_only})')
    print()

    successes = []
    failures = []
    t0 = time.time()
    for i, c in enumerate(candidates, 1):
        fid = c['fid']
        elapsed = time.time() - t0
        print(f'[{i:>3}/{len(candidates)}] fid={fid} {c["name"]} ({c["county_name"]})  '
              f'elapsed={elapsed:.0f}s')
        try:
            ip.main(str(fid))
            successes.append(fid)
        except SystemExit as e:
            print(f'   skipped: {e}')
            failures.append((fid, str(e)))
        except Exception as e:
            print(f'   ERROR: {type(e).__name__}: {e}')
            traceback.print_exc()
            failures.append((fid, str(e)))
        print()

    elapsed = time.time() - t0
    print(f'\n=== Done in {elapsed:.0f}s ===')
    print(f'Inserted: {len(successes)} / {len(candidates)}')
    if failures:
        print(f'\nFailures:')
        for fid, msg in failures[:20]:
            print(f'  fid={fid}: {msg[:100]}')


if __name__ == '__main__':
    main()
