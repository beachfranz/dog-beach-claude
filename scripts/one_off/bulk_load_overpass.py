"""bulk_load_overpass.py — pre-load Overpass natural=beach per state.

Idempotent + resumable + Nano-tier-friendly. Calls into load_state.py
with --skip-pad-us --skip-noaa --skip-promote so each per-state run
just does the Overpass fetch + state-clipped osm_landing insert.

Note: load_state.py doesn't currently have --skip-promote; this runner
calls a narrower path directly via fetch_overpass_beaches() to avoid
firing the cluster + promote chain on every state.

Usage:
  python scripts/one_off/bulk_load_overpass.py
  python scripts/one_off/bulk_load_overpass.py --priority 1
  python scripts/one_off/bulk_load_overpass.py --states ME,FL
"""
from __future__ import annotations
import argparse, os, sys, time, urllib.parse
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

# Reuse load_state.py's Overpass fetcher
sys.path.insert(0, str(ROOT / 'scripts'))
from load_state import fetch_overpass_beaches, STATE_BBOX

# Add bbox catalog for the states we'll need (load_state.py only has CA/OR/WA).
# Source: USGS state bounding boxes, padded slightly.
EXTRA_BBOX = {
    'AK': (54.5, -180.0, 71.5, -129.0),  # Alaska — huge, Pacific facing
    'ME': (42.97, -71.10, 47.46, -66.94),
    'NH': (42.70, -72.56, 45.31, -70.61),
    'MA': (41.18, -73.51, 42.89, -69.86),
    'RI': (41.13, -71.91, 42.02, -71.12),
    'CT': (40.98, -73.73, 42.05, -71.79),
    'NY': (40.50, -79.76, 45.02, -71.78),
    'NJ': (38.93, -75.56, 41.36, -73.89),
    'DE': (38.45, -75.79, 39.84, -75.05),
    'MD': (37.91, -79.49, 39.72, -75.05),
    'VA': (36.54, -83.68, 39.47, -75.24),
    'NC': (33.84, -84.33, 36.59, -75.46),
    'SC': (32.03, -83.36, 35.22, -78.55),
    'GA': (30.36, -85.61, 35.00, -80.84),
    'FL': (24.40, -87.64, 31.00, -80.03),
    'AL': (30.14, -88.47, 35.01, -84.89),
    'MS': (30.17, -91.66, 35.01, -88.10),
    'LA': (28.93, -94.04, 33.02, -88.82),
    'TX': (25.84, -106.65, 36.50, -93.51),
    'HI': (18.91, -160.25, 22.24, -154.81),
    'MI': (41.70, -90.42, 48.31, -82.41),
    'MN': (43.50, -97.24, 49.38, -89.49),
    'WI': (42.49, -92.89, 47.08, -86.81),
    'IL': (36.97, -91.51, 42.51, -87.50),
    'IN': (37.77, -88.10, 41.76, -84.78),
    'OH': (38.40, -84.82, 41.98, -80.52),
    'PA': (39.72, -80.52, 42.27, -74.69),
}

PRIORITY_1 = list(EXTRA_BBOX.keys())  # coastal + Great Lakes


def q(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall()


def osm_landing_count(state: str) -> int:
    """Count osm_landing rows whose centroid falls in the state's bbox."""
    if state in STATE_BBOX:
        s, w, n, e = STATE_BBOX[state]
    elif state in EXTRA_BBOX:
        s, w, n, e = EXTRA_BBOX[state]
    else:
        return 0
    rs = q("""select count(*) c from public.osm_landing
              where lat between %s and %s and lon between %s and %s""",
           (s, n, w, e))
    return rs[0]['c']


def log(msg: str):
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--priority', type=int, choices=[1], default=1)
    ap.add_argument('--states', help='Comma-sep override')
    ap.add_argument('--rest-seconds', type=int, default=10,
                    help='Sleep between Overpass calls (be polite)')
    ap.add_argument('--max-records-per-state', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    else:
        states = PRIORITY_1

    # Inject EXTRA_BBOX into load_state's catalog so fetch_overpass_beaches works
    STATE_BBOX.update(EXTRA_BBOX)

    log(f'Plan: load Overpass natural=beach for {len(states)} states')
    log(f'States: {", ".join(states)}')

    if args.dry_run:
        for st in states:
            n = osm_landing_count(st)
            tag = 'SKIP (already loaded)' if n > 50 else 'WOULD LOAD'
            log(f'  {st}: {n:>6} osm_landing rows in bbox  {tag}')
        return

    loaded, skipped, failed = 0, 0, 0
    for st in states:
        n = osm_landing_count(st)
        if n > 50:
            log(f'{st}: {n} osm_landing rows already; skipping')
            skipped += 1
            continue

        log(f'{st}: fetching Overpass...')
        t0 = time.time()
        try:
            inserted = fetch_overpass_beaches(st, args.max_records_per_state)
            elapsed = time.time() - t0
            log(f'  {st}: inserted {inserted} osm_landing rows in {elapsed:.0f}s')
            loaded += 1
        except Exception as e:
            log(f'  {st}: FAILED — {e}')
            failed += 1

        time.sleep(args.rest_seconds)

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
