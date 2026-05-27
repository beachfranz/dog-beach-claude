"""bulk_load_amenities.py — pre-load OSM amenities (osm_amenities) per state.

Idempotent + resumable + Nano-tier-friendly. Calls into the existing
external_sources.py registry (which already has osm_amenities wired up
with the canonical taxonomy). Each per-state run is its own Overpass
pull → upsert into public.osm_amenities.

Usage:
  python scripts/one_off/bulk_load_amenities.py
  python scripts/one_off/bulk_load_amenities.py --priority 1
  python scripts/one_off/bulk_load_amenities.py --states ME,FL
  python scripts/one_off/bulk_load_amenities.py --dry-run

Notes:
- external_sources.py only has CA in its built-in _STATE_BBOX. We inject
  the priority-1 bboxes at runtime (same trick as bulk_load_overpass.py).
- Skip rule: state already has >1000 osm_amenities rows → skip. CA
  baseline is ~329K, so 1000 is a safe lower bound for any coastal state.
- Polite to Overpass: sleep 10s between states.
- Writes to public.external_source_status on each state.
"""
from __future__ import annotations
import argparse, os, subprocess, sys, time, urllib.parse
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

# Sibling helper for external_source_status writes
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _external_source_status import record_status  # noqa: E402

# Import external_sources.py's bbox dict so we can extend it before its
# overpass fetcher reads it. external_sources is in scripts/, not on the
# path by default.
sys.path.insert(0, str(ROOT / 'scripts'))
import external_sources  # noqa: E402

# Same priority-1 bboxes used by bulk_load_overpass.py — keep these
# two scripts in sync.
EXTRA_BBOX = {
    'AK': (54.5, -180.0, 71.5, -129.0),
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
    # Inland (lake/reservoir beaches + dog parks) — added 2026-05-27 for full-50 coverage
    'UT': (36.99, -114.07, 42.00, -109.04),
    'AZ': (31.33, -114.82, 37.00, -109.05),
    'NV': (35.00, -120.01, 42.00, -114.04),
    'ID': (41.99, -117.24, 49.00, -111.04),
    'MT': (44.36, -116.05, 49.00, -104.04),
    'WY': (40.99, -111.06, 45.01, -104.05),
    'CO': (36.99, -109.06, 41.00, -102.04),
    'NM': (31.33, -109.05, 37.00, -103.00),
    'ND': (45.94, -104.05, 49.00, -96.55),
    'SD': (42.48, -104.06, 45.95, -96.44),
    'NE': (39.99, -104.05, 43.00, -95.31),
    'KS': (36.99, -102.05, 40.00, -94.59),
    'OK': (33.62, -103.00, 37.00, -94.43),
    'IA': (40.38, -96.64, 43.50, -90.14),
    'MO': (35.99, -95.77, 40.61, -89.10),
    'AR': (33.00, -94.62, 36.50, -89.65),
    'KY': (36.50, -89.57, 39.15, -81.96),
    'TN': (34.98, -90.31, 36.68, -81.65),
    'WV': (37.20, -82.64, 40.64, -77.72),
    'VT': (42.73, -73.44, 45.02, -71.46),
    'DC': (38.79, -77.12, 38.99, -76.91),
}
PRIORITY_1 = list(EXTRA_BBOX.keys())


def q(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall()


def amenities_count(state: str) -> int:
    rs = q("select count(*) c from public.osm_amenities where state=%s", (state,))
    return rs[0]['c']


def db_size_mb() -> int:
    rs = q("select pg_database_size(current_database())/1024/1024 mb")
    return rs[0]['mb']


def log(msg: str):
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--priority', type=int, choices=[1], default=1)
    ap.add_argument('--states', help='Comma-sep override')
    ap.add_argument('--rest-seconds', type=int, default=10,
                    help='Sleep between Overpass calls')
    ap.add_argument('--storage-cap-mb', type=int, default=11000,
                    help='Default 11000 MB (~1GB headroom on 12GB Supabase provision per Franz 2026-05-27)')
    ap.add_argument('--skip-threshold', type=int, default=1000)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    else:
        states = PRIORITY_1

    # Inject EXTRA_BBOX into external_sources' bbox catalog so the
    # overpass fetcher can scope each state.
    external_sources._STATE_BBOX.update(EXTRA_BBOX)

    log(f'Plan: load OSM amenities for {len(states)} states')
    log(f'States: {", ".join(states)}')
    log(f'Storage cap: {args.storage_cap_mb} MB')
    log(f'Current DB size: {db_size_mb()} MB')

    if args.dry_run:
        for st in states:
            n = amenities_count(st)
            tag = 'SKIP (already loaded)' if n > args.skip_threshold else 'WOULD LOAD'
            log(f'  {st}: {n:>7} osm_amenities rows  {tag}')
        return

    def safe_status(*a, **kw):
        try:
            record_status(*a, **kw)
        except Exception as se:
            log(f'    (status write failed: {se})')

    def safe_count():
        for _ in range(3):
            try:
                return amenities_count(st), db_size_mb()
            except Exception as ce:
                log(f'    (DB probe failed: {ce}; retrying)')
                time.sleep(5)
        return None, None

    loaded, skipped, failed = 0, 0, 0
    for st in states:
        n, size = safe_count()
        if n is None:
            log(f'{st}: DB unreachable for count; skipping this state')
            time.sleep(args.rest_seconds)
            continue
        if n > args.skip_threshold:
            log(f'{st}: {n} osm_amenities rows already; skipping')
            safe_status(PG, 'osm_amenities', st, 'skipped', n, 'already loaded')
            skipped += 1
            continue

        if size > args.storage_cap_mb:
            log(f'STORAGE GATE: {size} MB > cap {args.storage_cap_mb} MB; stopping')
            break

        log(f'{st}: fetching osm_amenities (DB at {size} MB)')
        t0 = time.time()
        rc = subprocess.run(
            [sys.executable, str(ROOT / 'scripts' / 'external_sources.py'),
             'load', 'osm_amenities', '--state', st],
            capture_output=True, text=True
        )
        elapsed = time.time() - t0
        if rc.returncode == 0:
            new_n, _ = safe_count()
            new_n = new_n if new_n is not None else 0
            log(f'  {st}: loaded {new_n} rows in {elapsed:.0f}s')
            safe_status(PG, 'osm_amenities', st, 'ok', new_n,
                        f'fetched in {elapsed:.0f}s')
            loaded += 1
        else:
            tail = (rc.stderr or '')[-500:]
            log(f'  {st}: FAILED rc={rc.returncode} elapsed={elapsed:.0f}s')
            log(f'    stderr tail: {tail}')
            safe_status(PG, 'osm_amenities', st, 'failed', None,
                        tail or f'rc={rc.returncode}')
            failed += 1

        time.sleep(args.rest_seconds)

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
