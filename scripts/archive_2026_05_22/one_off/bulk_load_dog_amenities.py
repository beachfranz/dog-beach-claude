"""bulk_load_dog_amenities.py — pull OSM vets / pet shops / dog parks
per state into public.dog_amenities. Backs the "dogs-active-catchment"
signal (count of dog amenities within R miles of a beach).

Per-state Overpass query: amenity=veterinary OR shop=pet OR
leisure=dog_park within the state's bbox. Results normalized to
points (way/relation centroids derived via Overpass `out center`).

Idempotent: skips if external_source_status has dog_amenities=ok for
the state, OR if the state already has >=20 dog_amenities rows.

Usage:
  python scripts/one_off/bulk_load_dog_amenities.py
  python scripts/one_off/bulk_load_dog_amenities.py --states CA,OR,WA
  python scripts/one_off/bulk_load_dog_amenities.py --skip-existing
  python scripts/one_off/bulk_load_dog_amenities.py --states MA --dry-run
"""
from __future__ import annotations
import argparse, json, os, subprocess, sys, time, urllib.parse, urllib.request
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _external_source_status import record_status  # noqa: E402

OVERPASS_URL = 'https://overpass-api.de/api/interpreter'
OVERPASS_TIMEOUT = 180

# Bbox per state (south, west, north, east). Same set used by
# bulk_load_amenities.py / bulk_load_overpass.py — keep in sync.
STATE_BBOX = {
    'AK': (54.5, -180.0, 71.5, -129.0),
    'CA': (32.53, -124.41, 42.01, -114.13),
    'OR': (41.99, -124.57, 46.30, -116.46),
    'WA': (45.54, -124.79, 49.00, -116.92),
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


def log(m: str): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def already_loaded(state: str) -> bool:
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""select 1 from public.external_source_status
                            where source='dog_amenities' and state=%s and status='ok'""",
                        (state,))
            return cur.fetchone() is not None


def state_count(state: str) -> int:
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("select count(*) from public.dog_amenities where state=%s", (state,))
            return int(cur.fetchone()[0])


def overpass_query(bbox: tuple[float, float, float, float]) -> dict | None:
    """Pull veterinary, pet shop, and dog park features within bbox.
    `out center` gives way/relation centroids so we always have a point."""
    s, w, n, e = bbox
    bbox_str = f'{s},{w},{n},{e}'
    q = f"""
    [out:json][timeout:{OVERPASS_TIMEOUT}];
    (
      node["amenity"="veterinary"]({bbox_str});
      way["amenity"="veterinary"]({bbox_str});
      relation["amenity"="veterinary"]({bbox_str});
      node["shop"="pet"]({bbox_str});
      way["shop"="pet"]({bbox_str});
      relation["shop"="pet"]({bbox_str});
      node["leisure"="dog_park"]({bbox_str});
      way["leisure"="dog_park"]({bbox_str});
      relation["leisure"="dog_park"]({bbox_str});
    );
    out center;
    """
    data = urllib.parse.urlencode({'data': q}).encode()
    req = urllib.request.Request(OVERPASS_URL, data=data,
                                  headers={'User-Agent': 'dog-beach-claude/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=OVERPASS_TIMEOUT + 30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f'    overpass failed: {e}')
        return None


def normalize(elems: list[dict], state: str) -> list[dict]:
    out = []
    for el in elems:
        kind_raw = el.get('type')   # node | way | relation
        if kind_raw == 'node':
            lat, lon = el.get('lat'), el.get('lon')
        else:
            c = el.get('center') or {}
            lat, lon = c.get('lat'), c.get('lon')
        if lat is None or lon is None:
            continue
        tags = el.get('tags', {}) or {}
        # Decide our 'kind' bucket
        if tags.get('amenity') == 'veterinary':
            kind = 'veterinary'
        elif tags.get('shop') == 'pet':
            kind = 'pet_shop'
        elif tags.get('leisure') == 'dog_park':
            kind = 'dog_park'
        else:
            continue
        out.append({
            'osm_id':      el.get('id'),
            'osm_kind':    kind_raw,
            'kind':        kind,
            'raw_amenity': tags.get('amenity'),
            'raw_shop':    tags.get('shop'),
            'raw_leisure': tags.get('leisure'),
            'name':        tags.get('name'),
            'state':       state,
            'lat':         lat,
            'lon':         lon,
            'raw_tags':    tags,
        })
    return out


def push(rows: list[dict]) -> int:
    if not rows: return 0
    BATCH = 500
    n = 0
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            for i in range(0, len(rows), BATCH):
                batch = rows[i:i + BATCH]
                cur.execute("select public.load_dog_amenities_batch(%s::jsonb)",
                            (json.dumps(batch),))
                n += int(cur.fetchone()[0])
    return n


def load_one_state(state: str, dry_run: bool = False) -> tuple[bool, int, str]:
    bbox = STATE_BBOX.get(state)
    if not bbox: return False, 0, f'no bbox for {state}'
    log(f'    overpass query for {state} bbox={bbox}')
    t0 = time.time()
    data = overpass_query(bbox)
    if data is None:
        return False, 0, 'overpass failed'
    elems = data.get('elements', [])
    log(f'    overpass returned {len(elems)} raw elements in {time.time()-t0:.0f}s')
    rows = normalize(elems, state)
    by_kind = {}
    for r in rows: by_kind[r['kind']] = by_kind.get(r['kind'], 0) + 1
    log(f'    normalized: {len(rows)} kept; by kind = {by_kind}')
    if dry_run:
        return True, len(rows), 'dry-run'
    n = push(rows)
    return True, n, 'ok'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', help='Comma-sep state list (default: all in STATE_BBOX)')
    ap.add_argument('--skip-existing', action='store_true',
                    help='Skip if status=ok in external_source_status')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    else:
        states = sorted(STATE_BBOX.keys())

    log(f'Plan: dog_amenities for {len(states)} states')
    log(f'States: {", ".join(states)}')

    loaded = skipped = failed = 0
    for state in states:
        if args.skip_existing and already_loaded(state):
            log(f'{state}: SKIP (status=ok)')
            skipped += 1
            continue
        existing = state_count(state)
        if existing >= 20 and not args.dry_run:
            log(f'{state}: SKIP ({existing} dog_amenities already)')
            try:
                record_status(PG, 'dog_amenities', state, 'ok', existing,
                              'pre-existing >=20 rows')
            except Exception:
                pass
            skipped += 1
            continue

        log(f'{state}: loading')
        t0 = time.time()
        ok, n, msg = load_one_state(state, dry_run=args.dry_run)
        elapsed = time.time() - t0
        if ok:
            log(f'  {state}: {msg} ({n} rows in {elapsed:.0f}s)')
            if not args.dry_run:
                try:
                    record_status(PG, 'dog_amenities', state, 'ok', n,
                                  f'OSM Overpass, {elapsed:.0f}s')
                except Exception as e:
                    log(f'    (status write failed: {e})')
            loaded += 1
        else:
            log(f'  {state}: FAILED — {msg}')
            if not args.dry_run:
                try:
                    record_status(PG, 'dog_amenities', state, 'failed', None, msg[:500])
                except Exception:
                    pass
            failed += 1
        time.sleep(10)  # polite to Overpass

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
