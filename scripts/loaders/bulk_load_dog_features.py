"""bulk_load_dog_features.py — pre-load dog-relevant OSM features per state.

Runs three queries per state into public.osm_features:
  1. leisure=dog_park              (dedicated off-leash zones)
  2. leisure=park   dog=yes|leashed|unleashed
  3. natural=beach  dog=yes|leashed|unleashed

Modeled on scripts/one_off/fetch_osm_dog_features_ca.py — same Overpass
query shapes (with ISO3166-2 admin-area filtering, not bbox), same
dog_status derivation, same priority-ordered dedup. Generalized over
states.

Idempotent + resumable. Skip rule: a (osm_dog_features, <state>) row
in public.external_source_status with status='ok' → skip the state.

Usage:
  python scripts/one_off/bulk_load_dog_features.py
  python scripts/one_off/bulk_load_dog_features.py --priority 1
  python scripts/one_off/bulk_load_dog_features.py --states ME,FL
  python scripts/one_off/bulk_load_dog_features.py --dry-run
"""
from __future__ import annotations
import argparse, json, os, sys, time, urllib.parse
from pathlib import Path

import httpx
import psycopg2, psycopg2.extras
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

sys.path.insert(0, str(ROOT))
from scripts.common.ssl_compat import get_ssl_context  # noqa: E402

# httpx defaults to certifi for cert verification, but certifi's bundle
# doesn't recognize the Overpass API cert chain on Python 3.14 Windows.
# Use Python's default cert store (more permissive) with VERIFY_X509_STRICT
# relaxed. Diagnosed 2026-05-22 LATE in DE virgin-state pipeline test.
_HTTPX_SSL = get_ssl_context()

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_DELAY_S = 30  # be polite — same delay as the CA fetcher

# Same priority-1 state list as bulk_load_overpass.py / bulk_load_amenities.py.
PRIORITY_1 = [
    'AK', 'ME', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'DE', 'MD',
    'VA', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'LA', 'TX', 'HI',
    'MI', 'MN', 'WI', 'IL', 'IN', 'OH', 'PA',
]


def overpass_query(state: str, kind: str) -> str:
    """Build an Overpass QL query scoped to US-<state> for one feature kind.

    kind ∈ {'dog_park', 'dog_friendly_park', 'dog_friendly_beach'}.
    """
    if kind == 'dog_park':
        body = f"""
  node["leisure"="dog_park"](area.s);
  way["leisure"="dog_park"](area.s);
  relation["leisure"="dog_park"](area.s);"""
        timeout = 120
    elif kind == 'dog_friendly_park':
        body = f"""
  node["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.s);
  way["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.s);
  relation["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.s);"""
        timeout = 120
    elif kind == 'dog_friendly_beach':
        body = f"""
  node["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.s);
  way["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.s);
  relation["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.s);"""
        timeout = 120
    else:
        raise ValueError(f"unknown kind: {kind}")
    return f"""
[out:json][timeout:{timeout}];
area["ISO3166-2"="US-{state}"]->.s;
({body}
);
out center tags;
""".strip()


def fetch_overpass(query: str) -> dict:
    r = httpx.post(OVERPASS_URL, data={"data": query}, timeout=180.0,
                   verify=_HTTPX_SSL,
                   headers={"User-Agent": "dog-beach-scout/1.0 (bulk dog-features)"})
    r.raise_for_status()
    return r.json()


def derive_dog_status(feature_type: str, tags: dict) -> str | None:
    """leisure=dog_park is implicitly off-leash; otherwise read tags.dog."""
    raw = (tags.get("dog") or "").strip().lower() or None
    if feature_type == "dog_park":
        return raw if raw in ("yes", "leashed", "unleashed") else "unleashed"
    return raw if raw in ("yes", "leashed", "unleashed") else None


def to_row(el: dict, feature_type: str) -> dict | None:
    osm_type = el.get("type")
    osm_id = el.get("id")
    if not osm_type or osm_id is None:
        return None
    lat = el.get("lat")
    lon = el.get("lon")
    if lat is None or lon is None:
        c = el.get("center") or {}
        lat, lon = c.get("lat"), c.get("lon")
    if lat is None or lon is None:
        return None
    tags = el.get("tags") or {}
    return {
        "osm_type":      osm_type,
        "osm_id":        osm_id,
        "feature_type":  feature_type,
        "dog_status":    derive_dog_status(feature_type, tags),
        "name":          tags.get("name"),
        "latitude":      lat,
        "longitude":     lon,
        "geom":          f"SRID=4326;POINT({lon} {lat})",
        "tags":          psycopg2.extras.Json(tags),
        "fee":           tags.get("fee"),
        "fence":         tags.get("fence"),
        "surface":       tags.get("surface"),
        "opening_hours": tags.get("opening_hours"),
        "website":       tags.get("website") or tags.get("contact:website"),
        "city":          tags.get("addr:city"),
    }


UPSERT_SQL = """
insert into public.osm_features
  (osm_type, osm_id, feature_type, dog_status, name,
   latitude, longitude, geom, tags,
   fee, fence, surface, opening_hours, website, city)
values
  (%(osm_type)s, %(osm_id)s, %(feature_type)s, %(dog_status)s, %(name)s,
   %(latitude)s, %(longitude)s, ST_GeogFromText(%(geom)s)::geometry, %(tags)s,
   %(fee)s, %(fence)s, %(surface)s, %(opening_hours)s, %(website)s, %(city)s)
on conflict (osm_type, osm_id) do update set
  feature_type  = excluded.feature_type,
  dog_status    = excluded.dog_status,
  name          = excluded.name,
  latitude      = excluded.latitude,
  longitude     = excluded.longitude,
  geom          = excluded.geom,
  tags          = excluded.tags,
  fee           = excluded.fee,
  fence         = excluded.fence,
  surface       = excluded.surface,
  opening_hours = excluded.opening_hours,
  website       = excluded.website,
  city          = excluded.city,
  loaded_at     = now()
"""


def upsert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    return len(rows)


def already_loaded(state: str) -> bool:
    """True if external_source_status has osm_dog_features=<state>='ok'."""
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""select status from public.external_source_status
                             where source='osm_dog_features' and state=%s""", (state,))
            r = cur.fetchone()
            return bool(r and r[0] == 'ok')


def log(msg: str):
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def safe_status(*a, **kw):
    try:
        record_status(*a, **kw)
    except Exception as se:
        log(f'    (status write failed: {se})')


def load_one_state(state: str) -> tuple[int, int, int]:
    """Returns (rows_upserted, named_count, by_kind_summary)."""
    seen: set[tuple[str, int]] = set()
    all_rows: list[dict] = []
    by_kind: dict[str, int] = {}

    kinds = ['dog_park', 'dog_friendly_park', 'dog_friendly_beach']
    for idx, kind in enumerate(kinds):
        if idx > 0:
            time.sleep(OVERPASS_DELAY_S)
        q = overpass_query(state, kind)
        t0 = time.monotonic()
        payload = fetch_overpass(q)
        elapsed = time.monotonic() - t0
        elements = payload.get("elements", [])
        new_rows = 0
        for el in elements:
            r = to_row(el, kind)
            if r is None:
                continue
            key = (r['osm_type'], r['osm_id'])
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(r)
            new_rows += 1
        by_kind[kind] = new_rows
        log(f'  {state}/{kind}: {len(elements)} elements, {new_rows} new ({elapsed:.0f}s)')

    written = upsert_rows(all_rows)
    return written, len(all_rows), by_kind


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--priority', type=int, choices=[1], default=1)
    ap.add_argument('--states', help='Comma-sep override')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    states = ([s.strip().upper() for s in args.states.split(',') if s.strip()]
              if args.states else PRIORITY_1)

    log(f'Plan: load OSM dog features for {len(states)} states')
    log(f'States: {", ".join(states)}')
    log(f'Per state: 3 Overpass queries (dog_park, dog_friendly_park, dog_friendly_beach)')
    log(f'Sleep {OVERPASS_DELAY_S}s between Overpass calls')

    if args.dry_run:
        for st in states:
            tag = 'SKIP (status=ok)' if already_loaded(st) else 'WOULD LOAD'
            log(f'  {st}: {tag}')
        return

    loaded, skipped, failed = 0, 0, 0
    for st in states:
        if already_loaded(st):
            log(f'{st}: already loaded; skipping')
            skipped += 1
            continue

        log(f'{st}: fetching dog features...')
        t0 = time.monotonic()
        try:
            written, _, by_kind = load_one_state(st)
            elapsed = time.monotonic() - t0
            summary = ', '.join(f'{k}={v}' for k, v in by_kind.items())
            log(f'  {st}: upserted {written} rows in {elapsed:.0f}s ({summary})')
            safe_status(PG, 'osm_dog_features', st, 'ok', written,
                        f'{summary}; in {elapsed:.0f}s')
            loaded += 1
        except Exception as e:
            log(f'  {st}: FAILED — {e}')
            safe_status(PG, 'osm_dog_features', st, 'failed', None, str(e)[:500])
            failed += 1

        # Inter-state pause (in addition to inter-query within load_one_state).
        time.sleep(OVERPASS_DELAY_S)

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
