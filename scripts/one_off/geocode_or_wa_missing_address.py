"""Reverse-geocode the OR/WA active beaches that don't have an address yet.
Uses Google Maps Geocoding API. ~$5/1000 calls.

For each missing-address beach: hit Google with lat/lng, parse the
formatted_address + components into address/_street/_city/_state/_zip,
write back to beaches_gold.

Usage:
  python scripts/one_off/geocode_or_wa_missing_address.py [--limit N] [--dry-run]
"""
from __future__ import annotations
import argparse, os, time, urllib.parse
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

API_KEY = os.environ['GOOGLE_MAPS_API_KEY']
URL = 'https://maps.googleapis.com/maps/api/geocode/json'


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def parse_components(comps: list[dict]) -> dict:
    out = {'street_number': None, 'route': None, 'city': None, 'state': None, 'zip': None}
    for c in comps:
        types = c.get('types') or []
        if 'street_number' in types: out['street_number'] = c.get('long_name')
        elif 'route' in types: out['route'] = c.get('short_name')
        elif 'locality' in types: out['city'] = c.get('long_name')
        elif 'administrative_area_level_3' in types and not out['city']: out['city'] = c.get('long_name')
        elif 'administrative_area_level_1' in types: out['state'] = c.get('short_name')
        elif 'postal_code' in types: out['zip'] = c.get('long_name')
    street_parts = [x for x in (out.get('street_number'), out.get('route')) if x]
    out['street'] = ' '.join(street_parts) if street_parts else None
    return out


def reverse_geocode(lat: float, lng: float) -> dict | None:
    try:
        r = httpx.get(URL, params={'latlng': f'{lat},{lng}', 'key': API_KEY,
                                    'result_type': 'street_address|premise|point_of_interest|park'},
                      timeout=30.0)
        r.raise_for_status()
        d = r.json()
        if d.get('status') != 'OK' or not d.get('results'):
            # Try broader
            r = httpx.get(URL, params={'latlng': f'{lat},{lng}', 'key': API_KEY},
                          timeout=30.0)
            d = r.json()
        if d.get('status') != 'OK' or not d.get('results'):
            return None
        top = d['results'][0]
        comps = parse_components(top.get('address_components') or [])
        return {
            'formatted': top.get('formatted_address'),
            'street':    comps['street'],
            'city':      comps['city'],
            'state':     comps['state'],
            'zip':       comps['zip'],
        }
    except Exception as e:
        log(f'  geocode error: {e}')
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--delay', type=float, default=0.05)
    args = ap.parse_args()

    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
              select fid, name, state, lat, lon
                from public.beaches_gold
               where is_active and state in ('OR','WA') and address is null
               order by state, fid
            """ + (f' limit {args.limit}' if args.limit else ''))
            targets = cur.fetchall()

    log(f'{len(targets)} OR/WA beaches without address')
    if args.dry_run:
        for t in targets[:5]:
            print(f"  fid={t['fid']} {t['name']!r:<40} {t['state']} ({t['lat']:.4f},{t['lon']:.4f})")
        return

    ok, fail = 0, 0
    for i, t in enumerate(targets, 1):
        rg = reverse_geocode(t['lat'], t['lon'])
        if rg is None:
            fail += 1
        else:
            with psycopg2.connect(**PG) as c:
                c.autocommit = True
                with c.cursor() as cur:
                    cur.execute("""
                      update public.beaches_gold
                         set address        = coalesce(address,        %s),
                             address_street = coalesce(address_street, %s),
                             address_city   = coalesce(address_city,   %s),
                             address_state  = coalesce(address_state,  %s),
                             address_zip    = coalesce(address_zip,    %s)
                       where fid = %s
                    """, (rg['formatted'], rg['street'], rg['city'], rg['state'], rg['zip'], t['fid']))
            ok += 1
        if i % 25 == 0 or i == len(targets):
            log(f'  [{i}/{len(targets)}] ok={ok} fail={fail}')
        time.sleep(args.delay)

    log(f'Done. ok={ok} fail={fail}')


if __name__ == '__main__':
    main()
