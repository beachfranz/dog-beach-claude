"""wiki_match_spike.py — coverage spike for Wikipedia/Wikidata matching.

For a county's scoreable beaches, try two matching strategies:
  A. Wikidata SPARQL — find any entity with instance-of=beach (and
     subclasses) within 5km of the beach point. Returns Wikidata QID +
     linked en.wikipedia article if any.
  B. Wikipedia text search — search wikipedia by beach name, fetch
     candidate's coords, accept if within 5km.

Reports per-beach: A-hit, B-hit, agreement (both → same article?),
and any disagreements. Free APIs, no key.

Usage:
  python scripts/one_off/wiki_match_spike.py --county "Santa Cruz" --state CA
"""
from __future__ import annotations
import argparse, json, math, os, time, urllib.parse, urllib.request, sys
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

UA = 'dog-beach-claude wiki-spike (franz@franzfunk.com)'
WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'
WIKI_API = 'https://en.wikipedia.org/w/api.php'
WIKI_REST = 'https://en.wikipedia.org/api/rest_v1/page/summary'

RADIUS_KM = 5.0


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))


def http_json(url, params=None):
    full = url + ('?' + urllib.parse.urlencode(params) if params else '')
    req = urllib.request.Request(full, headers={'User-Agent': UA, 'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


# ─── Strategy A: Wikidata SPARQL ───────────────────────────────────────
def wikidata_nearby_beaches(lat: float, lon: float, radius_km: float = RADIUS_KM):
    """Return list of {qid, label, article, lat, lon, dist_km} for any
    entity that is a beach (Q40080) or subclass, within radius_km."""
    query = f"""
SELECT ?item ?itemLabel ?coord ?article WHERE {{
  SERVICE wikibase:around {{
    ?item wdt:P625 ?coord .
    bd:serviceParam wikibase:center "Point({lon} {lat})"^^geo:wktLiteral .
    bd:serviceParam wikibase:radius "{radius_km}" .
  }}
  ?item wdt:P31/wdt:P279* wd:Q40080 .
  OPTIONAL {{
    ?article schema:about ?item ;
             schema:isPartOf <https://en.wikipedia.org/> .
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
LIMIT 25
"""
    try:
        data = http_json(WIKIDATA_SPARQL, {'query': query, 'format': 'json'})
    except Exception as e:
        return [], f'sparql_error: {e}'
    out = []
    for b in data['results']['bindings']:
        qid = b['item']['value'].split('/')[-1]
        label = b.get('itemLabel', {}).get('value', '')
        article = b.get('article', {}).get('value')
        coord = b['coord']['value']  # "Point(lon lat)"
        m = coord.replace('Point(', '').replace(')', '').split()
        clon, clat = float(m[0]), float(m[1])
        out.append({
            'qid': qid, 'label': label, 'article': article,
            'lat': clat, 'lon': clon,
            'dist_km': haversine_km(lat, lon, clat, clon),
        })
    out.sort(key=lambda x: x['dist_km'])
    return out, None


# ─── Strategy B: Wikipedia text search + coord verify ──────────────────
def wikipedia_search(name: str):
    """Return list of {title, pageid} candidates from text search."""
    try:
        data = http_json(WIKI_API, {
            'action': 'query', 'list': 'search', 'srsearch': name,
            'srlimit': 5, 'format': 'json',
        })
    except Exception:
        return []
    return [{'title': h['title'], 'pageid': h['pageid']}
            for h in data.get('query', {}).get('search', [])]


def wikipedia_summary(title: str):
    """Return {title, summary, lat, lon, url} or None."""
    enc = urllib.parse.quote(title)
    try:
        data = http_json(f'{WIKI_REST}/{enc}')
    except Exception:
        return None
    coords = data.get('coordinates') or {}
    return {
        'title': data.get('title', title),
        'summary': data.get('extract', ''),
        'lat': coords.get('lat'),
        'lon': coords.get('lon'),
        'url': data.get('content_urls', {}).get('desktop', {}).get('page'),
    }


def wikipedia_match_by_name(name: str, lat: float, lon: float):
    """Search Wikipedia by name; for each candidate, fetch summary +
    coords; accept first candidate within RADIUS_KM of the beach point.
    Returns the accepted candidate (with dist_km) or None."""
    hits = wikipedia_search(name)
    for h in hits:
        s = wikipedia_summary(h['title'])
        if not s or s['lat'] is None or s['lon'] is None:
            continue
        d = haversine_km(lat, lon, s['lat'], s['lon'])
        if d <= RADIUS_KM:
            return {**s, 'dist_km': d, 'pageid': h['pageid']}
    return None


# ─── Main ──────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--state', default='CA')
    ap.add_argument('--county', required=True, help='e.g. "Santa Cruz"')
    ap.add_argument('--limit', type=int, default=50)
    args = ap.parse_args()

    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""select fid, coalesce(display_name_override, name), lat, lon
                       from public.beaches_gold
                       where state=%s and county_name=%s and is_active and is_scoreable
                       order by name limit %s""", (args.state, args.county, args.limit))
        beaches = cur.fetchall()

    print(f'{len(beaches)} scoreable beaches in {args.county} County, {args.state}.')
    print(f'Strategy A: Wikidata SPARQL beach-near-point ({RADIUS_KM}km)')
    print(f'Strategy B: Wikipedia name search + coord verify ({RADIUS_KM}km)')
    print()

    results = []
    a_hits = b_hits = both = either = 0
    agree = disagree = 0

    for fid, name, lat, lon in beaches:
        a_list, a_err = wikidata_nearby_beaches(lat, lon)
        a_top = a_list[0] if a_list else None
        time.sleep(0.5)  # polite to Wikidata SPARQL
        b_top = wikipedia_match_by_name(name, lat, lon)
        time.sleep(0.5)

        a_url = (a_top or {}).get('article')
        b_url = (b_top or {}).get('url')
        a_present = bool(a_url)
        b_present = bool(b_url)
        if a_present: a_hits += 1
        if b_present: b_hits += 1
        if a_present and b_present:
            both += 1
            if a_url == b_url:
                agree += 1
            else:
                disagree += 1
        if a_present or b_present: either += 1

        results.append({
            'fid': fid, 'name': name,
            'a_label': (a_top or {}).get('label'),
            'a_article': a_url,
            'a_dist': (a_top or {}).get('dist_km'),
            'b_title': (b_top or {}).get('title'),
            'b_article': b_url,
            'b_dist': (b_top or {}).get('dist_km'),
        })
        a_str = f"A:{(a_top or {}).get('label', '—'):<35}" if a_top else f"A:{'—':<35}"
        b_str = f"B:{(b_top or {}).get('title', '—'):<35}" if b_top else f"B:{'—':<35}"
        print(f"  {fid:<6} {name[:30]:<30}  {a_str}  {b_str}")

    n = len(beaches) or 1
    print()
    print(f'COVERAGE:')
    print(f'  Strategy A (Wikidata):  {a_hits}/{n} = {100*a_hits/n:.0f}%')
    print(f'  Strategy B (Wiki name): {b_hits}/{n} = {100*b_hits/n:.0f}%')
    print(f'  Either:                 {either}/{n} = {100*either/n:.0f}%')
    print(f'  Both:                   {both}/{n}  (agree {agree}, disagree {disagree})')

    if disagree:
        print('\nDISAGREEMENTS:')
        for r in results:
            if r['a_article'] and r['b_article'] and r['a_article'] != r['b_article']:
                print(f"  fid {r['fid']} {r['name']}")
                print(f"     A: {r['a_label']}  →  {r['a_article']}")
                print(f"     B: {r['b_title']}  →  {r['b_article']}")


if __name__ == '__main__':
    main()
