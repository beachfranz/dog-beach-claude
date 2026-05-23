"""prototype_wsprc_title_match.py — name-match WSPRC fanout photos
to a specific beach (filename / vision-desc heading) OR park centroid
as fallback.

REPORT-ONLY: prints what WOULD happen, no DB writes.

Approach per unique image_url:
  1. Build the photo's "title text" = URL-decoded filename + first 200
     chars of vision.description.
  2. Find the candidate beaches: those currently attributed to this URL
     (the fanout set).
  3. If exactly one of their names appears verbatim in the title text,
     mark as UNIQUE-MATCH → would attribute to that beach only.
  4. Else if the park has a known centroid (via pad_us_units polygon),
     mark as PARK-CENTROID → set distance_m = each beach's distance to
     the park centroid; photo stays fanned out but distance is meaningful.
  5. Else: NO-CHANGE.

Cost: zero (no LLM, pure SQL + string ops). Runtime: ~1 min.

Usage:
  python scripts/prototype_wsprc_title_match.py
"""
from __future__ import annotations
import sys, re, urllib.parse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from collections import defaultdict

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect


def normalize(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    s = urllib.parse.unquote(s or '').lower()
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def name_to_tokens(name: str) -> list[str]:
    """Drop the generic suffixes; return remaining tokens for matching."""
    n = normalize(name)
    # strip common beach-name suffixes that don't disambiguate
    for suf in (' state park beach', ' state park', ' beach', ' park', ' state recreation site',
                ' state recreation area', ' wayside', ' viewpoint'):
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
            break
    return [t for t in n.split() if len(t) >= 3]


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    # 1. Per WSPRC unique image_url, get all (beach_fid, beach_name, park, filename, vision)
    cur.execute("""
      WITH urls AS (
        SELECT image_url
          FROM beach_photos
         WHERE source='wsprc' AND image_url IS NOT NULL
         GROUP BY image_url
      )
      SELECT bp.image_url,
             bp.source_meta->>'park_name' AS park,
             bp.source_meta->>'filename'  AS filename,
             LEFT(bp.source_meta->'vision'->>'description', 250) AS vd,
             bp.arena_group_id AS fid,
             g.name AS beach_name,
             ST_Y(g.geom::geometry) AS lat,
             ST_X(g.geom::geometry) AS lon
        FROM beach_photos bp
        JOIN beaches_gold g ON g.fid = bp.arena_group_id
       WHERE bp.source='wsprc' AND bp.image_url IN (SELECT image_url FROM urls)
       ORDER BY bp.image_url, bp.arena_group_id
    """)
    rows = cur.fetchall()
    by_url: dict[str, list[dict]] = defaultdict(list)
    for url, park, fname, vd, fid, name, lat, lon in rows:
        by_url[url].append({
            'park': park, 'filename': fname or '', 'vd': vd or '',
            'fid': fid, 'name': name, 'lat': lat, 'lon': lon,
        })

    # 2. Park polygon centroid lookup (best-effort via pad_us_units mng_type=STAT/SPR + name match)
    cur.execute("""
      SELECT unit_name, ST_Y(ST_Centroid(geom)::geometry) AS lat,
                       ST_X(ST_Centroid(geom)::geometry) AS lon
        FROM pad_us_units
       WHERE state='WA' AND mng_type='STAT' AND mng_name='SPR'
    """)
    park_centroids: dict[str, tuple[float, float]] = {}
    for unit_name, lat, lon in cur.fetchall():
        if lat is None or lon is None: continue
        # Loose key: normalized park name with suffixes stripped
        for k in (normalize(unit_name),
                   normalize(unit_name).replace(' state park','').replace(' state recreation site','').strip()):
            if k and k not in park_centroids:
                park_centroids[k] = (float(lat), float(lon))

    def park_centroid_for(park_name: str):
        if not park_name: return None
        n = normalize(park_name)
        if n in park_centroids: return park_centroids[n]
        # fallback: substring
        for k, c in park_centroids.items():
            if n in k or k in n: return c
        return None

    def haversine_m(a, b):
        import math
        lat1, lon1 = a; lat2, lon2 = b
        R = 6371000.0
        dlat = math.radians(lat2 - lat1); dlon = math.radians(lon2 - lon1)
        h = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
        return 2 * R * math.asin(math.sqrt(h))

    # 3. Classify each unique URL
    n_unique_attr   = 0  # already 1 beach today (no fanout to disambig)
    n_unique_match  = 0  # title-name-match resolves to ONE beach
    n_centroid      = 0  # multi-beach but park centroid found
    n_no_change     = 0  # multi-beach, no centroid, no name match
    samples = {'unique_match': [], 'centroid': [], 'no_change': []}

    for url, beaches in by_url.items():
        if len(beaches) == 1:
            n_unique_attr += 1
            continue
        # Build title text from any beach's record (filename + vd are the same for all in this fanout)
        b0 = beaches[0]
        title = normalize(b0['filename'] + ' ' + b0['vd'])
        # Try name-match against each candidate beach's tokens
        matches = []
        for b in beaches:
            toks = name_to_tokens(b['name'])
            # require at least one DISTINCTIVE token (length >= 4) to appear
            distinct = [t for t in toks if len(t) >= 4]
            for t in distinct:
                if re.search(r'\b' + re.escape(t) + r'\b', title):
                    matches.append(b)
                    break
        if len(matches) == 1:
            n_unique_match += 1
            if len(samples['unique_match']) < 5:
                samples['unique_match'].append((url, matches[0]['name'], b0['park'], title[:120]))
            continue
        # Fall back to park centroid
        centroid = park_centroid_for(b0['park'])
        if centroid:
            n_centroid += 1
            if len(samples['centroid']) < 5:
                dists = [(b['name'], int(haversine_m((b['lat'], b['lon']), centroid))) for b in beaches]
                samples['centroid'].append((url, b0['park'], dists))
            continue
        n_no_change += 1
        if len(samples['no_change']) < 5:
            samples['no_change'].append((url, b0['park'], [b['name'] for b in beaches]))

    total = len(by_url)
    print(f'=== WSPRC title-name-match prototype (report only) ===')
    print(f'  unique image_urls:                 {total}')
    print(f'  already single-beach (no fanout):  {n_unique_attr}  ({n_unique_attr/total:.0%})')
    print(f'  fanout, UNIQUE-MATCH on title:     {n_unique_match}  ({n_unique_match/total:.0%}) → would attribute to ONE beach')
    print(f'  fanout, PARK-CENTROID fallback:    {n_centroid}  ({n_centroid/total:.0%}) → distance_m populated per beach')
    print(f'  fanout, NO-CHANGE:                 {n_no_change}  ({n_no_change/total:.0%}) → no improvement signal')
    print(f'  park-centroid coverage:            {len(park_centroids)} unique WA STAT/SPR centroids in pad_us_units')

    if samples['unique_match']:
        print()
        print('=== Sample UNIQUE-MATCH cases ===')
        for url, name, park, title in samples['unique_match']:
            print(f'  → {name} (park={park})')
            print(f'    title: {title[:90]}')
            print(f'    url:   {url[-60:]}')

    if samples['centroid']:
        print()
        print('=== Sample PARK-CENTROID cases (distance_m per fanout beach) ===')
        for url, park, dists in samples['centroid']:
            print(f'  park={park}  url=...{url[-60:]}')
            for n, d in sorted(dists, key=lambda x: x[1]):
                print(f'    {d:>5}m  {n}')

    if samples['no_change']:
        print()
        print('=== Sample NO-CHANGE cases (no centroid available) ===')
        for url, park, names in samples['no_change']:
            print(f'  park={park}  → fanout: {names}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
