"""Dog-park geom backfill: re-fetch geometry from OSM Overpass for
osm_landing dog-park rows with NULL geom_full.

Same pattern as backfill_osm_landing_geom.py (note/description cohort) —
just targets dog parks instead. As of 2026-05-11 there are ~726 such rows
out of 1,691 total dog parks (43% missing geom).

Use case: better polygon containment for "is beach inside dog park?" plus
accurate map rendering. Currently we only have point lat/lng for most.

Idempotent — re-run is safe. Skips rows that get hydrated mid-run.
"""
import os, json, urllib.parse, urllib.request, time, sys
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

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
UA = "DogBeachScout/1.0 (franz@franzfunk.com)"
BATCH = 80
RATE_SLEEP = 8.0


def fetch_targets():
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""
          select type, id from public.osm_landing
           where is_active and geom_full is null
             and (tags->>'leisure' in ('dog_park','dog_run')
                  or tags->>'amenity' = 'dog_park'
                  or tags->>'dog' = 'designated')
        """)
        return cur.fetchall()


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def overpass_fetch(kind: str, ids: list[int]) -> list[dict]:
    id_csv = ",".join(str(i) for i in ids)
    q = f"[out:json][timeout:60];({kind}(id:{id_csv}););out geom tags;"
    data = urllib.parse.urlencode({"data": q}).encode()
    req = urllib.request.Request(OVERPASS_URL, method="POST",
        data=data, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r:
        resp = json.loads(r.read())
    return resp.get("elements", [])


def element_to_geojson(e: dict):
    t = e.get("type")
    if t == "node":
        if e.get("lat") is None or e.get("lon") is None: return None
        return {"type": "Point", "coordinates": [e["lon"], e["lat"]]}
    if t == "way":
        geom = e.get("geometry") or []
        if len(geom) < 2: return None
        coords = [[p["lon"], p["lat"]] for p in geom]
        if len(coords) >= 4 and coords[0] == coords[-1]:
            return {"type": "Polygon", "coordinates": [coords]}
        return {"type": "LineString", "coordinates": coords}
    if t == "relation":
        outers = []
        for m in e.get("members") or []:
            if m.get("role") != "outer": continue
            g = m.get("geometry") or []
            if len(g) < 2: continue
            ring = [[p["lon"], p["lat"]] for p in g]
            if len(ring) >= 4 and ring[0] == ring[-1]:
                outers.append([ring])
        if not outers: return None
        if len(outers) == 1:
            return {"type": "Polygon", "coordinates": outers[0]}
        return {"type": "MultiPolygon", "coordinates": outers}
    return None


def upsert_geom(rows):
    if not rows: return 0
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("set statement_timeout = '60s'")
        n = 0
        for kind, oid, gj in rows:
            try:
                cur.execute("""
                  update public.osm_landing
                     set geom_full = st_setsrid(st_geomfromgeojson(%s::text), 4326),
                         geometry = %s::jsonb
                   where type = %s and id = %s
                """, (json.dumps(gj), json.dumps(gj), kind, oid))
                n += cur.rowcount
            except Exception as e:
                print(f"  upsert err {kind}/{oid}: {str(e)[:120]}", file=sys.stderr)
        return n


def main():
    targets = fetch_targets()
    print(f"targets: {len(targets)} dog-park rows with NULL geom_full", flush=True)
    if not targets: return

    by_kind = {"way": [], "node": [], "relation": []}
    for kind, oid in targets:
        if kind in by_kind: by_kind[kind].append(oid)
        else: print(f"  unexpected type: {kind}/{oid}", file=sys.stderr)

    total_hydrated = 0
    total_fetched = 0
    t0 = time.time()

    for kind in ("way", "node", "relation"):
        ids = by_kind[kind]
        if not ids: continue
        print(f"\n=== {kind}: {len(ids)} ids in {(len(ids)+BATCH-1)//BATCH} batches ===", flush=True)
        for i, batch in enumerate(chunks(ids, BATCH), 1):
            t = time.time()
            try:
                elems = overpass_fetch(kind, batch)
            except Exception as e:
                print(f"  [batch {i}] overpass err: {str(e)[:160]}", flush=True)
                time.sleep(RATE_SLEEP); continue
            total_fetched += len(elems)
            rows = []
            for e in elems:
                gj = element_to_geojson(e)
                if gj: rows.append((kind, e["id"], gj))
            hydrated = upsert_geom(rows)
            total_hydrated += hydrated
            print(f"  [batch {i}] requested={len(batch):3d}  returned={len(elems):3d}  "
                  f"hydrated={hydrated:3d}  time={time.time()-t:.1f}s", flush=True)
            time.sleep(RATE_SLEEP)

    print(f"\n=== DONE === hydrated={total_hydrated} of {len(targets)}  wall={time.time()-t0:.1f}s", flush=True)

    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""select count(*) filter (where geom_full is null) still_null,
                              count(*) total
                         from public.osm_landing where is_active
                          and (tags->>'leisure' in ('dog_park','dog_run')
                            or tags->>'amenity' = 'dog_park'
                            or tags->>'dog' = 'designated')""")
        r = cur.fetchone()
        print(f"dog-park cohort: {r[1]-r[0]}/{r[1]} now have geom_full ({r[0]} still null)")


if __name__ == "__main__":
    main()
