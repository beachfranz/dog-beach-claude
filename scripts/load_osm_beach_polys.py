"""load_osm_beach_polys.py — load OSM natural=beach polygons for California.

Pulls every way + relation tagged natural=beach within California from the
Overpass API, assembles geometry, and loads into public.osm_beach_polys
as a PostGIS table. Single-purpose; runs once or on refresh.

Why this exists: our existing osm_features table loaded beach features as
POINT centroids only — zero polygons. We need actual beach footprints for
the consumer maps so beach extents render correctly (vs uniform-circle
markers losing the "100m cove vs 5km strip" signal). See spec memo
project_zone_rules_design_locked.md catalog-vs-zones rule for how this
relates to gold-fid attribution.

Usage:
  python scripts/load_osm_beach_polys.py
  python scripts/load_osm_beach_polys.py --dry-run         # don't write
  python scripts/load_osm_beach_polys.py --bbox SoCal      # quick subset

Idempotent — drops + recreates the table on each run. ~2-5K features
expected for CA. ~1-3 minutes runtime depending on Overpass load.
"""

from __future__ import annotations
import argparse
import io
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_MIRROR = "https://overpass.kumi.systems/api/interpreter"

# California bbox (rough): south=32.5, west=-124.5, north=42.0, east=-114.1
BBOXES = {
    "CA":     "32.5,-124.5,42.0,-114.1",
    "SoCal":  "32.5,-121.5,35.0,-117.0",   # quick test slice
    "Marin":  "37.8,-123.0,38.3,-122.4",   # tiny test
}


def overpass_query(bbox: str) -> str:
    """Query for all natural=beach ways and relations within bbox.

    `out geom` includes inline coordinates per node so we don't need a
    second roundtrip to resolve node IDs.
    """
    return f"""
[out:json][timeout:300];
(
  way["natural"="beach"]({bbox});
  relation["natural"="beach"]({bbox});
);
out geom;
""".strip()


def fetch_overpass(query: str, url: str = OVERPASS_URL) -> dict:
    print(f"  POST {url}  ({len(query)} chars query)", flush=True)
    data = urllib.parse.urlencode({"data": query}).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                 headers={"User-Agent": "dog-beach-scout/1.0"})
    with urllib.request.urlopen(req, timeout=600) as resp:
        return json.loads(resp.read().decode("utf-8"))


def way_to_geojson_polygon(way: dict) -> dict | None:
    """An OSM way with closed geometry becomes a Polygon. Open ways are skipped."""
    geom = way.get("geometry") or []
    if len(geom) < 4:  # need at least 3 distinct points + closing
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geom]
    if coords[0] != coords[-1]:
        coords.append(coords[0])  # close the ring if OSM didn't
    return {"type": "Polygon", "coordinates": [coords]}


def relation_to_geojson_multipolygon(rel: dict) -> dict | None:
    """OSM multipolygon relation -> GeoJSON MultiPolygon.

    Strategy: collect 'outer' member ways, treat each as a separate ring;
    'inner' members become holes in the closest containing outer (we use
    the simple heuristic of attaching all inners to the largest outer).
    For v1 we ignore complex multi-ring relations and just stitch outers.
    Ways from members come pre-resolved with `out geom`.
    """
    outers = []
    inners = []
    for m in rel.get("members") or []:
        if m.get("type") != "way":
            continue
        coords = [[pt["lon"], pt["lat"]] for pt in (m.get("geometry") or [])]
        if len(coords) < 4:
            continue
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        if m.get("role") == "inner":
            inners.append(coords)
        else:
            outers.append(coords)
    if not outers:
        return None
    polys = [[outer] for outer in outers]
    # Naive: all inners attached to first outer. Most beach relations are
    # single-outer anyway; complex ones can be revisited later.
    if inners and polys:
        polys[0].extend(inners)
    return {"type": "MultiPolygon", "coordinates": polys}


def features_from_overpass(raw: dict) -> list[dict]:
    out = []
    for el in raw.get("elements") or []:
        t = el.get("type")
        if t == "way":
            geom = way_to_geojson_polygon(el)
        elif t == "relation":
            geom = relation_to_geojson_multipolygon(el)
        else:
            continue
        if not geom:
            continue
        tags = el.get("tags") or {}
        out.append({
            "osm_id": el["id"],
            "osm_type": t,
            "name": tags.get("name"),
            "name_short": tags.get("short_name"),
            "tags": tags,
            "geom_geojson": geom,
        })
    return out


def create_and_load(features: list[dict], dry_run: bool = False):
    if dry_run:
        print(f"  --dry-run: not writing. {len(features)} features ready.")
        return

    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                drop table if exists public.osm_beach_polys;
                create table public.osm_beach_polys (
                  id          bigserial primary key,
                  osm_id      bigint not null,
                  osm_type    text   not null check (osm_type in ('way','relation')),
                  name        text,
                  name_short  text,
                  tags        jsonb not null,
                  geom        geometry(Geometry, 4326) not null,
                  area_m2     double precision,
                  loaded_at   timestamptz default now(),
                  unique (osm_type, osm_id)
                );
                create index osm_beach_polys_geom_gix on public.osm_beach_polys using gist (geom);
                create index osm_beach_polys_name_idx on public.osm_beach_polys (lower(name));
            """)
            conn.commit()

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                insert into public.osm_beach_polys
                  (osm_id, osm_type, name, name_short, tags, geom, area_m2)
                values (
                  %(osm_id)s, %(osm_type)s, %(name)s, %(name_short)s,
                  %(tags)s::jsonb,
                  st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326)),
                  st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326))::geography)
                )
                on conflict (osm_type, osm_id) do update
                  set name = excluded.name, tags = excluded.tags,
                      geom = excluded.geom, area_m2 = excluded.area_m2
            """, [{
                "osm_id": f["osm_id"],
                "osm_type": f["osm_type"],
                "name": f["name"],
                "name_short": f["name_short"],
                "tags": json.dumps(f["tags"]),
                "geom": json.dumps(f["geom_geojson"]),
            } for f in features], page_size=100)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("select count(*), count(name), avg(area_m2)::int from public.osm_beach_polys")
            n, n_named, avg_area = cur.fetchone()
            print(f"  loaded: {n} rows ({n_named} named) avg area {avg_area:,} m2")


def main():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--bbox", default="CA", help="CA | SoCal | Marin")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--mirror", action="store_true",
                    help="Use overpass.kumi.systems mirror (sometimes faster)")
    args = ap.parse_args()

    bbox = BBOXES.get(args.bbox, args.bbox)
    print(f"Fetching natural=beach for bbox={bbox}")
    url = OVERPASS_MIRROR if args.mirror else OVERPASS_URL
    raw = fetch_overpass(overpass_query(bbox), url=url)
    print(f"  Overpass returned {len(raw.get('elements') or [])} elements")

    features = features_from_overpass(raw)
    n_way = sum(1 for f in features if f["osm_type"] == "way")
    n_rel = sum(1 for f in features if f["osm_type"] == "relation")
    n_named = sum(1 for f in features if f["name"])
    print(f"  built {len(features)} polygons ({n_way} ways, {n_rel} relations, {n_named} named)")

    if not features:
        print("nothing to load")
        return 0

    create_and_load(features, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
