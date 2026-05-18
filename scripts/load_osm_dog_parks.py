"""load_osm_dog_parks.py — load OSM leisure=dog_park polygons for the whole US.

Pulls every way + relation tagged leisure=dog_park within the continental
US (plus AK + HI as separate chunks) from the Overpass API and loads into
public.osm_dog_parks as a PostGIS table.

Why: dog parks are (a) a points-of-interest source for filtering PAD-US
polygons to "beach-or-dogpark relevant" (saves 99% of PIP cost), and
(b) a first-class polygon source for codification — city dog-park
ordinances (off-leash hours, breed restrictions, capacity limits) attach
to dog-park polygons in the codify → PIP → resolver pipeline. See
docs/codify_pip_resolver_architecture.md.

Operator is captured as an explicit column (not just inside the tags
jsonb) because the codify pipeline needs to route dog-park rules to the
right governing jurisdiction; operator is the most direct signal.

Usage:
  python scripts/load_osm_dog_parks.py
  python scripts/load_osm_dog_parks.py --dry-run         # don't write
  python scripts/load_osm_dog_parks.py --chunk continental_us  # one chunk
  python scripts/load_osm_dog_parks.py --mirror          # use kumi mirror

Idempotent — drops + recreates the table on each run. ~5-15K features
expected for whole US. ~2-5 minutes runtime depending on Overpass load.
"""

from __future__ import annotations

# Truststore for AV-MITM Windows SSL (same idiom as generate_beach_descriptions.py).
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import argparse
import io
import json
import os
import sys
import time
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

# US bboxes — continental split into smaller chunks (~6° x 8° max) because
# bigger regional queries 504 on Overpass even with regional splits.
# Each chunk targets ~500-1500 features and completes in <60s.
CHUNKS = {
    # West coast
    "wa_or":            "42.0,-125.0,49.5,-116.0",
    "n_california":     "37.0,-125.0,42.0,-119.5",
    "s_california":     "32.0,-121.5,37.0,-114.0",
    # Mountain
    "id_mt":            "42.0,-117.0,49.5,-104.0",
    "ut_nv":            "37.0,-120.0,42.0,-109.0",
    "az_nm":            "31.0,-115.0,37.0,-103.0",
    "wy_co":            "37.0,-112.0,45.5,-102.0",
    # Plains
    "dakotas_ne":       "40.0,-104.0,49.5,-96.0",
    "ks_ok":            "33.5,-103.0,40.0,-94.0",
    "tx_north":         "31.0,-107.0,36.5,-94.0",
    "tx_south":         "25.5,-107.0,31.0,-93.0",
    # Midwest
    "mn_ia_wi":         "42.0,-97.0,49.5,-87.0",
    "mo_ar":            "35.0,-95.0,40.5,-89.0",
    "il_in_mi":         "37.5,-91.0,48.5,-82.0",
    "oh_ky":            "36.5,-85.0,42.0,-80.5",
    # South
    "la_ms_al":         "30.0,-94.0,35.0,-84.5",
    "tn_nc_sc":         "32.0,-91.0,36.7,-75.5",
    "ga":               "30.5,-86.0,35.0,-80.5",
    "fl_n":             "27.0,-87.5,31.5,-79.5",
    "fl_s":             "24.0,-83.0,27.5,-79.8",
    # Northeast / Mid-Atlantic
    "va_wv_md":         "36.5,-83.0,40.5,-74.5",
    "pa_nj_de":         "38.5,-81.0,42.5,-74.0",
    "ny":               "40.4,-80.0,45.1,-71.8",
    "new_england":      "40.9,-74.0,47.5,-66.5",
    # Non-contiguous
    "alaska":           "51.0,-180.0,71.5,-130.0",
    "hawaii":           "18.5,-160.5,22.5,-154.5",
}


def overpass_query(bbox: str) -> str:
    """Query for all leisure=dog_park ways and relations within bbox.

    `out geom` includes inline coordinates per node so we don't need a
    second roundtrip to resolve node IDs.
    """
    return f"""
[out:json][timeout:300];
(
  way["leisure"="dog_park"]({bbox});
  relation["leisure"="dog_park"]({bbox});
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
    geom = way.get("geometry") or []
    if len(geom) < 4:
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geom]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}


def relation_to_geojson_multipolygon(rel: dict) -> dict | None:
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
            "osm_id":   el["id"],
            "osm_type": t,
            "name":     tags.get("name"),
            "operator": tags.get("operator"),
            "operator_type": tags.get("operator:type"),
            "access":   tags.get("access"),  # public / private / permissive / customers
            "tags":     tags,
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
                drop table if exists public.osm_dog_parks;
                create table public.osm_dog_parks (
                  id            bigserial primary key,
                  osm_id        bigint not null,
                  osm_type      text   not null check (osm_type in ('way','relation')),
                  name          text,
                  operator      text,
                  operator_type text,
                  access        text,
                  tags          jsonb not null,
                  geom          geometry(Geometry, 4326) not null,
                  area_m2       double precision,
                  loaded_at     timestamptz default now(),
                  unique (osm_type, osm_id)
                );
                create index osm_dog_parks_geom_gix on public.osm_dog_parks using gist (geom);
                create index osm_dog_parks_name_idx on public.osm_dog_parks (lower(name));
                create index osm_dog_parks_operator_idx on public.osm_dog_parks (lower(operator));
            """)
            conn.commit()

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                insert into public.osm_dog_parks
                  (osm_id, osm_type, name, operator, operator_type, access, tags, geom, area_m2)
                values (
                  %(osm_id)s, %(osm_type)s, %(name)s, %(operator)s,
                  %(operator_type)s, %(access)s,
                  %(tags)s::jsonb,
                  st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326)),
                  st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326))::geography)
                )
                on conflict (osm_type, osm_id) do update
                  set name = excluded.name, operator = excluded.operator,
                      operator_type = excluded.operator_type, access = excluded.access,
                      tags = excluded.tags, geom = excluded.geom,
                      area_m2 = excluded.area_m2
            """, [{
                "osm_id":   f["osm_id"],
                "osm_type": f["osm_type"],
                "name":     f["name"],
                "operator": f["operator"],
                "operator_type": f["operator_type"],
                "access":   f["access"],
                "tags":     json.dumps(f["tags"]),
                "geom":     json.dumps(f["geom_geojson"]),
            } for f in features], page_size=100)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("""
                select count(*) as n,
                       count(name) as n_named,
                       count(operator) as n_operator,
                       avg(area_m2)::int as avg_area
                from public.osm_dog_parks
            """)
            n, n_named, n_op, avg_area = cur.fetchone()
            print(f"  loaded: {n} rows ({n_named} named, {n_op} with operator) "
                  f"avg area {avg_area:,} m2")


def fetch_chunk_with_fallback(chunk_name: str, bbox: str, preferred_url: str) -> dict | None:
    """Try preferred Overpass; on 504/timeout, try the other endpoint.
    Returns None if both fail (caller continues with remaining chunks)."""
    other_url = OVERPASS_MIRROR if preferred_url == OVERPASS_URL else OVERPASS_URL
    for attempt, url in enumerate((preferred_url, other_url)):
        try:
            return fetch_overpass(overpass_query(bbox), url=url)
        except urllib.error.HTTPError as e:
            if e.code == 504:
                print(f"  ! {url} → HTTP 504 (Gateway Timeout); "
                      f"{'retrying with fallback' if attempt == 0 else 'giving up'}")
                if attempt == 0:
                    time.sleep(3)
                continue
            print(f"  ! HTTP {e.code} on {url}: {e.reason}")
            return None
        except Exception as e:
            print(f"  ! {type(e).__name__} on {url}: {e}")
            return None
    return None


def main():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk", default="all",
                    help="single chunk name | all")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--mirror", action="store_true",
                    help="Try kumi mirror first (default tries main first)")
    args = ap.parse_args()

    preferred_url = OVERPASS_MIRROR if args.mirror else OVERPASS_URL

    chunks = list(CHUNKS) if args.chunk == "all" else [args.chunk]
    all_features = []
    failed_chunks = []

    for chunk_name in chunks:
        bbox = CHUNKS[chunk_name]
        print(f"\nFetching leisure=dog_park for chunk={chunk_name} bbox={bbox}")
        raw = fetch_chunk_with_fallback(chunk_name, bbox, preferred_url)
        if raw is None:
            failed_chunks.append(chunk_name)
            continue
        n_el = len(raw.get("elements") or [])
        print(f"  Overpass returned {n_el} elements")
        features = features_from_overpass(raw)
        n_way = sum(1 for f in features if f["osm_type"] == "way")
        n_rel = sum(1 for f in features if f["osm_type"] == "relation")
        n_named = sum(1 for f in features if f["name"])
        n_op = sum(1 for f in features if f["operator"])
        print(f"  built {len(features)} polygons ({n_way} ways, {n_rel} relations, "
              f"{n_named} named, {n_op} with operator)")
        all_features.extend(features)
        if len(chunks) > 1:
            time.sleep(1.5)  # gentle pacing between chunks

    if not all_features:
        print("nothing to load")
        return 0

    # Dedup across chunks (AK/HI bboxes can overlap continental edges in rare cases)
    seen = set()
    deduped = []
    for f in all_features:
        key = (f["osm_type"], f["osm_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(f)
    if len(deduped) != len(all_features):
        print(f"\nDeduped {len(all_features)} → {len(deduped)} features across chunks")

    print(f"\nLoading {len(deduped)} features into public.osm_dog_parks")
    create_and_load(deduped, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
