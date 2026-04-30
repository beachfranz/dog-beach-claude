"""
load_ccc_landing.py
-------------------
Fetches the CCC Public Access Points ArcGIS FeatureServer and writes
each feature to public.ccc_landing.

Mirrors the admin-load-ccc edge function but lands raw features rather
than upserting into public.ccc_access_points.

Usage:
    python scripts/one_off/load_ccc_landing.py
"""

from __future__ import annotations
import json
import os
import urllib.parse
from pathlib import Path
import httpx
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432,
          user=p.username, password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

CCC_URL = (
    "https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/services/"
    "AccessPoints/FeatureServer/0/query"
    "?where=1%3D1"
    "&outFields=*"
    "&returnGeometry=true"
    "&outSR=4326"
    "&f=geojson"
)
FETCHED_BY = "load_ccc_landing"


def main() -> int:
    print(f"Fetching {CCC_URL}")
    r = httpx.get(CCC_URL, timeout=120.0)
    r.raise_for_status()
    fc = r.json()
    features = fc.get("features", []) or []
    print(f"  {len(features)} features")

    rows = []
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or None
        objectid = props.get("OBJECTID")
        if objectid is None:
            continue

        # Build a PostGIS geom from the GeoJSON Point if present
        geom_wkt = None
        if geom and geom.get("type") == "Point":
            coords = geom.get("coordinates") or []
            if len(coords) >= 2:
                geom_wkt = f"SRID=4326;POINT({coords[0]} {coords[1]})"

        rows.append((
            FETCHED_BY,
            objectid,
            props.get("Name"),
            props.get("COUNTY"),
            props.get("DISTRICT"),
            props.get("Archived"),
            geom_wkt,
            Json(props),
            Json(geom) if geom else None,
        ))

    print(f"Inserting {len(rows)} rows into ccc_landing")
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, """
            insert into public.ccc_landing
              (fetched_by, objectid, name, county, district, archived,
               geom, properties, geometry)
            values %s
            on conflict (objectid, fetched_at) do nothing
        """, rows, page_size=200)
        cur.execute("select count(*) from public.ccc_landing")
        total = cur.fetchone()[0]
    print(f"Done. ccc_landing now has {total} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
