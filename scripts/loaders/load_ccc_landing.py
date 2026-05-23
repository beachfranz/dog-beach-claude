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
import sys
import httpx
from psycopg2.extras import execute_values, Json

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.common.db import connect

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
    with connect() as conn, conn.cursor() as cur:
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
