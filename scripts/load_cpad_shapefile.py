"""
load_cpad_shapefile.py
----------------------
Loads the CPAD Units shapefile (downloaded release, NAD83 California Teale
Albers) into the `cpad_units` PostGIS table. Reprojects geometry to WGS84
on the fly and posts features in GeoJSON batches to admin-load-cpad-batch.

The local shapefile has 17,239 Unit polygons — far smaller than the online
Holdings-layer (160k) we tried paginating through, and every field we care
about for beach jurisdiction (UNIT_NAME, AGNCY_*, MNG_*, PARK_URL,
ACCESS_TYP, COUNTY) is present.

Usage:
    python scripts/load_cpad_shapefile.py [path/to/CPAD_2025b_Units]
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import shapefile
from pyproj import Transformer

SUPABASE_URL = "https://ehlzbwtrsxaaukurekau.supabase.co"
ANON_KEY     = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu")
ENDPOINT     = f"{SUPABASE_URL}/functions/v1/admin-load-cpad-batch"

DEFAULT_SHP = Path(r"C:\Users\beach\Downloads\cpad_release_2025b\CPAD_Release_2025b\CPAD_2025b_Units\CPAD_2025b_Units")

BATCH_SIZE = 100    # smaller batches — some CPAD units are huge multipolygons
WORKERS    = 4      # parallel batch uploads
TIMEOUT_S  = 300    # per-batch HTTP timeout

# Shapefile fields → RPC expected field names. Only fields in this map are
# forwarded; everything else is dropped. RPC table schema matches the
# right-hand side.
FIELD_MAP = {
    "ACCESS_TYP": "ACCESS_TYP",
    "UNIT_ID":    "UNIT_ID",
    "UNIT_NAME":  "UNIT_NAME",
    "SUID_NMA":   "SUID_NMA",
    "AGNCY_ID":   "AGNCY_ID",
    "AGNCY_NAME": "AGNCY_NAME",
    "AGNCY_LEV":  "AGNCY_LEV",
    "AGNCY_TYP":  "AGNCY_TYP",
    "AGNCY_WEB":  "AGNCY_WEB",
    "LAYER":      "LAYER",
    "MNG_AG_ID":  "MNG_AG_ID",
    "MNG_AGENCY": "MNG_AGNCY",   # shapefile spells it differently
    "MNG_AG_LEV": "MNG_AG_LEV",
    "MNG_AG_TYP": "MNG_AG_TYP",
    "PARK_URL":   "PARK_URL",
    "COUNTY":     "COUNTY",
    "ACRES":      "ACRES",
    "LABEL_NAME": "LABEL_NAME",
    "YR_EST":     "YR_EST",
}


def make_transformer():
    # EPSG:3310 = NAD83 / California Albers (Teale)
    return Transformer.from_crs("EPSG:3310", "EPSG:4326", always_xy=True)


def shape_to_polygon_geojson(shape, transformer):
    # Build GeoJSON manually from shape.parts / shape.points so pyshp's
    # strict ring-sampling doesn't reject degenerate rings. PostGIS's
    # ST_MakeValid (or the geometry constructor's tolerance) will handle
    # any resulting topology quirks; for our use case (ST_DWithin lookup)
    # exact ring orientation doesn't matter.
    if shape.shapeType not in (5, 15, 25):  # POLYGON / POLYGONZ / POLYGONM
        return None
    points = shape.points
    parts  = list(shape.parts) + [len(points)]
    rings  = []
    for i in range(len(parts) - 1):
        raw_ring = points[parts[i]:parts[i+1]]
        if len(raw_ring) < 4:                 # need ≥4 points (incl closing)
            continue
        projected = []
        for x, y in raw_ring:
            px, py = transformer.transform(x, y)
            projected.append([px, py])
        rings.append(projected)
    if not rings:
        return None
    # Treat as a single Polygon with all rings as outer (PostGIS tolerates;
    # ST_Multi wraps it into a MultiPolygon to match the column type).
    return {"type": "Polygon", "coordinates": rings}


def build_feature(shape, record_dict, transformer, synthetic_oid):
    geom = shape_to_polygon_geojson(shape, transformer)
    if geom is None:
        return None

    props = {"OBJECTID": synthetic_oid}
    for src, dst in FIELD_MAP.items():
        v = record_dict.get(src)
        if v is None or v == "":
            continue
        props[dst] = v

    return {"type": "Feature", "properties": props, "geometry": geom}


def upload_batch(features):
    r = requests.post(
        ENDPOINT,
        headers={
            "Authorization":  f"Bearer {ANON_KEY}",
            "Content-Type":   "application/json",
            "x-admin-secret": ADMIN_SECRET,
        },
        json={"features": features},
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def chunk(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    shp_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SHP
    print(f"Reading {shp_path}...")
    reader = shapefile.Reader(str(shp_path))
    print(f"  {len(reader):,} features, {len(reader.fields)-1} fields")

    transformer = make_transformer()
    fieldnames = [f[0] for f in reader.fields[1:]]

    print("Building features (with reprojection)...")
    t0 = time.time()
    features = []
    for i, sr in enumerate(reader.iterShapeRecords()):
        record_dict = dict(zip(fieldnames, list(sr.record)))
        f = build_feature(sr.shape, record_dict, transformer, synthetic_oid=i + 1)
        if f:
            features.append(f)
        if (i + 1) % 2000 == 0:
            print(f"  {i+1:,}/{len(reader):,} — {time.time()-t0:.0f}s elapsed")
    print(f"  built {len(features):,} features in {time.time()-t0:.0f}s")

    print(f"\nUploading in batches of {BATCH_SIZE} with {WORKERS} parallel workers...")
    t_up = time.time()
    totals = {"affected": 0, "skipped": 0, "total": 0}
    batches = list(chunk(features, BATCH_SIZE))
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(upload_batch, b): i for i, b in enumerate(batches)}
        for f in as_completed(futures):
            i = futures[f]
            try:
                r = f.result()
                for k in totals:
                    totals[k] += r.get(k, 0) or 0
                if (i+1) % 5 == 0 or i == len(batches) - 1:
                    print(f"  batch {i+1}/{len(batches)} — totals: {totals}")
            except Exception as e:
                print(f"  batch {i+1} FAILED: {e}", file=sys.stderr)

    print(f"\nDone in {time.time()-t0:.0f}s total ({time.time()-t_up:.0f}s upload). Totals: {totals}")


if __name__ == "__main__":
    main()
