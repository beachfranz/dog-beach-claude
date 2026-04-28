"""
load_places_shapefile.py
------------------------
Loads TIGER/Line 2024 Places shapefile for California (STATEFP='06')
into the public.jurisdictions table. CA has ~540 places total: ~480
incorporated (CLASSFP starting with 'C') and ~60 CDPs (CLASSFP 'U1').

Reprojects NAD83 (EPSG:4269) → WGS84 (EPSG:4326) per
project_crs_convention.md. Same direct-SQL batch pattern as
load_counties_shapefile.py, applied via `supabase db query --linked -f`.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

DEFAULT_SHP  = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\tiger\tl_2024_06_place")
SQL_DIR      = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\places_sql")
BATCH_SIZE   = 25
PROJECT_ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")

FIELDS = ["STATEFP", "PLACEFP", "GEOID", "NAME", "NAMELSAD",
          "CLASSFP", "FUNCSTAT", "ALAND", "AWATER",
          "INTPTLAT", "INTPTLON"]


def _signed_area(ring):
    # Shoelace on raw shapefile (lon, lat) coords. Sign tells outer vs hole:
    # ESRI convention is outer rings CW (negative), inner rings CCW (positive).
    s = 0.0
    n = len(ring)
    for i in range(n - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        s += x1 * y2 - x2 * y1
    return s / 2.0


def shape_to_polygon_geojson(shape, transformer):
    if shape.shapeType not in (5, 15, 25):
        return None
    points = shape.points
    parts  = list(shape.parts) + [len(points)]
    polygons = []  # list of [outer_ring, *holes]
    for i in range(len(parts) - 1):
        raw = points[parts[i]:parts[i+1]]
        if len(raw) < 4:
            continue
        projected = [[*transformer.transform(x, y)] for x, y in raw]
        if _signed_area(raw) < 0:
            # outer ring → start a new polygon
            polygons.append([projected])
        else:
            # inner ring → attach to the most recent outer
            if polygons:
                polygons[-1].append(projected)
            else:
                # orphan hole with no preceding outer; treat as outer to be safe
                polygons.append([projected])
    if not polygons:
        return None
    if len(polygons) == 1:
        return {"type": "Polygon", "coordinates": polygons[0]}
    return {"type": "MultiPolygon", "coordinates": polygons}


def run_sql_file(path):
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(path)],
        capture_output=True, text=True, timeout=600,
        cwd=str(PROJECT_ROOT),
    )
    return r.returncode, r.stderr


def chunk(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    shp_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SHP
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"):
        old.unlink()

    print(f"Reading {shp_path}...")
    reader = shapefile.Reader(str(shp_path))
    fieldnames = [f[0] for f in reader.fields[1:]]
    idx = {name: fieldnames.index(name) for name in FIELDS}
    print(f"  {len(reader):,} total features")

    transformer = Transformer.from_crs("EPSG:4269", "EPSG:4326", always_xy=True)

    print("Building features...")
    t0 = time.time()
    features = []
    for sr in reader.iterShapeRecords():
        geom = shape_to_polygon_geojson(sr.shape, transformer)
        if geom is None:
            continue
        props = {name: sr.record[idx[name]] for name in FIELDS}
        features.append({"props": props, "geom": json.dumps(geom)})
    print(f"  built {len(features)} features in {time.time()-t0:.0f}s")

    # Count incorporated vs CDP for sanity
    incorp = sum(1 for f in features if (f["props"].get("CLASSFP") or "").startswith("C"))
    cdp    = sum(1 for f in features if (f["props"].get("CLASSFP") or "") == "U1")
    other  = len(features) - incorp - cdp
    print(f"  incorporated: {incorp}  CDP: {cdp}  other: {other}")

    print(f"Writing SQL files (batches of {BATCH_SIZE})...")
    for b, batch in enumerate(chunk(features, BATCH_SIZE)):
        json_literal = json.dumps(batch).replace("'", "''")
        sql = f"select load_jurisdictions_batch('{json_literal}'::jsonb);\n"
        (SQL_DIR / f"batch_{b:03d}.sql").write_text(sql, encoding="utf-8")
    n_batches = (len(features) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  wrote {n_batches} files to {SQL_DIR}")

    print("Applying SQL files...")
    t_apply = time.time()
    ok, fail = 0, 0
    for b in range(n_batches):
        path = SQL_DIR / f"batch_{b:03d}.sql"
        rc, err = run_sql_file(path)
        if rc == 0:
            ok += 1
        else:
            fail += 1
            print(f"  batch {b} FAILED: {err[:200].strip()}", file=sys.stderr)
        if (b + 1) % 5 == 0 or b + 1 == n_batches:
            print(f"  {b+1}/{n_batches} - ok={ok} fail={fail} - {time.time()-t_apply:.0f}s")

    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} fail={fail}.")


if __name__ == "__main__":
    main()
