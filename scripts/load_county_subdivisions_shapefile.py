"""
load_county_subdivisions_shapefile.py
-------------------------------------
Loads a TIGER/Line 2024 COUSUB (County Subdivisions / MCD) shapefile for
ONE state into public.tiger_county_subdivisions. Mirrors
load_places_shapefile.py per directive 2026-05-23 LATE:
"be sure to model mcds using same process as we use for C1s".

Town-strong states (per public.state_government_strength) get this load
so the towns/townships/boroughs that hold dog-policy authority become
visible to the operator pipeline. For NH the load is ~250 rows
(234 towns + 13 cities + a few unorganized territories).

Reprojects NAD83 (EPSG:4269) → WGS84 (EPSG:4326) per
project_crs_convention.md. Same direct-SQL batch pattern as
load_places_shapefile.py, applied via `supabase db query --linked -f`.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

SQL_DIR      = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\cousub_sql")
BATCH_SIZE   = 25
PROJECT_ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")

FIELDS = ["STATEFP", "COUNTYFP", "COUSUBFP", "GEOID", "NAME", "NAMELSAD",
          "LSAD", "CLASSFP", "MTFCC", "CNECTAFP", "NECTAFP", "NCTADVFP",
          "FUNCSTAT", "ALAND", "AWATER", "INTPTLAT", "INTPTLON"]


def _signed_area(ring):
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
    polygons = []
    for i in range(len(parts) - 1):
        raw = points[parts[i]:parts[i+1]]
        if len(raw) < 4:
            continue
        projected = [[*transformer.transform(x, y)] for x, y in raw]
        if _signed_area(raw) < 0:
            polygons.append([projected])
        else:
            if polygons:
                polygons[-1].append(projected)
            else:
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
    if len(sys.argv) < 2:
        print("Usage: load_county_subdivisions_shapefile.py <shapefile-base>", file=sys.stderr)
        sys.exit(2)
    shp_path = Path(sys.argv[1])
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"):
        old.unlink()

    print(f"Reading {shp_path}...")
    reader = shapefile.Reader(str(shp_path))
    fieldnames = [f[0] for f in reader.fields[1:]]
    idx = {name: fieldnames.index(name) for name in FIELDS if name in fieldnames}
    print(f"  {len(reader):,} total features")
    missing = [n for n in FIELDS if n not in idx]
    if missing:
        print(f"  note: fields not in shapefile (ok if unused): {missing}")

    transformer = Transformer.from_crs("EPSG:4269", "EPSG:4326", always_xy=True)

    print("Building features...")
    t0 = time.time()
    features = []
    for sr in reader.iterShapeRecords():
        geom = shape_to_polygon_geojson(sr.shape, transformer)
        if geom is None:
            continue
        props = {name: sr.record[idx[name]] for name in idx}
        features.append({"props": props, "geom": json.dumps(geom)})
    print(f"  built {len(features)} features in {time.time()-t0:.0f}s")

    # CLASSFP breakdown for sanity
    from collections import Counter
    classfp_counts = Counter((f["props"].get("CLASSFP") or "<none>") for f in features)
    print(f"  CLASSFP distribution: {dict(classfp_counts)}")

    print(f"Writing SQL files (batches of {BATCH_SIZE})...")
    for b, batch in enumerate(chunk(features, BATCH_SIZE)):
        json_literal = json.dumps(batch).replace("'", "''")
        sql = f"select load_county_subdivisions_batch('{json_literal}'::jsonb);\n"
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
