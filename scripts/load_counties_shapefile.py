"""
load_counties_shapefile.py
--------------------------
Loads TIGER/Line 2024 US counties shapefile, filtered to California
(STATEFP = '06' → 58 counties), reprojected from NAD83 (EPSG:4269) to
WGS84 (EPSG:4326), into the PostGIS `counties` table.

Same direct-SQL pattern as load_cpad_shapefile_direct.py — writes batches
of GeoJSON features as .sql files and applies via `supabase db query --linked
-f`, bypassing the edge function gateway entirely.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

DEFAULT_SHP = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\tiger\tl_2024_us_county")
SQL_DIR     = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\counties_sql")
BATCH_SIZE  = 10          # CA has some massive counties (San Bernardino, Kern, Inyo) — keep batches small

FIELDS = [
    "STATEFP", "COUNTYFP", "GEOID", "NAME", "NAMELSAD",
    "ALAND", "AWATER", "INTPTLAT", "INTPTLON",
]


def shape_to_polygon_geojson(shape, transformer):
    """Build GeoJSON Polygon/MultiPolygon from shapefile parts, reprojecting
    each coordinate. Matches the approach in load_cpad_shapefile_direct.py."""
    if shape.shapeType not in (5, 15, 25):  # POLYGON / POLYGONZ / POLYGONM
        return None
    points = shape.points
    parts  = list(shape.parts) + [len(points)]
    rings  = []
    for i in range(len(parts) - 1):
        raw = points[parts[i]:parts[i+1]]
        if len(raw) < 4:
            continue
        rings.append([[*transformer.transform(x, y)] for x, y in raw])
    return {"type": "Polygon", "coordinates": rings} if rings else None


PROJECT_ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")


def run_sql_file(path):
    # Run from project root so supabase CLI can find supabase/config.toml
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
    # Optional state filter: pass FIPS code as second arg (e.g. "06" for CA).
    # Default: no filter → load all ~3,235 US counties.
    state_filter = sys.argv[2] if len(sys.argv) > 2 else None
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"):
        old.unlink()

    print(f"Reading {shp_path}...")
    reader = shapefile.Reader(str(shp_path))
    fieldnames = [f[0] for f in reader.fields[1:]]
    idx = {name: fieldnames.index(name) for name in FIELDS}
    print(f"  {len(reader):,} total features")

    transformer = Transformer.from_crs("EPSG:4269", "EPSG:4326", always_xy=True)

    filter_desc = f"to STATEFP='{state_filter}'" if state_filter else "no filter (national)"
    print(f"Building features ({filter_desc})...")
    t0 = time.time()
    features = []
    for sr in reader.iterShapeRecords():
        if state_filter and sr.record[idx["STATEFP"]] != state_filter:
            continue
        geom = shape_to_polygon_geojson(sr.shape, transformer)
        if geom is None:
            continue
        props = {name: sr.record[idx[name]] for name in FIELDS}
        features.append({"type": "Feature", "properties": props, "geometry": geom})
    print(f"  built {len(features)} features in {time.time()-t0:.0f}s")

    # Write SQL files
    print(f"Writing SQL files (batches of {BATCH_SIZE})...")
    for b, batch in enumerate(chunk(features, BATCH_SIZE)):
        json_literal = json.dumps(batch).replace("'", "''")
        sql = f"select load_counties_batch('{json_literal}'::jsonb);\n"
        (SQL_DIR / f"batch_{b:03d}.sql").write_text(sql, encoding="utf-8")
    n_batches = (len(features) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  wrote {n_batches} files to {SQL_DIR}")

    # Apply sequentially
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
        print(f"  {b+1}/{n_batches} — ok={ok} fail={fail} — {time.time()-t_apply:.0f}s")

    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} fail={fail}.")


if __name__ == "__main__":
    main()
