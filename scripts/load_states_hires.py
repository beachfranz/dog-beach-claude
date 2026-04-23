"""
load_states_hires.py
--------------------
Replace the simplified PublicaMundi state polygons (89 KB, loaded via
admin-load-us-states) with hi-res TIGER 2024 state polygons. Same
schema — existing load_state_feature RPC handles the upsert.

Truncates public.states first so leftover PublicaMundi rows (which
use the same state_code keys) cleanly hand over to TIGER.

Reprojects NAD83 geographic (EPSG:4269) → WGS84 (EPSG:4326) per
project_crs_convention.md.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

SHP  = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\tiger\tl_2024_us_state")
SQL_DIR = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\states_hires_sql")
ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")


def shape_to_polygon_geojson(shape, transformer):
    """Build GeoJSON Polygon from shapefile parts, reprojecting each
    coordinate. Matches load_cpad_shapefile_direct / load_counties_shapefile."""
    if shape.shapeType not in (5, 15, 25):
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


def run_sql_file(path):
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(path)],
        capture_output=True, text=True, timeout=600, cwd=str(ROOT),
    )
    return r.returncode, r.stderr


def main():
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"): old.unlink()

    print(f"Reading {SHP}...")
    reader = shapefile.Reader(str(SHP))
    fieldnames = [f[0] for f in reader.fields[1:]]
    idx = {k: fieldnames.index(k) for k in ("STUSPS", "NAME")}
    print(f"  {len(reader):,} features")

    transformer = Transformer.from_crs("EPSG:4269", "EPSG:4326", always_xy=True)

    # Truncate first so PublicaMundi rows cleanly hand over
    (SQL_DIR / "000_truncate.sql").write_text("truncate public.states restart identity;\n", encoding="utf-8")

    print("Building per-state SQL files...")
    sql_files = ["000_truncate.sql"]
    for i, sr in enumerate(reader.iterShapeRecords()):
        code = sr.record[idx["STUSPS"]].strip()
        name = sr.record[idx["NAME"]].strip()
        geom = shape_to_polygon_geojson(sr.shape, transformer)
        if geom is None:
            print(f"  skip {code} — no geometry", file=sys.stderr)
            continue
        geojson_literal = json.dumps(geom).replace("'", "''")
        sql = (
            f"select load_state_feature('{code}', '{name.replace(chr(39), chr(39)+chr(39))}', "
            f"'{geojson_literal}');\n"
        )
        fname = f"{i+1:03d}_{code}.sql"
        (SQL_DIR / fname).write_text(sql, encoding="utf-8")
        sql_files.append(fname)
    print(f"  wrote {len(sql_files)} files")

    print("Applying (1 file at a time — states have big perimeters, can't batch)...")
    t0 = time.time()
    ok = 0; fail = 0
    for fname in sql_files:
        rc, err = run_sql_file(SQL_DIR / fname)
        if rc == 0:
            ok += 1
        else:
            fail += 1
            print(f"  {fname} FAILED: {err[:200].strip()}", file=sys.stderr)
    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} fail={fail}.")


if __name__ == "__main__":
    main()
