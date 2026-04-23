"""
load_cpad_shapefile_direct.py
-----------------------------
Same input as load_cpad_shapefile.py but bypasses the edge function
entirely — writes batches of SQL to disk and applies them via
`supabase db query --linked -f <file>`. Sidesteps the gateway timeouts
and admin rate limit that were failing on complex polygons.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

DEFAULT_SHP = Path(r"C:\Users\beach\Downloads\cpad_release_2025b\CPAD_Release_2025b\CPAD_2025b_Units\CPAD_2025b_Units")
SQL_DIR     = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\cpad_sql")
BATCH_SIZE  = 200

FIELD_MAP = {
    "ACCESS_TYP": "ACCESS_TYP", "UNIT_ID": "UNIT_ID", "UNIT_NAME": "UNIT_NAME",
    "SUID_NMA": "SUID_NMA", "AGNCY_ID": "AGNCY_ID", "AGNCY_NAME": "AGNCY_NAME",
    "AGNCY_LEV": "AGNCY_LEV", "AGNCY_TYP": "AGNCY_TYP", "AGNCY_WEB": "AGNCY_WEB",
    "LAYER": "LAYER", "MNG_AG_ID": "MNG_AG_ID", "MNG_AGENCY": "MNG_AGNCY",
    "MNG_AG_LEV": "MNG_AG_LEV", "MNG_AG_TYP": "MNG_AG_TYP",
    "PARK_URL": "PARK_URL", "COUNTY": "COUNTY", "ACRES": "ACRES",
    "LABEL_NAME": "LABEL_NAME", "YR_EST": "YR_EST",
}


def shape_to_polygon_geojson(shape, transformer):
    if shape.shapeType not in (5, 15, 25): return None
    points = shape.points
    parts  = list(shape.parts) + [len(points)]
    rings  = []
    for i in range(len(parts) - 1):
        raw = points[parts[i]:parts[i+1]]
        if len(raw) < 4: continue
        proj = []
        for x, y in raw:
            px, py = transformer.transform(x, y)
            proj.append([px, py])
        rings.append(proj)
    return {"type": "Polygon", "coordinates": rings} if rings else None


def build_feature(shape, record_dict, transformer, oid):
    geom = shape_to_polygon_geojson(shape, transformer)
    if geom is None: return None
    props = {"OBJECTID": oid}
    for src, dst in FIELD_MAP.items():
        v = record_dict.get(src)
        if v is None or v == "": continue
        props[dst] = v
    return {"type": "Feature", "properties": props, "geometry": geom}


def run_sql_file(path):
    # supabase db query --linked -f <path>  — rely on CLI being in PATH.
    result = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(path)],
        capture_output=True, text=True, timeout=600,
    )
    return result.returncode, result.stdout, result.stderr


def main():
    shp_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SHP
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"):
        old.unlink()

    print(f"Reading {shp_path}...")
    reader = shapefile.Reader(str(shp_path))
    print(f"  {len(reader):,} features")

    transformer = make_tr = Transformer.from_crs("EPSG:3310", "EPSG:4326", always_xy=True)
    fieldnames = [f[0] for f in reader.fields[1:]]

    print("Building features...")
    t0 = time.time()
    features = []
    for i, sr in enumerate(reader.iterShapeRecords()):
        rd = dict(zip(fieldnames, list(sr.record)))
        f = build_feature(sr.shape, rd, transformer, oid=i + 1)
        if f: features.append(f)
    print(f"  built {len(features):,} in {time.time()-t0:.0f}s")

    # ── Write SQL files ─────────────────────────────────────────────────────
    print(f"Writing SQL files (batches of {BATCH_SIZE})...")
    n_batches = (len(features) + BATCH_SIZE - 1) // BATCH_SIZE
    for b in range(n_batches):
        batch = features[b * BATCH_SIZE:(b + 1) * BATCH_SIZE]
        # Escape single quotes in JSON (Postgres literal '...')
        json_literal = json.dumps(batch).replace("'", "''")
        sql = f"select load_cpad_batch('{json_literal}'::jsonb);\n"
        (SQL_DIR / f"batch_{b:04d}.sql").write_text(sql, encoding="utf-8")
    print(f"  wrote {n_batches} files to {SQL_DIR}")

    # ── Apply each in sequence ──────────────────────────────────────────────
    print("Applying SQL files via supabase db query...")
    t_apply = time.time()
    ok = 0; fail = 0; fail_list = []
    for b in range(n_batches):
        path = SQL_DIR / f"batch_{b:04d}.sql"
        rc, _, err = run_sql_file(path)
        if rc == 0:
            ok += 1
        else:
            fail += 1
            fail_list.append(b)
            if fail <= 5: print(f"  batch {b} FAILED: {err[:200]}", file=sys.stderr)
        if (b + 1) % 10 == 0 or b == n_batches - 1:
            print(f"  {b+1}/{n_batches} — ok={ok} fail={fail} — {time.time()-t_apply:.0f}s")

    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} fail={fail}.")
    if fail_list: print(f"Failed batches: {fail_list}")


if __name__ == "__main__":
    main()
