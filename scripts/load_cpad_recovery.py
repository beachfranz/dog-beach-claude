"""
load_cpad_recovery.py
---------------------
Re-loads only the CPAD features that didn't make it through the main
load (typically huge multipolygons whose SQL payload exceeded Supabase
Management API's request size limit at batch_size=200). Uses smaller
batches and falls back to one-at-a-time for any remaining stragglers.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import shapefile
from pyproj import Transformer

SHP = Path(r"C:\Users\beach\Downloads\cpad_release_2025b\CPAD_Release_2025b\CPAD_2025b_Units\CPAD_2025b_Units")
SQL_DIR = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\cpad_sql_recovery")

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
        rings.append([[*transformer.transform(x, y)] for x, y in raw])
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


def get_existing_oids():
    r = subprocess.run(
        ["supabase", "db", "query", "--linked",
         "select objectid from cpad_units order by objectid"],
        capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        sys.exit(f"Failed to fetch existing OIDs: {r.stderr}")
    # Output is JSON with "rows": [{"objectid": N}, ...]
    # Find the JSON block in output — Supabase CLI prints other stuff too.
    start = r.stdout.find("{")
    end   = r.stdout.rfind("}")
    doc   = json.loads(r.stdout[start:end+1])
    return {row["objectid"] for row in doc.get("rows", [])}


def run_sql_file(path):
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(path)],
        capture_output=True, text=True, timeout=600,
    )
    return r.returncode, r.stderr


def apply_batch(features, batch_idx):
    path = SQL_DIR / f"recovery_{batch_idx:04d}.sql"
    json_literal = json.dumps(features).replace("'", "''")
    sql = f"select load_cpad_batch('{json_literal}'::jsonb);\n"
    path.write_text(sql, encoding="utf-8")
    rc, err = run_sql_file(path)
    return rc == 0, err


def main():
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for f in SQL_DIR.glob("*.sql"): f.unlink()

    print("Reading existing OBJECTIDs from DB...")
    existing = get_existing_oids()
    print(f"  {len(existing):,} already loaded")

    print("Reading shapefile + finding missing...")
    reader = shapefile.Reader(str(SHP))
    total = len(reader)
    transformer = Transformer.from_crs("EPSG:3310", "EPSG:4326", always_xy=True)
    fieldnames = [f[0] for f in reader.fields[1:]]

    missing_features = []
    for i, sr in enumerate(reader.iterShapeRecords()):
        oid = i + 1
        if oid in existing: continue
        rd = dict(zip(fieldnames, list(sr.record)))
        f = build_feature(sr.shape, rd, transformer, oid=oid)
        if f: missing_features.append(f)

    print(f"  {len(missing_features)} features missing from DB (of {total} total)")
    if not missing_features:
        print("Nothing to do.")
        return

    # ── Descending batch sizes: 25 → 5 → 1 ─────────────────────────────────
    remaining = missing_features
    for batch_size in (25, 5, 1):
        if not remaining: break
        print(f"\nRound with batch_size={batch_size} over {len(remaining)} features")
        retry = []
        idx = 0
        t0 = time.time()
        for i in range(0, len(remaining), batch_size):
            batch = remaining[i:i + batch_size]
            ok, err = apply_batch(batch, idx)
            idx += 1
            if ok:
                pass
            else:
                if batch_size == 1:
                    print(f"  feature OBJECTID={batch[0]['properties']['OBJECTID']} UNABLE: {err[:120].strip()}")
                else:
                    retry.extend(batch)
            if idx % 20 == 0:
                print(f"  applied {idx} batches in {time.time()-t0:.0f}s, retry={len(retry)}")
        remaining = retry

    print(f"\nDone. Final unresolved: {len(remaining)}")


if __name__ == "__main__":
    main()
