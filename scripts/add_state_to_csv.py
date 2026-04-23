"""
add_state_to_csv.py
-------------------
One-off: add a STATE column to US_beaches.csv by classifying each row's
WKT point against the PostGIS states table (via admin-classify-points).
Writes the enriched CSV to share/Dog_Beaches/US_beaches_with_state.csv.

Usage:
    python scripts/add_state_to_csv.py [input.csv] [output.csv]

Defaults:
    input  = share/Dog_Beaches/US_beaches.csv
    output = share/Dog_Beaches/US_beaches_with_state.csv
"""

import csv
import os
import re
import sys
from pathlib import Path

import requests

SUPABASE_URL = "https://ehlzbwtrsxaaukurekau.supabase.co"
ANON_KEY     = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu")
ENDPOINT     = f"{SUPABASE_URL}/functions/v1/admin-classify-points"
BATCH_SIZE   = 1000

REPO_ROOT  = Path(__file__).parent.parent
DEFAULT_IN  = REPO_ROOT / "share" / "Dog_Beaches" / "US_beaches.csv"
DEFAULT_OUT = REPO_ROOT / "share" / "Dog_Beaches" / "US_beaches_with_state.csv"

WKT_RE = re.compile(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", re.IGNORECASE)


def parse_wkt(wkt: str):
    m = WKT_RE.match(wkt or "")
    if not m: return None
    return float(m.group(1)), float(m.group(2))  # lon, lat


def classify(points):
    resp = requests.post(
        ENDPOINT,
        headers={
            "Authorization":  f"Bearer {ANON_KEY}",
            "Content-Type":   "application/json",
            "x-admin-secret": ADMIN_SECRET,
        },
        json={"points": points},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"API: {data['error']}")
    return data["classifications"]


def main():
    in_path  = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IN
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT

    print(f"Reading {in_path}...")
    with open(in_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    print(f"  {len(rows):,} rows")

    # Build classification requests for rows with valid WKT+fid.
    to_classify = []
    for r in rows:
        coords = parse_wkt(r.get("WKT", ""))
        fid = r.get("fid", "").strip()
        if coords and fid:
            lon, lat = coords
            to_classify.append({"fid": fid, "latitude": lat, "longitude": lon})

    print(f"  {len(to_classify):,} rows with valid coords")

    # Batch call the API.
    fid_to_state = {}
    far_points = []  # rows more than 50km from any state (sanity flag)
    for i in range(0, len(to_classify), BATCH_SIZE):
        batch = to_classify[i:i + BATCH_SIZE]
        result = classify(batch)
        for c in result:
            fid_to_state[str(c["fid"])] = c.get("state_code")
            if (c.get("distance_m") or 0) > 50_000:
                far_points.append(c)
        print(f"  batch {i // BATCH_SIZE + 1}: classified {len(result)} points")

    # Write new CSV with STATE column.
    out_fields = list(fieldnames)
    if "STATE" not in out_fields:
        out_fields.append("STATE")

    print(f"Writing {out_path}...")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        for r in rows:
            r["STATE"] = fid_to_state.get((r.get("fid") or "").strip(), "")
            writer.writerow(r)

    # Summary
    counts = {}
    for v in fid_to_state.values():
        counts[v] = counts.get(v, 0) + 1
    print(f"\nDone. {len(fid_to_state):,} rows classified.")
    print(f"Top states:")
    for state, n in sorted(counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {state or '(null)'}: {n:,}")
    if far_points:
        print(f"\nNote: {len(far_points)} rows are >50 km from any state polygon "
              f"(Channel Islands, offshore, or bad coords) — STATE still set to nearest.")


if __name__ == "__main__":
    main()
