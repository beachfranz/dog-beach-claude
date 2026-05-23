"""
export_locations_stage_csv.py
-----------------------------
Dump public.locations_stage to share/locations_stage.csv via PostgREST.

PostGIS `geom` is excluded by default (it returns a hex EWKB string
which is awkward in CSV). Pass --with-geom to include it.

Usage:
  python scripts/one_off/export_locations_stage_csv.py [--with-geom]
"""

from __future__ import annotations
import argparse, csv, os, sys
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

OUTPUT = Path(__file__).parent.parent.parent / "share" / "locations_stage.csv"
PAGE   = 1000


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--with-geom", action="store_true",
                   help="Include the PostGIS geom column (EWKB hex). Off by default.")
    args = p.parse_args()

    headers = {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Accept":        "application/json",
    }
    all_rows: list[dict] = []
    for offset in range(0, 100_000, PAGE):
        url = f"{SUPABASE_URL}/rest/v1/locations_stage?select=*&offset={offset}&limit={PAGE}"
        r = httpx.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        batch = r.json()
        all_rows.extend(batch)
        if len(batch) < PAGE:
            break
    print(f"Fetched {len(all_rows)} rows")

    if not all_rows:
        print("No rows; nothing to write.")
        return 0

    # Stable column order: union of all keys observed (covers nullable
    # columns that show up only on some rows).
    fieldnames: list[str] = []
    seen = set()
    for r in all_rows:
        for k in r.keys():
            if k in seen: continue
            if not args.with_geom and k == "geom": continue
            seen.add(k); fieldnames.append(k)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in all_rows:
            # Flatten any list/dict values to JSON strings for CSV safety.
            flat = {}
            for k, v in row.items():
                if k not in seen: continue
                if isinstance(v, (list, dict)):
                    import json
                    flat[k] = json.dumps(v, separators=(",", ":"))
                else:
                    flat[k] = v
            w.writerow(flat)
    print(f"Wrote {OUTPUT} ({len(all_rows)} rows × {len(fieldnames)} cols)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
