"""
load_poi_landing.py
-------------------
Reads share/Dog_Beaches/US_beaches.csv and inserts each row into
public.poi_landing. One row in -> one row out. Only the columns that
are actually in the CSV are written; spatial enrichment (state /
county / cpad) is filled by the table's BEFORE INSERT trigger.

Usage:
    python scripts/one_off/load_poi_landing.py
"""

from __future__ import annotations
import csv
import sys
from pathlib import Path
from psycopg2.extras import execute_values

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = ROOT / "share" / "Dog_Beaches" / "US_beaches.csv"
FETCHED_BY = "load_poi_landing"


def main() -> int:
    rows = []
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                FETCHED_BY,
                int(r["fid"]),
                r["WKT"] or None,
                (r.get("NAME") or "").strip() or None,
                (r.get("COUNTRY") or "").strip() or None,
                (r.get("ADDR1") or "").strip() or None,
                (r.get("ADDR2") or "").strip() or None,
                (r.get("ADDR3") or "").strip() or None,
                (r.get("ADDR4") or "").strip() or None,
                (r.get("ADDR5") or "").strip() or None,
                (r.get("CAT_MOD") or "").strip() or None,
            ))
    print(f"Read {len(rows)} rows from {CSV_PATH.name}")

    sql = """
        insert into public.poi_landing
          (fetched_by, fid, raw_wkt, name, country,
           addr1, addr2, addr3, addr4, addr5, cat_mod)
        values %s
        on conflict (fid, fetched_at) do nothing
    """
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        cur.execute("select count(*) from public.poi_landing")
        total = cur.fetchone()[0]
    print(f"Inserted. poi_landing now has {total} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
