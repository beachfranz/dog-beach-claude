"""
load_us_beach_points.py
-----------------------
Loads share/Dog_Beaches/US_beaches_with_state.csv (8,041 rows) into
public.us_beach_points via load_us_beach_points_batch RPC.

Uses the direct-SQL pattern (supabase db query --linked -f file.sql)
— same approach as load_cpad_shapefile_direct.py and load_counties
— since 8,041 rows × ~15 columns × ~300 bytes of JSON per row fits
comfortably in chunked SQL files well under Management API limits.
"""

import csv
import json
import subprocess
import sys
import time
from pathlib import Path

CSV_PATH  = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches_with_state.csv")
SQL_DIR   = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\us_beach_points_sql")
ROOT      = Path(r"C:\Users\beach\Documents\dog-beach-claude")
BATCH     = 500


def run_sql_file(path):
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(path)],
        capture_output=True, text=True, timeout=600, cwd=str(ROOT),
    )
    return r.returncode, r.stderr


def main():
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for old in SQL_DIR.glob("*.sql"):
        old.unlink()

    print(f"Reading {CSV_PATH}...")
    with open(CSV_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  {len(rows):,} rows")

    # Drop any rows without a WKT or fid — the RPC filters them out anyway,
    # but trimming here makes the SQL smaller.
    clean = [r for r in rows if r.get("WKT") and r.get("fid")]
    print(f"  {len(clean):,} rows with valid WKT+fid")

    # Write SQL files
    print(f"Writing SQL files (batches of {BATCH})...")
    n_batches = (len(clean) + BATCH - 1) // BATCH
    for b in range(n_batches):
        batch = clean[b * BATCH:(b + 1) * BATCH]
        json_literal = json.dumps(batch).replace("'", "''")
        sql = f"select load_us_beach_points_batch('{json_literal}'::jsonb);\n"
        (SQL_DIR / f"batch_{b:04d}.sql").write_text(sql, encoding="utf-8")
    print(f"  wrote {n_batches} files")

    # Apply
    print("Applying...")
    t0 = time.time()
    ok = 0
    fail = 0
    for b in range(n_batches):
        path = SQL_DIR / f"batch_{b:04d}.sql"
        rc, err = run_sql_file(path)
        if rc == 0:
            ok += 1
        else:
            fail += 1
            if fail <= 3:
                print(f"  batch {b} FAILED: {err[:200].strip()}", file=sys.stderr)
        if (b + 1) % 5 == 0 or b == n_batches - 1:
            print(f"  {b+1}/{n_batches} — ok={ok} fail={fail} — {time.time()-t0:.0f}s")

    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} fail={fail}.")


if __name__ == "__main__":
    main()
