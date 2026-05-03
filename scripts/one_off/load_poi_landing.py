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
import os
import urllib.parse
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

# Build psycopg2 args from .env + supabase pooler-url cache
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(
    host=p.hostname, port=p.port or 5432,
    user=p.username, password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(p.path or "/postgres").lstrip("/"),
    sslmode="require",
)

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
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        cur.execute("select count(*) from public.poi_landing")
        total = cur.fetchone()[0]
    print(f"Inserted. poi_landing now has {total} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
