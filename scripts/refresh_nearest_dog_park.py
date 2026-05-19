"""Refresh nearest_dog_park_* materialized columns on beaches_gold.

Chunked initial-population + maintenance script per
[[chunked-subprocess]] HARD rule — calling
refresh_nearest_dog_park_for_all_beaches() in a single SQL statement
hit the pooler timeout. This commits per CHUNK so the pooler never
sees a long-running statement.

Usage:
  python scripts/refresh_nearest_dog_park.py                  # all active beaches
  python scripts/refresh_nearest_dog_park.py --state CA       # one state
  python scripts/refresh_nearest_dog_park.py --fid 6212       # one beach
  python scripts/refresh_nearest_dog_park.py --chunk 100      # smaller commits
"""

from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import os
import time
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect_pg():
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def main():
    ap = argparse.ArgumentParser(description="Refresh beaches_gold.nearest_dog_park_*")
    ap.add_argument("--state", help="ISO 2-letter state (limits scope)")
    ap.add_argument("--fid",   type=int, help="Single beach FID (for debugging)")
    ap.add_argument("--chunk", type=int, default=250, help="Commit per N beaches")
    args = ap.parse_args()

    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            if args.fid:
                fids = [args.fid]
            else:
                sql = "SELECT fid FROM public.beaches_gold WHERE is_active = true"
                params: tuple = ()
                if args.state:
                    sql += " AND state = %s"
                    params = (args.state,)
                sql += " ORDER BY fid"
                cur.execute(sql, params)
                fids = [r[0] for r in cur.fetchall()]

        print(f"Refreshing nearest_dog_park for {len(fids)} beaches "
              f"(chunk={args.chunk})…")
        t0 = time.time()
        n = 0
        for i in range(0, len(fids), args.chunk):
            chunk = fids[i:i + args.chunk]
            with conn.cursor() as cur:
                # One UPDATE per fid (function does its own UPDATE inside)
                for fid in chunk:
                    cur.execute(
                        "SELECT public.refresh_nearest_dog_park_for_beach(%s)",
                        (fid,),
                    )
            conn.commit()
            n += len(chunk)
            elapsed = time.time() - t0
            rate = n / elapsed if elapsed > 0 else 0
            print(f"  {n}/{len(fids)} processed  "
                  f"({elapsed:.1f}s @ {rate:.1f} beach/s)")
        print(f"\n✔ done — {n} beaches refreshed in {time.time() - t0:.1f}s")

        # Coverage summary
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  count(*) FILTER (WHERE nearest_dog_park_fid IS NOT NULL) AS with_dp,
                  count(*) FILTER (WHERE nearest_dog_park_fid IS NULL
                                   AND is_active = true)                   AS no_dp,
                  ROUND((percentile_cont(0.5) WITHIN GROUP
                         (ORDER BY nearest_dog_park_distance_m) / 1609.344)::numeric, 2)
                                                                           AS median_mi,
                  ROUND((MAX(nearest_dog_park_distance_m) / 1609.344)::numeric, 2)
                                                                           AS max_mi
                FROM public.beaches_gold
                WHERE is_active = true
            """)
            row = cur.fetchone()
            if row:
                w, nope, med, mx = row
                print(f"\n=== COVERAGE ===")
                print(f"  beaches with nearest_dp:    {w}")
                print(f"  beaches no DP in 25mi cap:  {nope}")
                print(f"  median distance (mi):       {med}")
                print(f"  max distance (mi):          {mx}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
