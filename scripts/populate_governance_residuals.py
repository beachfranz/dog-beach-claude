"""Populate governance attribution for unattributed beaches.

After the 2026-05-19 beach_governance_status view fix unlocked 459
already-attributed beaches, ~85 beaches still have no canonical
governance row. This script runs populate_governance_gold(fid) for each
remaining unattributed beach.

Chunked per [[chunked-subprocess]] HARD rule — single SQL statement
loops over hundreds of beaches hit pooler timeouts in past sessions.

Usage:
  python scripts/populate_governance_residuals.py                # all states
  python scripts/populate_governance_residuals.py --state OR     # one state
  python scripts/populate_governance_residuals.py --chunk 25     # smaller commits
  python scripts/populate_governance_residuals.py --dry-run      # list only

Verifies the unattributed count via count_unattributed_beaches() before
+ after; reports the delta.
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


def _baseline(cur, states):
    out = {}
    for st in states:
        cur.execute(
            "SELECT * FROM public.count_unattributed_beaches(%s)", (st,))
        row = cur.fetchone()
        if row:
            out[st] = {"unattributed": row[1], "total": row[2]}
    return out


def main():
    ap = argparse.ArgumentParser(description="Populate governance for unattributed beaches")
    ap.add_argument("--state", help="Limit to one state (e.g., OR)")
    ap.add_argument("--chunk", type=int, default=25, help="Commit per N beaches")
    ap.add_argument("--dry-run", action="store_true", help="List unattributed; don't write")
    args = ap.parse_args()

    states = [args.state] if args.state else ["CA", "OR", "WA"]

    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            print("=== BASELINE ===")
            base = _baseline(cur, states)
            for st in states:
                b = base.get(st, {"unattributed": 0, "total": 0})
                print(f"  {st}: {b['unattributed']} unattributed / {b['total']} scoreable")

            # Pull the unattributed FIDs
            cur.execute("""
                SELECT s.fid, s.state, s.name
                  FROM public.beach_governance_status s
                 WHERE s.is_active = true
                   AND s.is_scoring_active = true
                   AND s.state = ANY(%s)
                   AND NOT s.is_attributed
                 ORDER BY s.state, s.name
            """, (states,))
            targets = cur.fetchall()
            print(f"\n=== TARGETS: {len(targets)} unattributed beaches ===")

            if args.dry_run:
                for fid, st, name in targets:
                    print(f"  {st}  fid={fid:>10}  {name}")
                return 0

            if not targets:
                print("\n✔ Nothing to do — all beaches attributed.")
                return 0

            print(f"\nRunning populate_governance_gold per fid (chunk={args.chunk})…")
            t0 = time.time()
            n_processed = 0
            n_returned_nonzero = 0
            failures: list[tuple] = []

            for i in range(0, len(targets), args.chunk):
                batch = targets[i:i + args.chunk]
                with conn.cursor() as cur2:
                    for fid, st, name in batch:
                        try:
                            cur2.execute(
                                "SELECT public.populate_governance_gold(%s)",
                                (fid,),
                            )
                            v = cur2.fetchone()
                            rows_written = (v and v[0]) or 0
                            if rows_written > 0:
                                n_returned_nonzero += 1
                        except Exception as e:
                            failures.append((fid, st, name, str(e)[:80]))
                        n_processed += 1
                conn.commit()
                elapsed = time.time() - t0
                rate = n_processed / elapsed if elapsed > 0 else 0
                print(f"  {n_processed}/{len(targets)} processed  "
                      f"({elapsed:.1f}s @ {rate:.1f} beach/s, {n_returned_nonzero} wrote rows)")

            print(f"\n=== AFTER ===")
            after = _baseline(cur, states)
            for st in states:
                a = after.get(st, {"unattributed": 0, "total": 0})
                b = base.get(st, {"unattributed": 0, "total": 0})
                delta = b['unattributed'] - a['unattributed']
                print(f"  {st}: {a['unattributed']} unattributed / {a['total']} scoreable  "
                      f"(was {b['unattributed']}; resolved {delta})")

            if failures:
                print(f"\n=== {len(failures)} FAILURES ===")
                for fid, st, name, err in failures[:20]:
                    print(f"  {st} fid={fid} {name}: {err}")

            # List still-unattributed for follow-up
            cur.execute("""
                SELECT s.fid, s.state, s.name
                  FROM public.beach_governance_status s
                 WHERE s.is_active = true AND s.is_scoring_active = true
                   AND s.state = ANY(%s)
                   AND NOT s.is_attributed
                 ORDER BY s.state, s.name
            """, (states,))
            still = cur.fetchall()
            if still:
                print(f"\n=== STILL UNATTRIBUTED: {len(still)} ===")
                for fid, st, name in still[:30]:
                    print(f"  {st}  fid={fid:>10}  {name}")
                if len(still) > 30:
                    print(f"  ... ({len(still)-30} more)")
                print("\nThese likely need polygon_containment BEP rows backfilled "
                      "first, or have no PAD-US polygon coverage (manual curator).")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
