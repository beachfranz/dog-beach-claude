"""process_extraction_queue.py — drain beach_extraction_queue.

Phase C of the state-launch pipeline. Picks up rows from
beach_extraction_queue (populated by SQL's enqueue_missing_extractions)
and dispatches each to the appropriate Python extractor.

Usage:
  python scripts/process_extraction_queue.py [--max-fids N] [--budget USD]

Behavior:
  * Pulls up to --max-fids pending rows (FOR UPDATE SKIP LOCKED so multiple
    workers don't conflict)
  * Dispatches by `kind`:
      dogs_extraction      → extract_for_orphans.py per-fid
      park_url_discovery   → extract_for_orphans.py with URL discovery
      full_refresh         → both
  * On success: SET completed_at=now()
  * On error: SET attempts=attempts+1, last_error=<msg>; rows with
    attempts>=3 are skipped on subsequent runs.
  * Budget gate: tracks API calls and stops at --budget USD.

Run via cron / Dagster every 15 min, or kick off manually after a
state load (load_state.py invokes it).
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

# Estimated cost per kind (rough; tune from observed Anthropic billing)
COST_PER_KIND = {
    "dogs_extraction":     0.05,
    "park_url_discovery":  0.02,
    "full_refresh":        0.07,
}


def claim_batch(conn, max_fids: int):
    """Atomically claim up to max_fids rows. SKIP LOCKED so concurrent workers don't fight."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            update public.beach_extraction_queue q
               set picked_at = now(),
                   attempts = attempts + 1
              from (
                select id from public.beach_extraction_queue
                 where completed_at is null and attempts < 3
                 order by queued_at asc
                 limit %s
                 for update skip locked
              ) c
             where q.id = c.id
            returning q.id, q.fid, q.kind
        """, (max_fids,))
        return cur.fetchall()


def mark_done(conn, qid: int):
    with conn.cursor() as cur:
        cur.execute("update public.beach_extraction_queue set completed_at=now() where id=%s",
                    (qid,))


def mark_failed(conn, qid: int, err: str):
    with conn.cursor() as cur:
        cur.execute("update public.beach_extraction_queue set last_error=%s where id=%s",
                    (err[:500], qid))


def dispatch(kind: str, fid: int) -> bool:
    """Run the right extractor. Returns True on success, False on error."""
    # NOTE: stub today. Wire to actual extractors when their --fid mode is
    # ready. extract_for_orphans.py is the closest existing entry point;
    # extract_for_gold_v3 targets a specific 25-beach set, not arbitrary fids.
    if kind == "dogs_extraction":
        cmd = ["python", str(ROOT / "scripts" / "extract_for_orphans.py"),
               "--fid", str(fid), "--apply"]
    elif kind == "park_url_discovery":
        # extract_for_orphans handles URL discovery internally when no
        # existing source URL is in the DB. Same command.
        cmd = ["python", str(ROOT / "scripts" / "extract_for_orphans.py"),
               "--fid", str(fid), "--discover-urls", "--apply"]
    elif kind == "full_refresh":
        cmd = ["python", str(ROOT / "scripts" / "extract_for_orphans.py"),
               "--fid", str(fid), "--apply"]
    else:
        print(f"  unknown kind: {kind}")
        return False

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"  failed: {result.stderr[:200]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"  timeout (300s)")
        return False
    except Exception as e:
        print(f"  exception: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-fids", type=int, default=20)
    ap.add_argument("--budget", type=float, default=5.00,
                    help="Stop when estimated spend exceeds this $")
    args = ap.parse_args()

    spent = 0.0
    processed = 0
    failed = 0

    with psycopg2.connect(**PG) as conn:
        conn.autocommit = True

        rows = claim_batch(conn, args.max_fids)
        print(f"Claimed {len(rows)} rows from beach_extraction_queue")
        if not rows:
            return

        for r in rows:
            kind = r["kind"]
            fid = r["fid"]
            qid = r["id"]
            est = COST_PER_KIND.get(kind, 0.05)
            if spent + est > args.budget:
                print(f"Budget gate hit (spent ${spent:.2f}, would exceed ${args.budget})")
                # Reset picked_at so the row is re-claimable
                with conn.cursor() as cur:
                    cur.execute(
                        "update public.beach_extraction_queue set picked_at=null, attempts=greatest(attempts-1,0) where id=%s",
                        (qid,)
                    )
                break

            print(f"[{processed+failed+1}/{len(rows)}] fid={fid} kind={kind} (~${est})")
            ok = dispatch(kind, fid)
            spent += est
            if ok:
                mark_done(conn, qid)
                processed += 1
            else:
                mark_failed(conn, qid, "see worker stderr")
                failed += 1

    print(f"\nProcessed: {processed}, Failed: {failed}, Spent: ~${spent:.2f}")


if __name__ == "__main__":
    main()
