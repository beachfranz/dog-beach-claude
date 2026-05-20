"""Auto-curate — pick N best+diverse photos per beach without a human.

Per Franz 2026-05-19 photo curation v3. Bypasses manual curation by
calling get_beach_photos_diverse(fid, N) and writing the selection back
via the SAME curated_at flag a human curator uses — with curated_by
= 'auto:n=<N>' as the method discriminator.

Spec: docs/photo_curation_v3_spec.md §5.

Conflict rule: HUMAN ALWAYS WINS. Auto-curate skips any beach where at
least one photo has curated_at NOT NULL AND curated_by NOT LIKE 'auto:%'.

Run:
  python scripts/auto_curate.py                            # all eligible, N=6
  python scripts/auto_curate.py --n 8                      # gallery of 8
  python scripts/auto_curate.py --state WA                 # one state
  python scripts/auto_curate.py --fid 9806                 # single beach
  python scripts/auto_curate.py --dry-run                  # plan only, no writes
  python scripts/auto_curate.py --reset --state WA         # clear auto:* for WA
                                                            # (lets re-runs converge)
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


def _ping_or_reconnect(conn):
    """Return a live conn. Long --reset loops over the full catalog
    (13k+ fids) exceed pgbouncer's idle timeout and the held conn dies.
    Ping SELECT 1; reconnect if broken.
    See feedback_pgbouncer_kills_held_conn."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1"); cur.fetchone()
        return conn
    except Exception:
        try: conn.close()
        except Exception: pass
        return _connect_pg()


def beach_has_human_curation(cur, fid: int) -> bool:
    """True if any photo on this beach was curated by a real human pick.
    Excludes AI-auto picks AND sibling-propagation artifacts (which look
    like human attributions but are auto-derived from a curator action at
    a DIFFERENT beach — they shouldn't block auto-curate at this beach)."""
    cur.execute("""
        SELECT 1 FROM public.beach_photos
         WHERE arena_group_id = %s
           AND curated_at IS NOT NULL
           AND curated_by NOT IN ('Curated:AI')
           AND curated_by NOT LIKE 'derived:%%'
           AND curated_by NOT LIKE 'auto:%%'
         LIMIT 1
    """, (fid,))
    return cur.fetchone() is not None


def clear_auto_curate_for_beach(cur, fid: int) -> int:
    """Reset any prior AI auto-curate selections on this beach so re-runs
    converge. Preserves Curated:Human + derived:sibling."""
    cur.execute("""
        UPDATE public.beach_photos
           SET curated_at = NULL, curated_by = NULL
         WHERE arena_group_id = %s
           AND (curated_by = 'Curated:AI' OR curated_by LIKE 'auto:%%')
    """, (fid,))
    return cur.rowcount


def auto_curate_beach(cur, fid: int, n: int, dry_run: bool = False) -> dict:
    """Auto-curate a single beach. Returns counts dict."""
    if beach_has_human_curation(cur, fid):
        return {"status": "skipped_human", "selected": 0, "cleared": 0}

    cur.execute("SELECT public.get_beach_photos_diverse(%s, %s)", (fid, n))
    row = cur.fetchone()
    selection = row[0] if row else []
    if not selection:
        return {"status": "no_photos", "selected": 0, "cleared": 0}

    # Filter to entries that actually have an id (defensive — should always be there post-v3)
    picks = [(p["id"], i + 1) for i, p in enumerate(selection) if "id" in p]
    if not picks:
        return {"status": "no_ids", "selected": 0, "cleared": 0}

    cleared = 0
    if not dry_run:
        cleared = clear_auto_curate_for_beach(cur, fid)
        method = "Curated:AI"  # was auto:n=<N>; collapsed to Curated:AI 2026-05-20
        for photo_id, rank in picks:
            cur.execute("""
                UPDATE public.beach_photos
                   SET curated_at = now(),
                       curated_by = %s,
                       sort_order = %s
                 WHERE id = %s
            """, (method, rank, photo_id))

    return {"status": "selected", "selected": len(picks), "cleared": cleared}


def main():
    ap = argparse.ArgumentParser(description="Auto-curate N best+diverse photos per beach")
    ap.add_argument("--n",        type=int, default=6, help="Photos per beach (default 6)")
    ap.add_argument("--state",    help="Limit to one state (e.g., WA)")
    ap.add_argument("--fid",      type=int, help="Single beach FID")
    ap.add_argument("--fids",     help="Comma-separated list of beach FIDs (e.g., gap-beach scope)")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Plan + report counts; no writes")
    ap.add_argument("--reset",    action="store_true",
                    help="Clear auto-curate selections for in-scope beaches and re-run "
                         "(human curations are NEVER cleared)")
    ap.add_argument("--chunk",    type=int, default=100, help="Commit per N beaches")
    args = ap.parse_args()

    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            # Build target set
            if args.fid:
                fids = [args.fid]
            elif args.fids:
                fids = [int(x) for x in args.fids.split(",") if x.strip()]
            else:
                sql = """
                  SELECT b.fid FROM public.beaches_gold b
                   WHERE b.is_active = true
                """
                params: tuple = ()
                if args.state:
                    sql += " AND b.state = %s"; params = (args.state,)
                sql += " ORDER BY b.fid"
                cur.execute(sql, params)
                fids = [r[0] for r in cur.fetchall()]

        print(f"Auto-curate scope: {len(fids)} beaches  n={args.n}  "
              f"dry_run={args.dry_run}  reset={args.reset}", flush=True)

        if args.reset and not args.dry_run:
            print("  --reset: clearing prior auto:* selections for in-scope beaches first…",
                  flush=True)
            for i in range(0, len(fids), args.chunk):
                batch = fids[i:i + args.chunk]
                conn = _ping_or_reconnect(conn)
                with conn.cursor() as cur:
                    for fid in batch:
                        if not beach_has_human_curation(cur, fid):
                            clear_auto_curate_for_beach(cur, fid)
                conn.commit()
            print("  reset complete.", flush=True)

        counts = {"selected": 0, "skipped_human": 0, "no_photos": 0, "no_ids": 0}
        beaches_with_selections = 0
        t0 = time.time()

        # Process in chunks; reconnect between chunks to survive pgbouncer
        # idle timeout on long full-catalog runs.
        for i, fid in enumerate(fids, 1):
            if (i - 1) % args.chunk == 0:
                conn = _ping_or_reconnect(conn)
            with conn.cursor() as cur:
                r = auto_curate_beach(cur, fid, args.n, dry_run=args.dry_run)
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            if r["status"] == "selected":
                counts["selected_photos"] = counts.get("selected_photos", 0) + r["selected"]
                beaches_with_selections += 1
            if i % args.chunk == 0:
                if not args.dry_run:
                    conn.commit()
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                print(f"  {i}/{len(fids)} processed  "
                      f"selected={beaches_with_selections}  "
                      f"skipped_human={counts.get('skipped_human', 0)}  "
                      f"no_photos={counts.get('no_photos', 0)}  "
                      f"({rate:.1f}/s)", flush=True)
        if not args.dry_run:
            conn.commit()

        print(f"\n=== SUMMARY ===", flush=True)
        print(f"  beaches processed:       {len(fids)}", flush=True)
        print(f"  selected (auto-curated): {counts.get('selected', 0)}", flush=True)
        print(f"  total photos selected:   {counts.get('selected_photos', 0)}", flush=True)
        print(f"  skipped (human owns):    {counts.get('skipped_human', 0)}", flush=True)
        print(f"  no photos available:     {counts.get('no_photos', 0)}", flush=True)
        if args.dry_run:
            print(f"\n  [dry-run] no writes performed", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
