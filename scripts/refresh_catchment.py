"""Pipeline orchestrator for the catchment → state_pct → scoring_tier
cascade.

Wraps the `public.refresh_catchment_cascade(state)` SQL function, looping
across states with proper logging, timing, and retry. Used after data
that affects catchment (OSM amenity loads, beach moves, new beach
ingestion) lands so the derived urbanicity + scoring-cadence signals
stay current.

Usage:
   python scripts/refresh_catchment.py --state CA
   python scripts/refresh_catchment.py --states CA,WA,OR
   python scripts/refresh_catchment.py --all      # cycle every scored state
   python scripts/refresh_catchment.py --dry-run --all   # show what would run

Idempotent. Re-running picks up correctly. Statement timeout set to 30 min
per state (the cascade does N spatial recomputes; CA at ~750 beaches
takes single-digit minutes in practice but 30 min covers slow networks).
"""
from __future__ import annotations
import argparse
import os
import sys
import time
import urllib.parse
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

STATEMENT_TIMEOUT = "30min"


def list_scored_states() -> list[str]:
    """Return states that currently have any beaches with catchment_score set."""
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""
            select state, count(*) n
              from public.beaches_gold
             where is_active and catchment_score is not null
               and state is not null
             group by state
             order by n desc
        """)
        return [r[0] for r in cur.fetchall()]


def refresh_state(state: str) -> tuple[int, int, int, float]:
    """Run the cascade for one state. Returns (catchment, state_pct, tier, secs)."""
    t0 = time.time()
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(f"set statement_timeout = '{STATEMENT_TIMEOUT}'")
            cur.execute("select * from public.refresh_catchment_cascade(%s)", (state,))
            row = cur.fetchone()
    return int(row[0]), int(row[1]), int(row[2]), time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--state",  help="single state code (CA, OR, ...)")
    grp.add_argument("--states", help="comma-separated state codes")
    grp.add_argument("--all",    action="store_true",
                     help="all states with scored beaches")
    ap.add_argument("--dry-run", action="store_true",
                    help="list the states that would be refreshed, don't run")
    args = ap.parse_args()

    if args.state:
        targets = [args.state]
    elif args.states:
        targets = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    else:
        targets = list_scored_states()

    if args.dry_run:
        print(f"[dry-run] would refresh {len(targets)} state(s): {','.join(targets)}")
        return

    print(f"refreshing {len(targets)} state(s): {','.join(targets)}", flush=True)
    t_overall = time.time()
    totals = {"catchment": 0, "state_pct": 0, "tier": 0}
    failed: list[tuple[str, str]] = []

    for i, st in enumerate(targets, 1):
        try:
            c, sp, t, secs = refresh_state(st)
            totals["catchment"] += c
            totals["state_pct"] += sp
            totals["tier"]      += t
            print(f"  [{i:2d}/{len(targets)}] {st}: "
                  f"catchment={c:5d}  state_pct={sp:5d}  tier={t:5d}  "
                  f"({secs:5.1f}s)", flush=True)
        except Exception as e:
            failed.append((st, str(e)[:200]))
            print(f"  [{i:2d}/{len(targets)}] {st}: FAILED {str(e)[:200]}",
                  file=sys.stderr, flush=True)

    print(f"\n=== DONE === states={len(targets)-len(failed)}/{len(targets)}  "
          f"catchment={totals['catchment']}  state_pct={totals['state_pct']}  "
          f"tier={totals['tier']}  wall={time.time()-t_overall:.1f}s", flush=True)
    if failed:
        print(f"failed: {','.join(s for s,_ in failed)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
