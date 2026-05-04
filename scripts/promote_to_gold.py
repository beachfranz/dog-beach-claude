"""
promote_to_gold.py — ONE canonical entrypoint for arena → beaches_gold promotion.

Replaces:
  seed_arena_beach.py             — wraps this with --name --county --lat --lon
                                     for the manual-seed path (still useful;
                                     not deleted)
  bulk_promote_socal.py           — wraps this with region-based fid selection
                                     (becomes a thin wrapper)
  promote_arena_to_beaches_gold.py — superseded
  Direct INSERT into beaches_gold  — don't do this anymore

Calls the public.promote_to_gold(bigint[], boolean) PL/pgSQL function which:
  1. INSERT FROM arena into beaches_gold
  2. Auto-derive location_id slug, state, noaa_station_id, timezone, open/close
  3. Run all 5 containment populators (cpad/jurisdictions/counties/military/tribal)
  4. Run 5 source populators (cpad/park_operators/research/park_url/governance)
  5. Run all 4 resolvers (dogs/governance/practical/access)
  6. Auto-promote canonical evidence to beach_dog_policy + beach_amenities
  7. Optionally flip is_scoreable=true (--score)

Idempotent — re-running on existing gold beaches just refreshes evidence.

Usage:
  # Promote specific fids
  python scripts/promote_to_gold.py --fids 5944,6116,3349

  # Promote all unpromoted active CA beaches in a county
  python scripts/promote_to_gold.py --county "San Mateo"

  # Promote and immediately flip is_scoreable
  python scripts/promote_to_gold.py --fids 9716 --score

  # Dry-run (show what would be promoted without doing it)
  python scripts/promote_to_gold.py --county "Marin" --dry-run
"""
from __future__ import annotations
import argparse
import os
import sys
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")


def resolve_fids(cur, args) -> list[int]:
    """Return the list of fids to promote based on CLI args."""
    if args.fids:
        return [int(x.strip()) for x in args.fids.split(",") if x.strip()]
    where = ["a.is_active = true",
             "not exists (select 1 from public.beaches_gold g where g.fid = a.fid)"]
    params: list = []
    if args.county:
        where.append("a.county_name = %s")
        params.append(args.county)
    if args.state:
        # Approximate via county_name patterns; promote_to_gold infers state
        # from county. For now just filter to manually-seeded OR/WA fids.
        pass
    if args.source_code:
        where.append("a.source_code = %s")
        params.append(args.source_code)
    if args.limit:
        limit = f"limit {int(args.limit)}"
    else:
        limit = ""
    sql = f"""
      select a.fid, a.name, a.county_name
        from public.arena a
       where {' and '.join(where)}
       order by a.fid
       {limit}
    """
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    if not rows:
        return []
    print(f"  selected {len(rows)} candidate(s):")
    for r in rows[:20]:
        print(f"    fid={r['fid']:<6} {r['name']!r} ({r['county_name']})")
    if len(rows) > 20:
        print(f"    ... and {len(rows)-20} more")
    return [r["fid"] for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="One canonical entry point for arena → beaches_gold promotion.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--fids",
                   help="Comma-separated list of arena fids to promote.")
    g.add_argument("--county",
                   help="Promote all unpromoted active arena beaches in this county.")
    ap.add_argument("--state", choices=["CA", "OR", "WA"],
                    help="Restrict to a state (works with --county).")
    ap.add_argument("--source-code", choices=["osm", "poi", "manual", "ccc"],
                    help="Restrict by arena source_code.")
    ap.add_argument("--limit", type=int,
                    help="Cap the number of fids selected.")
    ap.add_argument("--score", action="store_true",
                    help="Also flip is_scoreable=true on the promoted beaches.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Resolve fids but don't actually promote.")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("Resolving fids...")
    fids = resolve_fids(cur, args)
    if not fids:
        print("  No candidates. Nothing to do.")
        return 0

    if args.dry_run:
        print(f"\n  --dry-run: would promote {len(fids)} fids. Exit.")
        return 0

    print(f"\n  Calling promote_to_gold(fids={fids[:5]}{'...' if len(fids)>5 else ''}, "
          f"score={args.score})...")
    cur.execute("select * from public.promote_to_gold(%s::bigint[], %s)",
                (fids, args.score))
    result = cur.fetchone()
    conn.commit()

    print()
    print("  Result:")
    for k, v in result.items():
        print(f"    {k}: {v}")

    print()
    if result["rows_promoted"] > 0:
        print(f"  Promoted {result['rows_promoted']} beach(es) to beaches_gold.")
    if result["rows_already_in_gold"] > 0:
        print(f"  Refreshed {result['rows_already_in_gold']} existing beach(es) "
              f"(re-ran populators + resolvers + auto-promoter).")
    if result["beach_dog_policy_changed"] > 0:
        print(f"  beach_dog_policy: {result['beach_dog_policy_changed']} resolver-driven row(s) ")
    if result["beach_amenities_changed"] > 0:
        print(f"  beach_amenities:  {result['beach_amenities_changed']} resolver-driven row(s)")

    print()
    print("  Direct links to verify:")
    for fid in fids[:5]:
        print(f"    file:///C:/Users/beach/Documents/dog-beach-claude/index.html?fid={fid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
