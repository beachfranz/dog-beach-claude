"""
pick_gold_set_v3.py — populate gold_set_membership with the v3 candidates.

Picks 25 SoCal scoreable beaches across 5 archetypes (5 each) and writes them
to public.gold_set_membership(set_name='v3', archetype=...). Idempotent —
re-running with --apply just refreshes membership for any beach that's still
in the picks.

Archetypes:
  1. off_leash_dog_beach — famous off-leash spots; clearest positive signal
  2. geographic_sections — beaches with internal sectioned dog rules
  3. hard_no_dogs        — state parks etc. with explicit prohibition
  4. time_season_restricted — temporal-rule cases
  5. edge_cases          — lake / harbor / private / unusual mixes

Picks are hand-curated by name match against beaches_gold; we don't yet
have a programmatic way to detect archetype from data. Swap any pick by
editing the PICKS dict below.

Usage:
  python scripts/pick_gold_set_v3.py            # dry-run, prints planned picks
  python scripts/pick_gold_set_v3.py --apply    # writes to gold_set_membership
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
PG = dict(
    host=p.hostname, port=p.port or 5432, user=p.username,
    password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(p.path or "/postgres").lstrip("/"), sslmode="require",
)

DEFAULT_SET_NAME = "v3"

# Each archetype lists name patterns (ILIKE) tried in order until 5 distinct
# beaches are found in beaches_gold WHERE is_active AND is_scoreable AND
# state='CA' AND county_name in SoCal counties. First match wins per slot.
# Lists are intentionally long enough that --exclude-sets can pull a second
# batch (v3b) from the leftovers.
PICKS = {
    "off_leash_dog_beach": [
        "coronado dog beach",
        "rosie's dog beach",
        "del mar dog beach",
        "ocean beach dog beach",
        "fiesta island",         # off-leash dog park, San Diego
        # extended for v3b — only ~5 famous off-leash beaches in SoCal so v3b
        # may run short here; that's expected
        "dog beach",             # catches Huntington Dog (6212) + Ocean Beach Dog (8358)
        "carlsbad lagoon dog",
        "huntington dog beach",
    ],
    "geographic_sections": [
        # Beaches whose famous off-leash segment is encoded as a sub-section
        # of a larger beach in our data (the v3 sections[] use case)
        "huntington city beach",     # contains Huntington Dog Beach segment
        "huntington state beach",
        "bolsa chica state beach",
        "long beach city beach",
        "cardiff state beach",
        # extended for v3b — multi-section state beaches NOT in v3
        "corona del mar state beach",
        "torrey pines state beach",
        "silver strand state beach",
        "leo carillo state beach",   # note the misspelling matches arena fid 3671
        "imperial beach",
        "topanga county beach",
        "newport municipal",
        "el capitan beach",
    ],
    "hard_no_dogs": [
        "crystal cove",
        "will rogers state beach",
        "salt creek",
        "carpinteria state beach",
        "newport municipal beach",   # municipal rule disallows on-beach dogs
        # extended for v3b — state beaches usually prohibit dogs
        "el matador",
        "refugio state beach",
        "mcgrath state beach",
        "emma wood state beach",
        "san buenaventura state beach",
        "border field state park",
        "mandalay state beach",
        "dan blocker county beach",
        "royal palms",
    ],
    "time_season_restricted": [
        "venice beach",
        "santa monica state beach",
        "manhattan beach",
        "hermosa city beach",
        "torrance county beach",
        # extended for v3b
        "zuma beach",
        "redondo beach pier",
        "redondo beach state park",
        "dockweiler state beach",
        "mothers beach",
        "playa del rey",
        "el segundo beach",
    ],
    "edge_cases": [
        "mission beach",
        "south mission beach",
        "cameo shores",          # private (Newport)
        "baby beach",            # Dana Point harbor
        "north star beach",      # Newport bay
        # extended for v3b
        "newport dunes",
        "dana point headlands",
        "north lake beach club",  # private lake beach in OC
        "shaws cove",
        "windansea",
        "blacks beach",
        "frenchys cove beach",   # Channel Islands
        "harbor cove beach",
        "deer creek beach",
    ],
}


def pick_for_archetype(cur, patterns: list[str], excluded: set[int], target_n: int = 5):
    """Return up to target_n (fid, name, county) tuples matching patterns, skipping
    fids already chosen for another archetype (excluded set)."""
    chosen = []
    for pat in patterns:
        if len(chosen) >= target_n:
            break
        cur.execute("""
            select fid, name, county_name
              from public.beaches_gold
             where is_active and is_scoreable and state='CA'
               and county_name in ('Santa Barbara','Ventura','Los Angeles','Orange','San Diego')
               and name ilike %s
               and fid <> all(%s)
             order by name
        """, (f"%{pat}%", list(excluded) or [-1]))
        rows = cur.fetchall()
        for r in rows:
            if r['fid'] in excluded:
                continue
            chosen.append((r['fid'], r['name'], r['county_name'], pat))
            excluded.add(r['fid'])
            if len(chosen) >= target_n:
                break
    return chosen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set-name", default=DEFAULT_SET_NAME,
                    help="Target set_name in gold_set_membership (default 'v3').")
    ap.add_argument("--exclude-sets", default="",
                    help="Comma-separated set_names whose fids are excluded from this pick (e.g. 'v3' to seed v3b).")
    ap.add_argument("--apply", action="store_true",
                    help="Write picks to public.gold_set_membership.")
    ap.add_argument("--clear-first", action="store_true",
                    help="Delete existing membership rows for --set-name before inserting (drops curator state for that set).")
    args = ap.parse_args()
    set_name = args.set_name

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    excluded: set[int] = set()
    if args.exclude_sets:
        ex_sets = [s.strip() for s in args.exclude_sets.split(",") if s.strip()]
        cur.execute("""
            select fid from public.gold_set_membership
             where set_name = any(%s) and not excluded
        """, (ex_sets,))
        for r in cur.fetchall():
            excluded.add(r["fid"])
        print(f"excluding {len(excluded)} fids already in sets {ex_sets}")

    plan: list[tuple[str, list[tuple]]] = []
    for archetype, patterns in PICKS.items():
        picks = pick_for_archetype(cur, patterns, excluded, target_n=5)
        plan.append((archetype, picks))

    print(f"\nPlan for set_name='{set_name}':\n")
    total = 0
    for archetype, picks in plan:
        print(f"  [{archetype}] {len(picks)}/5")
        for fid, name, county, pat in picks:
            print(f"    fid={fid:>5}  {name[:36]:<36}  ({county})   <- matched '{pat}'")
        total += len(picks)
        if len(picks) < 5:
            print(f"    [!] short {5 - len(picks)} - extend PICKS[{archetype!r}] with more patterns")

    print(f"\nTotal: {total}/25")

    if not args.apply:
        print("\n(dry-run; rerun with --apply to write to gold_set_membership)")
        return 0

    if args.clear_first:
        cur.execute("delete from public.gold_set_membership where set_name = %s", (set_name,))
        print(f"  cleared {cur.rowcount} existing rows for set_name='{set_name}'")

    n_inserted = 0
    for archetype, picks in plan:
        for fid, name, county, _pat in picks:
            cur.execute("""
                insert into public.gold_set_membership (set_name, fid, archetype, notes)
                values (%s, %s, %s, %s)
                on conflict (set_name, fid) do update
                  set archetype = excluded.archetype,
                      notes     = excluded.notes,
                      excluded  = false
            """, (set_name, fid, archetype, f"picked by name match"))
            n_inserted += 1
    conn.commit()
    print(f"\nupserted {n_inserted} rows into gold_set_membership.")

    cur.execute("""
      select archetype, count(*) n
        from public.gold_set_membership
       where set_name = %s and not excluded
       group by 1 order by 1
    """, (set_name,))
    print("\nfinal membership:")
    for r in cur.fetchall():
        print(f"  {r['archetype']:<24} {r['n']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
