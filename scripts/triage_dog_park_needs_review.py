"""triage_dog_park_needs_review.py — spatial triage for needs_review queue rows.

For each `dog_park_discovery_queue` row with status='needs_review' (string
fuzzy match 0.60-0.79 — ambiguous):

  1. Google Places searchText → lat/lon for catalog_park_name in city
  2. Compute distance to best_match_fid's geom
  3. Reclass:
       <150m  → status='duplicate' (same park as best_match)
       >500m  → status='pending'   (real new park, route to ingest)
       150-500m → keep as 'needs_review' (genuinely ambiguous)

Per Franz 2026-05-25 LATE — arena-style spatial dedup is stronger than
string fuzzy alone.
"""
from __future__ import annotations
import argparse, json, os, sys
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.harvest.geocode import google_places_searchtext, derive_city_state


def triage_row(cur, row: dict, apply: bool) -> str:
    """Returns outcome string."""
    qid = row['id']
    name = row['catalog_park_name']
    best_fid = row['best_match_fid']
    op_name = row['operator_name'] or ''
    city, state = derive_city_state(op_name)
    state = state or 'CA'

    query = f"{name}, {city}, {state}" if city else f"{name}, {state}"
    print(f"\n[queue {qid}] '{name[:40]}' best_match=fid{best_fid}")
    print(f"  Places query: {query}")

    place = google_places_searchtext(query)
    if not place or not place.get('location'):
        print(f"  [-] google_places: no hit → keep as needs_review")
        return "no_geocode"
    lat = float(place['location']['latitude'])
    lng = float(place['location']['longitude'])

    # Distance to best_match_fid
    cur.execute("""
        SELECT ST_Distance(
                 ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                 geom::geography) AS dist_m,
               name
          FROM public.dog_parks_gold WHERE fid = %s
    """, (lng, lat, best_fid))
    r = cur.fetchone()
    if not r or r['dist_m'] is None:
        print(f"  [-] best_match_fid={best_fid} has no geom — keep needs_review")
        return "no_target_geom"
    dist_m = r['dist_m']
    print(f"  geocoded ({lat:.5f},{lng:.5f}) → {dist_m:.0f}m from \"{r['name']}\"")

    if dist_m < 150:
        outcome = ("duplicate", f"spatial_match_{dist_m:.0f}m_of_fid_{best_fid}")
        print(f"  → DUPLICATE (within 150m)")
    elif dist_m > 500:
        outcome = ("pending", f"spatial_distinct_{dist_m:.0f}m_from_fid_{best_fid}")
        print(f"  → PENDING (>500m apart, real new park)")
    else:
        outcome = (None, f"spatial_ambiguous_{dist_m:.0f}m_from_fid_{best_fid}")
        print(f"  → keep needs_review (150-500m, genuinely ambiguous)")

    if apply and outcome[0]:
        cur.execute("""
            UPDATE public.dog_park_discovery_queue
               SET status = %s,
                   review_notes = %s,
                   updated_at = now()
             WHERE id = %s
        """, (outcome[0], outcome[1], qid))
    elif apply:
        cur.execute("""
            UPDATE public.dog_park_discovery_queue
               SET review_notes = %s, updated_at = now()
             WHERE id = %s
        """, (outcome[1], qid))
    return outcome[0] or "ambiguous"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    conn = connect(); conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT id, operator_id, operator_name, catalog_park_name,
               best_similarity, best_match_fid
          FROM public.dog_park_discovery_queue
         WHERE status = 'needs_review'
         ORDER BY best_similarity DESC
         {f'LIMIT {int(args.limit)}' if args.limit else ''}
    """)
    rows = list(cur.fetchall())
    print(f"needs_review pool: {len(rows)}; apply={args.apply}")

    summary = {"duplicate": 0, "pending": 0, "ambiguous": 0,
               "no_geocode": 0, "no_target_geom": 0}
    for row in rows:
        outcome = triage_row(cur, row, args.apply)
        summary[outcome] = summary.get(outcome, 0) + 1
        if args.apply:
            conn.commit()

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:18} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
