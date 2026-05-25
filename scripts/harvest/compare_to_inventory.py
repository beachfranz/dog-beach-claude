"""Harvest stage: compare extracted locations to existing inventory.

For each operator_extracted_locations row with extraction_status='success'
and lat/lng populated, call the content_type's inventory_match_query() to
find the best match in the canonical inventory (osm_dog_parks for dog
parks; arena for beaches; etc.). Write match status + score back.

Usage:
    python -m scripts.harvest.compare_to_inventory --content-type dog_park
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect
from scripts.harvest import content_types as ctmod


def fetch_pending(conn, content_type: str):
    sql = """
      SELECT id, name, address, lat, lng, attributes
        FROM public.operator_extracted_locations
       WHERE content_type = %s
         AND extraction_status = 'success'
       ORDER BY operator_id, id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (content_type,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--content-type", required=True)
    args = ap.parse_args()

    ct = ctmod.get(args.content_type)
    conn = connect(application_name=f"harvest_compare_{args.content_type}")
    rows = fetch_pending(conn, args.content_type)
    if not rows:
        print("No rows to compare.")
        return

    print(f"Comparing {len(rows)} {args.content_type} rows against {ct.INVENTORY_TABLE}...")
    counts = {}
    with conn.cursor() as cur:
        for row in rows:
            result = ct.inventory_match_query(cur, row)
            counts[result["status"]] = counts.get(result["status"], 0) + 1
            with conn.cursor() as upd:
                upd.execute("""
                    UPDATE public.operator_extracted_locations
                       SET inventory_match_status = %s,
                           inventory_match_id = %s,
                           inventory_match_score = %s,
                           inventory_match_components = %s::jsonb,
                           compared_at = now()
                     WHERE id = %s
                """, (result["status"], result["match_id"], result["match_score"],
                      json.dumps(result.get("components") or {}), row["id"]))
        conn.commit()

    print()
    for status, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {status:<20} {n}")


if __name__ == "__main__":
    main()
