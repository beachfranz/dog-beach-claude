"""reclassify.py — flip mis-tagged + private-residence parks to is_active=false.

Two heuristics:
  A. area_m2 < 200 — apartment dog runs, mis-tagged park entrances, etc.
  B. name contains HOA / apartment / condo / villa / residents / complex

CA tested 2026-05-25 LATE: 23 parks flipped, denominator dropped 428→405,
coverage moved +5 percentage points.
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect

PRIVATE_NAME_PATTERNS = (
    '%HOA%', '%residents%', '%condo%', '%apartment%',
    '%complex%', '%villa%',
)


def reclassify_obvious_junk(state: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()

        cur.execute("""
            UPDATE public.dog_parks_gold
               SET is_active = false,
                   inactive_reason = 'tiny_area_likely_mistagged_or_private'
             WHERE state = %s AND is_active AND is_scoreable
               AND area_m2 IS NOT NULL AND area_m2 < 200
        """, (state,))
        n_tiny = cur.rowcount

        n_private = 0
        for pat in PRIVATE_NAME_PATTERNS:
            cur.execute("""
                UPDATE public.dog_parks_gold
                   SET is_active = false,
                       inactive_reason = 'private_residential_complex'
                 WHERE state = %s AND is_active AND is_scoreable
                   AND name ILIKE %s
            """, (state, pat))
            n_private += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    return {
        "op": "reclassify_obvious_junk",
        "state": state,
        "n_tiny_flipped": n_tiny,
        "n_private_flipped": n_private,
        "n_total_flipped": n_tiny + n_private,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
