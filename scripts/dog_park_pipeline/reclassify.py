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
    """Two-phase: (1) preview candidates with sample, (2) apply.
    Preview is logged in metrics for human inspection in Dagster UI.

    PROTECTED — these stay scoreable even if heuristics would match:
      - "Pet Area" / "Pet Exercise Area" (rest-stop facilities, road-tripper
        use case per [[dog-park-coverage-playbook]])
      - "Love's Dog Park" (truck-stop pet zones, same logic)
    """
    started = datetime.now(timezone.utc)
    conn = connect(); conn.set_client_encoding("UTF8")
    PROTECTED_NAME_PATTERNS = (
        '%Pet Area%', '%Pet Exercise%', '%Love%Dog Park%',
        '%Rest Area%', '%Rest Stop%',
    )

    def _protected_clause():
        # Build NOT (name ILIKE pat1 OR name ILIKE pat2 OR ...) clause + params
        clause = " AND NOT (" + " OR ".join(["name ILIKE %s"] * len(PROTECTED_NAME_PATTERNS)) + ")"
        return clause, list(PROTECTED_NAME_PATTERNS)

    try:
        cur = conn.cursor()

        # PREVIEW: tiny-area candidates
        prot_clause, prot_params = _protected_clause()
        cur.execute(f"""
            SELECT fid, name, area_m2, address_city
              FROM public.dog_parks_gold
             WHERE state = %s AND is_active AND is_scoreable
               AND area_m2 IS NOT NULL AND area_m2 < 200
               {prot_clause}
             ORDER BY area_m2 ASC LIMIT 25
        """, (state, *prot_params))
        tiny_preview = [{"fid": r[0], "name": r[1], "area_m2": float(r[2] or 0),
                         "city": r[3]} for r in cur.fetchall()]

        # PREVIEW: private/residential candidates
        private_preview = []
        for pat in PRIVATE_NAME_PATTERNS:
            cur.execute(f"""
                SELECT fid, name, address_city FROM public.dog_parks_gold
                 WHERE state = %s AND is_active AND is_scoreable
                   AND name ILIKE %s
                   {prot_clause}
                 LIMIT 10
            """, (state, pat, *prot_params))
            for r in cur.fetchall():
                private_preview.append({"fid": r[0], "name": r[1],
                                        "city": r[2], "matched_pattern": pat})

        # APPLY (tiny)
        cur.execute(f"""
            UPDATE public.dog_parks_gold
               SET is_active = false,
                   inactive_reason = 'tiny_area_likely_mistagged_or_private'
             WHERE state = %s AND is_active AND is_scoreable
               AND area_m2 IS NOT NULL AND area_m2 < 200
               {prot_clause}
        """, (state, *prot_params))
        n_tiny = cur.rowcount

        # APPLY (private)
        n_private = 0
        for pat in PRIVATE_NAME_PATTERNS:
            cur.execute(f"""
                UPDATE public.dog_parks_gold
                   SET is_active = false,
                       inactive_reason = 'private_residential_complex'
                 WHERE state = %s AND is_active AND is_scoreable
                   AND name ILIKE %s
                   {prot_clause}
            """, (state, pat, *prot_params))
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
        "tiny_preview": tiny_preview,
        "private_preview": private_preview,
        "protected_patterns": list(PROTECTED_NAME_PATTERNS),
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
