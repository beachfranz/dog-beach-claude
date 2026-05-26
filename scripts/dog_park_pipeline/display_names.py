"""display_names.py — city-prefix generic park names via display_name_override.

For parks named "Dog Park" / "Bark Park" / "Small Dog Park" / etc., set
display_name_override = "<city> <name>" so consumer surface shows "Lake
Elsinore Small Dog Park" instead of "Small Dog Park". Also makes the LLM
extractor's web_search query more disambiguatable.
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect

GENERIC = (
    'Dog Park', 'Bark Park', 'Dog Run', 'Dog Area', 'Off-Leash',
    'Off Leash', 'Small Dog Park', 'Small Dog Area',
    'Large Dog Park', 'Large Dog Area', 'Off-Leash Area',
    'Optional Leash Area', 'Green Belt',
)


def generic_name_display_override(state: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE public.dog_parks_gold
               SET display_name_override = address_city || ' ' || name
             WHERE state = %s AND is_active AND is_scoreable
               AND address_city IS NOT NULL
               AND trim(name) = ANY(%s)
               AND (display_name_override IS NULL OR display_name_override = name)
        """, (state, list(GENERIC)))
        n_updated = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return {
        "op": "generic_name_display_override",
        "state": state,
        "n_updated": n_updated,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
