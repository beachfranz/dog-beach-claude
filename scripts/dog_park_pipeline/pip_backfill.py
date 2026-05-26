"""pip_backfill.py — PIP-derive address_city for parks where it's null.

ST_Contains(jurisdictions.geom, dog_park.geom) — smallest containing polygon
(usually a city) wins. Cheap, idempotent, no LLM cost.
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def pip_address_city_backfill(state: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH pip AS (
              SELECT DISTINCT ON (dpg.fid) dpg.fid, j.name AS jur_name
                FROM public.dog_parks_gold dpg
                JOIN public.jurisdictions j ON ST_Contains(j.geom, dpg.geom)
               WHERE dpg.state = %s AND dpg.is_active
                 AND dpg.address_city IS NULL
                 AND dpg.geom IS NOT NULL
               ORDER BY dpg.fid, ST_Area(j.geom) ASC
            )
            UPDATE public.dog_parks_gold dpg
               SET address_city = pip.jur_name
              FROM pip
             WHERE dpg.fid = pip.fid
        """, (state,))
        n_updated = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return {
        "op": "pip_address_city_backfill",
        "state": state,
        "n_updated": n_updated,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
