"""seed_operators.py — read-only operator-coverage check for a state.

RETIRED as a writer 2026-10-13. It used to call add_canonical_operator()
for each city in state_operator_seeds.json and then backfill
inferred_operator_id by address_city. add_canonical_operator() has raised
NotImplementedError since the Phase 5a operator unification (2026-06-04),
so every city failed and no parks were attributed.

It is no longer needed: the walker and ingester now key on
COALESCE(canonical_operator_id, inferred_operator_id), and
canonical_operator_id is kept current by the tg_compute_dp_canonical_operator
trigger (_resolve_dog_park_operator — PAD-US / CPAD / TIGER city / county
polygons). City operator rows themselves come from the state launch's
operator seeding (populate_operators_for_state), not from here.

What it does now (no writes):
  - counts active parks with a canonical / inferred-only / no operator
  - lists registry cities that have no canonical city operator row, which
    is what the TIGER-city tier of the resolver needs to attribute a park

Kept under the same name so run.py / the Dagster dp_seed_operators asset
graph stays intact.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect

_SEEDS_PATH = Path(__file__).parent / "state_operator_seeds.json"


def seed_state_operators(state: str) -> dict:
    started = datetime.now(timezone.utc)

    try:
        seeds = json.loads(_SEEDS_PATH.read_text())
    except Exception as e:
        seeds = {}
        seeds_error = f"failed to read seeds: {e}"
    else:
        seeds_error = None
    cities = seeds.get(state, []) if isinstance(seeds.get(state), list) else []

    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE canonical_operator_id IS NOT NULL),
                   count(*) FILTER (WHERE canonical_operator_id IS NULL
                                      AND inferred_operator_id IS NOT NULL)
              FROM public.dog_parks_gold
             WHERE state = %s AND is_active AND is_scoreable
        """, (state,))
        n_parks, n_canonical, n_inferred_only = cur.fetchone()

        missing = []
        for city in cities:
            cur.execute("""
                SELECT 1 FROM public.operators
                 WHERE state_code = %s AND is_canonical AND is_active
                   AND lower(canonical_name) IN (lower(%s), lower(%s), lower(%s))
                 LIMIT 1
            """, (state, f"City of {city}", f"Town of {city}", city))
            if cur.fetchone() is None:
                missing.append(city)
    finally:
        conn.close()

    out = {
        "op": "seed_operators",
        "state": state,
        "mode": "read_only_check",
        "n_parks": n_parks,
        "n_with_canonical_operator": n_canonical,
        "n_inferred_operator_only": n_inferred_only,
        "n_no_operator": n_parks - n_canonical - n_inferred_only,
        "n_cities_in_registry": len(cities),
        "registry_cities_missing_operator": missing,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
    if seeds_error:
        out["seeds_error"] = seeds_error
    return out
