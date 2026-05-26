"""seed_operators.py — auto-seed city operators for a state from registry.

Reads state_operator_seeds.json + calls add_canonical_operator() for each
city the state hasn't seeded yet. Then PIP-backfills inferred_operator_id
on dog_parks_gold for parks in those cities (via address_city match).

Idempotent: cities already in public.operator are reused; parks already
attributed keep their inferred_operator_id.

Per OR/WA lesson 2026-05-25 LATE: without city operators in public.operator,
the walker step contributes 0 to coverage. Pre-seeding is the unblock.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
from scripts.common.operator import add_canonical_operator

_SEEDS_PATH = Path(__file__).parent / "state_operator_seeds.json"


def seed_state_operators(state: str) -> dict:
    started = datetime.now(timezone.utc)

    try:
        seeds = json.loads(_SEEDS_PATH.read_text())
    except Exception as e:
        return {
            "op": "seed_operators",
            "state": state,
            "error": f"failed to read seeds: {e}",
            "started_at": started.isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
        }

    cities = seeds.get(state, [])
    if not cities:
        return {
            "op": "seed_operators",
            "state": state,
            "n_cities_in_registry": 0,
            "note": f"no entries for {state} in state_operator_seeds.json — add cities to the registry first",
            "started_at": started.isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
        }

    seeded = []        # cities where helper actually inserted a row
    reused = []        # cities whose operator already existed
    failed = []        # (city, error)
    for city in cities:
        op_name = f"City of {city}"
        try:
            r = add_canonical_operator(
                name=op_name, op_type="city", state_code=state,
                plural_level="city", apply=True,
            )
            if r.get("singular_inserted"):
                seeded.append((city, r["singular_id"]))
            else:
                reused.append((city, r["singular_id"]))
        except Exception as e:
            failed.append((city, f"{type(e).__name__}: {e}"))

    # PIP-backfill inferred_operator_id on dog_parks_gold for parks in these cities.
    # Match by address_city (populated by pip_address_city_backfill op in step 2).
    conn = connect(); conn.set_client_encoding("UTF8")
    n_attributed = 0
    try:
        cur = conn.cursor()
        all_cities_with_ids = [(c, oid) for c, oid in seeded + reused]
        for city, op_id in all_cities_with_ids:
            cur.execute("""
                UPDATE public.dog_parks_gold
                   SET inferred_operator_id = %s,
                       attribution_method = 'seed_registry_city_match',
                       attribution_confidence = 'medium'
                 WHERE state = %s AND is_active AND is_scoreable
                   AND address_city = %s
                   AND inferred_operator_id IS NULL
            """, (op_id, state, city))
            n_attributed += cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return {
        "op": "seed_operators",
        "state": state,
        "n_cities_in_registry": len(cities),
        "n_seeded": len(seeded),
        "n_reused": len(reused),
        "n_failed": len(failed),
        "n_parks_newly_attributed": n_attributed,
        "seeded_cities": [c for c, _ in seeded],
        "reused_cities": [c for c, _ in reused],
        "failures": failed,
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
