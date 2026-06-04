"""triage.py — wrap triage_dog_park_needs_review.py as a pipeline op.

For each dog_park_discovery_queue row with status='needs_review' (string
fuzzy 0.60-0.79), Google Places geocode + ST_Distance against best_match_fid:
  <150m → flip status='duplicate' (same park, name variant)
  >500m → flip status='pending'  (real new park; ingest will pick up next run)
  150-500m → keep needs_review (genuinely ambiguous)

Per pin [[dog-park-coverage-playbook]] arena-style spatial > string fuzzy.
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
from scripts.dog_park_pipeline._inproc import call_main


def triage_needs_review(state: str, apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)

    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM public.dog_park_discovery_queue dpdq
              JOIN public.operators op ON op.id = dpdq.operator_id AND op.is_canonical = true
              JOIN public.dog_parks_gold dpg ON dpg.fid = dpdq.best_match_fid
             WHERE dpdq.status = 'needs_review' AND dpg.state = %s
        """, (state,))
        n_before = cur.fetchone()[0]
    finally:
        conn.close()

    # Defensive sys.path + importlib cache invalidation (Dagster namespace-
    # package cache bug — same fix as walk.py).
    import importlib
    _repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, _repo)
    importlib.invalidate_caches()
    from scripts.triage_dog_park_needs_review import main as triage_main
    args = ["--apply"] if apply else []
    exit_code, stdout = call_main(triage_main, "triage_dog_park_needs_review.py", args)

    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM public.dog_park_discovery_queue dpdq
              JOIN public.operators op ON op.id = dpdq.operator_id AND op.is_canonical = true
              JOIN public.dog_parks_gold dpg ON dpg.fid = dpdq.best_match_fid
             WHERE dpdq.status = 'needs_review' AND dpg.state = %s
        """, (state,))
        n_after = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "op": "triage_needs_review",
        "state": state,
        "exit_code": exit_code,
        "n_needs_review_before": n_before,
        "n_needs_review_after": n_after,
        "n_resolved": n_before - n_after,
        "stdout_tail": stdout[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
