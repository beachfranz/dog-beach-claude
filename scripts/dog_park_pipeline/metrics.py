"""metrics.py — write per-op run metrics for Dagster observability."""
from __future__ import annotations
import os, sys, json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def write_run_metric(state: str, op: str, started_at: datetime,
                     ended_at: datetime, metrics: dict) -> None:
    """Insert one row into dog_park_coverage_runs. Best-effort — swallows DB errors."""
    try:
        conn = connect(); conn.set_client_encoding("UTF8")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.dog_park_coverage_runs
                (state, op, started_at, ended_at, metrics)
            VALUES (%s, %s, %s, %s, %s::jsonb)
        """, (state, op, started_at, ended_at, json.dumps(metrics, default=str)))
        conn.commit()
        conn.close()
    except Exception as e:
        # Metrics table may not exist yet on first run — don't crash the op
        print(f"  [metrics] write failed (non-fatal): {e}", flush=True)
