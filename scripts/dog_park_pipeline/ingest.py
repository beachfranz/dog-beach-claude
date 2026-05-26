"""ingest.py — wrap ingest_dog_park_discovery_queue.py.

Geocodes pending queue rows via Google Places searchText, INSERTs new
dog_parks_gold rows (with website=catalog_park_url so the extractor's
OSM-shortcut route processes them on next pass).
"""
from __future__ import annotations
import os, subprocess, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def ingest_discovery_queue(state: str, apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)

    # Count pending for state's operators before
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*)
              FROM public.dog_park_discovery_queue
             WHERE status = 'pending'
        """)
        n_pending_before = cur.fetchone()[0]

        cur.execute("""
            SELECT count(*) FROM public.dog_parks_gold
             WHERE state = %s AND is_active AND is_scoreable
        """, (state,))
        n_gold_before = cur.fetchone()[0]
    finally:
        conn.close()

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cmd = [sys.executable, os.path.join(repo_root, "scripts", "ingest_dog_park_discovery_queue.py")]
    if apply:
        cmd.append("--apply")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    # Count gold after
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM public.dog_parks_gold
             WHERE state = %s AND is_active AND is_scoreable
        """, (state,))
        n_gold_after = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "op": "ingest_discovery_queue",
        "state": state,
        "exit_code": result.returncode,
        "n_pending_before": n_pending_before,
        "n_new_gold_rows": n_gold_after - n_gold_before,
        "stdout_tail": (result.stdout or "")[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
