"""walk.py — operator catalog walker (wraps walk_dog_park_operator_catalog.py).

For each top-N operator in the state (auto-discovered by park count),
web_search finds the operator's catalog → LLM enumerates per-park entries
→ fuzzy-match against gold → matched parks extract amenities; unmatched
queue to dog_park_discovery_queue for ingest pipeline.
"""
from __future__ import annotations
import os, subprocess, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def walk_catalogs(state: str, workers: int = 4, min_parks: int = 3,
                  apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)

    # Pre-count queue rows for delta
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM public.dog_park_discovery_queue")
        n_queue_before = cur.fetchone()[0]
    finally:
        conn.close()

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cmd = [
        sys.executable, os.path.join(repo_root, "scripts", "walk_dog_park_operator_catalog.py"),
        "--state", state,
        "--workers", str(workers),
        "--min-parks", str(min_parks),
        "--chunk", "3", "--sleep", "30",
    ]
    if apply:
        cmd.append("--apply")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

    # Re-count for delta
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM public.dog_park_discovery_queue")
        n_queue_after = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "op": "walk_catalogs",
        "state": state,
        "exit_code": result.returncode,
        "n_queue_added": n_queue_after - n_queue_before,
        "n_queue_total": n_queue_after,
        "stdout_tail": (result.stdout or "")[-2000:],
        "stderr_tail": (result.stderr or "")[-500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
