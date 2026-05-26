"""ingest.py — wrap ingest_dog_park_discovery_queue.py.

Geocodes pending queue rows via Google Places searchText, INSERTs new
dog_parks_gold rows (with website=catalog_park_url so the extractor's
OSM-shortcut route processes them on next pass).
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
from scripts.dog_park_pipeline._inproc import call_main


def ingest_discovery_queue(state: str, apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)

    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM public.dog_park_discovery_queue WHERE status='pending'")
        n_pending_before = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM public.dog_parks_gold WHERE state=%s AND is_active AND is_scoreable", (state,))
        n_gold_before = cur.fetchone()[0]
    finally:
        conn.close()

    from scripts.ingest_dog_park_discovery_queue import main as ingest_main
    args = ["--apply"] if apply else []
    exit_code, stdout = call_main(ingest_main, "ingest_dog_park_discovery_queue.py", args)

    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM public.dog_parks_gold WHERE state=%s AND is_active AND is_scoreable", (state,))
        n_gold_after = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "op": "ingest_discovery_queue",
        "state": state,
        "exit_code": exit_code,
        "n_pending_before": n_pending_before,
        "n_new_gold_rows": n_gold_after - n_gold_before,
        "stdout_tail": stdout[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
