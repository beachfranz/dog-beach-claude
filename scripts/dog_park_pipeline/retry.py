"""retry.py — wrap retry_dog_park_no_match.py.

For parks where the first web_search returned name_match=false, retry with
stronger prompt (≥2 distinct query phrasings, max_uses=5) and
display_name_override-aware names.
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.dog_park_pipeline.extract import _count_operator_posted_v2
from scripts.dog_park_pipeline._inproc import call_main


def retry_no_match(state: str, workers: int = 6, apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)
    n_v2_before = _count_operator_posted_v2(state)

    from scripts.retry_dog_park_no_match import main as retry_main
    args = ["--state", state, "--workers", str(workers)]
    if apply:
        args.append("--apply")
    exit_code, stdout = call_main(retry_main, "retry_dog_park_no_match.py", args)

    n_v2_after = _count_operator_posted_v2(state)
    return {
        "op": "retry_no_match",
        "state": state,
        "exit_code": exit_code,
        "n_v2_before": n_v2_before,
        "n_v2_after": n_v2_after,
        "n_v2_added": n_v2_after - n_v2_before,
        "stdout_tail": stdout[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
