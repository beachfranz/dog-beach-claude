"""retry.py — wrap retry_dog_park_no_match.py.

For parks where the first web_search returned name_match=false, retry with
stronger prompt (≥2 distinct query phrasings, max_uses=5) and
display_name_override-aware names.
"""
from __future__ import annotations
import os, subprocess, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.dog_park_pipeline.extract import _count_operator_posted_v2


def retry_no_match(state: str, workers: int = 6, apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)
    n_v2_before = _count_operator_posted_v2(state)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cmd = [
        sys.executable, os.path.join(repo_root, "scripts", "retry_dog_park_no_match.py"),
        "--state", state,
        "--workers", str(workers),
    ]
    if apply:
        cmd.append("--apply")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

    n_v2_after = _count_operator_posted_v2(state)
    return {
        "op": "retry_no_match",
        "state": state,
        "exit_code": result.returncode,
        "n_v2_before": n_v2_before,
        "n_v2_after": n_v2_after,
        "n_v2_added": n_v2_after - n_v2_before,
        "stdout_tail": (result.stdout or "")[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
