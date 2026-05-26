"""extract.py — wrap extract_dog_park_amenities.py.

Routes parks via:
  - OSM website tag (direct fetch + URL-slug fallback)
  - NO_PER_PARK_URL_HOSTS → web_search (SF Rec & Parks SPA + Cloudflare-blocked .gov sites)
  - No-website → web_search with city + name (via --include-no-website)
"""
from __future__ import annotations
import os, subprocess, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def _count_operator_posted_v2(state: str) -> int:
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM public.dog_park_dog_policy ddp
              JOIN public.dog_parks_gold dpg ON dpg.fid = ddp.dog_park_fid
             WHERE dpg.state = %s AND dpg.is_active AND dpg.is_scoreable
               AND ddp.source = 'operator_posted_v2'
        """, (state,))
        return cur.fetchone()[0]
    finally:
        conn.close()


def run_extractor(state: str, workers: int = 6, include_no_website: bool = True,
                  apply: bool = True) -> dict:
    started = datetime.now(timezone.utc)
    n_v2_before = _count_operator_posted_v2(state)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cmd = [
        sys.executable, os.path.join(repo_root, "scripts", "extract_dog_park_amenities.py"),
        "--state", state,
        "--workers", str(workers),
    ]
    if include_no_website:
        cmd.append("--include-no-website")
    if apply:
        cmd.append("--apply")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

    n_v2_after = _count_operator_posted_v2(state)
    return {
        "op": "run_extractor",
        "state": state,
        "exit_code": result.returncode,
        "n_v2_before": n_v2_before,
        "n_v2_after": n_v2_after,
        "n_v2_added": n_v2_after - n_v2_before,
        "stdout_tail": (result.stdout or "")[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
