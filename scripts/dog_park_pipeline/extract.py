"""extract.py — wrap extract_dog_park_amenities.py.

Routes parks via:
  - OSM website tag (direct fetch + URL-slug fallback)
  - NO_PER_PARK_URL_HOSTS → web_search (SF Rec & Parks SPA + Cloudflare-blocked .gov sites)
  - No-website → web_search with city + name (via --include-no-website)
"""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
from scripts.dog_park_pipeline._inproc import call_main


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
                  apply: bool = True, mode: str = "unsourced",
                  limit: int | None = None) -> dict:
    """Wrap extract_dog_park_amenities.py.

    mode:
      - 'unsourced' (default): parks without any source — initial state launch
      - 'all_active':          all active parks in state regardless of source —
                               use after prompt extensions (e.g. new amenity fields)
      - 'retry_failed':        parks with the no-result sentinel as latest BEP row
    """
    started = datetime.now(timezone.utc)
    n_v2_before = _count_operator_posted_v2(state)

    import importlib
    _repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, _repo)
    importlib.invalidate_caches()
    from scripts.extract_dog_park_amenities import main as extract_main
    args = ["--state", state, "--workers", str(workers)]
    if include_no_website:
        args.append("--include-no-website")
    if apply:
        args.append("--apply")
    if mode == "all_active":
        args.append("--all-active")
    elif mode == "retry_failed":
        args.append("--retry-failed")
    elif mode != "unsourced":
        raise ValueError(f"Unknown extractor mode: {mode}")
    if limit is not None:
        args.extend(["--limit", str(limit)])
    exit_code, stdout = call_main(extract_main, "extract_dog_park_amenities.py", args)

    n_v2_after = _count_operator_posted_v2(state)
    return {
        "op": "run_extractor",
        "state": state,
        "mode": mode,
        "limit": limit,
        "exit_code": exit_code,
        "n_v2_before": n_v2_before,
        "n_v2_after": n_v2_after,
        "n_v2_added": n_v2_after - n_v2_before,
        "stdout_tail": stdout[-1500:],
        "started_at": started.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }
