"""preflight.py — verify environment before pipeline runs."""
from __future__ import annotations
import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def preflight_check(state: str) -> dict:
    """Check Playwright browser binary present, state has active parks, etc.
    Returns metrics dict. Does NOT raise — surfaces issues in metrics."""
    started = datetime.now(timezone.utc)
    issues = []
    metrics = {"op": "preflight_check", "state": state, "issues": []}

    # Playwright browser binary
    pw_dir = os.path.expandvars(r"%LOCALAPPDATA%\ms-playwright")
    if os.path.isdir(pw_dir):
        # any chromium* directory
        chromes = [d for d in os.listdir(pw_dir) if d.startswith("chromium")]
        if not chromes:
            issues.append("playwright_browser_missing")
    else:
        issues.append("playwright_dir_missing")

    # State-level inventory
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM public.dog_parks_gold
             WHERE state = %s AND is_active AND is_scoreable
        """, (state,))
        metrics["n_active_parks"] = cur.fetchone()[0]
        if metrics["n_active_parks"] == 0:
            issues.append("no_active_parks_in_state")

        cur.execute("""
            SELECT count(*) FROM public.dog_parks_active_unsourced
             WHERE state = %s
        """, (state,))
        metrics["n_on_default_policy"] = cur.fetchone()[0]

        cur.execute("""
            SELECT count(*) FROM public.dog_park_dog_policy ddp
              JOIN public.dog_parks_gold dpg ON dpg.fid = ddp.dog_park_fid
             WHERE dpg.state = %s AND dpg.is_active AND dpg.is_scoreable
               AND ddp.source = 'operator_posted_v2'
        """, (state,))
        metrics["n_operator_posted_v2_at_start"] = cur.fetchone()[0]

        # Required env vars
        if not os.environ.get("ANTHROPIC_API_KEY"):
            issues.append("ANTHROPIC_API_KEY_missing")
        if not os.environ.get("GOOGLE_MAPS_API_KEY"):
            issues.append("GOOGLE_MAPS_API_KEY_missing")
    finally:
        conn.close()

    metrics["issues"] = issues
    metrics["ok"] = len(issues) == 0
    metrics["started_at"] = started.isoformat()
    metrics["ended_at"] = datetime.now(timezone.utc).isoformat()
    return metrics
