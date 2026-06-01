"""dp_photos.py — photo-loading + curate ops for the state-launch pipeline.

Five ops, run AFTER retry_no_match so all DPs in the state are known and
extracted before any photo work:

  dp_photos_load_flickr      — Flickr geo + name search
  dp_photos_load_wikimedia   — Commons geosearch
  dp_photos_load_websearch   — Tavily web image search
  dp_photos_vision_tag       — Two-pass Haiku vision tagger
  dp_photos_curate           — Pick top-3 visible per park (v4 gate)

The loader + tagger ops mirror the Dagster `_make_dp_photo_loader_asset`
pattern: subprocess each script per chunk of fids, parse `saved=N` from
stdout. The curate op runs a direct SQL UPDATE (no script).

Costs (rough, per state):
  Flickr / Wikimedia / Websearch loaders — external API, mostly free tier
  Vision tagging — ~$0.005 per photo (Haiku 4.5 two-pass)
  Curate — DB CPU only

Skip behavior: same as existing ops — use `--skip dp_photos_*` or
`--only` to scope.
"""
from __future__ import annotations
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def _state_dp_fids(state: str) -> list[int]:
    """Active+scoreable DPs in the state."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT array_agg(distinct fid) "
                "  FROM public.dog_parks_gold "
                " WHERE state = %s AND is_active AND is_scoreable",
                (state,),
            )
            return cur.fetchone()[0] or []
    finally:
        conn.close()


def _chunked_loader(state: str, script: str, chunk_size: int, timeout: int,
                    parse_pattern: str, workers: int | None = None) -> dict:
    """Common driver for the three DP photo loaders. Iterates state fids
    in chunks, calls the script with --entity dog_park --fids <chunk>,
    parses the per-chunk count from stdout, returns aggregate metrics."""
    fids = _state_dp_fids(state)
    if not fids:
        return {"state": state, "fids": 0, "saved": 0,
                "chunks_done": 0, "chunks_failed": 0, "skipped_reason": "no_dp_fids"}
    total = done = failed = 0
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for i in range(0, len(fids), chunk_size):
        chunk = fids[i:i + chunk_size]
        args = [sys.executable, script, "--entity", "dog_park",
                "--fids", ",".join(str(x) for x in chunk)]
        if workers and workers > 1:
            args += ["--workers", str(workers)]
        try:
            result = subprocess.run(args, cwd=repo, capture_output=True,
                                    text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            failed += 1
            print(f"    chunk {done + failed} timed out after {timeout}s "
                  f"(fids {chunk[0]}..{chunk[-1]})", flush=True)
            continue
        if result.returncode != 0:
            failed += 1
            tail = (result.stderr or "")[-300:]
            print(f"    chunk {done + failed} failed (fids {chunk[0]}..{chunk[-1]}): "
                  f"{tail}", flush=True)
            continue
        m = re.search(parse_pattern, result.stdout or "")
        if m:
            total += int(m.group(1))
        done += 1
    return {"state": state, "fids": len(fids), "saved": total,
            "chunks_done": done, "chunks_failed": failed}


def dp_photos_load_flickr(state: str, **kw) -> dict:
    """Load DP photos from Flickr (geo + name search). ~1.6 QPS effective."""
    out = _chunked_loader(
        state, "scripts/load_flickr_photos.py",
        chunk_size=50, timeout=600, parse_pattern=r"saved=(\d+)", workers=4)
    out["op"] = "dp_photos_load_flickr"
    return out


def dp_photos_load_wikimedia(state: str, **kw) -> dict:
    """Load DP photos from Wikimedia Commons (geosearch by lat/lng + 500m)."""
    out = _chunked_loader(
        state, "scripts/load_wikimedia_commons_photos.py",
        chunk_size=30, timeout=900, parse_pattern=r"saved=(\d+)", workers=None)
    out["op"] = "dp_photos_load_wikimedia"
    return out


def dp_photos_load_websearch(state: str, **kw) -> dict:
    """Load DP photos via Tavily web image search.

    Most relevant source for DPs since geo-tagged CC photos miss most parks.
    Tavily returns image URLs + AI descriptions; vision tagger + curate
    gate filter for relevance + quality."""
    out = _chunked_loader(
        state, "scripts/load_websearch_photos.py",
        chunk_size=30, timeout=900, parse_pattern=r"saved=(\d+)", workers=8)
    out["op"] = "dp_photos_load_websearch"
    return out


def dp_photos_curate(state: str, **kw) -> dict:
    """Pick top-3 photos per DP via Tavily's own relevance order.

    NO VISION TAGGING. The DE proof-of-concept (2026-06-01) showed
    that the biased Tavily query ("<name> dog park <city> <state>
    dogs playing") returns dog-content directly in relevance order;
    the post-load Haiku vision-tag + has_dog gate was redundant.

    Gate (no vision required):
      curated_at IS NULL  AND  hidden_at IS NULL
      AND source = 'websearch'

    ADDITIVE — only operates on parks with ZERO existing curated
    photos. Existing curated picks (vision-auto, Curated:Human,
    experiment:no-vision) are preserved. Once a park has loader-bias
    picks this is a no-op on it. Per Franz directive 2026-06-01:
    "I do not want you to wipe out what I've already curated."

    See [[dp-loader-dog-bias]] and [[dp-photos-are-dog-only]].

    Stamps curated_at=now(), curated_by='vision-auto', match_quality='medium'.
    Idempotent — only operates on uncurated rows; existing 'vision-auto'
    selections stay put.
    """
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
              WITH gap_parks AS (
                SELECT g.fid
                  FROM public.dog_parks_gold g
                 WHERE g.state = %s AND g.is_active AND g.is_scoreable
                   AND NOT EXISTS (
                     SELECT 1 FROM public.dog_park_photos p
                      WHERE p.dog_park_fid = g.fid
                        AND p.curated_at IS NOT NULL
                        AND p.hidden_at IS NULL
                   )
              ),
              ranked AS (
                SELECT p.id, p.dog_park_fid,
                       ROW_NUMBER() OVER (
                         PARTITION BY p.dog_park_fid
                         ORDER BY p.sort_order ASC NULLS LAST, p.id ASC
                       ) AS rk
                  FROM public.dog_park_photos p
                  JOIN gap_parks gp ON gp.fid = p.dog_park_fid
                 WHERE p.source = 'websearch'
                   AND p.curated_at IS NULL
                   AND p.hidden_at IS NULL
              )
              UPDATE public.dog_park_photos p
                 SET curated_at    = now(),
                     curated_by    = 'loader-bias',
                     match_quality = 'medium'
                FROM ranked r
               WHERE p.id = r.id AND r.rk <= 3
            """, (state,))
            n_curated = cur.rowcount
            conn.commit()
            cur.execute("""
              SELECT count(DISTINCT p.dog_park_fid)
                FROM public.dog_park_photos p
                JOIN public.dog_parks_gold g ON g.fid = p.dog_park_fid
               WHERE g.state = %s
                 AND p.curated_at IS NOT NULL
                 AND p.hidden_at IS NULL
            """, (state,))
            parks_with_photo = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM public.dog_parks_gold "
                " WHERE state = %s AND is_active AND is_scoreable",
                (state,),
            )
            total_parks = cur.fetchone()[0]
    finally:
        conn.close()
    return {
        "op": "dp_photos_curate",
        "state": state,
        "photos_curated": n_curated,
        "parks_with_photo": parks_with_photo,
        "total_parks": total_parks,
        "coverage_pct": round(100.0 * parks_with_photo / max(total_parks, 1), 1),
    }
