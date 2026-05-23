"""
test_key_features.py
--------------------
Re-prompt cached park_url_extractions.raw_text for a structured
`key_features` LIST instead of free-form prose. Doesn't write to DB —
prints results grouped by governing_body_type so we can eyeball quality.

Usage:
  python scripts/one_off/test_key_features.py   # uses the same 36 sampled IDs
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 5

# Same 36 IDs as the description re-extraction sample
SAMPLE_IDS = [
    633, 663, 713, 764,
    237, 463, 503, 577, 638,
    156, 223, 525, 777, 785,
    137, 153, 170, 215, 219,
    664, 694, 695, 699, 711,
    192, 301, 319, 419, 657,
    227, 600,
    180, 244, 253, 654, 753,
]

KEY_FEATURES_PROMPT = """\
Extract a list of 3-6 KEY FEATURES from this beach/park webpage for a
dog-focused beach finder app.

Each item must be:
- A single fact directly stated on the page (no inference, no invention)
- ≤ 100 characters
- Phrased as a complete short statement (not a fragment)

Order:
1. Dog-relevant items first (leash policy, allowed/prohibited zones,
   time windows, designated dog areas, service-animal exceptions)
2. Practical items second (hours, parking, restrooms, amenities)
3. Distinctive features last (terrain, location, notable history)

If the page genuinely lacks dog-related info, skip the dog tier and
list only what IS on the page. Return fewer items rather than padding
with generic statements.

Return ONLY a JSON object with this shape — no markdown, no preamble:

{ "key_features": ["fact 1", "fact 2", "fact 3", ...] }

If the page contains no useful facts (e.g., it's a generic landing page
or the wrong page entirely), return: { "key_features": [] }
"""


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(ids: list[int]) -> list[dict]:
    """Pull rows + their governing_body_type from locations_stage."""
    in_list = ",".join(str(i) for i in ids)
    select = "id,fid,source_url,raw_text,description"
    url = (f"{SUPABASE_URL}/rest/v1/park_url_extractions"
           f"?select={select}&id=in.({in_list})")
    r = httpx.get(url, headers=sb_headers(), timeout=60.0)
    r.raise_for_status()
    extractions = r.json()

    fids = sorted({e["fid"] for e in extractions})
    fid_in = ",".join(str(f) for f in fids)
    url2 = (f"{SUPABASE_URL}/rest/v1/locations_stage"
            f"?select=fid,display_name,governing_body_type,governing_body_name"
            f"&fid=in.({fid_in})")
    r2 = httpx.get(url2, headers=sb_headers(), timeout=60.0)
    r2.raise_for_status()
    by_fid = {row["fid"]: row for row in r2.json()}

    for e in extractions:
        meta = by_fid.get(e["fid"], {})
        e["display_name"]        = meta.get("display_name")
        e["governing_body_type"] = meta.get("governing_body_type") or "(null)"
        e["governing_body_name"] = meta.get("governing_body_name")
    return extractions


async def llm_call(client: httpx.AsyncClient, beach_hint: str, page_text: str) -> Optional[list[str]]:
    user = (f"Beach context (for orientation only — do not assume facts not on the page):\n"
            f"{beach_hint}\n\nPage content:\n{page_text}")
    payload = {
        "model":      MODEL,
        "max_tokens": 800,
        "system":     KEY_FEATURES_PROMPT,
        "messages":   [{"role": "user", "content": user}],
    }
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json=payload,
            timeout=60.0,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        data = json.loads(text)
        feats = data.get("key_features", [])
        if not isinstance(feats, list):
            return None
        return [str(f).strip() for f in feats if str(f).strip()]
    except Exception as e:
        print(f"    LLM error: {e}", file=sys.stderr)
        return None


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, row: dict) -> dict:
    async with sem:
        feats = await llm_call(client, f"source URL: {row['source_url']}", row["raw_text"])
        return {**row, "features": feats or []}


async def run() -> None:
    targets = fetch_targets(SAMPLE_IDS)
    print(f"Fetched {len(targets)} rows. Running key_features extraction…\n")
    if not targets:
        return

    sem = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    results: list[dict] = []
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, r) for r in targets]
        for t in asyncio.as_completed(tasks):
            results.append(await t)

    # Group by governing_body_type
    results.sort(key=lambda r: (r["governing_body_type"], r["fid"]))
    last_gtype = None
    for r in results:
        gtype = r["governing_body_type"]
        if gtype != last_gtype:
            print(f"\n========== {gtype.upper()} ==========")
            last_gtype = gtype
        print(f"\n  fid={r['fid']}  {r['display_name']!r}")
        print(f"    URL: {r['source_url']}")
        if not r["features"]:
            print(f"    (empty — page likely lacks usable info)")
        else:
            for i, f in enumerate(r["features"], 1):
                print(f"    {i}. {f}")


if __name__ == "__main__":
    asyncio.run(run())
