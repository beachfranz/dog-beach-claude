"""
test_hours_only_extraction.py — DRY RUN
----------------------------------------
Re-prompt the same cached pages used by extract_from_park_url.py with
a narrow HOURS-ONLY prompt. Compare:
  - fill rate for hours fields (old wide-prompt vs new narrow-prompt)
  - extraction_confidence
  - which would have changed the canonical winner

Reads page text from park_url_extractions.raw_text. Doesn't write to DB.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL = "claude-haiku-4-5-20251001"
WORKERS = 5

HOURS_ONLY_PROMPT = """\
You are extracting beach operating hours from a park or beach's official webpage.

Return ONLY valid JSON with these exact keys (use null when not stated):

{
  "hours_text":             string | null,    // plain-English hours, e.g. "6am-10pm daily" or "dawn to dusk"
  "open_time":              "HH:MM" | null,   // 24-hour format, e.g. "06:00"
  "close_time":             "HH:MM" | null,   // 24-hour format, e.g. "22:00"
  "is_24_hours":            true | false | null,
  "hours_notes":            string | null,    // any caveats: seasonal variation, day-of-week differences, etc.
  "confidence":             number 0.00-1.00,
  "notes":                  string | null     // explain how you derived the values
}

Rules:
- Look anywhere on the page: "Hours:", "Open:", "Daily:", or buried in prose.
- Convert "sunrise" / "sunset" to "06:00" / "20:00" (rough placeholder).
- "dawn to dusk" → hours_text = "dawn to dusk", open/close = null, is_24_hours = false.
- "24/7" or "always open" → is_24_hours = true.
- confidence: 0.95 for explicit "Hours: 6am-10pm" sections; 0.75 for prose mentions; 0.50 for ambiguous; 0.20 for educated guess; null in notes.
- Reply with raw JSON only — no markdown.
"""


def sb_headers() -> dict[str, str]:
    return {"apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json"}


def fetch_extractions() -> list[dict]:
    url = (f"{SUPABASE_URL}/rest/v1/park_url_extractions"
           "?extraction_status=eq.success&raw_text=not.is.null"
           "&select=fid,source_url,raw_text,hours_text,open_time,close_time,extraction_confidence")
    r = httpx.get(url, headers=sb_headers(), timeout=30)
    r.raise_for_status()
    return [row for row in r.json() if row.get("raw_text") and len(row["raw_text"]) > 200]


async def llm_call(client: httpx.AsyncClient, raw_text: str) -> dict[str, Any]:
    payload = {"model": MODEL, "max_tokens": 400,
               "system": HOURS_ONLY_PROMPT,
               "messages": [{"role": "user", "content": f"Page content:\n{raw_text[:25_000]}"}]}
    resp = await client.post("https://api.anthropic.com/v1/messages",
                             headers={"x-api-key": ANTHROPIC_API_KEY,
                                      "anthropic-version": "2023-06-01",
                                      "content-type": "application/json"},
                             json=payload, timeout=60.0)
    resp.raise_for_status()
    text = resp.json()["content"][0]["text"].strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


@dataclass
class Row:
    fid: int
    old_hours_text: Optional[str]
    old_open: Optional[str]
    old_close: Optional[str]
    old_conf: Optional[float]
    new_hours_text: Optional[str]
    new_open: Optional[str]
    new_close: Optional[str]
    new_conf: Optional[float]


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, row: dict) -> Row:
    async with sem:
        try:
            parsed = await llm_call(client, row["raw_text"])
        except Exception as e:
            print(f"  fid={row['fid']} ERROR: {e}", file=sys.stderr)
            parsed = {}
        return Row(
            fid=row["fid"],
            old_hours_text=row.get("hours_text"),
            old_open=row.get("open_time"),
            old_close=row.get("close_time"),
            old_conf=float(row["extraction_confidence"]) if row.get("extraction_confidence") else None,
            new_hours_text=parsed.get("hours_text"),
            new_open=parsed.get("open_time"),
            new_close=parsed.get("close_time"),
            new_conf=float(parsed["confidence"]) if parsed.get("confidence") is not None else None,
        )


async def run() -> None:
    rows = fetch_extractions()
    print(f"Loaded {len(rows)} successful extractions to re-prompt\n")

    sem = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    results: list[Row] = []
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, r) for r in rows]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            results.append(await t)
            if i % 20 == 0:
                print(f"  ...{i}/{len(rows)}", file=sys.stderr)

    # Tally
    n = len(results)
    old_with_text  = sum(1 for r in results if r.old_hours_text)
    new_with_text  = sum(1 for r in results if r.new_hours_text)
    old_with_open  = sum(1 for r in results if r.old_open)
    new_with_open  = sum(1 for r in results if r.new_open)
    both_have_text = sum(1 for r in results if r.old_hours_text and r.new_hours_text)
    only_new_text  = sum(1 for r in results if not r.old_hours_text and r.new_hours_text)
    only_old_text  = sum(1 for r in results if r.old_hours_text and not r.new_hours_text)

    avg_new_conf = sum(r.new_conf for r in results if r.new_conf is not None) / max(1, sum(1 for r in results if r.new_conf is not None))
    high_conf_new = sum(1 for r in results if r.new_conf and r.new_conf >= 0.65)

    print(f"\n=== Hours-only re-prompt summary ===")
    print(f"Total extractions tested:       {n}")
    print(f"Old (wide prompt) hours_text:   {old_with_text} ({100*old_with_text/n:.0f}%)")
    print(f"New (narrow prompt) hours_text: {new_with_text} ({100*new_with_text/n:.0f}%)")
    print(f"  Both filled:                  {both_have_text}")
    print(f"  Only new filled (gain):       {only_new_text}")
    print(f"  Only old filled (lost):       {only_old_text}")
    print(f"Old open_time filled:           {old_with_open}")
    print(f"New open_time filled:           {new_with_open}")
    print(f"\nAvg new confidence:             {avg_new_conf:.2f}")
    print(f"New rows with conf >= 0.65:     {high_conf_new}")

    # Sample of net wins
    print("\n=== Sample of NEW hours captured (old was null) ===")
    for r in [x for x in results if not x.old_hours_text and x.new_hours_text][:10]:
        print(f"  fid={r.fid}  conf={r.new_conf}  hours={r.new_hours_text!r}")

    print("\n=== Sample of disagreements (both filled, different) ===")
    for r in [x for x in results if x.old_hours_text and x.new_hours_text and x.old_hours_text != x.new_hours_text][:10]:
        print(f"  fid={r.fid}  old={r.old_hours_text!r}  new={r.new_hours_text!r}")


if __name__ == "__main__":
    asyncio.run(run())
