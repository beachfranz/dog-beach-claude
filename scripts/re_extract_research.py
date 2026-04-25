"""
re_extract_research.py
----------------------
Re-extract beach_policy_research raw_text dumps using the SAME numeric-
confidence prompt as extract_from_park_url.py. Writes results into the
v2 columns on beach_policy_research.

Why: the OLD pipeline asked the LLM for confidence as 'high'|'low' and
we mapped to 0.80|0.50 by fiat. That overstates the LLM's actual self-
estimated certainty. When compared against park_url's calibrated 0.00-
1.00 numeric confidence, research wins lopsidedly even when the LLM was
probably equally uncertain. Apples-to-apples requires the same prompt
on both sides.

This script:
  1. Reads beach_policy_research where parsed_at_v2 is null (or --force)
  2. For each: send raw_text through Haiku 4.5 with the EXTRACTION_PROMPT
  3. UPDATE the row's v2 columns

After running, populate_from_research uses the new numeric confidence
when present, falling back to the old text->num map when not.

Usage:
  python scripts/re_extract_research.py --limit 10
  python scripts/re_extract_research.py --all
  python scripts/re_extract_research.py --force --limit 20  # re-do already-parsed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 5
TEXT_CHAR_LIMIT       = 25_000

# Same prompt as extract_from_park_url.py — apples-to-apples
EXTRACTION_PROMPT = """\
You are extracting structured beach metadata from a park or beach's official webpage.

Return ONLY a valid JSON object with exactly these keys (use null when not stated on the page):

{
  "dogs_allowed":           "yes" | "no" | "seasonal" | "restricted" | "unknown" | null,
  "dogs_leash_required":    "required" | "off_leash_ok" | "mixed" | null,
  "dogs_restricted_hours":  [{"start":"HH:MM","end":"HH:MM"}] | null,
  "dogs_seasonal_rules":    [{"from":"MM-DD","to":"MM-DD","notes":string}] | null,
  "dogs_zone_description":  string | null,
  "dogs_policy_notes":      string | null,
  "hours_text":             string | null,
  "open_time":              "HH:MM" | null,
  "close_time":             "HH:MM" | null,
  "has_parking":            true | false | null,
  "parking_type":           "lot" | "street" | "metered" | "mixed" | "none" | null,
  "parking_notes":          string | null,
  "description":            string | null,
  "has_restrooms":          true | false | null,
  "has_showers":            true | false | null,
  "has_drinking_water":     true | false | null,
  "has_lifeguards":         true | false | null,
  "has_disabled_access":    true | false | null,
  "has_food":               true | false | null,
  "has_fire_pits":          true | false | null,
  "has_picnic_area":        true | false | null,
  "extraction_confidence":  number 0.00-1.00,
  "extraction_notes":       string | null
}

Rules:
- Extract ONLY what is explicitly stated. Do not infer or guess.
- "seasonal" = allowed at some times of year and not others.
- "restricted" = allowed with notable rules (specific zones, time windows, leash mandates beyond standard).
- For dogs_restricted_hours: hours when dogs are NOT allowed (the off-window).
- For dogs_seasonal_rules: explicit date ranges with different rules.
- extraction_confidence: 0.95 for clear structured "DOG RULES:" sections; 0.75 for inferred from prose; 0.50 for partial; lower if ambiguous.
- Reply with raw JSON only — no markdown fences, no preamble.
"""

# Field-set that maps directly to v2 columns on beach_policy_research
V2_FIELDS = {
    "dogs_allowed", "dogs_leash_required", "dogs_restricted_hours", "dogs_seasonal_rules",
    "dogs_zone_description", "dogs_policy_notes",
    "hours_text", "open_time", "close_time",
    "has_parking", "parking_type", "parking_notes",
    "has_restrooms", "has_showers", "has_drinking_water", "has_lifeguards",
    "has_disabled_access", "has_food", "has_fire_pits", "has_picnic_area",
    "extraction_confidence", "extraction_notes",
}


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def fetch_research_rows(limit: Optional[int], force: bool) -> list[dict]:
    parts = ["select=id,staging_id,source_url,source_type,raw_text"]
    if not force:
        parts.append("parsed_at_v2=is.null")
    # Only rows with usable raw text
    parts.append("raw_text=not.is.null")
    parts.append("order=id.asc")
    if limit:
        parts.append(f"limit={limit}")
    url = f"{SUPABASE_URL}/rest/v1/beach_policy_research?{'&'.join(parts)}"
    resp = httpx.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    return [r for r in resp.json() if r.get("raw_text") and len(r["raw_text"].strip()) > 100]


def update_row(row_id: int, fields: dict[str, Any], dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] id={row_id} conf={fields.get('extraction_confidence_v2')}")
        return
    url = f"{SUPABASE_URL}/rest/v1/beach_policy_research?id=eq.{row_id}"
    resp = httpx.patch(url, headers=sb_headers(), json=fields, timeout=15)
    if not resp.is_success:
        print(f"  update failed for id={row_id}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        resp.raise_for_status()


async def llm_extract(client: httpx.AsyncClient, raw_text: str, source_type: str) -> dict[str, Any]:
    payload = {
        "model": MODEL,
        "max_tokens": 1024,
        "system": EXTRACTION_PROMPT,
        "messages": [{
            "role": "user",
            "content": (
                f"Source type: {source_type}\n\n"
                f"Page content (truncated to {TEXT_CHAR_LIMIT} chars):\n"
                f"{raw_text[:TEXT_CHAR_LIMIT]}"
            )
        }],
    }
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
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"    parse error: {e}", file=sys.stderr)
        return {}


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, row: dict, dry_run: bool) -> str:
    async with sem:
        rid       = row["id"]
        sid       = row["staging_id"]
        st        = row["source_type"]
        raw       = row["raw_text"]
        try:
            parsed = await llm_extract(client, raw, st)
        except Exception as e:
            return f"  id={rid} sid={sid}  LLM_ERROR: {e}"

        if not parsed:
            return f"  id={rid} sid={sid}  PARSE_FAILED"

        # Build the update payload — map LLM keys to *_v2 column names.
        # Use ISO timestamp for parsed_at_v2 (PostgREST won't interpret 'now()').
        now_iso = datetime.now(timezone.utc).isoformat()
        update = {"parsed_at_v2": now_iso, "extraction_model_v2": MODEL}
        for k in V2_FIELDS:
            if k not in parsed:
                continue
            v = parsed[k]
            if k == "extraction_confidence":
                update["extraction_confidence_v2"] = v
            elif k == "extraction_notes":
                update["extraction_notes_v2"] = v
            else:
                update[f"{k}_v2"] = v

        update_row(rid, update, dry_run)
        return f"  id={rid} sid={sid}  ok  conf={parsed.get('extraction_confidence')}  dogs={parsed.get('dogs_allowed')}"


async def run(args: argparse.Namespace) -> None:
    rows = fetch_research_rows(args.limit, args.force)
    print(f"Loaded {len(rows)} research rows to process")
    if not rows:
        return

    sem = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, r, args.dry_run) for r in rows]
        for t in asyncio.as_completed(tasks):
            print(await t)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--all",   action="store_true")
    p.add_argument("--force", action="store_true", help="re-extract already-parsed rows")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.all:
        args.limit = None
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
