"""
reextract_descriptions.py
-------------------------
Re-prompt the LLM against cached park_url_extractions.raw_text to
regenerate park_url_extractions.description with a dog-focused bias.

Why: the original extraction prompt asked for a generic 1-2 sentence
beach summary; result was 544/546 descriptions never mentioning dogs.
For Dog Beach Scout, the most distinctive thing about a beach IS the
dog policy, so descriptions should lead with it.

Reads raw_text (no re-fetching). Updates only the `description`
column. Uses claude-haiku-4-5 (cheap, sufficient for this rewrite).

Usage:
  python scripts/one_off/reextract_descriptions.py --limit 5     # smoke test
  python scripts/one_off/reextract_descriptions.py --all         # full sweep
  python scripts/one_off/reextract_descriptions.py --all --dry-run
"""

from __future__ import annotations

import argparse
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
MIN_PAGE_CHARS        = 500
MAX_DESC_CHARS        = 400  # hard cap; LLM should stay well under

DESCRIPTION_PROMPT = """\
You are writing a 1-2 sentence beach description for a dog-focused
beach-finder app. The reader is a dog owner deciding whether to take
their dog here.

Lead with the dog-relevant facts. If the page mentions dog policy
(leash rules, off-leash zones, designated dog areas, time windows,
seasonal restrictions, prohibited zones), put that FIRST. After that,
add the most distinctive non-dog feature (location, terrain, key
amenity) in a phrase or short clause.

If the page genuinely says NOTHING about dogs/pets, fall back to a
plain beach description but keep it under 200 characters.

Hard rules:
- 1-2 sentences. Hard cap 400 characters.
- Plain prose, no markdown, no bullet lists, no quotes.
- Do not invent dog policy not stated on the page. If the page is
  silent on dogs, the description must not assert anything about them.
- Reply with ONLY the description text — no JSON, no preamble, no
  explanation.
"""


# ── Supabase REST helpers ──────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(limit: Optional[int], ids: Optional[list[int]]) -> list[dict]:
    """Pull successful extractions with non-empty raw_text. Returns list of
    {id, fid, source_url, raw_text, description (current)}."""
    select = "id,fid,source_url,raw_text,description"
    if ids:
        in_list = ",".join(str(i) for i in ids)
        url = (f"{SUPABASE_URL}/rest/v1/park_url_extractions"
               f"?select={select}&id=in.({in_list})")
    else:
        params = (f"{select}"
                  "&extraction_status=eq.success"
                  "&raw_text=not.is.null"
                  "&order=id.asc")
        if limit:
            params += f"&limit={limit}"
        url = f"{SUPABASE_URL}/rest/v1/park_url_extractions?select={params}"
    r = httpx.get(url, headers=sb_headers(), timeout=60.0)
    r.raise_for_status()
    rows = r.json()
    return [r for r in rows if r.get("raw_text") and len(r["raw_text"]) >= MIN_PAGE_CHARS]


def update_description(row_id: int, description: str, dry_run: bool) -> None:
    if dry_run:
        return
    url = f"{SUPABASE_URL}/rest/v1/park_url_extractions?id=eq.{row_id}"
    headers = {**sb_headers(), "Prefer": "return=minimal"}
    r = httpx.patch(url, headers=headers, json={"description": description}, timeout=15)
    if not r.is_success:
        print(f"    update failed for id={row_id}: {r.status_code} {r.text[:200]}", file=sys.stderr)


# ── LLM call ──────────────────────────────────────────────────────────────────

async def llm_describe(client: httpx.AsyncClient, beach_hint: str, page_text: str) -> Optional[str]:
    user = f"Beach context (for orientation only — do not assume facts not on the page):\n{beach_hint}\n\nPage content:\n{page_text}"
    payload = {
        "model":      MODEL,
        "max_tokens": 300,
        "system":     DESCRIPTION_PROMPT,
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
        # Strip stray quotes / fences in case the LLM ignores instructions.
        text = re.sub(r'^["\']|["\']$', '', text).strip()
        text = re.sub(r"^```.*?\n|\n```$", "", text, flags=re.S).strip()
        if not text:
            return None
        return text[:MAX_DESC_CHARS]
    except Exception as e:
        print(f"    LLM error: {e}", file=sys.stderr)
        return None


# ── Per-row worker ────────────────────────────────────────────────────────────

async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                       row: dict, dry_run: bool) -> str:
    async with sem:
        rid       = row["id"]
        fid       = row["fid"]
        url       = row["source_url"]
        raw_text  = row["raw_text"]
        old_desc  = row.get("description") or ""

        new_desc = await llm_describe(client, f"source URL: {url}", raw_text)
        if not new_desc:
            return f"  id={rid} fid={fid}  no-output  ({url})"

        # Heuristic: if the new description doesn't mention any dog-relevant
        # word AND the page text mentions dogs/pets/leash, the LLM probably
        # didn't pick up the dog content. Flag for visibility but still update.
        page_has_dog = bool(re.search(r"(?i)\b(dog|pet|leash|service\s+animal)\b", raw_text))
        new_has_dog  = bool(re.search(r"(?i)\b(dog|pet|leash|service\s+animal|on[- ]leash|off[- ]leash)\b", new_desc))
        flag         = "" if (new_has_dog or not page_has_dog) else "  [!] page mentions dogs, desc does not"

        update_description(rid, new_desc, dry_run)
        prefix = "[dry] " if dry_run else ""
        return f"  {prefix}id={rid} fid={fid}{flag}\n     OLD: {old_desc[:140]}\n     NEW: {new_desc[:140]}"


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    ids_arg = [int(x) for x in args.ids.split(",")] if args.ids else None
    targets = fetch_targets(args.limit, ids_arg)
    print(f"Loaded {len(targets)} extractions to re-describe (raw_text >= {MIN_PAGE_CHARS} chars)\n")
    if not targets:
        return

    sem    = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, r, args.dry_run) for r in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            line = await t
            print(line)
            if i % 50 == 0:
                print(f"  --- {i}/{len(targets)} done ---")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit",   type=int, default=5)
    p.add_argument("--all",     action="store_true")
    p.add_argument("--ids",     type=str, default=None,
                   help="Comma-separated list of park_url_extractions.id values")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.all:
        args.limit = None
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
