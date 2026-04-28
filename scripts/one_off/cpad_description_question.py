"""
cpad_description_question.py
----------------------------
Add `response_scope='description'` rows to geo_entity_response, reusing
the raw_text already cached on the dogs_allowed rows. No re-fetching.

For each CPAD unit that already has a dogs_allowed row with raw_text,
generate a 1-2 sentence dog-focused beach description and insert a
new row with response_scope='description'.

Usage:
  python scripts/one_off/cpad_description_question.py [--limit N] [--force]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 6
MAX_DESC_CHARS        = 400
RESPONSE_SCOPE        = "description"

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


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(limit: Optional[int], force: bool) -> list[dict]:
    """Pull existing geo_entity_response rows (CPAD, dogs_allowed scope)
    that have raw_text. Skip ones that already have a description row
    unless --force."""
    sql = """
    select distinct on (e.entity_id, e.source_url)
      e.entity_id as cpad_unit_id, e.source_url, e.raw_text
    from public.geo_entity_response e
    where e.entity_type = 'cpad'
      and e.response_scope = 'dogs_allowed'
      and e.raw_text is not null
      and length(e.raw_text) >= 300
    """
    if not force:
        sql += """
        and not exists (
          select 1 from public.geo_entity_response d
          where d.entity_type = 'cpad'
            and d.entity_id   = e.entity_id
            and d.source_url  = e.source_url
            and d.response_scope = 'description'
        )
        """
    sql += " order by e.entity_id, e.source_url, e.scraped_at desc"
    if limit:
        sql += f" limit {limit}"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tf:
        tf.write(sql + ';')
        tmp = tf.name
    try:
        r = subprocess.run(
            ['supabase', 'db', 'query', '--linked', '-f', tmp],
            capture_output=True, text=True, timeout=120,
            encoding='utf-8', errors='replace',
        )
        if r.returncode != 0:
            print(f"SQL failed: {r.stderr}", file=sys.stderr)
            return []
        out = r.stdout
        s, e = out.find('{'), out.rfind('}')
        return json.loads(out[s:e+1]).get('rows', [])
    finally:
        try: os.unlink(tmp)
        except: pass


def insert_row(row: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url, headers={**sb_headers(), "Prefer": "return=minimal"},
                   json={**row, "response_scope": RESPONSE_SCOPE,
                         "extraction_model": MODEL}, timeout=20)
    if not r.is_success:
        print(f"    insert failed: {r.status_code} {r.text[:200]}", file=sys.stderr)


async def llm_describe(client: httpx.AsyncClient, raw_text: str, source_url: str) -> Optional[str]:
    user = (f"Source URL (for context only): {source_url}\n\n"
            f"Page content:\n{raw_text}")
    payload = {
        "model": MODEL, "max_tokens": 300, "system": DESCRIPTION_PROMPT,
        "messages": [{"role": "user", "content": user}],
    }
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json=payload, timeout=45.0,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        text = re.sub(r'^["\']|["\']$', '', text).strip()
        text = re.sub(r"^```.*?\n|\n```$", "", text, flags=re.S).strip()
        if not text:
            return None
        return text[:MAX_DESC_CHARS]
    except Exception as e:
        print(f"    LLM error: {e}", file=sys.stderr)
        return None


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, target: dict) -> str:
    async with sem:
        cpad_id   = target["cpad_unit_id"]
        url       = target["source_url"]
        raw_text  = target["raw_text"]

        base = {
            "entity_type":  "cpad",
            "entity_id":    cpad_id,
            "source_url":   url,
            "raw_text":     raw_text,
        }

        desc = await llm_describe(client, raw_text, url)
        if not desc:
            insert_row({**base, "fetch_status": "llm_error"})
            return f"  unit={cpad_id}  llm_error"

        insert_row({**base,
                    "fetch_status": "success",
                    "response_value":  desc,
                    "response_confidence": 0.85,  # generic baseline; LLM doesn't self-rate descriptions
                    "extracted_at": "now()"})
        return f"  unit={cpad_id}  {desc[:120]}"


async def run(args: argparse.Namespace) -> None:
    targets = fetch_targets(args.limit, args.force)
    print(f"Loaded {len(targets)} geo_entity_response rows to describe\n")
    if not targets:
        return

    sem    = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, t) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            print(await t)
            if i % 25 == 0:
                print(f"  --- {i}/{len(targets)} done ---")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
