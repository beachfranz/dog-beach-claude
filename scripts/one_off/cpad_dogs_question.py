"""
cpad_dogs_question.py
---------------------
Ask "are dogs allowed?" against every cpad_units.park_url that
contains an active beach point in locations_stage.

Pipeline per CPAD unit:
  1. Reuse cached raw_text from park_url_extractions if available.
  2. Otherwise fetch the URL via httpx + BeautifulSoup (no Playwright;
     the few SPA pages will fail and we'll log fetch_failed).
  3. Section-target dog/pet/leash keyword windows from raw_text.
  4. Send the snippet to claude-haiku-4-5 with a focused prompt.
  5. Insert one row into cpad_unit_dog_extractions per (unit_id, url).

Idempotency: if a cpad_unit_dog_extractions row already exists for
this (entity_type='cpad', entity_id, source_url), skip — re-runs require deleting the
prior row first (or running with --force).

Usage:
  python scripts/one_off/cpad_dogs_question.py [--limit N] [--force]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 6
USER_AGENT            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
FETCH_CHAR_LIMIT      = 25_000
FETCH_TIMEOUT         = 25.0
MIN_PAGE_CHARS        = 300

KEYWORD_RE   = re.compile(r"(?i)\b(dog|dogs|pet|pets|leash|leashed|on[- ]leash|off[- ]leash|service\s+animal|canine)\b")
WINDOW_BEFORE = 200
WINDOW_AFTER  = 500
SNIPPET_CAP   = 2000

PROMPT = """\
You are reading a snippet from a beach/park webpage. Answer the single question:
ARE DOGS ALLOWED AT THIS BEACH?

Return ONLY this JSON object — no markdown, no preamble:

{
  "dogs_allowed": "yes" | "no" | "restricted" | "seasonal" | "unknown",
  "reason":      "one short clause quoting or paraphrasing the page basis",
  "confidence":  number 0.00-1.00
}

Definitions:
- "yes":        Dogs allowed without significant restriction (a leash requirement alone is fine).
- "no":         Dogs prohibited entirely (or only service animals are allowed).
- "restricted": Dogs allowed but with notable restrictions — specific zones only, time-of-day windows,
                strict leash mandates beyond the standard (e.g., 6-foot leash only AND only on a trail).
- "seasonal":   Dogs allowed at some times of year and not others.
- "unknown":    The snippet does not give a clear answer.

Confidence:
- 0.95+ when the page explicitly states the rule
- 0.7-0.85 when the rule is implied but stated clearly enough
- < 0.6 if you're guessing
"""


# ── Supabase REST helpers ──────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(limit: Optional[int], force: bool) -> list[dict]:
    """Return [{cpad_unit_id, park_url, cached_raw_text}] for CPADs that
    contain an active locations_stage beach. Skips already-processed
    (unit_id, url) pairs unless --force."""
    sql = """
    with active_beach_cpads as (
      select distinct c.unit_id as cpad_unit_id, trim(c.park_url) as park_url
      from public.locations_stage s
      join public.cpad_units c on st_contains(c.geom, s.geom::geometry)
      where s.is_active = true
        and c.park_url is not null and trim(c.park_url) <> ''
    ),
    cached as (
      select source_url, raw_text
      from (
        select source_url, raw_text,
          row_number() over (partition by source_url order by scraped_at desc) rn
        from public.park_url_extractions
        where extraction_status='success' and raw_text is not null
      ) x where rn=1
    )
    select a.cpad_unit_id, a.park_url, c.raw_text as cached_raw_text
    from active_beach_cpads a
    left join cached c on c.source_url = a.park_url
    """
    if not force:
        sql += """
        where not exists (
          select 1 from public.geo_entity_response e
          where e.entity_type = 'cpad' and e.entity_id = a.cpad_unit_id
            and e.source_url = a.park_url
            and e.response_scope = 'dogs_allowed'
        )
        """
    sql += " order by a.cpad_unit_id"
    if limit:
        sql += f" limit {limit}"

    import subprocess, json as jsonlib, tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tf:
        tf.write(sql + ';')
        tmpfile = tf.name
    try:
        r = subprocess.run(
            ['supabase', 'db', 'query', '--linked', '-f', tmpfile],
            capture_output=True, text=True, timeout=120,
            encoding='utf-8', errors='replace',
        )
        if r.returncode != 0:
            print(f"SQL failed: {r.stderr}", file=sys.stderr)
            return []
        out = r.stdout
        s, e = out.find('{'), out.rfind('}')
        return jsonlib.loads(out[s:e+1]).get('rows', [])
    finally:
        try: os.unlink(tmpfile)
        except: pass


RESPONSE_SCOPE = "dogs_allowed"


def insert_row(row: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url, headers={**sb_headers(), "Prefer": "return=minimal"},
                   json={**row, "response_scope": RESPONSE_SCOPE}, timeout=20)
    if not r.is_success:
        print(f"    insert failed: {r.status_code} {r.text[:200]}", file=sys.stderr)


# ── Fetch + extract ───────────────────────────────────────────────────────────

async def fetch_url(client: httpx.AsyncClient, url: str) -> tuple[Optional[str], int]:
    try:
        r = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=FETCH_TIMEOUT,
                              follow_redirects=True)
        if not r.is_success:
            return None, r.status_code
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n")).strip()[:FETCH_CHAR_LIMIT]
        return text, r.status_code
    except Exception as e:
        print(f"    fetch error ({url}): {type(e).__name__}: {e}", file=sys.stderr)
        return None, 0


def extract_snippet(raw_text: str) -> tuple[str, int]:
    hits = list(KEYWORD_RE.finditer(raw_text))
    if not hits:
        return "", 0
    windows = [(max(0, m.start() - WINDOW_BEFORE), min(len(raw_text), m.end() + WINDOW_AFTER)) for m in hits]
    windows.sort()
    merged: list[tuple[int, int]] = []
    for s, e in windows:
        if merged and s <= merged[-1][1] + 50:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    parts: list[str] = []
    total = 0
    for s, e in merged:
        chunk = raw_text[s:e]
        if total + len(chunk) > SNIPPET_CAP:
            parts.append(chunk[: SNIPPET_CAP - total])
            break
        parts.append(chunk)
        total += len(chunk)
    return "\n---\n".join(parts), len(hits)


async def llm_answer(client: httpx.AsyncClient, snippet: str) -> Optional[dict]:
    payload = {
        "model": MODEL, "max_tokens": 200, "system": PROMPT,
        "messages": [{"role": "user", "content": f"Snippet:\n{snippet}"}],
    }
    try:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json=payload, timeout=45.0,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            text = m.group(0)
        return json.loads(text)
    except Exception as e:
        print(f"    LLM error: {e}", file=sys.stderr)
        return None


# ── Per-target worker ─────────────────────────────────────────────────────────

async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, target: dict) -> str:
    async with sem:
        cpad_id = target["cpad_unit_id"]
        url     = target["park_url"]
        cached  = target.get("cached_raw_text")

        if cached:
            raw_text, http_status = cached, None
        else:
            raw_text, http_status = await fetch_url(client, url)

        base = {
            "entity_type":  "cpad",
            "entity_id":    cpad_id,
            "source_url":   url,
            "http_status":  http_status,
            "raw_text":     raw_text,
            "extraction_model": MODEL,
        }

        if not raw_text or len(raw_text) < MIN_PAGE_CHARS:
            insert_row({**base, "fetch_status": "fetch_failed", "has_keywords": False})
            return f"  unit={cpad_id}  fetch_failed  ({url})"

        snippet, hits = extract_snippet(raw_text)
        if not snippet:
            insert_row({**base, "fetch_status": "no_keywords", "has_keywords": False})
            return f"  unit={cpad_id}  no_keywords ({len(raw_text)} chars)"

        ans = await llm_answer(client, snippet)
        if not ans:
            insert_row({**base, "fetch_status": "llm_error", "has_keywords": True,
                        "snippet": snippet})
            return f"  unit={cpad_id}  llm_error"

        insert_row({**base, "fetch_status": "success", "has_keywords": True,
                    "snippet": snippet,
                    "response_value":      ans.get("dogs_allowed"),
                    "response_reason":     ans.get("reason"),
                    "response_confidence": ans.get("confidence"),
                    "extracted_at":        "now()"})
        return f"  unit={cpad_id}  {ans.get('dogs_allowed'):11s}  conf={ans.get('confidence')}  {ans.get('reason')[:90]}"


async def run(args: argparse.Namespace) -> None:
    targets = fetch_targets(args.limit, args.force)
    print(f"Loaded {len(targets)} CPAD units to process "
          f"({sum(1 for t in targets if t.get('cached_raw_text'))} cached, "
          f"{sum(1 for t in targets if not t.get('cached_raw_text'))} need fetch)\n")
    if not targets:
        return

    sem    = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    statuses: list[str] = []
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        tasks = [process_one(sem, client, t) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            line = await t
            print(line)
            statuses.append(line)
            if i % 25 == 0:
                print(f"  --- {i}/{len(targets)} done ---")

    print(f"\nProcessed {len(statuses)} CPAD units. Run summary query to see distribution:")
    print("  select fetch_status, dogs_allowed, count(*) from public.cpad_unit_dog_extractions group by 1,2;")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--force", action="store_true",
                   help="Re-process units that already have a row")
    args = p.parse_args()
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
