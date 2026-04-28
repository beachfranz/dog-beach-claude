"""
cpad_leash_question.py
----------------------
Extract leash policy per CPAD unit, writing one row per unit to
geo_entity_response (entity_type='cpad') with response_scope='leash_policy'.

Reuses cached raw_text from existing dogs_allowed rows. For units
where dogs_allowed='no', skips the LLM call and writes a stub
"not applicable" row so consumers can always look up leash_policy
without conditional logic.

Usage:
  python scripts/one_off/cpad_leash_question.py [--limit N] [--force]
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
RESPONSE_SCOPE        = "leash_policy"

KEYWORD_RE   = re.compile(r"(?i)\b(dog|dogs|pet|pets|leash|leashed|on[- ]leash|off[- ]leash|service\s+animal|canine)\b")
WINDOW_BEFORE = 250
WINDOW_AFTER  = 600
SNIPPET_CAP   = 2400

PROMPT = """\
You are reading a snippet from a beach/park webpage. Extract the dog
LEASH POLICY into the JSON object below. Each top-level question has
a sub-object you fill in (or null when the page is silent on that
aspect).

Return ONLY this JSON — no markdown, no preamble:

{
  "off_leash":   { "allowed": true|false|null, "zones": "where off-leash is allowed" or null },
  "on_leash":    { "zones":   "where leash is required" or null,
                   "max_leash_ft": number or null },
  "prohibited":  { "zones":   "where dogs are prohibited entirely" or null },
  "time_variations": {
    "daily":       [{"start":"HH:MM","end":"HH:MM","rule":"leashed"|"off_leash"|"prohibited","reason":"..."}] | null,
    "seasonal":    [{"start_mmdd":"MM-DD","end_mmdd":"MM-DD","rule":"leashed"|"off_leash"|"prohibited","reason":"..."}] | null,
    "day_of_week": [{"days":["mon","tue","wed","thu","fri","sat","sun"],"rule":"leashed"|"off_leash"|"prohibited","reason":"..."}] | null
  },
  "confidence":  number 0.00-1.00,
  "evidence":    "short quote or paraphrase from the page that supports the answer"
}

Rules:
- Extract ONLY what is explicitly stated. Do not infer.
- Sub-fields are null when the page doesn't address that question.
  Do not put empty arrays unless the page explicitly says e.g. "no
  seasonal restrictions".
- The `rule` field per time-window is the rule THAT TIME WINDOW (so
  "leash required from 10am to 4pm" → rule="leashed", start/end times
  bound the leash window; "off-leash before 9am" → rule="off_leash",
  start/end bound the off-leash window).
- max_leash_ft is null unless a length is explicitly stated (e.g.,
  "6-foot leash" → 6).
- Confidence: 0.95+ when the rule is explicit; 0.7-0.85 if implied;
  < 0.6 if guessing.
"""

# Default for units where dogs are not allowed at all.
NO_DOGS_DEFAULT = {
    "applicable": False,
    "off_leash":              {"allowed": False, "zones": None},
    "on_leash":               {"zones": None, "max_leash_ft": None},
    "prohibited":             {"zones": None},
    "time_variations":        {"daily": None, "seasonal": None, "day_of_week": None},
    "confidence": 1.0,
    "evidence":   "Derived from dogs_allowed='no' on this unit; leash policy N/A.",
    "derived_from": "dogs_allowed_no",
}


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(limit: Optional[int], force: bool) -> list[dict]:
    """Pull geo_entity_response (CPAD) rows with raw_text + their
    dogs_allowed answer (so we can short-circuit prohibited units).
    Skip already-processed leash_policy rows unless --force."""
    sql = """
    select distinct on (e.entity_id, e.source_url)
      e.entity_id as cpad_unit_id,
      e.source_url,
      e.raw_text,
      d.response_value as dogs_allowed
    from public.geo_entity_response e
    left join lateral (
      select response_value
      from public.geo_entity_response x
      where x.entity_type   = 'cpad'
        and x.entity_id     = e.entity_id
        and x.source_url    = e.source_url
        and x.response_scope = 'dogs_allowed'
        and x.fetch_status  = 'success'
      order by x.scraped_at desc limit 1
    ) d on true
    where e.entity_type = 'cpad'
      and e.response_scope = 'dogs_allowed'
      and e.raw_text is not null
      and length(e.raw_text) >= 300
    """
    if not force:
        sql += """
        and not exists (
          select 1 from public.geo_entity_response l
          where l.entity_type = 'cpad'
            and l.entity_id   = e.entity_id
            and l.source_url  = e.source_url
            and l.response_scope = 'leash_policy'
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
        "model": MODEL, "max_tokens": 800, "system": PROMPT,
        "messages": [{"role": "user", "content": f"Snippet:\n{snippet}"}],
    }
    try:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json=payload, timeout=60.0,
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


def derive_text_summary(payload: dict) -> str:
    """Short human-readable digest for the response_value text column."""
    if not payload.get("applicable", True):
        return "n/a (dogs prohibited)"
    parts = []
    off = payload.get("off_leash") or {}
    on  = payload.get("on_leash")  or {}
    prh = payload.get("prohibited") or {}
    if off.get("allowed") is True and off.get("zones"):
        parts.append(f"off-leash: {off['zones']}")
    elif off.get("allowed") is True:
        parts.append("off-leash allowed")
    elif off.get("allowed") is False:
        parts.append("no off-leash")
    if on.get("zones"):
        s = f"leashed: {on['zones']}"
        if on.get("max_leash_ft"):
            s += f" ({on['max_leash_ft']}ft max)"
        parts.append(s)
    if prh.get("zones"):
        parts.append(f"prohibited: {prh['zones']}")
    tv = payload.get("time_variations") or {}
    if tv.get("daily"):       parts.append(f"daily-rules:{len(tv['daily'])}")
    if tv.get("seasonal"):    parts.append(f"seasonal-rules:{len(tv['seasonal'])}")
    if tv.get("day_of_week"): parts.append(f"dow-rules:{len(tv['day_of_week'])}")
    return "; ".join(parts) if parts else "(no leash info on page)"


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, target: dict) -> str:
    async with sem:
        cpad_id = target["cpad_unit_id"]
        url     = target["source_url"]
        raw     = target["raw_text"]
        dogs    = target.get("dogs_allowed")

        base = {
            "entity_type":  "cpad",
            "entity_id":    cpad_id,
            "source_url":   url,
            "raw_text":     raw,
        }

        # Short-circuit prohibited units with the no-dogs default.
        if dogs == "no":
            insert_row({**base, "fetch_status": "success",
                        "has_keywords":   False,
                        "response_value": "n/a (dogs prohibited)",
                        "response_value_jsonb": NO_DOGS_DEFAULT,
                        "response_confidence":  1.0,
                        "extracted_at":  "now()"})
            return f"  unit={cpad_id}  default (dogs prohibited)"

        snippet, hits = extract_snippet(raw)
        if not snippet:
            insert_row({**base, "fetch_status": "no_keywords", "has_keywords": False})
            return f"  unit={cpad_id}  no_keywords"

        ans = await llm_answer(client, snippet)
        if not ans:
            insert_row({**base, "fetch_status": "llm_error", "has_keywords": True,
                        "snippet": snippet})
            return f"  unit={cpad_id}  llm_error"

        ans.setdefault("applicable", True)
        summary = derive_text_summary(ans)
        insert_row({**base,
                    "fetch_status":         "success",
                    "has_keywords":         True,
                    "snippet":              snippet,
                    "response_value":       summary,
                    "response_value_jsonb": ans,
                    "response_confidence":  ans.get("confidence"),
                    "extracted_at":         "now()"})
        return f"  unit={cpad_id}  conf={ans.get('confidence')}  {summary[:120]}"


async def run(args: argparse.Namespace) -> None:
    targets = fetch_targets(args.limit, args.force)
    n_skip = sum(1 for t in targets if t.get("dogs_allowed") == "no")
    print(f"Loaded {len(targets)} units to process ({n_skip} will use no-dogs default)\n")
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
