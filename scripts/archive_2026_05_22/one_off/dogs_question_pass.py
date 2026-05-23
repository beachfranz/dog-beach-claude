"""
dogs_question_pass.py
---------------------
Cheap section-targeted re-extraction of one question:
"Are dogs allowed at this beach?"

For each row in park_url_extractions where extraction_status='success'
and raw_text is not null:
  1. Regex out 200-char-before / 500-char-after windows around any
     occurrence of dog/pet/leash/service-animal keywords. Dedupe
     overlapping windows. Cap total at ~2000 chars.
  2. If no keyword hits, skip the LLM call — mark "no_dog_keywords".
  3. Otherwise, feed the snippet to claude-haiku-4-5 with a single-
     question prompt. Get back JSON: {dogs_allowed, reason, confidence}.

Doesn't write to DB. Reports coverage + answer distribution + sample
disagreements with existing park_url_extractions.dogs_allowed.

Usage:
  python scripts/one_off/dogs_question_pass.py [--limit N]
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
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 8

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


def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }


def fetch_targets(limit: Optional[int]) -> list[dict]:
    """Pull successful extractions with non-null raw_text. Includes the
    existing dogs_allowed for comparison."""
    select = "id,fid,source_url,raw_text,dogs_allowed"
    params = (f"{select}"
              "&extraction_status=eq.success"
              "&raw_text=not.is.null"
              "&order=id.asc")
    if limit:
        params += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/park_url_extractions?select={params}"
    r = httpx.get(url, headers=sb_headers(), timeout=60.0)
    r.raise_for_status()
    return [r for r in r.json() if r.get("raw_text") and len(r["raw_text"]) >= 200]


def extract_snippet(raw_text: str) -> tuple[str, int]:
    """Return (snippet, n_keyword_hits). Snippet is concatenated dedupe'd
    windows around keyword hits, capped at SNIPPET_CAP chars."""
    hits = list(KEYWORD_RE.finditer(raw_text))
    if not hits:
        return "", 0

    # Build (start, end) windows, then merge overlaps.
    windows: list[tuple[int, int]] = []
    for m in hits:
        s = max(0, m.start() - WINDOW_BEFORE)
        e = min(len(raw_text), m.end() + WINDOW_AFTER)
        windows.append((s, e))
    # Merge overlapping/adjacent
    windows.sort()
    merged: list[tuple[int, int]] = []
    for s, e in windows:
        if merged and s <= merged[-1][1] + 50:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))

    # Build snippet, cap at SNIPPET_CAP
    parts = []
    total = 0
    for s, e in merged:
        chunk = raw_text[s:e]
        if total + len(chunk) > SNIPPET_CAP:
            chunk = chunk[: SNIPPET_CAP - total]
            parts.append(chunk)
            break
        parts.append(chunk)
        total += len(chunk)

    snippet = "\n---\n".join(parts)
    return snippet, len(hits)


async def llm_answer(client: httpx.AsyncClient, snippet: str) -> Optional[dict]:
    payload = {
        "model":      MODEL,
        "max_tokens": 200,
        "system":     PROMPT,
        "messages":   [{"role": "user", "content": f"Snippet:\n{snippet}"}],
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
            timeout=45.0,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        # Pull the first {…} block defensively
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            text = m.group(0)
        return json.loads(text)
    except Exception as e:
        print(f"    LLM error: {e}", file=sys.stderr)
        return None


async def process_one(sem: asyncio.Semaphore, client: httpx.AsyncClient, row: dict) -> dict:
    async with sem:
        snippet, hits = extract_snippet(row["raw_text"])
        if not snippet:
            return {**row, "outcome": "no_keywords", "answer": None, "snippet_len": 0, "hits": 0}
        answer = await llm_answer(client, snippet)
        if not answer:
            return {**row, "outcome": "llm_error", "answer": None, "snippet_len": len(snippet), "hits": hits}
        return {**row, "outcome": "answered", "answer": answer, "snippet_len": len(snippet), "hits": hits}


async def run(args: argparse.Namespace) -> None:
    targets = fetch_targets(args.limit)
    print(f"Loaded {len(targets)} extractions to ask 'are dogs allowed?'\n")
    if not targets:
        return

    sem    = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)
    results: list[dict] = []
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [process_one(sem, client, r) for r in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            results.append(await t)
            if i % 50 == 0:
                print(f"  --- {i}/{len(targets)} done ---")

    # ── Stats ───────────────────────────────────────────────────────
    print("\n========== COVERAGE ==========")
    outcomes = Counter(r["outcome"] for r in results)
    for k, v in outcomes.most_common():
        print(f"  {k:20s} {v:4d}  ({v*100//len(results)}%)")

    print("\n========== DOGS_ALLOWED DISTRIBUTION (new answers) ==========")
    answers = Counter(
        r["answer"]["dogs_allowed"] for r in results
        if r["outcome"] == "answered" and r["answer"].get("dogs_allowed")
    )
    for k, v in answers.most_common():
        print(f"  {k:20s} {v:4d}")

    print("\n========== AVG SNIPPET LENGTH (chars) ==========")
    lens = [r["snippet_len"] for r in results if r["snippet_len"]]
    print(f"  avg={sum(lens)//len(lens) if lens else 0}  max={max(lens) if lens else 0}  min={min(lens) if lens else 0}")

    print("\n========== AGREEMENT vs EXISTING dogs_allowed ==========")
    pairs = [(r["dogs_allowed"], r["answer"]["dogs_allowed"]) for r in results
             if r["outcome"] == "answered" and r.get("dogs_allowed") and r["answer"].get("dogs_allowed")]
    agree = sum(1 for a, b in pairs if a == b)
    print(f"  pairs with both old+new: {len(pairs)}")
    print(f"  agree: {agree} ({agree*100//len(pairs) if pairs else 0}%)")
    print(f"  disagree: {len(pairs) - agree}")
    by_pair = Counter(pairs)
    print("  top transitions (old -> new):")
    for (old, new), n in by_pair.most_common(12):
        marker = "  " if old == new else "->"
        print(f"    {marker} {old:12s} -> {new:12s}  {n}")

    print("\n========== SAMPLES (no_keywords) ==========")
    for r in [x for x in results if x["outcome"] == "no_keywords"][:5]:
        print(f"  fid={r['fid']}  {r['source_url']}")

    print("\n========== SAMPLES (low confidence) ==========")
    low = sorted(
        [x for x in results if x["outcome"] == "answered" and x["answer"].get("confidence", 1) < 0.6],
        key=lambda x: x["answer"].get("confidence", 1),
    )[:8]
    for r in low:
        a = r["answer"]
        print(f"  fid={r['fid']}  conf={a.get('confidence')}  ans={a.get('dogs_allowed')}")
        print(f"    URL: {r['source_url']}")
        print(f"    reason: {a.get('reason')}")

    print("\n========== SAMPLES (disagreements with existing) ==========")
    disagree = [r for r in results
                if r["outcome"] == "answered"
                and r.get("dogs_allowed")
                and r["answer"].get("dogs_allowed")
                and r["dogs_allowed"] != r["answer"]["dogs_allowed"]]
    for r in disagree[:10]:
        a = r["answer"]
        print(f"  fid={r['fid']}  old={r['dogs_allowed']}  new={a['dogs_allowed']}  conf={a.get('confidence')}")
        print(f"    URL: {r['source_url']}")
        print(f"    reason: {a.get('reason')}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
