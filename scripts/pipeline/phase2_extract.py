#!/usr/bin/env python3
"""
Phase 2 — Structured field extraction for beaches already classified in Phase 1.

Runs only on records promoted to 'gold' by Phase 1 (confirmed or probable policy).
Re-fetches the best trusted source URL and uses an LLM to extract structured fields
from a page we already know is relevant and trustworthy.

Extracts:
  - allowed_hours_text   (e.g. "6:00am – 10:00pm daily")
  - seasonal_start       (e.g. "June 15")
  - seasonal_end         (e.g. "Labor Day")
  - dogs_prohibited_start (time window when dogs are prohibited)
  - dogs_prohibited_end
  - day_restrictions     (e.g. "Weekends only" or "No dogs Saturdays 10am-5pm")
  - zone_description     (e.g. "Designated dog area north of the pier")
  - access_rule          (refine: off_leash | on_leash | mixed)
  - dogs_prohibited_reason (if prohibited)

Usage:
  python phase2_extract.py --county "Orange" --state "CA"
  python phase2_extract.py --county "Orange" --state "CA" --dry-run
  python phase2_extract.py --county "Orange" --state "CA" --resume
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter
from tqdm import tqdm

load_dotenv(Path(__file__).parent / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

FETCH_CHAR_LIMIT = 20_000
FETCH_TIMEOUT    = 30.0
WORKERS          = 3
CHECKPOINT_EVERY = 10
MODEL            = "claude-haiku-4-5-20251001"

USER_AGENT = "dog-beach-scout/1.0 (contact: franz@franzfunk.com)"

# ── Supabase ──────────────────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def fetch_gold_beaches(county: str, state: str, limit: Optional[int] = None) -> list[dict]:
    from urllib.parse import quote
    params = (
        f"county=eq.{quote(county)}&state=eq.{quote(state)}&quality_tier=eq.gold"
        f"&or=(dedup_status.is.null,dedup_status.eq.reviewed)"
        f"&select=id,display_name,city,county,state,dogs_allowed,access_rule,"
        f"policy_source_url,policy_confidence,policy_notes"
        f"&order=id"
    )
    if limit:
        params += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/beaches_staging?{params}"
    resp = httpx.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()

def write_extraction(beach_id: int, fields: dict[str, Any], dry_run: bool) -> None:
    allowed_keys = {
        "allowed_hours_text", "seasonal_start", "seasonal_end",
        "dogs_prohibited_start", "dogs_prohibited_end",
        "day_restrictions", "zone_description", "access_rule",
        "dogs_prohibited_reason", "updated_at",
    }
    payload = {k: v for k, v in fields.items() if k in allowed_keys and v is not None}
    payload["updated_at"] = "now()"

    if dry_run:
        print(f"  [dry-run] id={beach_id}: {payload}")
        return

    url = f"{SUPABASE_URL}/rest/v1/beaches_staging?id=eq.{beach_id}"
    resp = httpx.patch(url, headers=sb_headers(), json=payload, timeout=15)
    resp.raise_for_status()

# ── Page fetch ────────────────────────────────────────────────────────────────

async def fetch_page(client: httpx.AsyncClient, url: str, cache_dir: Path) -> str:
    key = cache_dir / f"{hashlib.sha1(url.encode()).hexdigest()}.txt"
    if key.exists():
        return key.read_text(encoding="utf-8", errors="ignore")
    try:
        resp = await client.get(url, headers={"User-Agent": USER_AGENT},
                                timeout=FETCH_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "")
        if "pdf" in ctype.lower():
            key.write_text("")
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav", "form"]):
            tag.extract()
        texts = []
        for selector in ["main", "article", "body"]:
            node = soup.select_one(selector)
            if node:
                texts.append(node.get_text(" ", strip=True))
                break
        if not texts:
            texts.append(soup.get_text(" ", strip=True))
        text = re.sub(r"\s+", " ", " ".join(texts)).strip()[:FETCH_CHAR_LIMIT]
    except Exception:
        text = ""
    key.write_text(text, encoding="utf-8")
    return text

# ── LLM extraction ────────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
You are extracting structured dog-access policy fields from a beach or park web page.
Return ONLY a valid JSON object with these exact keys (use null for fields not found):

{
  "allowed_hours_text":      string | null,  // plain-English hours dogs are allowed, e.g. "6:00am–10:00pm daily"
  "seasonal_start":          string | null,  // start of period when dogs ARE allowed, e.g. "June 15" or "Labor Day"
  "seasonal_end":            string | null,  // end of that period, e.g. "September 15"
  "dogs_prohibited_start":   string | null,  // start of a prohibited window within a day, e.g. "10:00am"
  "dogs_prohibited_end":     string | null,  // end of that window, e.g. "5:00pm"
  "day_restrictions":        string | null,  // e.g. "Weekends only" or "No dogs Memorial Day–Labor Day"
  "zone_description":        string | null,  // where on the beach dogs are allowed, e.g. "North of the pier, designated area"
  "access_rule":             "off_leash" | "on_leash" | "mixed" | null,
  "dogs_prohibited_reason":  string | null   // only if dogs are prohibited, e.g. "Nesting shorebird habitat"
}

Rules:
- Extract only what is explicitly stated. Do not infer or guess.
- If multiple rules exist (e.g. seasonal + hourly), capture all of them in the appropriate fields.
- "mixed" access_rule means off-leash in some areas/times and on-leash in others.
- Return raw JSON only — no markdown, no commentary.
"""

@retry(wait=wait_exponential_jitter(1, 6), stop=stop_after_attempt(3),
       retry=retry_if_exception_type(httpx.HTTPError))
async def extract_fields(
    client: httpx.AsyncClient,
    beach_name: str,
    page_text: str,
) -> dict[str, Any]:
    if not page_text.strip():
        return {}

    user_content = (
        f"Beach name: {beach_name}\n\n"
        f"Page text (truncated to {FETCH_CHAR_LIMIT} chars):\n{page_text}"
    )

    payload = {
        "model": MODEL,
        "max_tokens": 512,
        "system": EXTRACTION_PROMPT,
        "messages": [{"role": "user", "content": user_content}],
    }

    resp = await client.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    text = resp.json()["content"][0]["text"].strip()

    # Strip markdown fences if present
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}

# ── Checkpoint ────────────────────────────────────────────────────────────────

def load_checkpoint(f: Path) -> set[int]:
    if not f.exists():
        return set()
    return set(json.loads(f.read_text()).get("completed_ids", []))

def save_checkpoint(f: Path, completed_ids: set[int]) -> None:
    f.write_text(json.dumps({"completed_ids": sorted(completed_ids), "updated_at": int(time.time())}, indent=2))

# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    checkpoint_dir  = Path(args.checkpoint_dir)
    cache_dir       = checkpoint_dir / "pages"
    checkpoint_file = checkpoint_dir / "progress.json"
    results_file    = checkpoint_dir / "results.jsonl"

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set in .env")
        return

    print(f"Fetching gold-tier beaches: county={args.county}, state={args.state}")
    beaches = fetch_gold_beaches(args.county, args.state, args.limit or None)
    print(f"  Found {len(beaches)} gold-tier beaches")

    completed_ids = load_checkpoint(checkpoint_file) if args.resume else set()
    pending = [b for b in beaches if b["id"] not in completed_ids and b.get("policy_source_url")]
    print(f"  {len(completed_ids)} already done, {len(pending)} to process")

    semaphore = asyncio.Semaphore(WORKERS)
    limits    = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)

    async with httpx.AsyncClient(limits=limits, http2=True) as client:
        pbar = tqdm(total=len(pending), desc="Extracting", unit="beach")
        results_fh = results_file.open("a", encoding="utf-8")

        async def worker(beach: dict) -> None:
            async with semaphore:
                beach_id  = beach["id"]
                name      = beach["display_name"]
                source_url = beach["policy_source_url"]

                page_text = await fetch_page(client, source_url, cache_dir)
                if not page_text:
                    tqdm.write(f"  Skipping id={beach_id} — could not fetch {source_url}")
                    completed_ids.add(beach_id)
                    pbar.update(1)
                    return

                fields = await extract_fields(client, name, page_text)

                row = {"id": beach_id, "display_name": name, "source_url": source_url, **fields}
                results_fh.write(json.dumps(row) + "\n")
                results_fh.flush()

                if fields:
                    write_extraction(beach_id, fields, dry_run=args.dry_run)
                else:
                    tqdm.write(f"  No fields extracted for id={beach_id} ({name})")

                completed_ids.add(beach_id)
                if len(completed_ids) % CHECKPOINT_EVERY == 0:
                    save_checkpoint(checkpoint_file, completed_ids)

                pbar.update(1)

        tasks = [asyncio.create_task(worker(b)) for b in pending]
        try:
            await asyncio.gather(*tasks)
        finally:
            pbar.close()
            results_fh.close()

    save_checkpoint(checkpoint_file, completed_ids)
    print(f"\nDone. Results: {results_file}")


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 2: structured field extraction")
    p.add_argument("--county",         required=True)
    p.add_argument("--state",          required=True)
    p.add_argument("--checkpoint-dir", default="./checkpoints/phase2")
    p.add_argument("--limit",          type=int, default=0)
    p.add_argument("--resume",         action="store_true")
    p.add_argument("--dry-run",        action="store_true")
    args = p.parse_args()
    asyncio.run(run(args))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
