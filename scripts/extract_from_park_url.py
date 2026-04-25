"""
extract_from_park_url.py
------------------------
Fetch CPAD park_url pages, BS4-clean to text, LLM-extract structured beach
metadata, and UPSERT into park_url_extractions.

Architecture mirrors phase2_extract.py:
  - Page caching by URL hash (re-runs are cheap)
  - Checkpoint + resume via park_url_extractions.scraped_at
  - Bounded concurrency (3 workers)
  - Allowed-key filter on writeback (defensive against unexpected LLM keys)

The DB-side `park_url_scrape_queue` view tells us what's pending.

Usage:
  python scripts/extract_from_park_url.py --limit 10
  python scripts/extract_from_park_url.py --limit 100 --dry-run
  python scripts/extract_from_park_url.py --fid 4641976
  python scripts/extract_from_park_url.py --all
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")

SUPABASE_URL          = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY     = os.environ["ANTHROPIC_API_KEY"]

MODEL                 = "claude-haiku-4-5-20251001"
WORKERS               = 3
FETCH_CHAR_LIMIT      = 25_000
FETCH_TIMEOUT         = 30.0
USER_AGENT            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

CACHE_DIR             = Path("./checkpoints/park_url_pages")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ── Supabase REST helpers ────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def fetch_queue(limit: Optional[int] = None, fid_filter: Optional[int] = None) -> list[dict]:
    if fid_filter is not None:
        # Single-fid: bypass the queue view's "stale" filter and just look up CPAD
        params = (
            f"fid=eq.{fid_filter}"
            f"&select=fid,display_name,state_code,cpad_unit_name,park_url,agncy_web,last_scraped_at,last_status"
        )
    else:
        params = (
            f"select=fid,display_name,state_code,cpad_unit_name,park_url,agncy_web,last_scraped_at,last_status"
            f"&order=last_scraped_at.asc"
        )
        if limit:
            params += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/park_url_scrape_queue?{params}"
    resp = httpx.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()

def upsert_extraction(row: dict, dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] fid={row['fid']} status={row.get('extraction_status')}")
        return
    url = f"{SUPABASE_URL}/rest/v1/park_url_extractions?on_conflict=fid,source_url"
    headers = {**sb_headers(), "Prefer": "resolution=merge-duplicates"}
    resp = httpx.post(url, headers=headers, json=row, timeout=15)
    if not resp.is_success:
        print(f"  upsert failed for fid={row['fid']}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        resp.raise_for_status()


# ── Fetch + BS4 strip ────────────────────────────────────────────────────────

async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[Optional[str], int]:
    """Returns (cleaned_text, http_status). Cleaned text is None if fetch failed."""
    cache_key = CACHE_DIR / f"{hashlib.sha1(url.encode()).hexdigest()}.txt"
    if cache_key.exists():
        return cache_key.read_text(encoding="utf-8", errors="ignore"), 200

    try:
        resp = await client.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        status = resp.status_code
        if not resp.is_success:
            return None, status
        ctype = resp.headers.get("content-type", "")
        if "html" not in ctype.lower():
            return None, status

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "noscript", "iframe", "nav", "footer", "header", "aside", "form"]):
            tag.decompose()
        main = soup.find("main") or soup.find("article") or soup.body or soup
        text = main.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()[:FETCH_CHAR_LIMIT]
        cache_key.write_text(text, encoding="utf-8")
        return text, status
    except Exception as e:
        print(f"    fetch error ({url}): {e}", file=sys.stderr)
        return None, 0


# ── LLM extraction ───────────────────────────────────────────────────────────

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
- Extract ONLY what is explicitly stated on the page. Do not infer or guess.
- "seasonal" for dogs_allowed = allowed at some times of year and not others.
- "restricted" = allowed with notable rules (specific zones, time windows, leash mandates beyond standard).
- For dogs_restricted_hours: hours when dogs are NOT allowed (the off-window).
- For dogs_seasonal_rules: explicit date ranges with different rules.
- extraction_confidence: 0.95 for clear structured "DOG RULES:" sections; 0.75 for inferred from prose; 0.50 for partial; lower if ambiguous.
- Reply with raw JSON only — no markdown fences, no preamble.
"""

ALLOWED_KEYS = {
    "dogs_allowed", "dogs_leash_required", "dogs_restricted_hours", "dogs_seasonal_rules",
    "dogs_zone_description", "dogs_policy_notes",
    "hours_text", "open_time", "close_time",
    "has_parking", "parking_type", "parking_notes",
    "description",
    "has_restrooms", "has_showers", "has_drinking_water", "has_lifeguards",
    "has_disabled_access", "has_food", "has_fire_pits", "has_picnic_area",
    "extraction_confidence", "extraction_notes",
}

async def extract_fields(client: httpx.AsyncClient, beach_name: str, page_text: str) -> dict[str, Any]:
    if not page_text or len(page_text.strip()) < 100:
        return {}
    payload = {
        "model": MODEL,
        "max_tokens": 1024,
        "system": EXTRACTION_PROMPT,
        "messages": [{
            "role": "user",
            "content": f"Beach name: {beach_name}\n\nPage content (truncated to {FETCH_CHAR_LIMIT} chars):\n{page_text}"
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
    # Strip markdown fences if Claude included them
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"    parse error: {e}", file=sys.stderr)
        print(f"    first 200 chars: {text[:200]}", file=sys.stderr)
        return {}
    # Filter to allowed keys (defensive against LLM adding extras)
    return {k: v for k, v in parsed.items() if k in ALLOWED_KEYS}


# ── Main ────────────────────────────────────────────────────────────────────

async def process_one(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    beach: dict,
    dry_run: bool,
) -> str:
    """Returns a one-line status string."""
    async with sem:
        fid       = beach["fid"]
        name      = beach["display_name"]
        url       = beach["park_url"]
        text, status = await fetch_page(client, url)
        if text is None:
            row = {
                "fid": fid, "source_url": url,
                "extraction_status": "fetch_failed",
                "http_status": status,
                "scraped_at": "now()",
            }
            upsert_extraction(row, dry_run)
            return f"  fid={fid} {name!r}  FETCH_FAILED status={status}"

        content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]

        try:
            parsed = await extract_fields(client, name, text)
        except Exception as e:
            row = {
                "fid": fid, "source_url": url,
                "extraction_status": "parse_failed",
                "http_status": status,
                "raw_text": text[:8000],
                "content_hash": content_hash,
                "extraction_notes": f"LLM error: {e}",
                "scraped_at": "now()",
            }
            upsert_extraction(row, dry_run)
            return f"  fid={fid} {name!r}  PARSE_FAILED {e}"

        if not parsed:
            row = {
                "fid": fid, "source_url": url,
                "extraction_status": "no_data",
                "http_status": status,
                "raw_text": text[:8000],
                "content_hash": content_hash,
                "scraped_at": "now()",
            }
            upsert_extraction(row, dry_run)
            return f"  fid={fid} {name!r}  NO_DATA"

        # Success — build the row
        row = {
            "fid": fid, "source_url": url,
            "extraction_status": "success",
            "http_status": status,
            "raw_text": text[:8000],
            "content_hash": content_hash,
            "extraction_model": MODEL,
            "scraped_at": "now()",
            **{k: parsed.get(k) for k in ALLOWED_KEYS},
        }
        # JSONB fields need to be passed as-is (httpx serializes via json=)
        upsert_extraction(row, dry_run)
        conf = parsed.get("extraction_confidence")
        return f"  fid={fid} {name!r}  ok  conf={conf}  dogs={parsed.get('dogs_allowed')}"


async def run(args: argparse.Namespace) -> None:
    if args.fid:
        beaches = fetch_queue(fid_filter=args.fid)
    elif args.all:
        beaches = fetch_queue()
    else:
        beaches = fetch_queue(limit=args.limit)

    print(f"Loaded {len(beaches)} beaches from queue")
    if not beaches:
        print("Nothing to do.")
        return

    sem = asyncio.Semaphore(WORKERS)
    limits = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)

    async with httpx.AsyncClient(limits=limits, http2=True) as client:
        tasks = [process_one(sem, client, b, args.dry_run) for b in beaches]
        for t in asyncio.as_completed(tasks):
            line = await t
            print(line)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=10, help="how many to process (default 10)")
    p.add_argument("--all",   action="store_true", help="process the entire queue")
    p.add_argument("--fid",   type=int, help="process only this fid (skips queue staleness check)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
