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

# Playwright is optional — only loaded when an httpx fetch hits a 403/429/connect
# error and we want to retry with a real browser. Failure to import is non-fatal
# (script falls back to httpx-only mode).
try:
    from playwright.async_api import async_playwright, Browser as PlaywrightBrowser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightBrowser = None

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

# Candidate selectors for the "main content" of a page, tried in order. We pick
# the candidate with the most text, since some sites (parks.ca.gov) wrap the
# real content in a generic `<div class="main">` while leaving an empty
# semantic `<article>` shell that BS4's find_all-first picks up.
CONTENT_SELECTORS = [
    "main", "article",
    "div.main", "div.content", "div#main", "div#content",
    "div.entry-content", "div.parkContent", "div.page-content",
    "section.content",
]


def _pick_main_content(soup: BeautifulSoup) -> Any:
    """Return the soup node with the most text from a list of common
    main-content containers. Falls back to body, then the whole soup."""
    best = None
    best_len = 0
    for sel in CONTENT_SELECTORS:
        for el in soup.select(sel):
            n = len(el.get_text(strip=True))
            if n > best_len:
                best, best_len = el, n
    if best is not None and best_len > 200:
        return best
    return soup.body or soup


def _strip_chrome(soup: BeautifulSoup) -> None:
    """Remove navigation, scripts, and small forms from `soup` in place.
    `<form>` is kept if it has > 500 chars of text — ASP.NET WebForms
    (e.g. smgov.net) wraps the entire page in a single `<form runat="server">`,
    so a blanket form decompose would zero out the body."""
    for tag in soup(["script", "style", "noscript", "iframe", "nav", "footer", "header", "aside"]):
        tag.decompose()
    for form in soup("form"):
        if len(form.get_text(strip=True)) < 500:
            form.decompose()

# Min cleaned-page length to bother sending to LLM. Pages below this are
# typically JS-rendered SPAs where BS4 stripped everything.
MIN_PAGE_CHARS        = 500

# Retry settings
MAX_FETCH_RETRIES     = 3
FETCH_RETRY_DELAY_S   = 2.0


# ── Supabase REST helpers ────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

_QUEUE_SELECT = (
    "fid,display_name,state_code,cpad_unit_name,park_url,agncy_web,"
    "cpad_distance_m,candidate_rank,discovery_source,extraction_type,"
    "last_scraped_at,last_status"
)

def fetch_queue(limit: Optional[int] = None, fid_filter: Optional[int] = None) -> list[dict]:
    if fid_filter is not None:
        # Single-fid: bypass the queue view's "stale" filter and just look up CPAD
        params = f"fid=eq.{fid_filter}&select={_QUEUE_SELECT}"
    else:
        params = f"select={_QUEUE_SELECT}&order=last_scraped_at.asc"
        if limit:
            params += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/park_url_scrape_queue?{params}"
    resp = httpx.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()

_HHMM_RE = re.compile(r"^\d{2}:\d{2}$")

def _sanitize_row(row: dict) -> dict:
    """Drop values that won't fit their target column type. The LLM occasionally
    puts phrases like 'sunset' / 'dawn' into open_time/close_time; the column
    is Postgres `time`, so we null them out and stash the phrase in hours_text
    if hours_text wasn't otherwise filled."""
    extras = []
    for tcol in ("open_time", "close_time"):
        v = row.get(tcol)
        if v is not None and not (isinstance(v, str) and _HHMM_RE.match(v)):
            extras.append(f"{tcol}={v!r}")
            row[tcol] = None
    if extras:
        existing_notes = row.get("extraction_notes") or ""
        row["extraction_notes"] = (existing_notes + " | non-HHMM time: " + ", ".join(extras)).strip(" |")
        if not row.get("hours_text"):
            row["hours_text"] = ", ".join(extras)
    return row

def upsert_extraction(row: dict, dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] fid={row['fid']} status={row.get('extraction_status')}")
        return
    row = _sanitize_row(row)
    url = f"{SUPABASE_URL}/rest/v1/park_url_extractions?on_conflict=fid,source_url"
    headers = {**sb_headers(), "Prefer": "resolution=merge-duplicates"}
    try:
        resp = httpx.post(url, headers=headers, json=row, timeout=15)
        if not resp.is_success:
            print(f"  upsert failed for fid={row['fid']}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
    except Exception as e:
        # Never let one bad row kill the whole run.
        print(f"  upsert exception for fid={row['fid']}: {e}", file=sys.stderr)


# ── Fetch + BS4 strip ────────────────────────────────────────────────────────

_playwright_browser: Optional["PlaywrightBrowser"] = None
_playwright_lock = asyncio.Lock()

async def _get_browser():
    """Lazy-init shared Playwright browser. Returns None if Playwright unavailable."""
    global _playwright_browser
    if not PLAYWRIGHT_AVAILABLE:
        return None
    async with _playwright_lock:
        if _playwright_browser is None:
            pw = await async_playwright().start()
            _playwright_browser = await pw.chromium.launch(headless=True)
    return _playwright_browser


async def _fetch_with_playwright(url: str) -> tuple[Optional[str], int]:
    """Render the page in Chromium and return cleaned text. Falls back to None on failure."""
    browser = await _get_browser()
    if browser is None:
        return None, 0
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 800},
    )
    page = await context.new_page()
    try:
        resp = await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
        status = resp.status if resp else 0
        if status >= 400:
            return None, status
        # Wait briefly for client-side render
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        _strip_chrome(soup)
        main = _pick_main_content(soup)
        text = main.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()[:FETCH_CHAR_LIMIT]
        return text, status
    except Exception as e:
        print(f"    playwright error ({url}): {e}", file=sys.stderr)
        return None, 0
    finally:
        await context.close()


_PARKS_CA_GOV_LEGACY = re.compile(
    r"^http://www\.parks\.ca\.gov/default\.asp\?", re.IGNORECASE
)
_PARKS_CA_GOV_HTTP = re.compile(
    r"^http://www\.parks\.ca\.gov/", re.IGNORECASE
)

def _normalize_url(url: str) -> str:
    """Rewrite known-deprecated URL patterns to working equivalents.
    parks.ca.gov retired the http://default.asp?page_id= form years ago;
    the modern https://?page_id= still works. Plain http:// also dies on
    that domain; force https://. CPAD has not refreshed."""
    if _PARKS_CA_GOV_LEGACY.match(url):
        return _PARKS_CA_GOV_LEGACY.sub("https://www.parks.ca.gov/?", url)
    if _PARKS_CA_GOV_HTTP.match(url):
        return _PARKS_CA_GOV_HTTP.sub("https://www.parks.ca.gov/", url)
    return url


async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[Optional[str], int]:
    """Returns (cleaned_text, http_status). Cleaned text is None if fetch failed.
    Retries on connection errors + 5xx; gives up on 4xx (won't change with retry).
    Falls back to Playwright when httpx hits 403/429/connection errors OR when
    the BS4-stripped text is below MIN_PAGE_CHARS (JS-rendered SPA)."""
    url = _normalize_url(url)
    cache_key = CACHE_DIR / f"{hashlib.sha1(url.encode()).hexdigest()}.txt"
    if cache_key.exists():
        cached = cache_key.read_text(encoding="utf-8", errors="ignore")
        # Stale cache from before the JS-render fallback — re-fetch via Playwright
        if len(cached.strip()) >= MIN_PAGE_CHARS:
            return cached, 200

    last_status = 0
    for attempt in range(MAX_FETCH_RETRIES):
        try:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                timeout=FETCH_TIMEOUT,
                follow_redirects=True,
            )
            last_status = resp.status_code
            # 403/429: bot-blocked — fall through to Playwright
            if resp.status_code in (403, 429):
                pw_text, pw_status = await _fetch_with_playwright(url)
                if pw_text is not None:
                    cache_key.write_text(pw_text, encoding="utf-8")
                    return pw_text, pw_status
                return None, last_status
            # Other 4xx: giving up — won't change with retry
            if 400 <= resp.status_code < 500:
                return None, last_status
            # 5xx: retry with backoff
            if resp.status_code >= 500:
                if attempt < MAX_FETCH_RETRIES - 1:
                    await asyncio.sleep(FETCH_RETRY_DELAY_S * (2 ** attempt))
                    continue
                return None, last_status
            # 2xx — process
            ctype = resp.headers.get("content-type", "")
            if "html" not in ctype.lower():
                return None, last_status

            soup = BeautifulSoup(resp.text, "lxml")
            _strip_chrome(soup)
            main = _pick_main_content(soup)
            text = main.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()[:FETCH_CHAR_LIMIT]
            # Thin DOM after BS4 strip → JS-rendered SPA; retry with Playwright.
            if len(text) < MIN_PAGE_CHARS:
                pw_text, pw_status = await _fetch_with_playwright(url)
                if pw_text is not None and len(pw_text) >= MIN_PAGE_CHARS:
                    cache_key.write_text(pw_text, encoding="utf-8")
                    return pw_text, pw_status or last_status
                # Playwright also empty — cache the thin text so we don't re-try forever
                cache_key.write_text(text, encoding="utf-8")
                return text, last_status
            cache_key.write_text(text, encoding="utf-8")
            return text, last_status
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
            if attempt < MAX_FETCH_RETRIES - 1:
                await asyncio.sleep(FETCH_RETRY_DELAY_S * (2 ** attempt))
                continue
            # Final attempt failed — try Playwright as last resort
            pw_text, pw_status = await _fetch_with_playwright(url)
            if pw_text is not None:
                cache_key.write_text(pw_text, encoding="utf-8")
                return pw_text, pw_status
            print(f"    fetch error after {MAX_FETCH_RETRIES} retries ({url}): {e}", file=sys.stderr)
            return None, 0
        except Exception as e:
            print(f"    fetch error ({url}): {e}", file=sys.stderr)
            return None, 0
    return None, last_status


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

DOG_FOCUSED_PROMPT = """\
Extract beach dog/pet policy from this page. The page may not have a
"DOG RULES" section — look for ANY mention of dogs, pets, leashes,
service animals, or pet-related restrictions, even buried in prose.

Return ONLY JSON with these keys (null when not stated):

{
  "dogs_allowed":           "yes" | "no" | "seasonal" | "restricted" | "unknown" | null,
  "dogs_leash_required":    "required" | "off_leash_ok" | "mixed" | null,
  "dogs_restricted_hours":  [{"start":"HH:MM","end":"HH:MM"}] | null,
  "dogs_seasonal_rules":    [{"from":"MM-DD","to":"MM-DD","notes":string}] | null,
  "dogs_zone_description":  string | null,
  "dogs_policy_notes":      string | null,
  "extraction_confidence":  number 0.00-1.00,
  "extraction_notes":       string | null
}

Common phrasings to look for:
- "Pets are welcome" / "Pets must be" / "no pets" / "service animals only"
- "Dogs allowed on..." / "leashed at all times" / "6-foot leash"
- "Pets prohibited on beaches" / "Pets allowed in developed areas only"
- "Dogs allowed before 9am and after 6pm" (time windows)

Reply with raw JSON only. If truly nothing about dogs/pets is
mentioned, return all-null with extraction_notes explaining.
"""


async def _llm_call(client: httpx.AsyncClient, system: str, user: str) -> dict[str, Any]:
    """Single LLM call with JSON-fence stripping. Returns parsed dict or {}."""
    payload = {
        "model": MODEL,
        "max_tokens": 1024,
        "system": system,
        "messages": [{"role": "user", "content": user}],
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
        print(f"    first 200 chars: {text[:200]}", file=sys.stderr)
        return {}


def _has_useful_data(parsed: dict[str, Any]) -> bool:
    """True if at least one substantive field beyond the meta keys is filled."""
    substantive = {k for k in ALLOWED_KEYS if k not in {"extraction_confidence", "extraction_notes"}}
    return any(parsed.get(k) is not None for k in substantive)


async def extract_fields(client: httpx.AsyncClient, beach_name: str, page_text: str) -> dict[str, Any]:
    if not page_text or len(page_text.strip()) < MIN_PAGE_CHARS:
        return {}

    # Pass 1 — full structured extraction
    user = f"Beach name: {beach_name}\n\nPage content (truncated to {FETCH_CHAR_LIMIT} chars):\n{page_text}"
    parsed = await _llm_call(client, EXTRACTION_PROMPT, user)
    parsed = {k: v for k, v in parsed.items() if k in ALLOWED_KEYS}

    # Pass 2 — if pass 1 yielded no useful data AND the page mentions dogs/pets,
    # re-prompt with the dog-focused framing.
    if not _has_useful_data(parsed) and re.search(r"(?i)\b(dog|pet|leash)", page_text):
        dog_user = f"Beach name: {beach_name}\n\nPage content:\n{page_text}"
        dog_parsed = await _llm_call(client, DOG_FOCUSED_PROMPT, dog_user)
        dog_parsed = {k: v for k, v in dog_parsed.items() if k in ALLOWED_KEYS}
        if _has_useful_data(dog_parsed):
            # Merge: dog-focused fills any nulls from pass 1
            for k, v in dog_parsed.items():
                if parsed.get(k) is None and v is not None:
                    parsed[k] = v
            # Annotate that this was a multi-pass result
            existing = parsed.get("extraction_notes") or ""
            parsed["extraction_notes"] = (existing + " | dog-focused-rescue").strip(" |")

    return parsed


# ── Main ────────────────────────────────────────────────────────────────────

def _audit_fields(beach: dict) -> dict:
    """Audit columns to carry into every park_url_extractions row so the
    populator can attribute evidence to the source CPAD + extraction method."""
    return {
        "cpad_unit_name":  beach.get("cpad_unit_name"),
        "extraction_type": beach.get("extraction_type"),
    }


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
        audit     = _audit_fields(beach)
        text, status = await fetch_page(client, url)
        if text is None:
            row = {
                "fid": fid, "source_url": url,
                "extraction_status": "fetch_failed",
                "http_status": status,
                "scraped_at": "now()",
                **audit,
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
                **audit,
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
                **audit,
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
            **audit,
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

    # Clean up Playwright browser if it was started
    global _playwright_browser
    if _playwright_browser is not None:
        try:
            await _playwright_browser.close()
        except Exception:
            pass


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
