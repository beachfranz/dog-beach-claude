"""harvest_park_text.py — harvest descriptive prose from state-park
leaf URLs into park_url_extractions.

Fills the description text reservoir gap (Gap 2 per
project_deferred_description_text_reservoir.md) for OR/WA where pad_us
polygons don't carry park_url. Reuses each state's existing
StateParksLoader subclass for the polygon → leaf-URL mapping (already
proven for photos), fetches each leaf page, BS4-cleans, and upserts to
park_url_extractions(fid, source_url, raw_text) — one row per
(beach, leaf-url) pair.

The description generator (scripts/generate_beach_descriptions.py)
already reads raw_text from park_url_extractions, so once rows land
the next description run picks them up automatically. No structured
LLM extraction here (dogs_*, hours_*, accessibility) — that's a
deferred follow-on per the pin. raw_text alone is the grounding
signal descriptions need.

CA: skipped. CPAD already gives every park polygon a park_url which
extract_from_park_url.py harvests via the park_url_scrape_queue view.

Usage:
  python scripts/harvest_park_text.py --state OR --dry-run
  python scripts/harvest_park_text.py --state WA --workers 6
  python scripts/harvest_park_text.py --state OR --refresh
  python scripts/harvest_park_text.py --state OR --skip-if-fresh-within 90

Freshness guard: --skip-if-fresh-within DAYS (default 90, matches the
CA park_url_scrape_queue stale window). 0 disables.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# sys.path bootstrap per HARD pin sys-path-bootstrap-for-common-imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scripts.loaders._base import BROWSER_HEADERS
from scripts.loaders.oprd import OprdLoader
from scripts.loaders.wsprc import WsprcLoader

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(
    host=_p.hostname, port=_p.port or 5432, user=_p.username,
    password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require",
)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# Same content-selectors + chrome-strip as extract_from_park_url.py
# (the CA park-URL harvester). Keep them mirrored — if one improves,
# revisit both.
CONTENT_SELECTORS = [
    "main", "article",
    "div.main", "div.content", "div#main", "div#content",
    "div.entry-content", "div.parkContent", "div.page-content",
    "section.content",
]
CHROME_TAGS = ["script", "style", "noscript", "iframe", "nav",
               "footer", "header", "aside"]

# Per-leaf raw_text cap. Matches park_url_extractions.raw_text observed
# p90 of 8KB. Higher than extract_from_park_url's 25000 because we want
# more grounding text and aren't sending to LLM here.
RAW_TEXT_CAP = 8000
MIN_USEFUL_CHARS = 500
FETCH_TIMEOUT_S = 30.0

LOADERS = {"OR": OprdLoader, "WA": WsprcLoader}


def pick_main_content(soup: BeautifulSoup):
    best, best_len = None, 0
    for sel in CONTENT_SELECTORS:
        for el in soup.select(sel):
            n = len(el.get_text(strip=True))
            if n > best_len:
                best, best_len = el, n
    if best is not None and best_len > 200:
        return best
    return soup.body or soup


def strip_chrome(soup: BeautifulSoup) -> None:
    for tag in soup(CHROME_TAGS):
        tag.decompose()
    for form in soup("form"):
        if len(form.get_text(strip=True)) < 500:
            form.decompose()


def fetch_and_clean(url: str) -> tuple[int | None, str]:
    """Fetch + BS4-clean a leaf URL. Returns (http_status, raw_text).
    raw_text is '' on any failure (including <MIN_USEFUL_CHARS)."""
    try:
        with httpx.Client(headers=BROWSER_HEADERS, follow_redirects=True,
                          timeout=FETCH_TIMEOUT_S) as c:
            r = c.get(url)
    except httpx.HTTPError as e:
        return None, ""
    if r.status_code != 200:
        return r.status_code, ""
    soup = BeautifulSoup(r.text, "html.parser")
    strip_chrome(soup)
    content = pick_main_content(soup)
    text = " ".join(content.get_text(separator=" ", strip=True).split())
    if len(text) < MIN_USEFUL_CHARS:
        return r.status_code, ""
    return r.status_code, text[:RAW_TEXT_CAP]


_SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}


def upsert(fid: int, url: str, http_status: int | None, raw_text: str,
           park_name: str, dry_run: bool) -> bool:
    """Returns True if the row landed (or dry-run pretended to land)."""
    if dry_run:
        kind = "ok" if raw_text else "empty"
        print(f"  [dry-run] fid={fid} {kind} {len(raw_text)}ch {url}")
        return True
    row = {
        "fid": fid,
        "arena_group_id": fid,
        "source_url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "extraction_status": "success" if raw_text else "no_data",
        "http_status": http_status,
        "raw_text": raw_text or None,
        "extraction_type": "leaf_page_harvest",
        "extraction_notes": f"park={park_name}",
    }
    u = f"{SUPABASE_URL}/rest/v1/park_url_extractions?on_conflict=fid,source_url"
    try:
        resp = httpx.post(u, headers=_SUPA_HEADERS, json=row, timeout=15)
        if not resp.is_success:
            print(f"  upsert FAIL fid={fid}: {resp.status_code} {resp.text[:200]}",
                  file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"  upsert EXC fid={fid}: {e}", file=sys.stderr)
        return False


def fid_skip_set(pg, state: str, skip_days: int) -> set[int]:
    """Return fids that already have a fresh leaf_page_harvest row.
    Within --skip-if-fresh-within days. Idempotent re-runs are free."""
    if skip_days <= 0:
        return set()
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.fid
              FROM public.park_url_extractions p
              JOIN public.beaches_gold g ON g.fid = p.fid
             WHERE g.state = %s AND g.is_active
               AND p.extraction_type = 'leaf_page_harvest'
               AND p.scraped_at > now() - (%s::int || ' days')::interval
               AND p.extraction_status = 'success'
            """,
            (state, skip_days),
        )
        return {r[0] for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, choices=sorted(LOADERS.keys()),
                    help="State code (OR or WA). CA harvested by "
                         "extract_from_park_url.py via park_url_scrape_queue.")
    ap.add_argument("--workers", type=int, default=4,
                    help="Parallel page fetches. Default 4 — pacing.")
    ap.add_argument("--limit", type=int, default=0,
                    help="Cap parks processed (0 = all matched).")
    ap.add_argument("--refresh", action="store_true",
                    help="Re-fetch even if a recent row exists.")
    ap.add_argument("--skip-if-fresh-within", type=int, default=90,
                    metavar="DAYS",
                    help="Skip fids with leaf_page_harvest row newer than "
                         "N days. Default 90 (matches CA stale window). "
                         "0 to disable. Implied 0 when --refresh.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    state = args.state
    cls = LOADERS[state]
    loader = cls()
    skip_days = 0 if args.refresh else args.skip_if_fresh_within

    pg = psycopg2.connect(**PG)
    try:
        # 1. Discover parks (state-park-system master list)
        with loader.http_session() as http:
            parks_raw = loader.discover_parks(http)
        if not parks_raw:
            print(f"[{state}] discover_parks returned 0 parks", file=sys.stderr)
            return 1
        print(f"[{state}] {len(parks_raw)} parks discovered")

        # 2. State PAD-US polygons containing ≥1 scoreable beach
        polygons = loader.get_state_polygons(pg)
        print(f"[{state}] {len(polygons)} pad_us polygons containing scoreable beaches")

        # 3. Match parks → polygons (name match + token-subset fallback)
        parks = loader.resolve_park_polygons(parks_raw, polygons)
        if not parks:
            print(f"[{state}] resolve_park_polygons matched 0 parks "
                  f"against polygons — name normalization mismatch?",
                  file=sys.stderr)
            return 1
        print(f"[{state}] {len(parks)} parks matched to polygons "
              f"(carrying {sum(len(p.fids) for p in parks.values())} beach fids)")

        # 4. Freshness skip
        skip_fids = fid_skip_set(pg, state, skip_days)
        if skip_fids:
            print(f"[{state}] {len(skip_fids)} fids fresh within {skip_days}d — skipping")

        # 5. Resolve effective parks (drop parks where every fid is fresh)
        work = []
        for park_id, p in parks.items():
            todo_fids = [f for f in p.fids if f not in skip_fids]
            if not todo_fids:
                continue
            work.append((park_id, p, todo_fids))
        if args.limit:
            work = work[:args.limit]
        print(f"[{state}] {len(work)} parks to fetch "
              f"({sum(len(t[2]) for t in work)} (beach,url) rows to write)")

        # 6. Parallel fetch
        t0 = time.time()
        done = 0
        empty = 0
        fail = 0
        rows_written = 0
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futs = {ex.submit(fetch_and_clean, p.url): (pid, p, fids)
                    for (pid, p, fids) in work}
            for fut in as_completed(futs):
                pid, p, todo_fids = futs[fut]
                http_status, text = fut.result()
                if not text:
                    empty += 1
                    if http_status is None:
                        fail += 1
                    continue
                done += 1
                for fid in todo_fids:
                    if upsert(fid, p.url, http_status, text, p.name, args.dry_run):
                        rows_written += 1

        elapsed = time.time() - t0
        print(f"\n[{state}] done in {elapsed:.0f}s. "
              f"parks_ok={done} parks_empty={empty} parks_fetch_fail={fail} "
              f"rows_written={rows_written}")
        return 0
    finally:
        pg.close()


if __name__ == "__main__":
    sys.exit(main())
