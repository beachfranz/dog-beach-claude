"""Harvest stage: extract structured locations from operator listing pages.

Reads operator_location_sources for the given content_type, fetches each
URL, runs the content_type's EXTRACTION_PROMPT, writes results to
operator_extracted_locations.

Fetcher dispatch:
  - shape='js_rendered'        → Playwright (system Chrome) directly
  - any other shape            → requests first; if 403, fall through
                                  to Playwright (per
                                  [[403-means-playwright-skip-ua-tricks]])

Idempotent: rows with extraction_status='success' within last 30 days are
skipped unless --force.

Usage:
    python -m scripts.harvest.extract_locations --content-type dog_park
    python -m scripts.harvest.extract_locations --content-type dog_park --operator-id 165
    python -m scripts.harvest.extract_locations --content-type dog_park --force
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.llm import SONNET, call_json
from scripts.common.db import connect
from scripts.harvest import content_types as ctmod
from scripts.harvest._playwright_fetch import fetch_via_playwright

STALE_DAYS = 30
HTTP_TIMEOUT = 30
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
MAX_PAGE_TEXT = 30000


def fetch_html(url: str) -> tuple[str | None, str | None]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return None, f"fetch_error: {e}"
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    return text[:MAX_PAGE_TEXT], None


def fetch_for_extract(url: str, shape: str | None) -> tuple[str | None, str | None]:
    """Dispatch fetcher based on shape, with 403 → Playwright fallthrough.

    shape='js_rendered'  → straight to Playwright
    requests success     → return that
    requests 403         → fall through to Playwright per pin
    requests other error → return the error (don't burn Playwright budget)
    """
    if shape == "js_rendered":
        text, err = fetch_via_playwright(url)
        return (text[:MAX_PAGE_TEXT] if text else text, err)

    text, err = fetch_html(url)
    if err and "403" in err:
        # Per [[403-means-playwright-skip-ua-tricks]]: don't iterate on
        # UAs; route directly to a real browser.
        text, err = fetch_via_playwright(url)
        return (text[:MAX_PAGE_TEXT] if text else text, err)
    return text, err


def fetch_targets(conn, content_type: str, operator_id: int | None, force: bool):
    sql = """
      SELECT op.id, op.name, ols.source_url, ols.shape
        FROM public.operator_location_sources ols
        JOIN public.operator op ON op.id = ols.operator_id
       WHERE ols.content_type = %s
         AND (%s::int IS NULL OR op.id = %s)
         AND (%s
              OR ols.last_scraped_at IS NULL
              OR ols.last_scraped_at < now() - interval '30 days'
              OR ols.extraction_status IN ('fetch_error', 'parse_error'))
       ORDER BY op.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (content_type, operator_id, operator_id, force))
        return cur.fetchall()


def upsert_extracted(conn, operator_id: int, content_type: str, source_url: str, park: dict):
    name = (park.get("name") or "").strip()
    if not name:
        return
    attributes = park.get("attributes") or {}
    subpage = park.get("subpage_url")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.operator_extracted_locations
              (operator_id, content_type, source_url, name,
               address, address_city, address_state, subpage_url,
               attributes, extraction_status, inventory_match_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'success', 'pending_compare')
            ON CONFLICT (operator_id, content_type, name, source_url) DO UPDATE
              SET address = EXCLUDED.address,
                  address_city = EXCLUDED.address_city,
                  address_state = EXCLUDED.address_state,
                  subpage_url = EXCLUDED.subpage_url,
                  attributes = EXCLUDED.attributes,
                  extraction_status = EXCLUDED.extraction_status,
                  extracted_at = now()
        """, (operator_id, content_type, source_url, name,
              park.get("address"), park.get("address_city"), park.get("address_state"),
              subpage, json.dumps(attributes)))


def mark_source_scraped(conn, operator_id: int, content_type: str, source_url: str,
                         status: str, count: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.operator_location_sources
               SET last_scraped_at = now(),
                   extraction_status = %s,
                   locations_extracted_count = %s
             WHERE operator_id = %s AND content_type = %s AND source_url = %s
        """, (status, count, operator_id, content_type, source_url))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--content-type", required=True, help="e.g., dog_park")
    ap.add_argument("--operator-id", type=int)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    ct = ctmod.get(args.content_type)

    conn = connect(application_name=f"harvest_extract_{args.content_type}")
    targets = fetch_targets(conn, args.content_type, args.operator_id, args.force)

    if not targets:
        print(f"No targets to scrape for content_type={args.content_type}.")
        return

    print(f"Extracting {args.content_type} from {len(targets)} operator URL(s)...")
    t0 = time.time()

    for op_id, op_name, url, shape in targets:
        print(f"\n  op={op_id} ({op_name[:40]})  shape={shape or '?'}\n    {url}")
        text, err = fetch_for_extract(url, shape)
        if err:
            print(f"    {err}")
            mark_source_scraped(conn, op_id, args.content_type, url, "fetch_error", 0)
            conn.commit()
            continue

        try:
            resp = call_json(ct.EXTRACTION_PROMPT,
                             {"operator_name": op_name,
                              "page_url": url,
                              "page_text": text},
                             model=SONNET, max_tokens=8000)
            parks = resp["parsed"].get("parks", [])
        except Exception as e:
            print(f"    LLM extract error: {e}")
            # LLM/network error may have left the DB connection in a bad
            # state; reconnect defensively before marking the source.
            try:
                if conn.closed:
                    conn = connect(application_name=f"harvest_extract_{args.content_type}")
                else:
                    conn.rollback()
                mark_source_scraped(conn, op_id, args.content_type, url, "parse_error", 0)
                conn.commit()
            except Exception as e2:
                print(f"    (also failed to mark source: {e2}; reconnecting)")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect(application_name=f"harvest_extract_{args.content_type}")
                try:
                    mark_source_scraped(conn, op_id, args.content_type, url, "parse_error", 0)
                    conn.commit()
                except Exception:
                    pass
            continue

        print(f"    extracted {len(parks)} location(s)")
        for p in parks[:5]:
            print(f"      - {(p.get('name') or '?')[:50]:<50}  {(p.get('address') or '—')[:40]}")
        if len(parks) > 5:
            print(f"      ... +{len(parks)-5} more")

        for p in parks:
            upsert_extracted(conn, op_id, args.content_type, url, p)
        status = "success" if parks else "no_locations_found"
        mark_source_scraped(conn, op_id, args.content_type, url, status, len(parks))
        conn.commit()

    print(f"\nDone in {time.time()-t0:.1f}s.")


if __name__ == "__main__":
    main()
