"""Scrape parks.ca.gov park-page galleries into beach_photos.

Targets CDPR (California State Parks) beaches with parks.ca.gov page
URLs already in beach_enrichment_provenance.source_url. Each park page
has a /Gallery/<page_id>/ subpage with ~15-20 images at predictable URLs
(parks.ca.gov/pages/<page_id>/images/<filename>.jpg).

Pattern: one park page maps to many sub-beaches (e.g. Sonoma Coast SP
= 26 beaches, one gallery). We fan out: every sub-beach gets every
photo, sort_order=1000+i so they sit BEHIND any geo-matched Flickr/WC
photos in the curate flow. match_quality='park_gallery_shared'.

Filter umbrella pages: page_id=23973 and 30471 are CDPR home pages
covering 100+ beaches each — not actual parks, no gallery worth scraping.

Run:
  python scripts/load_cdpr_park_gallery.py --dry-run     # show plan, no DB write
  python scripts/load_cdpr_park_gallery.py --pilot 5     # 5 parks
  python scripts/load_cdpr_park_gallery.py --full        # all parks
"""
from __future__ import annotations
import argparse
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

UA = ("DogBeachScout/1.0 (https://dogbeachscout.com; "
      "franz@franzfunk.com) cdpr-gallery-scrape")
PACING_S = 1.0       # polite per-park rate

# parks.ca.gov page IDs that are umbrella/home pages, not actual parks.
# Both cover 60+ "beaches" in our governance data because state-park
# stubs default-attributed to them — they have no real per-park gallery.
UMBRELLA_PAGE_IDS = {"23973", "30471"}

# Image filename pattern in the gallery markup
IMG_RE = re.compile(
    r'(https?://www\.parks\.ca\.gov/pages/\d+/images/[^"\'\\s<>]+\.(?:jpg|jpeg|png))',
    re.I,
)


def fetch_gallery_imgs(page_id: str) -> list[str]:
    """Returns a list of img URLs for /Gallery/<page_id>/."""
    url = f"https://www.parks.ca.gov/Gallery/{page_id}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  fetch fail page_id={page_id}: {e}", flush=True)
        return []
    # De-dupe while preserving order
    seen = set()
    out = []
    for m in IMG_RE.finditer(html):
        u = m.group(1).replace("http://", "https://")
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def park_pages_from_db(conn) -> dict[str, list[int]]:
    """{page_id: [beach_fid, ...]} for CDPR beaches with parks.ca.gov URLs."""
    # Filter to gold_fids that still exist in beaches_gold — governance
    # data can outlive dedup'd beaches (e.g. today's beaches_gold dedup
    # left a few orphan gov rows).
    sql = """
    select bep.source_url, c.gold_fid
      from beach_enrichment_provenance bep
      join (
        select gold_fid from beach_enrichment_provenance
         where field_group='governance' and is_canonical=true
           and claimed_values->>'name' = 'California Department of Parks and Recreation'
      ) c on c.gold_fid = bep.gold_fid
      join beaches_gold g on g.fid = c.gold_fid and g.is_active
     where bep.source_url ilike '%parks.ca.gov%'
    """
    PAGE_RE = re.compile(r"page_id=(\d+)")
    by_page: dict[str, list[int]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for url, fid in cur.fetchall():
            m = PAGE_RE.search(url)
            if not m: continue
            page_id = m.group(1)
            if page_id in UMBRELLA_PAGE_IDS: continue
            by_page.setdefault(page_id, [])
            if fid not in by_page[page_id]:
                by_page[page_id].append(fid)
    return by_page


def insert_photos(conn, page_id: str, fids: list[int], img_urls: list[str],
                  dry_run: bool):
    """Insert one beach_photos row per (sub-beach × photo). Idempotent on
    (arena_group_id, source, external_id). external_id = filename basename."""
    if dry_run:
        print(f"  [dry-run] would insert {len(fids)*len(img_urls)} rows "
              f"({len(fids)} beaches × {len(img_urls)} photos)")
        return 0

    rows = []
    now = datetime.now(timezone.utc)
    for fid in fids:
        for i, url in enumerate(img_urls):
            ext_id = url.rsplit("/", 1)[-1]   # filename, stable per park page
            rows.append((fid, "cdpr", ext_id, url, url,
                         "California State Parks", "cdpr_state_parks",
                         f"https://www.parks.ca.gov/?page_id={page_id}",
                         None,           # distance_m — no geo
                         1000 + i,       # sort_order — behind geo-matched photos
                         psycopg2.extras.Json({
                             "park_page_id": page_id,
                             "gallery_index": i,
                             "kind": "park_gallery_shared",
                         }),
                         None,           # match_quality — leave for curator
                         now))

    sql = """
    insert into public.beach_photos
      (arena_group_id, source, external_id, image_url, thumb_url,
       attribution, license, page_url, distance_m, sort_order, source_meta,
       match_quality, loaded_at)
    values %s
    on conflict (arena_group_id, source, external_id) do nothing
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Plan only; no DB writes")
    ap.add_argument("--pilot", type=int,
                    help="Process only the first N parks")
    ap.add_argument("--full", action="store_true",
                    help="Process all parks (~94)")
    args = ap.parse_args()

    if not (args.dry_run or args.pilot or args.full):
        ap.error("Pass --dry-run, --pilot N, or --full")

    conn = psycopg2.connect(**PG)
    by_page = park_pages_from_db(conn)
    pages = sorted(by_page.keys(), key=lambda k: -len(by_page[k]))
    if args.pilot:
        pages = pages[:args.pilot]

    sub_beach_count = sum(len(by_page[pid]) for pid in pages)
    print(f"CDPR parks to scrape: {len(pages)}")
    print(f"Total sub-beaches: {sub_beach_count}")
    print()

    total_imgs = 0
    total_rows = 0
    fail = 0
    t0 = time.time()
    for i, page_id in enumerate(pages, 1):
        fids = by_page[page_id]
        imgs = fetch_gallery_imgs(page_id)
        if not imgs:
            print(f"  [{i}/{len(pages)}] page_id={page_id} -> 0 images "
                  f"({len(fids)} sub-beaches)", flush=True)
            fail += 1
        else:
            total_imgs += len(imgs)
            inserted = insert_photos(conn, page_id, fids, imgs, args.dry_run)
            total_rows += inserted
            print(f"  [{i}/{len(pages)}] page_id={page_id}: {len(imgs)} imgs "
                  f"× {len(fids)} beaches = {inserted} new rows  "
                  f"(elapsed {time.time()-t0:.0f}s)", flush=True)
        time.sleep(PACING_S)

    print()
    print(f"Done in {time.time()-t0:.0f}s")
    print(f"  parks ok:     {len(pages) - fail}")
    print(f"  parks failed: {fail}")
    print(f"  total imgs:   {total_imgs}")
    print(f"  rows inserted: {total_rows}")


if __name__ == "__main__":
    main()
