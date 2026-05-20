"""Load WSPRC (Washington State Parks) photos into beach_photos.

Per project_per_state_photo_source_discovery_2026_05_19.md — WSPRC covers
104 WA scoreable beaches via STAT/SPR PAD-US units. ~10 photos/park × 43
parks-with-scoreable-beaches.

Source: parks.wa.gov park-detail pages (Drupal CMS, deterministic image
paths under /sites/default/files/styles/.../public/<YYYY-MM>/<file>.jpg).
We strip the `styles/<preset>/public/` portion to recover the original-
size image URL.

Architecture (mirrors load_nps_photos.py):
- Identify WSPRC parks containing WA scoreable beaches via
  beach_polygon_membership where mng_type='STAT' AND mng_name='SPR'.
- Try to resolve each park name → parks.wa.gov slug. Try a few slug
  variants; report unresolved parks for manual map-fixup.
- Fetch each resolved park URL, extract image URLs, convert to original
  size, dedupe by filename.
- Fan out per-park: insert one row per (beach_fid, photo) for every
  scoreable beach inside the park (same pattern as NPS/CDPR).
- Idempotent via UNIQUE(arena_group_id, source, external_id) and DELETE
  before insert on --refresh.

Run:
  python scripts/load_wsprc_photos.py --pilot 5     # 5 parks, dry-run insert
  python scripts/load_wsprc_photos.py --full        # all WA WSPRC parks
  python scripts/load_wsprc_photos.py --park 'Cama Beach'
  python scripts/load_wsprc_photos.py --refresh     # purge + reload
  python scripts/load_wsprc_photos.py --dry-run     # show plan, no DB writes
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import truststore
truststore.inject_into_ssl()

import argparse
import json
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

WSPRC_BASE = "https://parks.wa.gov"
PARK_PATH  = "/find-parks/state-parks"
UA = ("DogBeachScout/1.0 (https://dogbeachscout.com; "
      "franz@franzfunk.com) wsprc-park-page-scraper")
PACING_S = 1.0   # be polite — small server
TIMEOUT_S = 30

# Drupal CMS image pattern. The "styles/<preset>/public/" prefix is added
# for derivative sizes; strip to get the original.
IMG_RE = re.compile(
    r'src="(/sites/default/files/styles/[^"]+/public/(\d{4}-\d{2})/([^"?]+\.(?:jpg|jpeg|png))[^"]*)"',
    re.IGNORECASE,
)
# Also catch background-image patterns + srcset
BG_RE = re.compile(
    r'(?:background-image:\s*url\(|srcset=")(\'?)(/sites/default/files/styles/[^"\')]+/public/(\d{4}-\d{2})/([^"\')]+\.(?:jpg|jpeg|png))[^"\')]*)',
    re.IGNORECASE,
)

# Filename patterns we DROP (icons, SVG, generic)
DROP_FILENAMES = re.compile(r'(?:logo|icon|_icon\.|kayaking\.|beach-exploration\.|paddleboarding\.)', re.IGNORECASE)


def _http():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html"})
    return s


# ─── Slug discovery ──────────────────────────────────────────────────

def park_name_to_slug_variants(park_name: str) -> list[str]:
    """Try a few slug formats. parks.wa.gov uses lowercase-dashed slugs
    with either '-state-park' or '-historical-state-park' suffix."""
    base = re.sub(r'[^a-z0-9]+', '-', park_name.lower()).strip('-')
    return [
        f"{base}-state-park",
        f"{base}-historical-state-park",
        f"{base}-historic-state-park",
        base,
    ]


def resolve_park_url(http, park_name: str) -> tuple[str | None, str | None]:
    """Try slug variants until one returns 200. Returns (url, slug)."""
    for slug in park_name_to_slug_variants(park_name):
        url = f"{WSPRC_BASE}{PARK_PATH}/{slug}"
        try:
            r = http.head(url, timeout=TIMEOUT_S, allow_redirects=True)
            if r.status_code == 200:
                return url, slug
            elif r.status_code == 405:
                # Some Drupal sites don't support HEAD; retry with GET
                r = http.get(url, timeout=TIMEOUT_S, allow_redirects=True)
                if r.status_code == 200:
                    return url, slug
        except requests.RequestException:
            continue
        time.sleep(PACING_S * 0.5)
    return None, None


# ─── HTML scrape ─────────────────────────────────────────────────────

def fetch_park_photos(http, park_url: str) -> list[dict]:
    """Returns deduped list of {url, filename, page_url}."""
    try:
        r = http.get(park_url, timeout=TIMEOUT_S)
        if r.status_code != 200:
            return []
        html = r.text
    except requests.RequestException:
        return []

    found: dict[str, dict] = {}
    for m in IMG_RE.finditer(html):
        path, _ym, filename = m.group(1), m.group(2), m.group(3)
        if DROP_FILENAMES.search(filename):
            continue
        # Convert to original-size: strip "styles/<preset>/public/" -> "public/"
        # but real Drupal originals live at /sites/default/files/<YYYY-MM>/<file>
        original = re.sub(r'/styles/[^/]+/public/', '/', path)
        url = urllib.parse.urljoin(WSPRC_BASE, original)
        if filename in found:
            continue
        found[filename] = {
            "url": url,
            "filename": filename,
            "page_url": park_url,
            "external_id": f"wsprc:{filename}",  # filename is the stable per-park key
        }
    return list(found.values())


# ─── DB ──────────────────────────────────────────────────────────────

def get_wsprc_parks_with_beaches(pg) -> list[dict]:
    """WSPRC parks containing ≥1 WA scoreable beach. Returns
    [{park, beach_fids: [...]}, ...]."""
    with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT bpm.polygon_name AS park,
                   array_agg(DISTINCT bpm.gold_fid ORDER BY bpm.gold_fid) AS beach_fids
              FROM beach_polygon_membership bpm
              JOIN beaches_gold g ON g.fid = bpm.gold_fid
             WHERE g.state='WA' AND g.is_active
               AND g.scoring_tier IN ('daily','hourly')
               AND bpm.polygon_kind='pad_us_unit'
               AND bpm.mng_type='STAT' AND bpm.mng_name='SPR'
               -- match_strength 1+ = within 2km (the migration's outer bound).
               -- Strict (>=2) excludes some legit park-adjacent beaches like
               -- Cama Beach where the beach centroid is 500m-2km from the
               -- mapped park polygon. Photos are still representative.
             GROUP BY bpm.polygon_name
             ORDER BY array_length(array_agg(DISTINCT bpm.gold_fid), 1) DESC, bpm.polygon_name
        """)
        return [dict(r) for r in cur.fetchall()]


def insert_photos_for_fid(pg, fid: int, photos: list[dict], park_name: str,
                          refresh: bool) -> int:
    """Insert one row per photo for the given fid. Returns rows inserted."""
    if not photos:
        return 0
    with pg.cursor() as cur:
        if refresh:
            cur.execute("DELETE FROM beach_photos WHERE arena_group_id = %s AND source = 'wsprc'", (fid,))
        n = 0
        for p in photos:
            try:
                cur.execute("""
                    INSERT INTO beach_photos
                      (arena_group_id, source, external_id, image_url, thumb_url,
                       attribution, license, page_url, sort_order, source_meta, loaded_at)
                    VALUES (%s, 'wsprc', %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT DO NOTHING
                """, (
                    fid, p["external_id"], p["url"], p["url"],
                    "Washington State Parks and Recreation Commission",
                    "unknown",
                    p["page_url"],
                    20,  # WSPRC ranks below CCC(10) + above Unsplash(25); same band as CDPR(20)
                    json.dumps({"park_name": park_name, "filename": p["filename"]}),
                ))
                if cur.rowcount > 0:
                    n += 1
            except psycopg2.Error as e:
                print(f"  [INSERT FAIL] fid={fid} {p['filename']}: {e}", flush=True)
                pg.rollback()
                return n
        pg.commit()
    return n


# ─── Main ────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", type=int, help="Limit to first N parks")
    grp.add_argument("--full", action="store_true", help="All WA WSPRC parks")
    grp.add_argument("--park", help="Just this one park (by name)")
    ap.add_argument("--refresh", action="store_true",
                    help="DELETE existing wsprc rows for each beach before insert")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print plan + photos found, but skip DB writes")
    args = ap.parse_args()

    if not (args.pilot or args.full or args.park):
        print("ERROR: provide --pilot N, --full, or --park NAME", file=sys.stderr)
        return 2

    pg = psycopg2.connect(**PG)
    pg.autocommit = False
    try:
        parks = get_wsprc_parks_with_beaches(pg)
        if args.park:
            parks = [p for p in parks if p["park"].lower() == args.park.lower()]
            if not parks:
                print(f"ERROR: no WSPRC park named {args.park!r} contains WA scoreable beaches", file=sys.stderr)
                return 2
        elif args.pilot:
            parks = parks[:args.pilot]

        print(f"Targets: {len(parks)} parks", flush=True)
        if args.dry_run: print("  [DRY-RUN] no DB writes\n", flush=True)

        http = _http()
        total_resolved = total_photos = total_inserted = total_unresolved = 0
        for i, p in enumerate(parks, 1):
            print(f"[{i}/{len(parks)}] {p['park']}  ({len(p['beach_fids'])} beaches)", flush=True)
            url, slug = resolve_park_url(http, p["park"])
            if not url:
                print(f"  [no slug found] tried: {park_name_to_slug_variants(p['park'])}", flush=True)
                total_unresolved += 1
                continue
            time.sleep(PACING_S)
            photos = fetch_park_photos(http, url)
            print(f"  url={url}\n  photos={len(photos)}", flush=True)
            total_resolved += 1
            total_photos += len(photos)
            if args.dry_run:
                for ph in photos[:3]: print(f"    {ph['filename']}", flush=True)
                if len(photos) > 3: print(f"    ... +{len(photos)-3} more", flush=True)
                continue
            # Insert per-fid for every beach in this park
            for fid in p["beach_fids"]:
                n = insert_photos_for_fid(pg, fid, photos, p["park"], args.refresh)
                total_inserted += n
            time.sleep(PACING_S)

        print(f"\n=== TOTALS ===")
        print(f"  parks targeted:    {len(parks)}")
        print(f"  parks resolved:    {total_resolved}")
        print(f"  parks unresolved:  {total_unresolved}")
        print(f"  unique photos found: {total_photos}")
        print(f"  rows inserted:     {total_inserted}")
    finally:
        pg.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
