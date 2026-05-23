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

import argparse
import json
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

WSPRC_BASE = "https://parks.wa.gov"
PARK_PATH  = "/find-parks/state-parks"
# parks.wa.gov 403s our custom UA — use browser-like UA + standard headers.
# (Identifying contact preserved in code comments + commit history.)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BROWSER_HEADERS = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}
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

# Small-thumbnail style presets = sidebar cross-promo strips. Drop these
# unconditionally; the main park gallery uses square_* presets.
DROP_STYLES = re.compile(r'/styles/(?:small_thumbnail_|teaser_|hero_thumb_)', re.IGNORECASE)


def _http():
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


def _park_name_tokens(park_name: str) -> set[str]:
    """Tokens to require in image filenames so we drop cross-promo thumbnails
    that the WSPRC page surfaces for other parks. Drops short/common tokens
    like 'state', 'park', 'island' that would over-match."""
    stop = {"state","park","historical","historic","area","beach","island",
            "harbor","point","bay","lake","river","cove","spit","the","of"}
    toks = [t for t in re.sub(r'[^a-z]+', ' ', park_name.lower()).split() if t and t not in stop and len(t) >= 4]
    return set(toks)


# ─── Slug discovery ──────────────────────────────────────────────────

def fetch_sitemap_park_urls(http) -> dict[str, str]:
    """One-shot fetch of parks.wa.gov sitemap.xml. Returns
    {normalized_park_name: url} for every /find-parks/state-parks/* page.

    Normalization: lowercase, strip 'state-park'/'historical'/'marine'/etc.
    suffixes so 'Stuart Island' matches 'stuart-island-marine-state-park'.
    """
    try:
        r = http.get(f"{WSPRC_BASE}/sitemap.xml", timeout=TIMEOUT_S)
        if r.status_code != 200:
            return {}
    except requests.RequestException:
        return {}
    park_urls = re.findall(r'<loc>(https://parks\.wa\.gov/find-parks/state-parks/[^<]+)</loc>', r.text)
    out = {}
    SUFFIX_STRIP = ('-marine-state-park-trail','-state-park-trail',
                    '-marine-state-park','-historical-state-park',
                    '-historic-state-park','-state-park','-state',
                    '-conservation-area','-recreation-area')
    for url in park_urls:
        slug = url.rsplit('/', 1)[-1]
        # Drop trailing -<digits> archive suffix (e.g., -retired-...-472027)
        slug = re.sub(r'-(?:retired|archived|deleted)-.*$', '', slug)
        bare = slug
        for sfx in SUFFIX_STRIP:
            if bare.endswith(sfx):
                bare = bare[:-len(sfx)]
                break
        bare = re.sub(r'-+', ' ', bare).strip()
        if bare:
            # Keep the first match in case of dupes (sitemap usually ordered)
            out.setdefault(bare, url)
    return out


def resolve_park_url(http, park_name: str, sitemap: dict[str, str]
                     ) -> tuple[str | None, str | None]:
    """Resolve a park name to its parks.wa.gov URL via sitemap.

    Strategy:
      1. Exact normalized-name match.
      2. Token-subset match: park_name's tokens are a subset of a sitemap
         entry's tokens (so 'Cama Beach' matches 'cama beach historical'
         after suffix-strip, but 'Bay View' doesn't match 'lake bay view').
      3. Parent-park fallback: strip 'Underwater Park' / 'Marine Park'
         suffix and retry (PAD-US lists these as separate sub-units but
         WSPRC publishes them under the parent park page).
      4. None.
    """
    def _try(name: str):
        norm = re.sub(r'[^a-z0-9 ]+', ' ', name.lower()).strip()
        if not norm:
            return None
        if norm in sitemap:
            return sitemap[norm]
        p_tokens = set(norm.split())
        if not p_tokens:
            return None
        best = None
        for cand, url in sitemap.items():
            c_tokens = set(cand.split())
            if p_tokens.issubset(c_tokens):
                if best is None or len(c_tokens) < len(set(best[0].split())):
                    best = (cand, url)
        return best[1] if best else None

    url = _try(park_name)
    if url:
        return url, url.rsplit('/', 1)[-1]
    # Parent-park fallback for sub-units
    parent = re.sub(r'\s*(?:underwater park|marine park|underwater)\s*$',
                    '', park_name, flags=re.IGNORECASE).strip()
    if parent and parent != park_name:
        url = _try(parent)
        if url:
            return url, url.rsplit('/', 1)[-1]
    return None, None


# ─── HTML scrape ─────────────────────────────────────────────────────

def fetch_park_photos(http, park_url: str, park_name: str) -> list[dict]:
    """Returns deduped list of {url, filename, page_url} filtered to this
    park (drops cross-promo + sidebar thumbnails for other parks)."""
    try:
        r = http.get(park_url, timeout=TIMEOUT_S)
        if r.status_code != 200:
            return []
        html = r.text
    except requests.RequestException:
        return []

    park_tokens = _park_name_tokens(park_name)
    found: dict[str, dict] = {}
    for m in IMG_RE.finditer(html):
        path, _ym, filename = m.group(1), m.group(2), m.group(3)
        if DROP_FILENAMES.search(filename):
            continue
        if DROP_STYLES.search(path):
            continue  # sidebar cross-promo strip
        # Park-name token filter: drop if no distinctive park-name token
        # appears in the filename (decoded URL chars). Skip filter when
        # park has no distinctive tokens (rare; e.g., 'Bay View').
        if park_tokens:
            fname_norm = re.sub(r'[^a-z]+', ' ',
                                 urllib.parse.unquote(filename).lower())
            fname_words = set(fname_norm.split())
            if not (park_tokens & fname_words):
                # Loose fallback: substring check, in case tokens are
                # smushed together (e.g., 'CamaBeach_*' has no separator
                # between 'cama' and 'beach').
                low = fname_norm.replace(' ', '')
                if not any(t in low for t in park_tokens):
                    continue
        # Convert to original-size: strip "styles/<preset>/public/" -> "/"
        # — Drupal originals live at /sites/default/files/<YYYY-MM>/<file>
        original = re.sub(r'/styles/[^/]+/public/', '/', path)
        url = urllib.parse.urljoin(WSPRC_BASE, original)
        if filename in found:
            continue
        found[filename] = {
            "url": url,
            "filename": filename,
            "page_url": park_url,
            "external_id": f"wsprc:{filename}",  # filename is stable per-park key
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
    ap.add_argument("--workers", type=int, default=5,
                    help="Parallel HTTP fetch workers for park pages (default: 5)")
    args = ap.parse_args()

    if not (args.pilot or args.full or args.park):
        print("ERROR: provide --pilot N, --full, or --park NAME", file=sys.stderr)
        return 2

    pg = connect()
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
        print("Fetching parks.wa.gov sitemap for slug resolution...", flush=True)
        sitemap = fetch_sitemap_park_urls(http)
        print(f"  sitemap: {len(sitemap)} park URLs indexed\n", flush=True)

        # Phase 1: resolve every park URL (in-memory lookup, fast)
        resolved: list[tuple[dict, str]] = []  # (park_record, url)
        total_unresolved = 0
        for p in parks:
            url, _slug = resolve_park_url(http, p["park"], sitemap)
            if url:
                resolved.append((p, url))
            else:
                total_unresolved += 1
                print(f"  [unresolved] {p['park']}", flush=True)
        print(f"\nResolved {len(resolved)}/{len(parks)} parks. "
              f"Fetching pages with {args.workers} workers...\n", flush=True)

        # Phase 2: parallel page fetch (I/O-bound, ThreadPoolExecutor)
        # Per-thread http session (requests.Session is thread-safe for GET
        # but a fresh session per worker avoids any shared-state issues).
        _local = threading.local()
        def _worker_http():
            if not hasattr(_local, "s"):
                _local.s = _http()
            return _local.s
        def _fetch_one(park_url):
            park, url = park_url
            try:
                photos = fetch_park_photos(_worker_http(), url, park["park"])
                return park, url, photos, None
            except Exception as e:
                return park, url, [], str(e)

        fetch_results: list[tuple[dict, str, list, str | None]] = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futs = [pool.submit(_fetch_one, pu) for pu in resolved]
            for i, fut in enumerate(as_completed(futs), 1):
                park, url, photos, err = fut.result()
                fetch_results.append((park, url, photos, err))
                if err:
                    print(f"  [{i}/{len(resolved)}] {park['park']} ERROR: {err}", flush=True)
                else:
                    print(f"  [{i}/{len(resolved)}] {park['park']:<40}  photos={len(photos)}", flush=True)

        # Phase 3: insert per-fid (sequential — DB writes are cheap, parallel
        # would just lock-contend with no real gain)
        total_resolved = total_photos = total_inserted = 0
        for park, url, photos, err in fetch_results:
            if err: continue
            total_resolved += 1
            total_photos += len(photos)
            if args.dry_run:
                for ph in photos[:3]: print(f"    {ph['filename']}", flush=True)
                if len(photos) > 3: print(f"    ... +{len(photos)-3} more", flush=True)
                continue
            for fid in park["beach_fids"]:
                n = insert_photos_for_fid(pg, fid, photos, park["park"], args.refresh)
                total_inserted += n

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
