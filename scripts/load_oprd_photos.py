"""Load OPRD (Oregon Parks and Recreation Department) photos into beach_photos.

Per project_per_state_photo_source_discovery_2026_05_19.md — OPRD covers
~44/151 OR scoreable beaches via STAT/SPR PAD-US units (currently limited
by sparse OR PAD-US coverage; will lift when the OR ocean-shore proxy
polygon is wired into beach_polygon_membership).

Source: stateparks.oregon.gov park-profile pages. Each park's profile is
ColdFusion-rendered HTML that embeds a `serverData.photos = [...]` JSON
array with `imageFile` URLs pointing at the OPRD image-resizer endpoint
(`/index.cfm?do=image.get&name=<file>&park=<id>...`). We also pick up
`serverData.a11yPhotos` (ADA-accessible photos).

Park enumeration: stateparks.oregon.gov/index.cfm?do=visit.find embeds
a complete JSON list of all 193 OPRD-managed parks with their park_id +
park_name. No login or auth needed.

Architecture (mirrors load_wsprc_photos.py):
- Fetch the OPRD find page once, extract {park_id: park_name} for all
  OPRD parks.
- Identify OPRD parks containing OR scoreable beaches via
  beach_polygon_membership where mng_type='STAT' AND mng_name='SPR'.
  Names match by exact-normalized form, then token-subset fallback.
- For each resolved park, fetch the park profile page, parse the
  serverData.photos + a11yPhotos arrays, build the canonical image URL.
- Fan out per-park: insert one row per (beach_fid, photo) for every
  scoreable beach inside the park polygon (same pattern as WSPRC/CDPR).
- Idempotent via UNIQUE(arena_group_id, source, external_id) and DELETE
  before insert on --refresh.

Run:
  python scripts/load_oprd_photos.py --pilot 3      # 3 parks, sanity check
  python scripts/load_oprd_photos.py --full         # all OR OPRD parks
  python scripts/load_oprd_photos.py --park 'Ecola State Park'
  python scripts/load_oprd_photos.py --fids 9711,9712  # pipeline chunked
  python scripts/load_oprd_photos.py --refresh      # purge + reload
  python scripts/load_oprd_photos.py --dry-run      # show plan, no DB writes
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

OPRD_BASE = "https://stateparks.oregon.gov"
FIND_PATH = "/index.cfm?do=visit.find"
PROFILE_PATH = "/index.cfm?do=park.profile&parkId={park_id}"
# stateparks.oregon.gov is ColdFusion; behaves fine with a normal browser UA.
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BROWSER_HEADERS = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}
PACING_S = 1.0   # be polite — single small ColdFusion box
TIMEOUT_S = 30

# Embedded park list on /index.cfm?do=visit.find — JSON array of
# {"park_id":N,"hub_property_id":N,"park_name":"..."}.  We only need
# park_id + park_name.
PARK_LIST_RE = re.compile(
    r'\{"park_id":(\d+),"hub_property_id":\d+,"park_name":"([^"]+)"'
)
# serverData.photos and serverData.a11yPhotos on park.profile
PHOTOS_RE = re.compile(
    r'serverData\.(photos|a11yPhotos)\s*=\s*(\[.*?\]);', re.DOTALL
)


def _http():
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


def _park_name_tokens(park_name: str) -> set[str]:
    """Tokens to require in image filenames if we ever filter; OPRD
    filenames are inconsistent so we don't filter on tokens (unlike
    WSPRC where sidebar cross-promo strips share the same Drupal path).
    Kept here for parity with WSPRC code shape."""
    stop = {"state","park","historical","historic","area","beach","island",
            "harbor","point","bay","lake","river","cove","spit","the","of",
            "recreation","site","scenic","viewpoint","corridor","natural",
            "wayside","heritage"}
    toks = [t for t in re.sub(r'[^a-z]+', ' ', park_name.lower()).split()
            if t and t not in stop and len(t) >= 4]
    return set(toks)


# ─── Park-list discovery ─────────────────────────────────────────────

def fetch_oprd_park_list(http) -> dict[int, str]:
    """One-shot fetch of the OPRD find page. Returns {park_id: park_name}
    for all OPRD-managed parks (~193). Names are exactly as OPRD lists
    them ('Ecola State Park', 'Cape Lookout State Park', etc.).
    """
    try:
        r = http.get(OPRD_BASE + FIND_PATH, timeout=TIMEOUT_S)
        if r.status_code != 200:
            return {}
    except requests.RequestException:
        return {}
    return {int(pid): name for pid, name in PARK_LIST_RE.findall(r.text)}


def _normalize_park_name(s: str) -> str:
    """Strip common OPRD/WSPRC-style suffixes + non-alnum so park names
    match polygon names that may use a shorter form."""
    s = s.lower()
    # OPRD-canonical suffix order: longest first so the strip is greedy
    SUFFIXES = (
        ' state recreation site', ' state recreation area',
        ' state scenic viewpoint', ' state scenic corridor',
        ' state natural area', ' state natural site',
        ' state heritage site', ' state historical site',
        ' state historic site', ' state wayside',
        ' state park', ' state trail',
    )
    for sfx in SUFFIXES:
        if s.endswith(sfx):
            s = s[:-len(sfx)]
            break
    return re.sub(r'[^a-z0-9 ]+', ' ', s).strip()


def resolve_park_polygons(park_names: dict[int, str],
                          polygons_by_norm: dict[str, dict]
                          ) -> dict[int, dict]:
    """Match each OPRD park (by normalized name) to a polygon record in
    beach_polygon_membership. Returns {park_id: polygon_record}.

    Strategy:
      1. Exact normalized-name match.
      2. Token-subset match: OPRD name's tokens are a subset of a
         polygon's tokens, or vice versa. Prefer the shortest polygon
         name on ties (more specific).
    """
    resolved: dict[int, dict] = {}
    for pid, name in park_names.items():
        n = _normalize_park_name(name)
        if not n:
            continue
        if n in polygons_by_norm:
            resolved[pid] = polygons_by_norm[n]
            continue
        # token-subset fallback
        p_tokens = set(n.split())
        if not p_tokens:
            continue
        best = None
        for pn, rec in polygons_by_norm.items():
            c_tokens = set(pn.split())
            if not c_tokens:
                continue
            if p_tokens.issubset(c_tokens) or c_tokens.issubset(p_tokens):
                if best is None or len(c_tokens) < len(set(best[0].split())):
                    best = (pn, rec)
        if best:
            resolved[pid] = best[1]
    return resolved


# ─── HTML scrape ─────────────────────────────────────────────────────

def fetch_park_photos(http, park_id: int, park_name: str) -> list[dict]:
    """Returns deduped list of {url, filename, page_url, external_id}
    for the given OPRD park. Pulls both serverData.photos and
    serverData.a11yPhotos. Dedupe by park_image_name."""
    url = OPRD_BASE + PROFILE_PATH.format(park_id=park_id)
    try:
        r = http.get(url, timeout=TIMEOUT_S)
        if r.status_code != 200:
            return []
        html = r.text
    except requests.RequestException:
        return []

    found: dict[str, dict] = {}
    for kind, raw in PHOTOS_RE.findall(html):
        try:
            arr = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(arr, list):
            continue
        for entry in arr:
            if not isinstance(entry, dict):
                continue
            fname = entry.get("park_image_file") or entry.get("park_image_name")
            img_url = entry.get("imageFile")
            if not (fname and img_url):
                continue
            # Skip if already captured (a11yPhotos can overlap photos)
            if fname in found:
                continue
            # The OPRD image-resizer URL works as-is. We keep the
            # server-supplied form (already has &fit=cover&w=1200&h=600 or
            # similar). Empirically the size params are ignored — server
            # returns the original. Leaving them in for parity.
            found[fname] = {
                "url": img_url,
                "filename": fname,
                "page_url": url,
                "external_id": f"oprd:{park_id}:{fname}",
                "kind": kind,                              # 'photos' or 'a11yPhotos'
                "park_image_id": entry.get("park_image_id"),
                "description": entry.get("park_image_description") or "",
                "title": entry.get("park_image_title") or "",
            }
    return list(found.values())


# ─── DB ──────────────────────────────────────────────────────────────

def get_or_oprd_polygons(pg, fids_filter: list[int] | None = None) -> dict[str, dict]:
    """OR pad_us_unit polygons (mng_type='STAT', mng_name='SPR') containing
    ≥1 OR scoreable beach. Returns {normalized_polygon_name: record} with
    record = {polygon_name, beach_fids: [...]}.

    fids_filter (optional): restrict to polygons containing at least one
    of the given beach_fids (for pipeline chunked dispatch).
    """
    with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        sql = """
            SELECT bpm.polygon_name,
                   array_agg(DISTINCT bpm.gold_fid ORDER BY bpm.gold_fid) AS beach_fids
              FROM beach_polygon_membership bpm
              JOIN beaches_gold g ON g.fid = bpm.gold_fid
             WHERE g.state='OR' AND g.is_active
               AND g.scoring_tier IN ('daily','hourly')
               AND bpm.polygon_kind='pad_us_unit'
               AND bpm.mng_type='STAT' AND bpm.mng_name='SPR'
        """
        params: list = []
        if fids_filter:
            sql += " AND bpm.gold_fid = ANY(%s)"
            params.append(fids_filter)
        sql += " GROUP BY bpm.polygon_name"
        cur.execute(sql, params)
        rows = cur.fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        n = _normalize_park_name(r["polygon_name"])
        if n:
            # First occurrence wins (rare to have collisions)
            out.setdefault(n, dict(r))
    return out


def insert_photos_for_fid(pg, fid: int, photos: list[dict], park_name: str,
                          park_id: int, refresh: bool) -> int:
    """Insert one row per photo for the given fid. Returns rows inserted."""
    if not photos:
        return 0
    with pg.cursor() as cur:
        if refresh:
            cur.execute(
                "DELETE FROM beach_photos WHERE arena_group_id = %s AND source = 'oprd'",
                (fid,))
        n = 0
        for i, p in enumerate(photos):
            try:
                cur.execute("""
                    INSERT INTO beach_photos
                      (arena_group_id, source, external_id, image_url, thumb_url,
                       attribution, license, page_url, distance_m, sort_order,
                       source_meta, loaded_at)
                    VALUES (%s, 'oprd', %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (arena_group_id, source, external_id) DO NOTHING
                """, (
                    fid,
                    p["external_id"],
                    p["url"],
                    p["url"],
                    "Oregon Parks and Recreation Department",
                    "unknown",
                    p["page_url"],
                    None,                       # distance_m — park-gallery shared, no geo
                    20 + i,                     # OPRD ranks at same band as CDPR/WSPRC
                    json.dumps({
                        "park_name": park_name,
                        "park_id": park_id,
                        "filename": p["filename"],
                        "park_image_id": p.get("park_image_id"),
                        "kind": p.get("kind"),
                        "description": p.get("description"),
                        "title": p.get("title"),
                    }),
                ))
                if cur.rowcount > 0:
                    n += 1
            except psycopg2.Error as e:
                print(f"  [INSERT FAIL] fid={fid} {p['filename']}: {e}",
                      flush=True)
                pg.rollback()
                return n
        pg.commit()
    return n


# ─── Main ────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", type=int, help="Limit to first N parks")
    grp.add_argument("--full", action="store_true", help="All OR OPRD parks")
    grp.add_argument("--park", help="Just this one park (by exact OPRD name)")
    ap.add_argument("--fids", help="Comma-separated beach fids — restrict to "
                    "OPRD parks containing at least one of these beaches "
                    "(pipeline chunked dispatch)")
    ap.add_argument("--refresh", action="store_true",
                    help="DELETE existing oprd rows for each beach before insert")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print plan + photos found, but skip DB writes")
    ap.add_argument("--workers", type=int, default=5,
                    help="Parallel HTTP fetch workers for park pages (default: 5)")
    args = ap.parse_args()

    if not (args.pilot or args.full or args.park or args.fids):
        print("ERROR: provide --pilot N, --full, --park NAME, or --fids LIST",
              file=sys.stderr)
        return 2

    fids_filter: list[int] | None = None
    if args.fids:
        try:
            fids_filter = [int(x.strip()) for x in args.fids.split(",") if x.strip()]
        except ValueError:
            print(f"ERROR: --fids must be comma-separated integers", file=sys.stderr)
            return 2

    pg = connect()
    pg.autocommit = False
    try:
        http = _http()
        print("Fetching OPRD park list...", flush=True)
        oprd_parks = fetch_oprd_park_list(http)
        if not oprd_parks:
            print("ERROR: failed to load OPRD park list from " + OPRD_BASE + FIND_PATH,
                  file=sys.stderr)
            return 1
        print(f"  OPRD parks: {len(oprd_parks)}", flush=True)

        polygons_by_norm = get_or_oprd_polygons(pg, fids_filter=fids_filter)
        print(f"  OR scoreable polygons (STAT/SPR): {len(polygons_by_norm)}",
              flush=True)

        # Resolve OPRD parks → polygons (so we only fetch profiles for parks
        # that actually map to at least one OR scoreable beach).
        resolved = resolve_park_polygons(oprd_parks, polygons_by_norm)
        if args.park:
            wanted = args.park.lower()
            resolved = {pid: rec for pid, rec in resolved.items()
                        if oprd_parks[pid].lower() == wanted}
            if not resolved:
                # Park may exist in OPRD list but not in polygon-match —
                # still allow fetch for diagnostic purposes (no insert).
                pid_match = next((pid for pid, n in oprd_parks.items()
                                  if n.lower() == wanted), None)
                if pid_match is None:
                    print(f"ERROR: no OPRD park named {args.park!r}",
                          file=sys.stderr)
                    return 2
                resolved = {pid_match: {"polygon_name": args.park, "beach_fids": []}}
        elif args.pilot:
            # Pick first N by park name (deterministic)
            keys = sorted(resolved.keys(), key=lambda k: oprd_parks[k])
            resolved = {k: resolved[k] for k in keys[:args.pilot]}

        if not resolved:
            print("\nNo OPRD parks resolve to OR scoreable beaches via PAD-US "
                  "polygon membership. (OR PAD-US is sparse; see project_per_state_"
                  "photo_source_discovery_2026_05_19.md and 20260508_oprd_ocean_"
                  "shore_proxy.sql — the synthesized ocean-shore proxy is not yet "
                  "wired into beach_polygon_membership.)", flush=True)
            return 0

        print(f"\nTargets: {len(resolved)} resolved parks", flush=True)
        if args.dry_run:
            print("  [DRY-RUN] no DB writes\n", flush=True)

        # Parallel page fetch (I/O-bound, ThreadPoolExecutor). Pacing is
        # enforced informally by the workers count — keep it low (≤5).
        _local = threading.local()
        def _worker_http():
            if not hasattr(_local, "s"):
                _local.s = _http()
            return _local.s

        def _fetch_one(pid):
            park_name = oprd_parks[pid]
            try:
                photos = fetch_park_photos(_worker_http(), pid, park_name)
                return pid, park_name, photos, None
            except Exception as e:
                return pid, park_name, [], str(e)

        fetch_results: list[tuple[int, str, list, str | None]] = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futs = [pool.submit(_fetch_one, pid) for pid in resolved.keys()]
            for i, fut in enumerate(as_completed(futs), 1):
                pid, name, photos, err = fut.result()
                fetch_results.append((pid, name, photos, err))
                if err:
                    print(f"  [{i}/{len(resolved)}] {name} ERROR: {err}",
                          flush=True)
                else:
                    print(f"  [{i}/{len(resolved)}] {name:<45}  photos={len(photos)}",
                          flush=True)

        # Insert per-fid (sequential — DB writes are cheap)
        total_photos = total_inserted = 0
        for pid, name, photos, err in fetch_results:
            if err:
                continue
            total_photos += len(photos)
            rec = resolved.get(pid, {})
            fids = rec.get("beach_fids", []) or []
            if args.dry_run:
                for ph in photos[:3]:
                    print(f"    {ph['filename']}  -> {ph['url'][:100]}",
                          flush=True)
                if len(photos) > 3:
                    print(f"    ... +{len(photos)-3} more", flush=True)
                continue
            for fid in fids:
                n = insert_photos_for_fid(pg, fid, photos, name, pid,
                                          args.refresh)
                total_inserted += n

        # Count distinct beaches with at least one oprd photo after the run.
        beaches_with_oprd = 0
        if not args.dry_run:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(DISTINCT arena_group_id) FROM beach_photos "
                    "WHERE source = 'oprd'")
                beaches_with_oprd = cur.fetchone()[0]

        print(f"\n=== TOTALS ===")
        print(f"  OPRD parks (catalog):  {len(oprd_parks)}")
        print(f"  resolved to OR poly:   {len(resolved)}")
        print(f"  unique photos found:   {total_photos}")
        print(f"  rows inserted:         {total_inserted}")
        if not args.dry_run:
            print(f"  OR beaches w/ oprd:    {beaches_with_oprd}")
    finally:
        pg.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
