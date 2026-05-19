"""Load NPS Multimedia API photos into beach_photos.

NPS Developer API (developer.nps.gov, free key via api.data.gov):
  GET /multimedia/galleries/assets?parkCode=X
    → returns image assets with title, description, credit, URLs

License: NPS-created content is **public domain** (US government works
per 17 U.S.C. § 105). Third-party content embedded in NPS galleries
may carry separate licenses — we check `copyright` field and skip
photos with explicit copyright notices.

source='nps' in beach_photos (pre-allowed in the source check
constraint per commit 26d2579's sibling pre-work).

Architecture:
- Match our NPS-governed beaches to parkCodes. 57 of 72 NPS beaches
  have an nps.gov/<parkcode>/ URL already in beach_enrichment_provenance
  (extracted in 2026-05-12 morning audit). The other 15 we'd need a
  name-fuzzy fallback via /parks endpoint.
- Per parkCode: fetch up to N image assets via /multimedia/galleries/assets
- Fan out to all sub-beaches under that parkCode (same pattern as CDPR
  park gallery scraper — one park, many sub-beaches share the photos)
- Insert into beach_photos with source='nps', license='public_domain',
  match_quality=NULL (curator decides; CDPR-style)

Cost: free API. Vision tagging of new photos later via the existing
load_photo_vision_tags.py pass (~$0.003/photo).

Run:
  python scripts/load_nps_photos.py --dry-run          # show plan, no DB
  python scripts/load_nps_photos.py --pilot 3          # 3 parks
  python scripts/load_nps_photos.py --state CA --full  # all NPS in state
  python scripts/load_nps_photos.py --full             # all states

Env:
  NPS_API_KEY  — free, instant signup at developer.nps.gov
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import truststore                # use OS cert store (Win Python 3.14 certifi gap)
truststore.inject_into_ssl()

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "DogBeachScout-temp"})

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

NPS_API_KEY = os.environ.get("NPS_API_KEY")
NPS_BASE = "https://developer.nps.gov/api/v1"
UA = ("DogBeachScout/1.0 (https://dogbeachscout.com; "
      "franz@franzfunk.com) nps-multimedia-loader")
PACING_S = 0.4   # NPS is generous (tier 1000 req/hr) but be polite

PARKCODE_RE = re.compile(r"nps\.gov/([a-z]{4})/", re.I)
MAX_PHOTOS_PER_PARK = 30   # cap per park to avoid swamping any one beach


def fetch_park_gallery_assets(park_code: str, limit: int = 50) -> list[dict]:
    """GET /multimedia/galleries/assets?parkCode=X → list of image assets."""
    params = {
        "parkCode": park_code,
        "limit": limit,
        "api_key": NPS_API_KEY,
    }
    url = f"{NPS_BASE}/multimedia/galleries/assets"
    try:
        r = _HTTP.get(url, params=params, headers={"User-Agent": UA}, timeout=30)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code} on parkCode={park_code}: {r.text[:120]}", flush=True)
            return []
        d = r.json()
    except Exception as e:
        print(f"  fetch fail parkCode={park_code}: {e}", flush=True)
        return []

    items = d.get("data", []) or []
    # Filter to images only (galleries may include videos). The API returns
    # fileType as a MIME-ish string like 'image/jpeg', not just 'jpeg'.
    images = []
    for it in items:
        ft = it.get("fileInfo", {}).get("fileType", "").lower()
        if "image" not in ft and not ft.endswith(("jpg", "jpeg", "png")):
            continue
        # Skip explicit copyright assertions
        cr = (it.get("copyright") or "").strip().lower()
        if cr and "public domain" not in cr and "u.s. government" not in cr:
            continue
        images.append(it)
    return images


def _fetch_all_nps_parks() -> list[dict]:
    """One-shot fetch of every NPS park via /parks API. ~470 parks total."""
    url = f"{NPS_BASE}/parks"
    r = _HTTP.get(url, params={"limit": 600, "api_key": NPS_API_KEY},
                  headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.json().get("data", []) or []


def _haversine_km(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    la1, lo1, la2, lo2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return 2*6371*asin(sqrt(a))


def park_codes_from_db(conn) -> dict[str, list[int]]:
    """{parkCode: [beach_fid, ...]} for NPS-governed beaches.
    Two-tier resolution:
      1) extract parkCode from any nps.gov/<code>/ URL in BEP source_url
      2) for beaches without (1), fuzzy-match nearest NPS park by lat/lng
         using the /parks API. Catches beaches whose governance is
         NPS-attributed but the URL didn't carry the parkCode (e.g.
         attributed via PAD-US polygon-containment or operator_id rather
         than a park-URL extraction)."""
    nps_gov_sql = """
    select c.gold_fid, g.lat, g.lon
      from beaches_gold g
      join (
        select gold_fid from beach_enrichment_provenance
         where field_group='governance' and is_canonical=true
           and claimed_values->>'type'='federal'
           and (claimed_values->>'name' ilike '%national park service%'
                or claimed_values->>'name' ilike '%national seashore%'
                or claimed_values->>'name' ilike '%national recreation area%'
                or claimed_values->>'name' ilike '%national monument%'
                or claimed_values->>'name' ilike '%national park%')
      ) c on c.gold_fid = g.fid
     where g.is_active
    """
    nps_url_sql = """
    select distinct gold_fid, source_url
      from beach_enrichment_provenance
     where source_url ilike '%nps.gov/%'
    """
    by_code: dict[str, list[int]] = {}
    fid_to_pos: dict[int, tuple[float, float]] = {}
    with conn.cursor() as cur:
        cur.execute(nps_gov_sql)
        for fid, lat, lon in cur.fetchall():
            fid_to_pos[fid] = (lat, lon)

        # Tier 1: parkCode from BEP URL
        cur.execute(nps_url_sql)
        for fid, url in cur.fetchall():
            if fid not in fid_to_pos: continue
            m = PARKCODE_RE.search(url)
            if not m: continue
            code = m.group(1).lower()
            by_code.setdefault(code, [])
            if fid not in by_code[code]:
                by_code[code].append(fid)

    # Tier 2: fuzzy-match orphans by nearest NPS park
    already_attributed = {f for fids in by_code.values() for f in fids}
    orphans = [(fid, lat, lon) for fid, (lat, lon) in fid_to_pos.items()
               if fid not in already_attributed and lat and lon]
    if orphans:
        print(f"  {len(orphans)} orphan NPS beaches; fetching /parks for fuzzy match")
        parks = _fetch_all_nps_parks()
        # Build park centroids from latLong field (format "lat:X, long:Y")
        park_geo = []
        for p in parks:
            ll = p.get("latLong", "")
            try:
                la = float(ll.split("lat:")[1].split(",")[0])
                lo = float(ll.split("long:")[1].strip().rstrip(")"))
                park_geo.append((p["parkCode"], la, lo, p.get("fullName")))
            except (ValueError, IndexError):
                continue
        # Match each orphan to nearest park within 30km
        matched = 0
        for fid, lat, lon in orphans:
            best = min(park_geo, key=lambda p: _haversine_km(lat, lon, p[1], p[2]))
            d = _haversine_km(lat, lon, best[1], best[2])
            if d <= 30:
                code = best[0]
                by_code.setdefault(code, [])
                by_code[code].append(fid)
                matched += 1
                if matched <= 5:
                    print(f"    fid={fid} → {code} ({best[3][:40]}) {d:.1f}km")
        print(f"  matched {matched}/{len(orphans)} orphans within 30km")
    return by_code


def insert_photos(conn, park_code: str, fids: list[int], assets: list[dict],
                  dry_run: bool) -> int:
    if dry_run:
        print(f"  [dry-run] would insert {len(fids)*len(assets)} rows "
              f"({len(fids)} beaches × {len(assets)} photos)")
        return 0
    rows = []
    now = datetime.now(timezone.utc)
    for fid in fids:
        for i, a in enumerate(assets[:MAX_PHOTOS_PER_PARK]):
            ext_id = a.get("id")
            url = (a.get("fileInfo") or {}).get("url") or a.get("url")
            if not ext_id or not url: continue
            title = (a.get("title") or "").strip()[:300]
            description = (a.get("description") or a.get("altText") or "").strip()[:1500]
            credit = (a.get("credit") or "").strip()[:200]
            permalink = a.get("permalinkUrl")
            page_url = permalink or (a.get("relatedParks", [{}])[0].get("url") if a.get("relatedParks") else None)
            rows.append((fid, "nps", str(ext_id), url, url,
                         credit or "National Park Service",
                         "public_domain_us_gov",
                         page_url or f"https://www.nps.gov/{park_code}/",
                         None,                # distance_m — no per-photo geo
                         1100 + i,            # sort_order — behind CDPR (1000+)
                         psycopg2.extras.Json({
                             "park_code": park_code,
                             "gallery_index": i,
                             "title": title,
                             "description": description,
                             "kind": "park_gallery_shared",
                         }),
                         None,                # match_quality — curator decides
                         now))
    if not rows: return 0
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
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--state", help="Limit to one state (e.g. CA)")
    ap.add_argument("--pilot", type=int, help="Only N parks")
    ap.add_argument("--full", action="store_true", help="All NPS parks in scope")
    args = ap.parse_args()

    if not args.dry_run and not NPS_API_KEY:
        sys.exit("ERROR: NPS_API_KEY not set in env (free signup at developer.nps.gov)")

    conn = psycopg2.connect(**PG)
    by_code = park_codes_from_db(conn)

    # If --state, filter to parks that have at least one beach in that state
    if args.state:
        s = args.state.upper()
        with conn.cursor() as cur:
            cur.execute("select fid from beaches_gold where state=%s and is_active", (s,))
            state_fids = {r[0] for r in cur.fetchall()}
        by_code = {k: [f for f in v if f in state_fids]
                   for k, v in by_code.items()
                   if any(f in state_fids for f in v)}

    codes = sorted(by_code.keys(), key=lambda k: -len(by_code[k]))
    if args.pilot:
        codes = codes[:args.pilot]
    elif not args.full and not args.dry_run:
        ap.error("Pass --pilot N, --full, or --dry-run")

    print(f"NPS parks to fetch: {len(codes)}")
    total_imgs = 0; total_rows = 0; fail = 0
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        fids = by_code[code]
        assets = fetch_park_gallery_assets(code, limit=50) if not args.dry_run else [{"id":"dry","url":""}]
        if not assets:
            print(f"  [{i}/{len(codes)}] parkCode={code} (0 photos, {len(fids)} beaches)")
            fail += 1
        else:
            inserted = insert_photos(conn, code, fids, assets, args.dry_run)
            print(f"  [{i}/{len(codes)}] parkCode={code}: {len(assets)} imgs × {len(fids)} beaches → {inserted} rows ({time.time()-t0:.0f}s)")
            total_imgs += len(assets); total_rows += inserted
        time.sleep(PACING_S)

    print(f"\nDone in {time.time()-t0:.0f}s")
    print(f"  parks ok:      {len(codes) - fail}")
    print(f"  parks failed:  {fail}")
    print(f"  total imgs:    {total_imgs}")
    print(f"  rows inserted: {total_rows}")


if __name__ == "__main__":
    main()
