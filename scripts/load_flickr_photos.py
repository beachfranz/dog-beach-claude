"""Load Flickr Creative Commons photos for each beach into beach_photos.

Strategy:
  1. Search Flickr by beach name + "beach" + city for relevance
  2. Filter to CC-licensed (license codes 1, 2, 3, 4, 5, 6, 9, 10 — all CC)
  3. Within 5km of beach centroid (using Flickr's lat/lng metadata)
  4. Top 5 by Flickr's "interestingness" rank

Flickr license codes (https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html):
  0  All Rights Reserved (skip)
  1  CC BY-NC-SA 2.0
  2  CC BY-NC 2.0
  3  CC BY-NC-ND 2.0
  4  CC BY 2.0
  5  CC BY-SA 2.0
  6  CC BY-ND 2.0
  7  No known copyright restrictions (museum/archive)
  8  US Government Work (public domain)
  9  CC0 / Public Domain Dedication
  10 Public Domain Mark

For commercial-friendly use we want 4 (BY), 5 (BY-SA), 7, 8, 9, 10.
For our app we'll be permissive: any CC license is fine since we're
showing photos with attribution. Skip only the All Rights Reserved (0).

Usage:
  python scripts/load_flickr_photos.py --fids 6212,6202,8337,218
  python scripts/load_flickr_photos.py --pilot 20
  python scripts/load_flickr_photos.py --full

Environment:
  FLICKR_API_KEY — required, free tier from flickr.com/services/apps/create/
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
FLICKR_KEY   = (os.environ.get("FLICKR_API_KEY")
                or os.environ.get("FLICKR_KEY"))
if not FLICKR_KEY:
    print("ERROR: set FLICKR_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free key at https://www.flickr.com/services/apps/create/", file=sys.stderr)
    sys.exit(1)

PER_BEACH    = 5
RADIUS_KM    = 5
THROTTLE_S   = 1.0
# All CC license codes; 0 (ARR) excluded
CC_LICENSES  = "1,2,3,4,5,6,7,8,9,10"

LICENSE_LABEL = {
    "1": "CC-BY-NC-SA-2.0", "2": "CC-BY-NC-2.0", "3": "CC-BY-NC-ND-2.0",
    "4": "CC-BY-2.0", "5": "CC-BY-SA-2.0", "6": "CC-BY-ND-2.0",
    "7": "no-known-restrictions", "8": "US-government-public-domain",
    "9": "CC0", "10": "public-domain",
}


# ─── Supabase REST helpers ────────────────────────────────────────────────

def supa(path, *, method="GET", body=None, params=None, prefer=None):
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer: headers["Prefer"] = prefer
    req = urllib.request.Request(
        f"{SUPABASE_URL}{path}{qs}", method=method,
        data=(json.dumps(body).encode() if body is not None else None),
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "ignore")[:300]
        raise RuntimeError(f"Supabase {method} {path} -> HTTP {e.code}: {body_txt}") from None


def select_targets(args):
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,county_name,state",
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
    elif args.pilot:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,county_name,state",
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc", "limit": str(int(args.pilot))})
    elif args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,county_name,state",
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc"})
    else:
        print("ERROR: provide --fids, --pilot N, or --full", file=sys.stderr)
        sys.exit(1)

    # Add lat/lng via RPC
    out = []
    for r in rows or []:
        info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": r["fid"]})
        b = (info or {}).get("beach") or {}
        if b.get("lat") is None: continue
        out.append({
            "fid": r["fid"],
            "name": r.get("display_name_override") or r["name"],
            "county": r.get("county_name"),
            "state": r.get("state"),
            "lat": b["lat"], "lng": b["lng"],
        })
    return out


# ─── Flickr search ────────────────────────────────────────────────────────

def flickr_search(text, lat, lng, radius_km=RADIUS_KM, per_page=20):
    params = {
        "method":   "flickr.photos.search",
        "api_key":  FLICKR_KEY,
        "text":     text,
        "license":  CC_LICENSES,
        "lat":      f"{lat}",
        "lon":      f"{lng}",
        "radius":   f"{radius_km}",
        "radius_units": "km",
        "extras":   "license,owner_name,date_taken,geo,url_z,url_l,url_m,url_n",
        "sort":     "interestingness-desc",
        "media":    "photos",
        "per_page": str(per_page),
        "format":   "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "DogBeachScout/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read())
        if d.get("stat") != "ok":
            print(f"  flickr error: {d.get('message')}", file=sys.stderr)
            return []
        return (d.get("photos") or {}).get("photo") or []
    except Exception as e:
        print(f"  flickr error: {e}", file=sys.stderr)
        return []


def haversine_m(la1, lo1, la2, lo2):
    from math import radians, sin, cos, asin, sqrt
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return 2 * 6_371_000 * asin(sqrt(a))


def pick_best(photos, beach_lat, beach_lng, top_n=PER_BEACH):
    scored = []
    for p in photos:
        # Flickr returns latitude/longitude as strings; "0" means no geo
        try:
            plat = float(p.get("latitude") or 0)
            plng = float(p.get("longitude") or 0)
        except (TypeError, ValueError):
            plat = plng = 0
        if plat == 0 and plng == 0:
            # No geo data — keep but mark as unknown
            d = -1
        else:
            d = int(haversine_m(beach_lat, beach_lng, plat, plng))
        scored.append({**p, "_distance_m": d})
    # Prioritize geo-tagged photos (sorted by distance asc), then ungeo (preserve order)
    geo  = sorted([s for s in scored if s["_distance_m"] >= 0], key=lambda x: x["_distance_m"])
    ungeo = [s for s in scored if s["_distance_m"] < 0]
    return (geo + ungeo)[:top_n]


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_flickr(fid, photos):
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}",
        "source":         "eq.flickr",
    }, prefer="return=minimal")
    if not photos: return
    rows = []
    for i, p in enumerate(photos):
        owner = p.get("ownername") or "Flickr user"
        license_code = str(p.get("license", ""))
        license_label = LICENSE_LABEL.get(license_code, f"CC-{license_code}")
        attribution = f"© {owner} / {license_label} via Flickr"
        page_url = f"https://www.flickr.com/photos/{p.get('owner')}/{p.get('id')}"
        # Pick the best size: url_l (1024) for image, url_m (medium 500) for thumb
        image_url = p.get("url_l") or p.get("url_z") or p.get("url_m")
        thumb_url = p.get("url_n") or p.get("url_m")
        # date_taken format: "2010-05-23 14:32:11"
        cap = p.get("datetaken")
        if cap and cap != "0000-00-00 00:00:00":
            try:
                cap = datetime.fromisoformat(cap.replace(" ", "T") + "+00:00").isoformat()
            except Exception:
                cap = None
        else:
            cap = None
        try:
            plat = float(p.get("latitude") or 0) or None
            plng = float(p.get("longitude") or 0) or None
        except Exception:
            plat = plng = None
        rows.append({
            "arena_group_id": fid,
            "source":         "flickr",
            "external_id":    str(p.get("id")),
            "image_url":      image_url,
            "thumb_url":      thumb_url,
            "attribution":    attribution,
            "license":        license_label,
            "captured_at":    cap,
            "lat":            plat,
            "lng":            plng,
            "distance_m":     p["_distance_m"] if p["_distance_m"] >= 0 else None,
            "sort_order":     50 + i,  # flickr ranked above mapillary (100+)
            "page_url":       page_url,
            "source_meta":    {"title": p.get("title"), "license_code": license_code},
        })
    if rows:
        supa("/rest/v1/beach_photos", method="POST", body=rows,
             prefer="return=minimal,resolution=ignore-duplicates")


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids",  help="comma-separated list of fids")
    grp.add_argument("--pilot", type=int, help="run on first N beaches")
    grp.add_argument("--full",  action="store_true")
    ap.add_argument("--refresh", action="store_true", help="redo even if rows exist")
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches  (radius {RADIUS_KM}km, top {PER_BEACH} per beach)")

    saved = errored = 0
    for i, b in enumerate(targets, 1):
        try:
            # Search query: beach name + "beach" disambiguator + city/state
            query_parts = [b["name"]]
            if "beach" not in b["name"].lower():
                query_parts.append("beach")
            if b.get("county"): query_parts.append(b["county"])
            text = " ".join(query_parts)
            cands = flickr_search(text, b["lat"], b["lng"])
            picked = pick_best(cands, b["lat"], b["lng"])
            replace_flickr(b["fid"], picked)
            saved += len(picked)
            tag = f'{len(picked)} photos' if picked else '(none)'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  {tag}")
            if picked:
                p = picked[0]
                print(f'      top: "{p.get("title", "")[:50]}"  {p.get("ownername")}  '
                      f'{p["_distance_m"] if p["_distance_m"] >= 0 else "?"}m')
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)

    print(f"\n=== TOTALS ===  saved={saved}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
