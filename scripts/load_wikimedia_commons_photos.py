"""Load nearby Wikimedia Commons photos for each beach into beach_photos.

Wikimedia Commons hosts millions of CC-licensed photos with geographic
coordinates. We query the Commons MediaWiki API's geosearch around each
beach centroid, fetch image info + license/attribution, and store the
top N nearest photos.

Quality bias: Commons photos tend to be higher quality than Mapillary
street-view (often professional/scenic shots, not road approaches).
Coverage varies — popular beaches (Cape Cod NS, Coronado, Asilomar)
have rich Commons galleries; obscure beaches may have zero.

License: every image carries CC-BY / CC-BY-SA / CC0 / public-domain
license info in extmetadata. We store the LicenseShortName + Artist
verbatim from Commons; consumers handle attribution.

Usage:
  python scripts/load_wikimedia_commons_photos.py --fids 6212,6202
  python scripts/load_wikimedia_commons_photos.py --pilot 20
  python scripts/load_wikimedia_commons_photos.py --full
  python scripts/load_wikimedia_commons_photos.py --states MA,RI

Environment:
  No API key needed (Commons is open). Sets a polite User-Agent.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

SUPABASE_URL  = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]

USER_AGENT    = "DogBeachScout/1.0 (https://dogbeachscout.app; data@dogbeachscout.app) commons-loader"
COMMONS_API   = "https://commons.wikimedia.org/w/api.php"
RADIUS_M      = 500          # geosearch radius around beach centroid
PER_BEACH     = 5            # max photos to keep per beach
MIN_WIDTH     = 800          # skip thumbnails / icons
THROTTLE_S    = 0.3          # sleep between API calls — Commons is generous but be polite
PHOTO_EXTS    = {"jpg", "jpeg", "png", "tif", "tiff", "webp"}
SKIP_EXTS     = {"svg", "pdf", "ogv", "ogg", "webm", "mp3", "wav"}


# ─── Supabase REST helpers ────────────────────────────────────────────────

def supa(path: str, *, method: str = "GET", body=None, params=None, prefer=None):
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(
        f"{SUPABASE_URL}{path}{qs}", method=method,
        data=(json.dumps(body).encode() if body is not None else None),
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if not raw: return None
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "ignore")[:300]
        raise RuntimeError(f"Supabase {method} {path} -> HTTP {e.code}: {body_txt}") from None


def select_targets(args) -> list[dict]:
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,lat,lon,state",
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
        return _shape(rows or [])
    if args.states:
        states = [s.strip().upper() for s in args.states.split(",")]
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,lat,lon,state",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "state": f"in.({','.join(states)})",
                            "order": "fid.asc"})
        return _shape(rows or [])
    if args.pilot or args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,lat,lon,state",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "order": "fid.asc",
                            **({"limit": str(int(args.pilot))} if args.pilot else {})})
        return _shape(rows or [])
    print("ERROR: provide --fids, --pilot N, --full, or --states XX,YY", file=sys.stderr)
    sys.exit(1)


def _shape(rows):
    return [{"fid": r["fid"],
             "name": r.get("display_name_override") or r["name"],
             "lat": r.get("lat"), "lng": r.get("lon"), "state": r.get("state")}
            for r in rows if r.get("lat") is not None and r.get("lon") is not None]


# ─── Commons API ──────────────────────────────────────────────────────────

def commons_geosearch(lat: float, lng: float, radius_m: int = RADIUS_M, limit: int = 30) -> list[dict]:
    """Find files within radius_m of (lat, lng). Returns [{pageid, title, lat, lon, dist}]."""
    params = {
        "action": "query", "list": "geosearch",
        "gscoord": f"{lat}|{lng}",
        "gsradius": str(radius_m),
        "gsnamespace": "6",       # File: namespace
        "gslimit": str(limit),
        "format": "json",
    }
    return _commons_call(params).get("query", {}).get("geosearch", []) or []


def commons_imageinfo(pageids: list[int]) -> dict[int, dict]:
    """Batch fetch imageinfo for pageids. Returns {pageid: imageinfo dict}."""
    if not pageids: return {}
    params = {
        "action": "query", "prop": "imageinfo",
        "pageids": "|".join(str(p) for p in pageids),
        "iiprop": "url|size|mime|extmetadata",
        "iiextmetadatafilter": "License|LicenseShortName|Artist|ImageDescription|Credit|UsageTerms",
        "format": "json",
    }
    out = {}
    pages = _commons_call(params).get("query", {}).get("pages", {}) or {}
    for pid_str, page in pages.items():
        pid = int(pid_str)
        info_list = page.get("imageinfo") or []
        if info_list:
            out[pid] = info_list[0] | {"_title": page.get("title")}
    return out


def _commons_call(params: dict) -> dict:
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{COMMONS_API}?{qs}", headers={"User-Agent": USER_AGENT})
    delays = [3, 10, 30]
    for attempt, delay in enumerate([0, *delays]):
        if delay > 0:
            time.sleep(delay)
            print(f"  Commons backoff {delay}s (attempt {attempt}/{len(delays)})", file=sys.stderr)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")[:200]
            if e.code in (429, 500, 502, 503, 504) and attempt < len(delays):
                continue
            print(f"  Commons HTTP {e.code}: {body}", file=sys.stderr)
            return {}
        except Exception as e:
            if attempt < len(delays): continue
            print(f"  Commons error: {e}", file=sys.stderr)
            return {}
    return {}


# ─── Filter / rank ────────────────────────────────────────────────────────

def _ext_of(title_or_url: str) -> str:
    s = title_or_url.lower().split("?")[0].rstrip("/")
    if "." in s: return s.rsplit(".", 1)[-1]
    return ""


def _strip_html(s: str | None) -> str | None:
    if not s: return s
    import re
    return re.sub(r"<[^>]+>", "", s).strip()


def _meta(extmd: dict, key: str) -> str | None:
    v = (extmd or {}).get(key, {})
    if isinstance(v, dict): return _strip_html(v.get("value"))
    return None


def haversine_m(la1, lo1, la2, lo2):
    from math import radians, sin, cos, asin, sqrt
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return 2 * 6_371_000 * asin(sqrt(a))


def rank_and_pick(geo_results: list[dict], info_map: dict[int, dict],
                  beach_lat: float, beach_lng: float,
                  max_radius_m: int, top_n: int) -> list[dict]:
    scored = []
    for g in geo_results:
        pid = g.get("pageid")
        info = info_map.get(pid)
        if not info: continue
        title = info.get("_title") or g.get("title", "")
        ext = _ext_of(title)
        # Skip non-photo formats
        if ext in SKIP_EXTS: continue
        if PHOTO_EXTS and ext and ext not in PHOTO_EXTS: continue
        # Skip undersized images
        if (info.get("width") or 0) < MIN_WIDTH: continue
        # Skip non-image MIME
        mime = info.get("mime") or ""
        if mime and not mime.startswith("image/"): continue
        d = haversine_m(beach_lat, beach_lng, g.get("lat") or 0, g.get("lon") or 0)
        if d > max_radius_m: continue
        scored.append({**g, "_info": info, "_distance_m": d, "_title": title})
    scored.sort(key=lambda x: x["_distance_m"])
    return scored[:top_n]


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_commons(fid: int, photos: list[dict]):
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}",
        "source":         "eq.wikimedia_commons",
    }, prefer="return=minimal")
    if not photos: return
    rows = []
    for i, p in enumerate(photos):
        info = p["_info"]
        extmd = info.get("extmetadata") or {}
        artist = _meta(extmd, "Artist") or "Wikimedia Commons contributor"
        license_short = _meta(extmd, "LicenseShortName") or _meta(extmd, "License") or "see Commons"
        description = _meta(extmd, "ImageDescription")
        page_url = f"https://commons.wikimedia.org/wiki/{urllib.parse.quote(p['_title'])}"
        # Build a sensible thumb URL via Commons' thumbnail endpoint
        full_url = info.get("url") or ""
        rows.append({
            "arena_group_id": fid,
            "source":         "wikimedia",
            "external_id":    str(p.get("pageid")),
            "image_url":      full_url,
            "thumb_url":      _thumb_url(full_url, 1024),
            "attribution":    f"{artist} / {license_short} via Wikimedia Commons",
            "license":        license_short,
            "width":          info.get("width"),
            "height":         info.get("height"),
            "lat":            p.get("lat"),
            "lng":            p.get("lon"),
            "distance_m":     int(p["_distance_m"]),
            "sort_order":     50 + i,        # rank Commons just below CCC, above Mapillary
            "page_url":       page_url,
            "source_meta":    {"description": (description or "")[:500],
                               "title": p["_title"],
                               "credit": _meta(extmd, "Credit"),
                               "usage_terms": _meta(extmd, "UsageTerms")},
        })
    supa("/rest/v1/beach_photos", method="POST", body=rows,
         prefer="return=minimal,resolution=ignore-duplicates")


def _thumb_url(full_url: str, target_width: int) -> str | None:
    """Convert Commons File URL to a thumb URL of the given width.
    Commons URL pattern:
      https://upload.wikimedia.org/wikipedia/commons/<a>/<ab>/<filename>
    Thumb pattern:
      https://upload.wikimedia.org/wikipedia/commons/thumb/<a>/<ab>/<filename>/<width>px-<filename>
    """
    if not full_url or "/commons/" not in full_url: return None
    if "/commons/thumb/" in full_url: return full_url
    parts = full_url.split("/commons/", 1)
    if len(parts) != 2: return None
    head, tail = parts
    # tail is "a/ab/filename.ext"
    segs = tail.split("/")
    if len(segs) < 3: return None
    filename = segs[-1]
    return f"{head}/commons/thumb/{tail}/{target_width}px-{filename}"


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--fids", help="Comma-separated beach fids")
    g.add_argument("--pilot", type=int, help="Sample first N tier-1+2 beaches")
    g.add_argument("--full", action="store_true", help="All tier-1+2 beaches")
    g.add_argument("--states", help="State codes, e.g. MA,RI,DE")
    ap.add_argument("--radius", type=int, default=RADIUS_M)
    ap.add_argument("--per-beach", type=int, default=PER_BEACH)
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches")

    saved_total = 0
    no_photos = 0
    for i, b in enumerate(targets):
        if i % 10 == 0:
            print(f"  [{i}/{len(targets)}] saved={saved_total} no_photos={no_photos}", flush=True)
        time.sleep(THROTTLE_S)
        try:
            geo = commons_geosearch(b["lat"], b["lng"], args.radius, limit=30)
            if not geo:
                no_photos += 1
                replace_commons(b["fid"], [])
                continue
            time.sleep(THROTTLE_S)
            info_map = commons_imageinfo([g["pageid"] for g in geo])
            picked = rank_and_pick(geo, info_map, b["lat"], b["lng"],
                                   args.radius, args.per_beach)
            if not picked:
                no_photos += 1
                replace_commons(b["fid"], [])
                continue
            replace_commons(b["fid"], picked)
            saved_total += len(picked)
        except Exception as e:
            print(f"  fid={b['fid']} ERR: {e}", file=sys.stderr)

    print(f"\nDone. {len(targets)} beaches, {saved_total} photos saved, {no_photos} no-coverage")


if __name__ == "__main__":
    main()
