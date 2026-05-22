"""Load nearby Mapillary photos for each beach into beach_photos.

Mapillary is street-level imagery with CC-BY-SA license. We query by
lat/lng (the closeto endpoint) and pick the top N images by distance
from the beach centroid. Stores URLs only — we link to Mapillary's
CDN, never proxy bytes.

Usage:
  python scripts/load_mapillary_photos.py --fids 6212,6202,8337,218
  python scripts/load_mapillary_photos.py --pilot 20
  python scripts/load_mapillary_photos.py --full
  python scripts/load_mapillary_photos.py --fids 6212 --refresh

Environment:
  MAPILLARY_ACCESS_TOKEN — required, free tier from mapillary.com/dashboard/developers

Each beach gets up to 5 photos within 200m of the centroid, sorted
by distance. Manual-source rows are protected from auto-replacement.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Windows AV-MITM cert verification blocks urllib's default SSL context.
# Use the system trust store (Windows certificate store) via truststore.
# Same pivot pattern as merge_operator_dogs_policy + Wikimedia loaders.
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

SUPABASE_URL  = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
MAPILLARY_KEY = (os.environ.get("MAPILLARY_ACCESS_TOKEN")
                 or os.environ.get("MAPILLARY_TOKEN")
                 or os.environ.get("MAIPILLARY_TOKEN"))
if not MAPILLARY_KEY:
    print("ERROR: set MAPILLARY_ACCESS_TOKEN in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free token at https://www.mapillary.com/dashboard/developers", file=sys.stderr)
    sys.exit(1)

RADIUS_M     = 200       # search radius around beach centroid
PER_BEACH    = 5         # max photos to keep per beach
THROTTLE_S   = 0.5       # sleep between Mapillary requests (be polite to free tier)


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
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "ignore")[:300]
        raise RuntimeError(f"Supabase {method} {path} -> HTTP {e.code}: {body_txt}") from None


def select_targets(args) -> list[dict]:
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override",
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
        # Pull lat/lng via RPC since we don't have direct geom access
        result = []
        for r in rows or []:
            info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": r["fid"]})
            b = (info or {}).get("beach") or {}
            if b.get("lat") is not None:
                result.append({"fid": r["fid"], "name": r.get("display_name_override") or r["name"],
                               "lat": b["lat"], "lng": b["lng"]})
        return result
    if args.pilot or args.full:
        # Use PostgREST + RPC for lat/lng
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "order": "fid.asc",
                            **({"limit": str(int(args.pilot))} if args.pilot else {})})
        result = []
        for r in rows or []:
            info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": r["fid"]})
            b = (info or {}).get("beach") or {}
            if b.get("lat") is not None:
                result.append({"fid": r["fid"], "name": r.get("display_name_override") or r["name"],
                               "lat": b["lat"], "lng": b["lng"]})
        return result
    print("ERROR: provide --fids, --pilot N, or --full", file=sys.stderr)
    sys.exit(1)


# ─── Mapillary fetch ──────────────────────────────────────────────────────

def fetch_mapillary(lat: float, lng: float, radius_m: int = RADIUS_M, limit: int = 30) -> list[dict]:
    """Pull image candidates near (lat, lng) from Mapillary Graph API.

    Mapillary's `closeto` parameter takes (lng, lat) and returns images
    within ~50m of that point. To get a wider radius we use bbox.
    """
    # Convert radius_m to a bbox in degrees (rough approx; lat-dependent)
    lat_deg = radius_m / 111_320.0
    lng_deg = radius_m / (111_320.0 * max(0.1, abs(__import__("math").cos(__import__("math").radians(lat)))))
    bbox = (lng - lng_deg, lat - lat_deg, lng + lng_deg, lat + lat_deg)
    fields = "id,thumb_2048_url,thumb_1024_url,thumb_256_url,captured_at,creator,compass_angle,geometry"
    url = (f"https://graph.mapillary.com/images?"
           f"access_token={MAPILLARY_KEY}"
           f"&fields={fields}"
           f"&bbox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
           f"&limit={limit}")
    req = urllib.request.Request(url, headers={"User-Agent": "DogBeachScout/1.0"})
    # Adaptive backoff on rate-limit / 5xx. Retry up to 4 times with
    # exponentially-growing waits (5s, 15s, 45s, 120s). 4xx (other than 429)
    # gets returned as empty so the run continues without retry — these are
    # legitimately "no data here" rather than transient.
    delays = [5, 15, 45, 120]
    for attempt, delay in enumerate([0, *delays]):
        if delay > 0:
            print(f"  Mapillary backoff {delay}s (attempt {attempt}/{len(delays)})", file=sys.stderr)
            time.sleep(delay)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return (json.loads(r.read()) or {}).get("data") or []
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")[:200]
            if e.code in (429, 500, 502, 503, 504) and attempt < len(delays):
                print(f"  Mapillary HTTP {e.code} (will retry): {body}", file=sys.stderr)
                continue
            print(f"  Mapillary HTTP {e.code}: {body}", file=sys.stderr)
            return []
        except Exception as e:
            if attempt < len(delays):
                print(f"  Mapillary error (will retry): {e}", file=sys.stderr)
                continue
            print(f"  Mapillary error: {e}", file=sys.stderr)
            return []
    return []


def haversine_m(la1, lo1, la2, lo2):
    from math import radians, sin, cos, asin, sqrt
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return 2 * 6_371_000 * asin(sqrt(a))


def rank_and_pick(images: list[dict], beach_lat: float, beach_lng: float,
                   max_radius_m: int, top_n: int) -> list[dict]:
    """Score by distance, drop anything outside radius, take top N."""
    scored = []
    for img in images:
        geom = img.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2: continue
        ilng, ilat = coords[0], coords[1]
        d = haversine_m(beach_lat, beach_lng, ilat, ilng)
        if d > max_radius_m: continue
        scored.append({**img, "_distance_m": d})
    scored.sort(key=lambda x: x["_distance_m"])
    return scored[:top_n]


# ─── Persistence ──────────────────────────────────────────────────────────

def existing_count(fid: int) -> int:
    rows = supa("/rest/v1/beach_photos", params={
        "arena_group_id": f"eq.{fid}",
        "source":         "eq.mapillary",
        "select":         "id",
    })
    return len(rows or [])


def replace_mapillary(fid: int, photos: list[dict]):
    # Delete old mapillary rows for this beach (manual rows are NOT touched)
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}",
        "source":         "eq.mapillary",
    }, prefer="return=minimal")
    if not photos:
        return
    # Insert new rows
    rows = []
    from datetime import datetime, timezone
    for i, p in enumerate(photos):
        creator = (p.get("creator") or {}).get("username") or "Mapillary contributor"
        attribution = f"© {creator} / CC-BY-SA via Mapillary"
        page_url = f"https://www.mapillary.com/app/?image_key={p['id']}"
        # Mapillary returns captured_at as epoch-milliseconds — convert to ISO
        cap = p.get("captured_at")
        if isinstance(cap, (int, float)):
            cap = datetime.fromtimestamp(cap / 1000.0, tz=timezone.utc).isoformat()
        elif isinstance(cap, str) and cap.isdigit():
            cap = datetime.fromtimestamp(int(cap) / 1000.0, tz=timezone.utc).isoformat()
        rows.append({
            "arena_group_id": fid,
            "source":         "mapillary",
            "external_id":    p["id"],
            "image_url":      p.get("thumb_2048_url") or p.get("thumb_1024_url"),
            "thumb_url":      p.get("thumb_256_url") or p.get("thumb_1024_url"),
            "attribution":    attribution,
            "license":        "CC-BY-SA",
            "captured_at":    cap,
            "lat":            (p.get("geometry") or {}).get("coordinates", [None,None])[1],
            "lng":            (p.get("geometry") or {}).get("coordinates", [None,None])[0],
            "distance_m":     int(p["_distance_m"]),
            "sort_order":     100 + i,  # mapillary rows ranked together
            "page_url":       page_url,
            "source_meta":    {"compass_angle": p.get("compass_angle")},
        })
    # PostgREST honors resolution=ignore-duplicates ONLY when on_conflict
    # is set; without it, dup-key raises 409 mid-batch. See Flickr loader.
    supa("/rest/v1/beach_photos", method="POST", body=rows,
         params={"on_conflict": "arena_group_id,source,external_id"},
         prefer="return=minimal,resolution=ignore-duplicates")


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids", help="comma-separated list of fids")
    grp.add_argument("--pilot", type=int, help="run on first N beaches")
    grp.add_argument("--full",  action="store_true", help="run on all active scoreable beaches")
    ap.add_argument("--refresh", action="store_true",
                    help="even if existing rows present, replace them")
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches  (radius {RADIUS_M}m, top {PER_BEACH} per beach)")

    saved = skipped = errored = 0
    for i, b in enumerate(targets, 1):
        try:
            if existing_count(b["fid"]) > 0 and not args.refresh:
                skipped += 1
                print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  has photos, skip (use --refresh to redo)")
                continue
            cands = fetch_mapillary(b["lat"], b["lng"], RADIUS_M, limit=30)
            picked = rank_and_pick(cands, b["lat"], b["lng"], RADIUS_M, PER_BEACH)
            replace_mapillary(b["fid"], picked)
            saved += len(picked)
            tag = f"{len(picked)} photos" if picked else "(none in radius)"
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  {tag}")
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)

    print(f"\n=== TOTALS ===")
    print(f"  photos saved: {saved}")
    print(f"  beaches skipped (already had data): {skipped}")
    print(f"  errors: {errored}")


if __name__ == "__main__":
    sys.exit(main())
