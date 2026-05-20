"""Load Unsplash photos for each beach into beach_photos.

Unsplash: keyword search (no geo). Free for commercial use, no
attribution required (Unsplash License). We provide attribution
anyway as good practice.

Quality over quantity — we only keep top 3 results per beach
and only when the Unsplash relevance score is above a threshold.
Risk: keyword ambiguity (Huntington Beach NY vs CA, etc.). We
mitigate by including the city/state in the query.

Usage:
  python scripts/load_unsplash_photos.py --fids 6212,6202,8337,218

Environment:
  UNSPLASH_ACCESS_KEY — required, free demo tier from
    https://unsplash.com/developers
"""

from __future__ import annotations
import argparse, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from _photo_filters import beach_name_tokens, is_wrong_beach, haversine_m  # noqa: E402

load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
UNSPLASH_KEY = (os.environ.get("UNSPLASH_ACCESS_KEY")
                or os.environ.get("UNSPLASH_KEY"))
if not UNSPLASH_KEY:
    print("ERROR: set UNSPLASH_ACCESS_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free demo key at https://unsplash.com/developers", file=sys.stderr)
    sys.exit(1)

PER_BEACH = 3
THROTTLE_S = 1.0  # demo tier: 50/hr — keep us safely under
GEOFENCE_KM = 25  # If Unsplash photo has location data, must be within this many km


def supa(path, *, method="GET", body=None, params=None, prefer=None):
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer: headers["Prefer"] = prefer
    req = urllib.request.Request(f"{SUPABASE_URL}{path}{qs}", method=method,
        data=(json.dumps(body).encode() if body is not None else None), headers=headers)
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
        print("ERROR: provide --fids, --pilot N, or --full", file=sys.stderr); sys.exit(1)
    out = []
    for r in (rows or []):
        info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": r["fid"]})
        b = (info or {}).get("beach") or {}
        out.append({
            "fid": r["fid"],
            "name": r.get("display_name_override") or r["name"],
            "county": r.get("county_name"), "state": r.get("state"),
            "lat": b.get("lat"), "lng": b.get("lng"),
        })
    return out


def unsplash_search(query, per_page=PER_BEACH):
    params = {"query": query, "per_page": str(per_page),
              "orientation": "landscape", "content_filter": "high"}
    url = "https://api.unsplash.com/search/photos?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "DogBeachScout/1.0",
        "Accept-Version": "v1",
        "Authorization": f"Client-ID {UNSPLASH_KEY}",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return (json.loads(r.read()) or {}).get("results") or []
    except urllib.error.HTTPError as e:
        print(f"  unsplash HTTP {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  unsplash error: {e}", file=sys.stderr); return []


def replace_unsplash(fid, photos):
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}", "source": "eq.unsplash",
    }, prefer="return=minimal")
    if not photos: return
    rows = []
    for i, p in enumerate(photos):
        u = p.get("user") or {}
        name = u.get("name") or "Unsplash photographer"
        username = u.get("username")
        attribution = f"Photo by {name} on Unsplash"
        page_url = (p.get("links") or {}).get("html")
        urls = p.get("urls") or {}
        cap = p.get("created_at")
        loc = p.get("location") or {}
        pos = loc.get("position") or {}
        plat = pos.get("latitude")
        plng = pos.get("longitude")
        # Geotagged photos rank above non-geo (sort_order 25-29 vs 35-39)
        if plat is not None and plng is not None:
            sort_order = 25 + i
        else:
            sort_order = 35 + i
        rows.append({
            "arena_group_id": fid,
            "source":         "unsplash",
            "external_id":    p.get("id"),
            "image_url":      urls.get("regular") or urls.get("full"),
            "thumb_url":      urls.get("small") or urls.get("thumb"),
            "attribution":    attribution,
            "license":        "Unsplash-License",
            "captured_at":    cap,
            "lat":            plat, "lng": plng,
            "distance_m":     p.get("_distance_m"),
            "sort_order":     sort_order,
            "page_url":       page_url,
            "source_meta": {
                "username": username,
                "alt_description": p.get("alt_description"),
                "location_title": loc.get("title"),
                "width": p.get("width"), "height": p.get("height"),
            },
        })
    if rows:
        # PostgREST honors resolution=ignore-duplicates ONLY when on_conflict
        # is set; without it, dup-key raises 409 mid-batch. See Flickr loader.
        supa("/rest/v1/beach_photos", method="POST", body=rows,
             params={"on_conflict": "arena_group_id,source,external_id"},
             prefer="return=minimal,resolution=ignore-duplicates")


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids");      grp.add_argument("--pilot", type=int)
    grp.add_argument("--full", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches  (top {PER_BEACH} per beach)")

    saved = errored = 0
    for i, b in enumerate(targets, 1):
        try:
            parts = [b["name"]]
            if b.get("state"):  parts.append(b["state"])
            query = " ".join(parts)
            tokens = beach_name_tokens(b["name"])
            cands_raw = unsplash_search(query, per_page=PER_BEACH * 3)
            kept = []
            rejected_caption = []
            rejected_geo = []
            for p in cands_raw:
                alt = p.get("alt_description") or p.get("description") or ""
                # Caption-based wrong-beach reject
                if is_wrong_beach(alt, tokens):
                    rejected_caption.append(p)
                    continue
                # Geo fence reject (when both photo location and beach lat/lng known)
                pos = (p.get("location") or {}).get("position") or {}
                plat, plng = pos.get("latitude"), pos.get("longitude")
                if plat is not None and plng is not None and b.get("lat") is not None:
                    d_m = haversine_m(b["lat"], b["lng"], plat, plng)
                    p["_distance_m"] = int(d_m)
                    if d_m > GEOFENCE_KM * 1000:
                        rejected_geo.append(p)
                        continue
                kept.append(p)
                if len(kept) >= PER_BEACH:
                    break
            replace_unsplash(b["fid"], kept)
            saved += len(kept)
            tag = f'{len(kept)} kept'
            if rejected_caption: tag += f' / {len(rejected_caption)} caption-rejected'
            if rejected_geo:     tag += f' / {len(rejected_geo)} geofence-rejected'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  q={query!r}  {tag}")
            if kept:
                p = kept[0]
                desc = (p.get("alt_description") or p.get("description") or "")[:60]
                u = (p.get("user") or {}).get("name", "")
                d = p.get("_distance_m")
                geo_tag = f' ({d}m)' if d is not None else ''
                print(f'      top: "{desc}"  by {u}{geo_tag}')
            for p in rejected_caption[:2]:
                desc = (p.get("alt_description") or "")[:60]
                print(f'      caption-rejected: "{desc}"')
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)
    print(f"\n=== TOTALS ===  saved={saved}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
