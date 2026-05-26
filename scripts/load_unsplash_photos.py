"""Load Unsplash photos for an entity (beach or dog park).

Entity-aware per Franz 2026-05-26: --entity beach|dog_park dispatches
table/FK choices via _photo_filters.ENTITIES. Beach is the default for
back-compat.

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
import argparse, json, os, sys, time, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timezone

# scripts.common loads .env + injects truststore at package init.
# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa
from scripts._photo_filters import beach_name_tokens, is_wrong_beach, haversine_m, ENTITIES

UNSPLASH_KEY = (os.environ.get("UNSPLASH_ACCESS_KEY")
                or os.environ.get("UNSPLASH_KEY"))
if not UNSPLASH_KEY:
    print("ERROR: set UNSPLASH_ACCESS_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free demo key at https://unsplash.com/developers", file=sys.stderr)
    sys.exit(1)

PER_BEACH = 3
THROTTLE_S = 1.0  # demo tier: 50/hr — keep us safely under
GEOFENCE_KM = 25  # If Unsplash photo has location data, must be within this many km


def select_targets(args):
    ent = ENTITIES[args.entity]
    table = ent["table"]
    sel = ent["select_fields"]
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
    elif args.pilot:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc", "limit": str(int(args.pilot))})
    elif args.full:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc"})
    elif args.states:
        states = [s.strip().upper() for s in args.states.split(",")]
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "state": f"in.({','.join(states)})",
                            "order": "fid.asc"})
    else:
        print("ERROR: provide --fids, --pilot N, --full, or --states", file=sys.stderr); sys.exit(1)
    out = []
    for r in (rows or []):
        name = r.get("display_name_override") or r.get("name")
        if ent["has_lat_lon"]:
            lat, lng = r.get("lat"), r.get("lon")
        else:
            info = supa(f"/rest/v1/rpc/{ent['lat_lon_rpc']}", method="POST", body={"p_fid": r["fid"]})
            b = (info or {}).get("beach") or {}
            lat, lng = b.get("lat"), b.get("lng")
        out.append({
            "fid": r["fid"],
            "name": name,
            "county": r.get("county_name") or r.get("address_city"),
            "state": r.get("state"),
            "lat": lat, "lng": lng,
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


def replace_unsplash(fid, photos, entity="beach"):
    ent = ENTITIES[entity]
    photo_table = ent["photo_table"]
    fk_col = ent["fk_col"]
    supa(f"/rest/v1/{photo_table}", method="DELETE", params={
        fk_col: f"eq.{fid}", "source": "eq.unsplash",
        "curated_at": "is.null",
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
        if plat is not None and plng is not None:
            sort_order = 25 + i
        else:
            sort_order = 35 + i
        rows.append({
            fk_col:           fid,
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
        supa(f"/rest/v1/{photo_table}", method="POST", body=rows,
             params={"on_conflict": f"{fk_col},source,external_id"},
             prefer="return=minimal,resolution=ignore-duplicates")


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids");      grp.add_argument("--pilot", type=int)
    grp.add_argument("--full", action="store_true")
    grp.add_argument("--states", help="State codes, e.g. CA,MD")
    ap.add_argument("--entity", default="beach", choices=["beach", "dog_park"],
                    help="Entity type. Default: beach (back-compat).")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches  (top {PER_BEACH} per beach)")

    saved = errored = 0
    for i, b in enumerate(targets, 1):
        try:
            ent_cfg = ENTITIES[args.entity]
            parts = [b["name"]]
            # Add entity keyword if not already in name (e.g. "Bark Park" already
            # implies dog park; "Bixby" alone needs disambiguation)
            kw = ent_cfg["default_query_kw"]
            if kw.lower() not in b["name"].lower():
                parts.append(kw)
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
            replace_unsplash(b["fid"], kept, entity=args.entity)
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
