"""Load Pixabay photos for each beach into beach_photos.

Pixabay: keyword search (no geo). Free for commercial use, no
attribution required (Pixabay Content License). We provide
attribution anyway as good practice.

Usage:
  python scripts/load_pixabay_photos.py --fids 6212,6202,8337,218
  python scripts/load_pixabay_photos.py --pilot 20
  python scripts/load_pixabay_photos.py --full

Environment:
  PIXABAY_API_KEY — required, free key from
    https://pixabay.com/api/docs/
"""

from __future__ import annotations
import argparse, json, os, sys, time, urllib.parse, urllib.request, urllib.error

# scripts.common loads .env + injects truststore at package init.
# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa

PIXABAY_KEY = (os.environ.get("PIXABAY_API_KEY")
               or os.environ.get("PIXABAY_KEY"))
if not PIXABAY_KEY:
    print("ERROR: set PIXABAY_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free key at https://pixabay.com/api/docs/", file=sys.stderr)
    sys.exit(1)

PER_BEACH  = 3
THROTTLE_S = 0.5  # Pixabay rate-limits at 100 req/60s; we're well under


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
    return [{
        "fid": r["fid"],
        "name": r.get("display_name_override") or r["name"],
        "county": r.get("county_name"), "state": r.get("state"),
    } for r in (rows or [])]


def pixabay_search(query, per_page=PER_BEACH):
    params = {
        "key": PIXABAY_KEY,
        "q": query,
        "image_type": "photo",
        "orientation": "horizontal",
        "safesearch": "true",
        "per_page": str(max(per_page, 3)),  # Pixabay min is 3
    }
    url = "https://pixabay.com/api/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "DogBeachScout/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return (json.loads(r.read()) or {}).get("hits") or []
    except urllib.error.HTTPError as e:
        print(f"  pixabay HTTP {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  pixabay error: {e}", file=sys.stderr); return []


def replace_pixabay(fid, photos):
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}", "source": "eq.pixabay",
    }, prefer="return=minimal")
    if not photos: return
    rows = []
    for i, p in enumerate(photos[:PER_BEACH]):
        user = p.get("user") or "Pixabay user"
        attribution = f"Image by {user} on Pixabay"
        page_url = p.get("pageURL")
        rows.append({
            "arena_group_id": fid,
            "source":         "pixabay",
            "external_id":    str(p.get("id")),
            "image_url":      p.get("largeImageURL") or p.get("webformatURL"),
            "thumb_url":      p.get("previewURL") or p.get("webformatURL"),
            "attribution":    attribution,
            "license":        "Pixabay-Content-License",
            "sort_order":     45 + i,  # pixabay between unsplash (30+) and pexels (40+)
            "page_url":       page_url,
            "source_meta": {
                "tags": p.get("tags"),
                "width": p.get("imageWidth"), "height": p.get("imageHeight"),
            },
        })
    if rows:
        supa("/rest/v1/beach_photos", method="POST", body=rows,
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
            if b.get("state"): parts.append(b["state"])
            query = " ".join(parts)
            cands = pixabay_search(query, per_page=PER_BEACH)
            replace_pixabay(b["fid"], cands)
            saved += min(len(cands), PER_BEACH)
            tag = f'{min(len(cands),PER_BEACH)} photos' if cands else '(none)'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  q={query!r}  {tag}")
            if cands:
                p = cands[0]
                print(f'      top: "{(p.get("tags") or "")[:60]}"  by {p.get("user")}')
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)
    print(f"\n=== TOTALS ===  saved={saved}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
