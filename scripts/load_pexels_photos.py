"""Load Pexels photos for each beach into beach_photos.

Pexels: keyword search (no geo). Free for commercial use, no
attribution required (Pexels License). We provide attribution
anyway as good practice.

Usage:
  python scripts/load_pexels_photos.py --fids 6212,6202,8337,218
  python scripts/load_pexels_photos.py --pilot 20
  python scripts/load_pexels_photos.py --full

Environment:
  PEXELS_API_KEY — required, free key from
    https://www.pexels.com/api/
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
from scripts._photo_filters import beach_name_tokens, is_wrong_beach

PEXELS_KEY = (os.environ.get("PEXELS_API_KEY")
              or os.environ.get("PEXELS_KEY"))
if not PEXELS_KEY:
    print("ERROR: set PEXELS_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free key at https://www.pexels.com/api/", file=sys.stderr)
    sys.exit(1)

PER_BEACH  = 3
THROTTLE_S = 0.5  # Pexels free tier is 200 req/hr; we're well under


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


def pexels_search(query, per_page=PER_BEACH):
    params = {"query": query, "per_page": str(per_page),
              "orientation": "landscape"}
    url = "https://api.pexels.com/v1/search?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "DogBeachScout/1.0",
        "Authorization": PEXELS_KEY,
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return (json.loads(r.read()) or {}).get("photos") or []
    except urllib.error.HTTPError as e:
        print(f"  pexels HTTP {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  pexels error: {e}", file=sys.stderr); return []


def replace_pexels(fid, photos):
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}", "source": "eq.pexels",
    }, prefer="return=minimal")
    if not photos: return
    rows = []
    for i, p in enumerate(photos):
        photographer = p.get("photographer") or "Pexels photographer"
        attribution  = f"Photo by {photographer} on Pexels"
        page_url     = p.get("url")
        srcs         = p.get("src") or {}
        rows.append({
            "arena_group_id": fid,
            "source":         "pexels",
            "external_id":    str(p.get("id")),
            "image_url":      srcs.get("large") or srcs.get("large2x") or srcs.get("medium"),
            "thumb_url":      srcs.get("small") or srcs.get("medium") or srcs.get("tiny"),
            "attribution":    attribution,
            "license":        "Pexels-License",
            "sort_order":     40 + i,  # pexels between unsplash (30+) and flickr (50+)
            "page_url":       page_url,
            "source_meta": {
                "alt":   p.get("alt"),
                "width": p.get("width"), "height": p.get("height"),
                "photographer_url": p.get("photographer_url"),
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
            # Pexels has no geo data — caption-based wrong-beach filter only.
            tokens = beach_name_tokens(b["name"])
            cands_raw = pexels_search(query, per_page=PER_BEACH * 3)
            kept = []
            rejected = []
            for p in cands_raw:
                alt = p.get("alt") or ""
                if is_wrong_beach(alt, tokens):
                    rejected.append(p)
                    continue
                kept.append(p)
                if len(kept) >= PER_BEACH:
                    break
            replace_pexels(b["fid"], kept)
            saved += len(kept)
            tag = f'{len(kept)} kept'
            if rejected:
                tag += f' / {len(rejected)} wrong-beach rejected'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  q={query!r}  {tag}")
            if kept:
                p = kept[0]
                desc = (p.get("alt") or "")[:60]
                print(f'      top: "{desc}"  by {p.get("photographer")}')
            for p in rejected[:2]:
                desc = (p.get("alt") or "")[:60]
                print(f'      rejected: "{desc}"')
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)
    print(f"\n=== TOTALS ===  saved={saved}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
