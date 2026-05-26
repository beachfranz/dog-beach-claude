"""Load web-search image results for an entity (beach or dog park).

Entity-aware per Franz 2026-05-26: --entity beach|dog_park dispatches
table/FK choices via _photo_filters.ENTITIES. Default: dog_park (this
loader was built for the dog-park gap — beaches have stronger geo-tagged
sources).

Source: Tavily image search with descriptions enabled. Query is
  "{display_name} {city} {entity-keyword}"
where entity-keyword is "dog park" / "beach" per ENTITIES dict.

Embed pattern (Franz 2026-05-26 "embed" decision):
- image_url = the third-party host URL Tavily returned (we do NOT
  download + re-host)
- page_url = the source page from Tavily's text results, or the image
  hosting page if no source page given
- attribution = "via web image search"
- license = "third-party / fair-use" — small thumbnail with link-back

Vision tagger gates final display (load_photo_vision_tags.py --entity
dog_park) so off-topic results get filtered before reaching consumer
surface. Rows land uncurated; dp_photos_curate marks the survivors.

Usage:
  python scripts/load_websearch_photos.py --entity dog_park --fids 1826,1838
  python scripts/load_websearch_photos.py --entity dog_park --state MD
  python scripts/load_websearch_photos.py --entity dog_park --full

Environment:
  TAVILY_API_KEY — required (already configured in scripts/pipeline/.env)
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import os
import time
from urllib.parse import urlparse

# Repo-root sys.path bootstrap (per [[sys-path-bootstrap-for-common-imports]])
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa
from scripts._photo_filters import ENTITIES

TAVILY_KEY = os.environ.get("TAVILY_API_KEY")
if not TAVILY_KEY:
    print("ERROR: set TAVILY_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    sys.exit(1)

PER_ENTITY = 5            # Top N images per entity
THROTTLE_S = 1.0          # Tavily allows generous rate; keep us polite
MAX_DESC_LEN = 400        # Truncate Tavily descriptions for source_meta


# ─── Tavily search ────────────────────────────────────────────────────────

def tavily_image_search(query: str, k: int = PER_ENTITY) -> dict:
    """Returns {'images': [{url, description}], 'results': [{url, ...}]}."""
    from tavily import TavilyClient
    c = TavilyClient(api_key=TAVILY_KEY)
    try:
        return c.search(
            query=query,
            include_images=True,
            include_image_descriptions=True,
            max_results=k,
        )
    except Exception as e:
        print(f"  tavily error: {e}", file=sys.stderr)
        return {"images": [], "results": []}


# ─── Target selection (mirrors load_flickr_photos.select_targets) ────────

def select_targets(args) -> list[dict]:
    ent = ENTITIES[args.entity]
    table = ent["table"]
    sel = ent["select_fields"]
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
    elif args.state:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "state": f"eq.{args.state}",
                            "order": "fid.asc"})
    elif args.pilot:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc",
                            "limit": str(int(args.pilot))})
    elif args.full:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "order": "fid.asc"})
    else:
        print("ERROR: provide --fids, --pilot N, --full, or --state",
              file=sys.stderr)
        sys.exit(1)

    out = []
    for r in (rows or []):
        name = r.get("display_name_override") or r.get("name")
        city = r.get("address_city") or r.get("county_name")
        out.append({
            "fid": r["fid"],
            "name": name,
            "city": city,
            "state": r.get("state"),
        })
    return out


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_websearch(fid: int, images: list[dict], results: list[dict],
                      entity: str = "dog_park") -> int:
    """Replace uncurated websearch rows for this entity+fid.

    Returns number of rows inserted. Curated rows are preserved.
    """
    if not images:
        return 0
    ent = ENTITIES[entity]
    photo_table = ent["photo_table"]
    fk_col = ent["fk_col"]
    # Wipe uncurated previous run results first
    supa(f"/rest/v1/{photo_table}", method="DELETE", params={
        fk_col:        f"eq.{fid}",
        "source":      "eq.websearch",
        "curated_at":  "is.null",
    }, prefer="return=minimal")

    # Try to associate each image to a top result page by host match
    result_pages_by_host = {}
    for r in (results or []):
        u = r.get("url") or ""
        try:
            host = urlparse(u).hostname or ""
            result_pages_by_host[host] = u
        except Exception:
            pass

    rows = []
    for i, img in enumerate(images):
        if isinstance(img, str):
            url, desc = img, None
        else:
            url = img.get("url")
            desc = (img.get("description") or "")[:MAX_DESC_LEN] or None
        if not url:
            continue
        try:
            host = urlparse(url).hostname or ""
        except Exception:
            host = ""
        # Prefer matching page from same host; else the image URL itself
        page_url = result_pages_by_host.get(host) or url
        rows.append({
            fk_col:           fid,
            "source":         "websearch",
            "external_id":    f"websearch:{url}",
            "image_url":      url,
            "thumb_url":      url,         # Tavily doesn't differentiate
            "attribution":    f"via web image search ({host})" if host else "via web image search",
            "license":        "third-party",
            "sort_order":     80 + i,       # rank below CC sources (50-60)
            "page_url":       page_url,
            "source_meta":    {
                "description": desc,
                "host": host,
                "rank": i,
            },
        })

    if rows:
        supa(f"/rest/v1/{photo_table}", method="POST", body=rows,
             params={"on_conflict": f"{fk_col},source,external_id"},
             prefer="return=minimal,resolution=ignore-duplicates")
    return len(rows)


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids", help="Comma-separated entity fids")
    grp.add_argument("--pilot", type=int, help="Sample first N entities")
    grp.add_argument("--full", action="store_true")
    grp.add_argument("--state", help="2-letter state code")
    ap.add_argument("--entity", default="dog_park",
                    choices=["beach", "dog_park"],
                    help="Entity type. Default: dog_park (loader's primary use).")
    ap.add_argument("--per-entity", type=int, default=PER_ENTITY)
    args = ap.parse_args()

    ent = ENTITIES[args.entity]
    kw = ent["default_query_kw"]
    targets = select_targets(args)
    print(f"Targets: {len(targets)} {args.entity}s  (top {args.per_entity} images per entity)")

    saved_total = 0
    errored = 0
    no_results = 0
    for i, b in enumerate(targets, 1):
        try:
            parts = [b["name"]]
            if kw.lower() not in (b["name"] or "").lower():
                parts.append(kw)
            if b.get("city"):  parts.append(b["city"])
            if b.get("state"): parts.append(b["state"])
            query = " ".join(parts)

            r = tavily_image_search(query, k=args.per_entity)
            images = r.get("images") or []
            results = r.get("results") or []

            n = replace_websearch(b["fid"], images, results, entity=args.entity)
            saved_total += n
            if not n: no_results += 1
            tag = f'{n} images' if n else '(none)'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  {tag}")
            if n and images:
                first = images[0]
                first_desc = (first.get('description') if isinstance(first, dict) else '') or '(no description)'
                print(f"      top: {first_desc[:80]}")
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}",
                  file=sys.stderr)
        time.sleep(THROTTLE_S)

    print(f"\n=== TOTALS ===  saved={saved_total}  no_results={no_results}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
