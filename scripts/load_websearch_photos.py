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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# Repo-root sys.path bootstrap (per [[sys-path-bootstrap-for-common-imports]])
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa
from scripts._photo_filters import ENTITIES, tight_name_match

TAVILY_KEY = os.environ.get("TAVILY_API_KEY")
if not TAVILY_KEY:
    print("ERROR: set TAVILY_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    sys.exit(1)

PER_ENTITY = 5            # Top N images per entity
THROTTLE_S = 0.25         # tighter — workers pace each other naturally
DEFAULT_WORKERS = 8       # Tavily handles this fine; net 8x throughput
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
        # Centroid for tight-name-match → photo lat/lng stamping.
        # Beach uses nav_lat/nav_lon (point-on-surface); dog_park uses lat/lon.
        lat = r.get("nav_lat") if r.get("nav_lat") is not None else r.get("lat")
        lon = r.get("nav_lon") if r.get("nav_lon") is not None else r.get("lon")
        out.append({
            "fid": r["fid"],
            "name": name,
            "city": city,
            "state": r.get("state"),
            "lat": lat,
            "lon": lon,
        })
    return out


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_websearch(fid: int, images: list[dict], results: list[dict],
                      entity: str = "dog_park",
                      entity_name: str | None = None,
                      entity_lat: float | None = None,
                      entity_lon: float | None = None) -> int:
    """Replace uncurated websearch rows for this entity+fid.

    Centroid attribution: when entity_name/lat/lon are provided AND a
    photo's description+page_url contains all distinctive name tokens,
    we stamp entity_lat/lon on the row (with distance_m=0). Photos that
    don't pass the tight match are still inserted but with NULL lat/lng
    — curate gates them out, which is the correct outcome for off-topic
    websearch hits.

    Returns number of rows inserted. Curated rows are preserved.
    """
    if not images:
        return 0
    ent = ENTITIES[entity]
    photo_table = ent["photo_table"]
    fk_col = ent["fk_col"]
    can_centroid = (entity_name and entity_lat is not None and entity_lon is not None)
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
        row = {
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
        }
        # Tight-match centroid attribution. Without ALL distinctive name
        # tokens present in haystack (desc+page_url+host), photo stays
        # NULL-coord and curate skips it — correct outcome for off-topic
        # web-search hits like "Pinterest pin titled 'beach'".
        if can_centroid:
            haystack = " ".join(filter(None, [desc or "", page_url or "", host or ""]))
            if tight_name_match(entity_name, haystack):
                row["lat"] = entity_lat
                row["lng"] = entity_lon
                row["distance_m"] = 0
        rows.append(row)

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
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"Concurrent Tavily search workers (default {DEFAULT_WORKERS}).")
    args = ap.parse_args()

    ent = ENTITIES[args.entity]
    kw = ent["default_query_kw"]
    targets = select_targets(args)
    print(f"Targets: {len(targets)} {args.entity}s  "
          f"(top {args.per_entity} images per entity, {args.workers} workers)")

    state = {"saved": 0, "errored": 0, "no_results": 0, "done": 0}
    lock = threading.Lock()

    def process_one(idx_b):
        i, b = idx_b
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
            n = replace_websearch(b["fid"], images, results,
                                  entity=args.entity,
                                  entity_name=b["name"],
                                  entity_lat=b.get("lat"),
                                  entity_lon=b.get("lon"))

            with lock:
                state["saved"] += n
                if not n: state["no_results"] += 1
                state["done"] += 1
                done = state["done"]
            tag = f'{n} images' if n else '(none)'
            print(f"  [{done}/{len(targets)}] fid={b['fid']}  "
                  f"{b['name'][:40]:40s}  {tag}", flush=True)
            if n and images:
                first = images[0]
                desc = (first.get('description') if isinstance(first, dict) else '') or '(no description)'
                print(f"      top: {desc[:80]}", flush=True)
            # Polite per-worker pacing (8 workers × 0.25s ≈ 32 QPS ceiling)
            time.sleep(THROTTLE_S)
        except Exception as e:
            with lock:
                state["errored"] += 1
                state["done"] += 1
                done = state["done"]
            print(f"  [{done}/{len(targets)}] fid={b['fid']}  ERROR: {e}",
                  file=sys.stderr, flush=True)

    if args.workers <= 1:
        for ib in enumerate(targets, 1):
            process_one(ib)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(ex.map(process_one, enumerate(targets, 1)))

    print(f"\n=== TOTALS ===  saved={state['saved']}  "
          f"no_results={state['no_results']}  errors={state['errored']}")


if __name__ == "__main__":
    sys.exit(main())
