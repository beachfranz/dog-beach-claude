"""Load nearby Wikimedia Commons photos for an entity (beach or dog park).

Entity-aware per Franz 2026-05-26: --entity beach|dog_park dispatches all
table/FK/RPC choices through _photo_filters.ENTITIES. Beach is the
default for back-compat.

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
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import time
import urllib.parse
import urllib.request
import urllib.error

# scripts.common loads .env + injects truststore at package init.
# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa

# Single source of truth for per-entity table / FK / RPC dispatch.
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from _photo_filters import ENTITIES  # noqa: E402

USER_AGENT    = "DogBeachScout/1.0 (https://dogbeachscout.app; data@dogbeachscout.app) commons-loader"
COMMONS_API   = "https://commons.wikimedia.org/w/api.php"
RADIUS_M      = 500          # geosearch radius around beach centroid
PER_BEACH     = 5            # max photos to keep per beach
MIN_WIDTH     = 800          # skip thumbnails / icons
THROTTLE_S    = 0.3          # sleep between API calls — Commons is generous but be polite
PHOTO_EXTS    = {"jpg", "jpeg", "png", "tif", "tiff", "webp"}
SKIP_EXTS     = {"svg", "pdf", "ogv", "ogg", "webm", "mp3", "wav"}


# ─── Supabase REST helpers ────────────────────────────────────────────────

def select_targets(args) -> list[dict]:
    ent = ENTITIES[args.entity]
    table = ent["table"]
    sel = ent["select_fields"]
    base = {"is_active": "eq.true"}
    # Scoreable filter: dog_parks_gold has is_scoreable boolean; beaches_gold
    # uses scoring_tier IN ('daily','hourly'). Entity-aware refactor 2026-05-26
    # left is_scoreable hardcoded which fails for beach (column doesn't exist).
    scoreable_filter = (
        {"scoring_tier": "in.(daily,hourly)"} if args.entity == "beach"
        else {"is_scoreable": "eq.true"}
    )
    if args.fids:
        ids = [int(s) for s in args.fids.split(",")]
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "fid": f"in.({','.join(map(str, ids))})",
                            **base})
    elif args.states:
        states = [s.strip().upper() for s in args.states.split(",")]
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            **base, **scoreable_filter,
                            "state": f"in.({','.join(states)})",
                            "order": "fid.asc"})
    elif args.pilot or args.full:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            **base, **scoreable_filter,
                            "order": "fid.asc",
                            **({"limit": str(int(args.pilot))} if args.pilot else {})})
    else:
        print("ERROR: provide --fids, --pilot N, --full, or --states XX,YY", file=sys.stderr)
        sys.exit(1)
    return _shape(rows or [], args.entity)


def _shape(rows, entity: str):
    """Normalize candidate rows into the loader's working dict. Per-entity
    lat/lng resolution: dog_park rows already have lat+lon columns; beach
    rows need a get_beach_info RPC trip to resolve lat/lng + dogs_allowed."""
    ent = ENTITIES[entity]
    has_latlon = ent["has_lat_lon"]
    out = []
    for r in rows:
        name = r.get("display_name_override") or r.get("name")
        if has_latlon:
            lat, lng = r.get("lat"), r.get("lon")
            if lat is None or lng is None: continue
            out.append({
                "fid": r["fid"], "name": name,
                "lat": lat, "lng": lng,
                "state": r.get("state"),
                "scoring_tier": None,
                "dogs_allowed": "yes",   # dog parks are definitionally yes
            })
        else:
            # Beach entity: PostgREST returns nav_lat/nav_lon (per
            # ENTITIES["beach"].select_fields). Earlier select_fields
            # used lat/lon directly; 2026-05-26 entity-aware refactor
            # renamed the columns to nav_* but missed this read site.
            # Result: every beach row was skipped, Wikimedia loads dropped
            # from ~700/day → 0 from 2026-05-26 onward (catalog audit
            # 2026-06-06). Fixed: read nav_lat/nav_lon directly. Fallback
            # to lat/lon for forward-compat if a future refactor renames.
            lat = r.get("nav_lat") if r.get("nav_lat") is not None else r.get("lat")
            lng = r.get("nav_lon") if r.get("nav_lon") is not None else r.get("lon")
            if lat is None or lng is None:
                continue
            try:
                info = supa(f"/rest/v1/rpc/{ent['lat_lon_rpc']}", method="POST",
                            body={"p_fid": r["fid"]}) or {}
                dp = info.get("dog_policy") or {}
                dogs_allowed = dp.get("dogs_allowed")
            except Exception:
                dogs_allowed = None
            out.append({
                "fid": r["fid"], "name": name,
                "lat": lat, "lng": lng,
                "state": r.get("state"),
                "scoring_tier": r.get("scoring_tier"),
                "dogs_allowed": dogs_allowed,
            })
    return out


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


# ─── Photographer blocklist ───────────────────────────────────────────────
# Auto-blocklist photographers whose past contributions average a negative
# relevance score (specimen photos, vehicles, etc.). Threshold: ≥3 photos
# AND mean relevance ≤ 0.0. Computed from prior wikimedia rows in
# beach_photos at loader start.

def fetch_blocked_photographers() -> set[str]:
    """Returns set of artist strings to skip on this run."""
    rows = supa("/rest/v1/rpc/wikimedia_blocked_photographers", method="POST", body={})
    if not rows: return set()
    return {r["artist"].lower() for r in rows if r.get("artist")}


def fetch_rejected_external_ids(fids: list[int]) -> dict[int, set[str]]:
    """Returns {fid: {external_id, ...}} of WC photos the curator has rejected.
    Loaders filter candidates against this before scoring so trashed photos
    don't come back next run.
    """
    if not fids: return {}
    try:
        rows = supa("/rest/v1/rpc/get_rejected_external_ids",
                    method="POST",
                    body={"p_fids": fids, "p_source": "wikimedia"})
    except Exception:
        return {}
    out: dict[int, set[str]] = {}
    for r in rows or []:
        out.setdefault(r["arena_group_id"], set()).add(r["external_id"])
    return out


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


# Keyword bias for ranking. Positive terms boost; negative terms penalize.
# Match is substring-insensitive against (title + description) lowercased.
# Narrowed pass per Franz 2026-05-09: just dog/sand/shore positive,
# train added to negatives (Del Mar got "Coaster Train" photos).
# Two-tier positive bias: dog-specific terms get 2× weight over generic
# beach terms. A dog-on-the-beach photo should outrank a generic sand
# photo every time. Franz iteration 4.
# Consolidated 2026-05-19 per Franz: term lists live in _photo_filters.py
# (single source of truth for both ingest exclusion + relevance scoring).
# Mammals (whale/dolphin/porpoise) were dropped from NEGATIVE_TERMS in the
# merge — beach spectacle, not specimen clutter.
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from _photo_filters import (  # noqa: E402
    POSITIVE_TERMS_DOG, POSITIVE_TERMS_GENERIC, POSITIVE_TERMS, NEGATIVE_TERMS,
)


def _relevance_score(title: str, description: str) -> float:
    """Weighted scoring:
       Dog-specific positive: +3.0 title / +2.0 desc (overweighted)
       Generic positive:      +1.5 title / +1.0 desc
       Negative:              -1.2 title / -0.8 desc
    Title hits weighted higher than description (titles are more curated)."""
    txt_title = (title or "").lower()
    txt_desc  = (description or "").lower()
    score = 0.0
    for t in POSITIVE_TERMS_DOG:
        if t in txt_title: score += 3.0
        elif t in txt_desc: score += 2.0
    for t in POSITIVE_TERMS_GENERIC:
        if t in txt_title: score += 1.5
        elif t in txt_desc: score += 1.0
    for t in NEGATIVE_TERMS:
        if t in txt_title: score -= 1.2
        elif t in txt_desc: score -= 0.8
    return score


# Mirror of load_flickr_photos.py — same name-match scoring across sources
# so the curate UI is consistent regardless of which loader populated the row.
NAME_STOPWORDS = {
    "beach", "the", "a", "an", "of", "and", "or",
    "state", "park", "county", "city",
    "point", "cove", "bay", "cape", "island",
    "public", "access", "parking", "lot",
    "north", "south", "east", "west",
}
import re as _re_global

# Dog-word filter ([[this-is-a-dog-app]] + [[apply-loader-bias-to-beach-photos]]).
# Commons geosearch returns whatever is geo-tagged near the centroid (boats,
# tents, panoramas, occasional dogs). API has no keyword arg, so we filter
# post-fetch: keep only files whose title OR description mentions a dog token.
# Word-bounded to skip "dogwood", "dogleg", "doggerel" etc. Catalog-wide
# audit 2026-06-06: untargeted geosearch yields ~1.7% has_dog; this filter
# is ~5x dog-yield uplift per Franz directive 2026-06-06 LATE.
_DOG_WORD_RE = _re_global.compile(
    r"\b(dogs?|puppy|puppies|canines?|retriever|labrador|poodle|terrier|"
    r"shepherd|dachshund|husky|corgi|beagle|chihuahua)\b",
    _re_global.I,
)


def _name_tokens(s: str) -> set[str]:
    if not s: return set()
    return {t for t in _re_global.findall(r"[a-z]+", s.lower())
            if len(t) > 2 and t not in NAME_STOPWORDS}


def _name_match_score(beach_name: str, title: str) -> float:
    """Reward photos whose title contains the beach's distinctive name tokens.
       Tokenize beach name + title (drop generic stopwords, len <= 2).
       0 matches -> 0, partial -> linear 3.0 * matched/total, full -> 4.0.
       See project_photo_scoring_spec.md for details."""
    beach_t = _name_tokens(beach_name)
    title_t = _name_tokens(title)
    if not beach_t or not title_t: return 0.0
    matched = len(beach_t & title_t)
    if matched == 0: return 0.0
    if matched >= len(beach_t): return 4.0
    return round(3.0 * matched / len(beach_t), 2)


def _has_dog_text(title: str, desc: str) -> bool:
    """True iff title or description mentions a dog token (word-bounded)."""
    return bool(_DOG_WORD_RE.search((title or "") + " " + (desc or "")))


def rank_and_pick(geo_results: list[dict], info_map: dict[int, dict],
                  beach_lat: float, beach_lng: float,
                  max_radius_m: int, top_n: int,
                  blocked_artists: set[str] | None = None,
                  beach_name: str = "",
                  beach_meta: dict | None = None,
                  require_dog: bool = True) -> list[dict]:
    """Pick the best Wikimedia Commons candidates per the unified v3
    ingest filter. Refactored 2026-05-19 per Franz collapsed-architecture
    decision — same pattern as load_flickr_photos.pick_best().

    All source-side picking logic (negative regex, rare-keyword override,
    source weight, tier cap, dog-loose-radius) lives in
    _photo_filters.pre_vision_rank(). MIN_WIDTH removed — vision tagger's
    quality_issue flag catches genuinely bad images.

    Kept WM-specific filters:
      - Extension/MIME check (skip SVG/PDF/video that aren't photos)
      - max_radius_m enforced by pre_vision_rank's 500m default (this
        arg now informational; dog-loose extends to 2km automatically)

    Args:
      beach_meta — {scoring_tier, dogs_allowed} for tier cap
      top_n      — DEPRECATED; cap is from beach_meta. Back-compat only.
    """
    from _photo_filters import pre_vision_rank  # local import to avoid cycle

    enriched = []
    for g in geo_results:
        pid = g.get("pageid")
        info = info_map.get(pid)
        if not info: continue
        title = info.get("_title") or g.get("title", "")
        ext = _ext_of(title)
        if ext in SKIP_EXTS: continue
        if PHOTO_EXTS and ext and ext not in PHOTO_EXTS: continue
        mime = info.get("mime") or ""
        if mime and not mime.startswith("image/"): continue
        d = int(haversine_m(beach_lat, beach_lng, g.get("lat") or 0, g.get("lon") or 0))

        extmd = info.get("extmetadata") or {}
        desc = (extmd.get("ImageDescription", {}) or {}).get("value") or ""
        desc = _re_global.sub(r"<[^>]+>", "", desc)

        # Dog-word filter (require_dog=True by default per [[this-is-a-dog-app]]).
        # Skips files whose title+description don't mention a dog token.
        # Drops ~98% of geosearch hits (boats, tents, panoramas) and lifts
        # the surviving has_dog rate substantially. Override with
        # --no-dog-filter for backfills (e.g., scenic placeholders).
        if require_dog and not _has_dog_text(title, desc):
            continue

        # Skip blocklisted photographers
        artist_raw = _meta(extmd, "Artist") or ""
        if blocked_artists and artist_raw and artist_raw.lower() in blocked_artists:
            continue

        rel    = _relevance_score(title, desc)
        name_m = _name_match_score(beach_name, title)

        enriched.append({
            **g,
            "_info":          info,
            "_distance_m":    d,
            "_title":         title,
            "_relevance":     rel,
            "_name_match":    name_m,
            "_composite":     rel + name_m + (1.0 - d / max(max_radius_m, 1)) * 2.0,
            # Normalized keys for shared scorer
            "source":          "wikimedia",
            "distance_m":      d,
            "title_text":      title + " " + (desc or "") + " " + (artist_raw or ""),
            "captured_at":     None,
            "curator_touched": False,
            "photographer":    artist_raw,
        })

    bm = beach_meta or {"scoring_tier": None, "dogs_allowed": "yes"}
    kept = pre_vision_rank(enriched, bm)
    if top_n is not None and len(kept) > top_n:
        kept = kept[:top_n]
    # Display sort: closest-first (curator UX preference, Franz 2026-05-11)
    kept.sort(key=lambda x: (x["_distance_m"], -x.get("_name_match", 0)))
    return kept


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_commons(fid: int, photos: list[dict], entity: str = "beach"):
    # API transient = "nothing to replace with" — preserve existing rather
    # than wipe-then-fail-to-refill. Diagnosed 2026-05-19 for Flickr; same
    # risk shape applies here.
    if not photos: return
    ent = ENTITIES[entity]
    photo_table = ent["photo_table"]
    fk_col = ent["fk_col"]
    supa(f"/rest/v1/{photo_table}", method="DELETE", params={
        fk_col:          f"eq.{fid}",
        "source":        "eq.wikimedia",
        "curated_at":    "is.null",
    }, prefer="return=minimal")
    rows = []
    for i, p in enumerate(photos):
        info = p["_info"]
        extmd = info.get("extmetadata") or {}
        artist = _meta(extmd, "Artist") or "Wikimedia Commons contributor"
        license_short = _meta(extmd, "LicenseShortName") or _meta(extmd, "License") or "see Commons"
        description = _meta(extmd, "ImageDescription")
        page_url = f"https://commons.wikimedia.org/wiki/{urllib.parse.quote(p['_title'])}"
        full_url = info.get("url") or ""
        rows.append({
            fk_col:           fid,
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
            "sort_order":     50 + i,
            "page_url":       page_url,
            "source_meta":    {"description": (description or "")[:500],
                               "title": p["_title"],
                               "artist": artist[:200],
                               "credit": _meta(extmd, "Credit"),
                               "usage_terms": _meta(extmd, "UsageTerms"),
                               "relevance_score":  round(p.get("_relevance", 0.0), 2),
                               "name_match_score": round(p.get("_name_match", 0.0), 2),
                               "composite_score":  round(p.get("_composite", 0.0), 2)},
        })
    supa(f"/rest/v1/{photo_table}", method="POST", body=rows,
         params={"on_conflict": f"{fk_col},source,external_id"},
         prefer="return=minimal,resolution=ignore-duplicates")


def _thumb_url(full_url: str, target_width: int) -> str | None:
    """Return a Special:FilePath redirect URL — Commons resolves it to the
    nearest standard thumbnail size. Stable across filenames with special
    chars (parens, ampersands, comma) where the direct /commons/thumb/...
    URL pattern fails with HTTP 400.

    Pattern:
      https://commons.wikimedia.org/wiki/Special:FilePath/<filename>?width=N
    """
    if not full_url or "/commons/" not in full_url: return None
    # Get filename from the last path segment, strip query string
    clean = full_url.split("?", 1)[0]
    filename_encoded = clean.rstrip("/").rsplit("/", 1)[-1]
    if not filename_encoded: return None
    # Decode %28 → (, %29 → ), %2C → ,, etc. — Special:FilePath accepts
    # both forms but unencoded is the canonical input.
    filename = urllib.parse.unquote(filename_encoded)
    # Re-encode as URL path segment (safe chars include parens for Commons)
    filename_for_url = urllib.parse.quote(filename, safe="()',- _.")
    return f"https://commons.wikimedia.org/wiki/Special:FilePath/{filename_for_url}?width={target_width}"


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--fids", help="Comma-separated entity fids")
    g.add_argument("--pilot", type=int, help="Sample first N tier-1+2 entities")
    g.add_argument("--full", action="store_true", help="All tier-1+2 entities")
    g.add_argument("--states", help="State codes, e.g. MA,RI,DE")
    ap.add_argument("--entity", default="beach", choices=["beach", "dog_park"],
                    help="Entity type. Default: beach (back-compat).")
    ap.add_argument("--radius", type=int, default=RADIUS_M)
    ap.add_argument("--per-beach", type=int, default=PER_BEACH)
    ap.add_argument("--no-dog-filter", action="store_true",
                    help="Disable the post-fetch title/description dog-word filter. "
                         "Default is to require dogs|puppy|retriever|... in title or "
                         "description (per [[this-is-a-dog-app]]). Use this flag only "
                         "for scenic-placeholder backfills.")
    args = ap.parse_args()

    ent = ENTITIES[args.entity]
    targets = select_targets(args)
    print(f"Targets: {len(targets)} {args.entity}s  (radius {args.radius}m, per_{args.entity} {args.per_beach})")

    # Beach-only RPCs — skip for dog_park (no curator/blocklist tables yet).
    blocked = fetch_blocked_photographers() if ent["supports_curator_rpcs"] else set()
    if blocked:
        print(f"Photographer blocklist: {len(blocked)} artists "
              f"(>=3 prior photos, mean rel <= 0)")

    rejected_by_fid = (fetch_rejected_external_ids([b["fid"] for b in targets])
                       if ent["supports_curator_rpcs"] else {})
    if rejected_by_fid:
        total_rej = sum(len(s) for s in rejected_by_fid.values())
        print(f"Curator-rejected tombstones: {total_rej} external_ids "
              f"across {len(rejected_by_fid)} entities (will skip)")

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
                replace_commons(b["fid"], [], entity=args.entity)
                continue
            # Filter curator-rejected external_ids — don't surface trashed photos
            rej = rejected_by_fid.get(b["fid"]) or set()
            if rej:
                geo = [g for g in geo if str(g.get("pageid")) not in rej]
                if not geo:
                    no_photos += 1
                    replace_commons(b["fid"], [], entity=args.entity)
                    continue
            time.sleep(THROTTLE_S)
            info_map = commons_imageinfo([g["pageid"] for g in geo])
            picked = rank_and_pick(geo, info_map, b["lat"], b["lng"],
                                   args.radius, args.per_beach, blocked,
                                   beach_name=b.get("name", ""),
                                   beach_meta={"scoring_tier": b.get("scoring_tier"),
                                               "dogs_allowed": b.get("dogs_allowed")},
                                   require_dog=not args.no_dog_filter)
            if not picked:
                no_photos += 1
                replace_commons(b["fid"], [], entity=args.entity)
                continue
            replace_commons(b["fid"], picked)
            saved_total += len(picked)
        except Exception as e:
            print(f"  fid={b['fid']} ERR: {e}", file=sys.stderr)

    print(f"\nDone. {len(targets)} beaches, {saved_total} photos saved, {no_photos} no-coverage")


if __name__ == "__main__":
    main()
