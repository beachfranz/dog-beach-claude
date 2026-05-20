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
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
import truststore                          # Win Python 3.14 OS-cert store
truststore.inject_into_ssl()

import argparse
import json
import os
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
                    params={"select": "fid,name,display_name_override,lat,lon,state,scoring_tier",
                            "fid": f"in.({','.join(map(str, ids))})",
                            "is_active": "eq.true"})
        return _shape(rows or [])
    if args.states:
        states = [s.strip().upper() for s in args.states.split(",")]
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,lat,lon,state,scoring_tier",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "state": f"in.({','.join(states)})",
                            "order": "fid.asc"})
        return _shape(rows or [])
    if args.pilot or args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid,name,display_name_override,lat,lon,state,scoring_tier",
                            "is_active": "eq.true",
                            "is_scoreable": "eq.true",
                            "order": "fid.asc",
                            **({"limit": str(int(args.pilot))} if args.pilot else {})})
        return _shape(rows or [])
    print("ERROR: provide --fids, --pilot N, --full, or --states XX,YY", file=sys.stderr)
    sys.exit(1)


def _shape(rows):
    # Fetch dogs_allowed per beach via get_beach_info (needed for v3 cap rule).
    # Keeps the loader self-contained — same pattern as load_flickr_photos.py.
    out = []
    for r in rows:
        if r.get("lat") is None or r.get("lon") is None:
            continue
        try:
            info = supa("/rest/v1/rpc/get_beach_info", method="POST", body={"p_fid": r["fid"]}) or {}
            dp = info.get("dog_policy") or {}
            dogs_allowed = dp.get("dogs_allowed")
        except Exception:
            dogs_allowed = None
        out.append({
            "fid": r["fid"],
            "name": r.get("display_name_override") or r["name"],
            "lat": r.get("lat"), "lng": r.get("lon"),
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


def rank_and_pick(geo_results: list[dict], info_map: dict[int, dict],
                  beach_lat: float, beach_lng: float,
                  max_radius_m: int, top_n: int,
                  blocked_artists: set[str] | None = None,
                  beach_name: str = "",
                  beach_meta: dict | None = None) -> list[dict]:
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

def replace_commons(fid: int, photos: list[dict]):
    # API transient = "nothing to replace with" — preserve existing rather
    # than wipe-then-fail-to-refill. Diagnosed 2026-05-19 for Flickr; same
    # risk shape applies here.
    if not photos: return
    # Delete ONLY uncurated rows (curated photos are preserved). Also: the
    # stored source is 'wikimedia' — historical bug used 'wikimedia_commons'
    # in the DELETE which never matched, making the loader silently additive.
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"eq.{fid}",
        "source":         "eq.wikimedia",
        "curated_at":     "is.null",
    }, prefer="return=minimal")
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
                               "artist": artist[:200],   # for blocklist grouping
                               "credit": _meta(extmd, "Credit"),
                               "usage_terms": _meta(extmd, "UsageTerms"),
                               "relevance_score":  round(p.get("_relevance", 0.0), 2),
                               "name_match_score": round(p.get("_name_match", 0.0), 2),
                               "composite_score":  round(p.get("_composite", 0.0), 2)},
        })
    # PostgREST needs on_conflict= to honor resolution=ignore-duplicates;
    # without it, dup-key 409s mid-batch leaves DELETE half-applied (lost
    # photos). Diagnosed 2026-05-19 — fid 6065 went 5→1.
    supa("/rest/v1/beach_photos", method="POST", body=rows,
         params={"on_conflict": "arena_group_id,source,external_id"},
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
    g.add_argument("--fids", help="Comma-separated beach fids")
    g.add_argument("--pilot", type=int, help="Sample first N tier-1+2 beaches")
    g.add_argument("--full", action="store_true", help="All tier-1+2 beaches")
    g.add_argument("--states", help="State codes, e.g. MA,RI,DE")
    ap.add_argument("--radius", type=int, default=RADIUS_M)
    ap.add_argument("--per-beach", type=int, default=PER_BEACH)
    args = ap.parse_args()

    targets = select_targets(args)
    print(f"Targets: {len(targets)} beaches")

    blocked = fetch_blocked_photographers()
    if blocked:
        print(f"Photographer blocklist: {len(blocked)} artists "
              f"(>=3 prior photos, mean rel <= 0)")

    rejected_by_fid = fetch_rejected_external_ids([b["fid"] for b in targets])
    if rejected_by_fid:
        total_rej = sum(len(s) for s in rejected_by_fid.values())
        print(f"Curator-rejected tombstones: {total_rej} external_ids "
              f"across {len(rejected_by_fid)} beaches (will skip)")

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
            # Filter curator-rejected external_ids — don't surface trashed photos
            rej = rejected_by_fid.get(b["fid"]) or set()
            if rej:
                geo = [g for g in geo if str(g.get("pageid")) not in rej]
                if not geo:
                    no_photos += 1
                    replace_commons(b["fid"], [])
                    continue
            time.sleep(THROTTLE_S)
            info_map = commons_imageinfo([g["pageid"] for g in geo])
            picked = rank_and_pick(geo, info_map, b["lat"], b["lng"],
                                   args.radius, args.per_beach, blocked,
                                   beach_name=b.get("name", ""),
                                   beach_meta={"scoring_tier": b.get("scoring_tier"),
                                               "dogs_allowed": b.get("dogs_allowed")})
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
