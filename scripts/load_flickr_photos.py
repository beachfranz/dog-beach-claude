"""Load Flickr Creative Commons photos for an entity (beach or dog park).

Strategy:
  1. Geo-search Flickr near each entity's lat/lng
  2. Filter to CC-licensed (license codes 1, 2, 3, 4, 5, 6, 9, 10 — all CC)
  3. Score by name-match + relevance + proximity
  4. Top N by composite, with auto-curate of the top photos for dog parks

Entity parameterization (Franz 2026-05-26): the loader takes --entity
beach|dog_park and dispatches all table/column/RPC choices through the
ENTITIES dict. Same code, two entity types. Per [[never-solve-same-
problem-twice]] — beat the urge to clone load_*_dog_park_photos.py.

Flickr license codes (https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html):
  0  All Rights Reserved (skip)
  1  CC BY-NC-SA 2.0
  2  CC BY-NC 2.0
  3  CC BY-NC-ND 2.0
  4  CC BY 2.0
  5  CC BY-SA 2.0
  6  CC BY-ND 2.0
  7  No known copyright restrictions (museum/archive)
  8  US Government Work (public domain)
  9  CC0 / Public Domain Dedication
  10 Public Domain Mark

For commercial-friendly use we want 4 (BY), 5 (BY-SA), 7, 8, 9, 10.
For our app we'll be permissive: any CC license is fine since we're
showing photos with attribution. Skip only the All Rights Reserved (0).

Usage:
  python scripts/load_flickr_photos.py --fids 6212,6202,8337,218
  python scripts/load_flickr_photos.py --pilot 20
  python scripts/load_flickr_photos.py --full

Environment:
  FLICKR_API_KEY — required, free tier from flickr.com/services/apps/create/
"""

from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone  # noqa: F401 (timezone used in auto-curate timestamp)
from pathlib import Path

# scripts.common loads .env + injects truststore at package init.
# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.supa import supa

ROOT = Path(__file__).resolve().parent.parent  # for data-file lookups
FLICKR_KEY = (os.environ.get("FLICKR_API_KEY")
              or os.environ.get("FLICKR_KEY"))
if not FLICKR_KEY:
    print("ERROR: set FLICKR_API_KEY in scripts/pipeline/.env", file=sys.stderr)
    print("  Get a free key at https://www.flickr.com/services/apps/create/", file=sys.stderr)
    sys.exit(1)

PER_BEACH    = 5
RADIUS_KM    = 0.5          # Match the HARD_CUTOFF_M (450m) closely. Wider
                            # radii (5km) let Flickr's interestingness sort
                            # crowd out our actually-near photos with famous
                            # landmarks at the edge of the radius — fid 9944
                            # (Lake Union Park) returned 0 within 450m at 5km
                            # but 6 within 450m at 0.5km. Tightening to 500m
                            # is the right precision tradeoff.
HARD_CUTOFF_M = 450         # Franz's wikicommons-curation finding: photos >450m
                            # from beach point are rarely actually of the right beach.
# Loose-radius escape hatch for dog-titled photos: validated at Del Mar Dog
# Beach 2026-05-12 (+8 photos including Surf Dog Surf-A-Thon at ~700-1500m).
# Photos with an explicit dog keyword in the title are high-signal even when
# they come from event/cluster photography at the parking lot or trail head.
DOG_LOOSE_CUTOFF_M = 2000
import re as _re_top
DOG_KEYWORDS_RE = _re_top.compile(
    r'\b(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\b',
    _re_top.I,
)
THROTTLE_S   = 2.5         # bumped from 1.5 after the 1st key got soft-flagged
                            # on a 250-burst. Per-key reputation matters more
                            # than documented limits — start conservative.
# All CC license codes; 0 (ARR) excluded
CC_LICENSES  = "1,2,3,4,5,6,7,8,9,10"

LICENSE_LABEL = {
    "1": "CC-BY-NC-SA-2.0", "2": "CC-BY-NC-2.0", "3": "CC-BY-NC-ND-2.0",
    "4": "CC-BY-2.0", "5": "CC-BY-SA-2.0", "6": "CC-BY-ND-2.0",
    "7": "no-known-restrictions", "8": "US-government-public-domain",
    "9": "CC0", "10": "public-domain",
}

# Keyword bias for ranking — mirror of load_wikimedia_commons_photos.py.
# Consolidated 2026-05-19 per Franz: term lists live in _photo_filters.py
# (single source of truth for both ingest exclusion + relevance scoring).
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from _photo_filters import (  # noqa: E402
    POSITIVE_TERMS_DOG, POSITIVE_TERMS_GENERIC, POSITIVE_TERMS, NEGATIVE_TERMS,
)


# Tokens that are too generic to count as name-match signal. Most beach names
# contain "Beach"; "the" / "of" / "and" are connectors; "Park" / "State" /
# "Point" / "Cove" / "Bay" / "Cape" / "Island" appear in many beach names and
# can't disambiguate them.
NAME_STOPWORDS = {
    "beach", "the", "a", "an", "of", "and", "or",
    "state", "park", "county", "city",
    "point", "cove", "bay", "cape", "island",
    "public", "access", "parking", "lot",
    "north", "south", "east", "west",   # directional alone is weak signal
}

import re as _re
def _name_tokens(s: str) -> set[str]:
    if not s: return set()
    return {t for t in _re.findall(r"[a-z]+", s.lower())
            if len(t) > 2 and t not in NAME_STOPWORDS}


def _name_match_score(beach_name: str, title: str) -> float:
    """Reward photos whose title contains the beach's distinctive name tokens.
       Tokenize beach name + title (lowercase, strip non-alpha, drop generic
       stopwords like 'beach'/'state'/'park'/'cove'), then:
         - 0 distinctive matches   ->  0
         - partial match           ->  3.0 * (matched / total)  [linear]
         - full match (all tokens) -> +4.0 (caps the linear value at +4)
       Examples:
         beach='Mile Rock Beach'     title='Mile Rock Beach'     -> 2/2 = +4.0
         beach='Rosie's Dog Beach'   title='Rosie's Dog Beach'   -> 2/2 = +4.0
         beach='Black Sands Beach'   title='Black sand beach...' -> 1/2 = +1.5
                                              (sand/sands plural miss)
         beach='Main Beach'          title='Laguna Beach'        -> 0/1 = 0
                                              ('Main' is the only distinctive token)
    """
    beach_t = _name_tokens(beach_name)
    title_t = _name_tokens(title)
    if not beach_t or not title_t: return 0.0
    matched = len(beach_t & title_t)
    if matched == 0: return 0.0
    if matched >= len(beach_t): return 4.0
    return round(3.0 * matched / len(beach_t), 2)


def _relevance_score(title: str, description: str = "") -> float:
    """Term-list scoring — copy of the WC loader's function.
       Dog-specific positive: +3.0 title / +2.0 desc
       Generic positive:      +1.5 title / +1.0 desc
       Negative:              -1.2 title / -0.8 desc
    Flickr photos rarely carry descriptions in the search response, so
    description is usually empty and only title-scoring fires.
    NOTE: this is one of TWO scoring components — name-match is computed
    separately by _name_match_score(). Composite combines both with proximity.
    """
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


# ─── Entity configuration (Franz 2026-05-26 parameterization) ─────────────
# Per [[never-solve-same-problem-twice]] the loader dispatches all entity-
# specific choices (table, FK column, photo table, lat/lng resolution,
# beach-only RPCs) through this dict. Beach is the default for backward
# compat with existing pipeline callers.

ENTITIES = {
    "beach": {
        "table":            "beaches_gold",
        "fk_col":           "arena_group_id",
        "photo_table":      "beach_photos",
        "select_fields":    "fid,name,display_name_override,county_name,state,scoring_tier",
        "has_lat_lon":      False,    # beach lat/lng comes via get_beach_info RPC
        "lat_lon_rpc":      "get_beach_info",
        "supports_agencies": True,
        "supports_curator_rpcs": True,  # blocked_photographers + rejected RPC
        "default_query_kw": "beach",
        "auto_curate_top": 0,           # beaches use admin curator UI
    },
    "dog_park": {
        "table":            "dog_parks_gold",
        "fk_col":           "dog_park_fid",
        "photo_table":      "dog_park_photos",
        "select_fields":    "fid,name,display_name_override,address_city,state,lat,lon",
        "has_lat_lon":      True,     # dog_parks_gold has lat + lon columns directly
        "lat_lon_rpc":      None,
        "supports_agencies": False,
        "supports_curator_rpcs": False,
        "default_query_kw": "dog park",
        "auto_curate_top": 3,           # no curator UI yet → auto-mark top 3
    },
}


# ─── Supabase REST helpers ────────────────────────────────────────────────

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
        # Preserve caller's order — Supabase REST `in.(...)` returns rows in
        # Postgres' arbitrary order otherwise.
        by_fid = {r["fid"]: r for r in (rows or [])}
        rows = [by_fid[i] for i in ids if i in by_fid]
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
    elif args.state:
        rows = supa(f"/rest/v1/{table}",
                    params={"select": sel,
                            "is_active": "eq.true", "is_scoreable": "eq.true",
                            "state": f"eq.{args.state}",
                            "order": "fid.asc"})
    else:
        print("ERROR: provide --fids, --pilot N, --full, or --state", file=sys.stderr)
        sys.exit(1)

    out = []
    for r in rows or []:
        name = r.get("display_name_override") or r["name"]
        if ent["has_lat_lon"]:
            lat, lng = r.get("lat"), r.get("lon")
            if lat is None or lng is None: continue
            out.append({
                "fid": r["fid"], "name": name,
                "county": r.get("address_city"),  # nearest equivalent for dog_park
                "state": r.get("state"),
                "scoring_tier": None,
                "dogs_allowed": "yes",  # dog parks are definitionally yes
                "lat": lat, "lng": lng,
            })
        else:
            info = supa(f"/rest/v1/rpc/{ent['lat_lon_rpc']}",
                        method="POST", body={"p_fid": r["fid"]})
            b = (info or {}).get("beach") or {}
            if b.get("lat") is None: continue
            dp = (info or {}).get("dog_policy") or {}
            out.append({
                "fid": r["fid"], "name": name,
                "county": r.get("county_name"),
                "state": r.get("state"),
                "scoring_tier": r.get("scoring_tier"),
                "dogs_allowed": dp.get("dogs_allowed"),
                "lat": b["lat"], "lng": b["lng"],
            })
    return out


# ─── Flickr search ────────────────────────────────────────────────────────

# ── Agency-account fetch (Franz 2026-05-19 photo curation v3.9) ────────
# Per [[per-state-photo-source-discovery-2026-05-19]]: state/county park
# sites mostly lack per-park URLs in BEP, so HTML scraping won't work.
# Instead, use agency Flickr accounts (curated by definition).
# Registry: scripts/pipeline/agency_flickr_accounts.csv

AGENCY_CSV = ROOT / "scripts" / "data" / "agency_flickr_accounts.csv"


def load_agency_accounts(state: str | None = None) -> list[dict]:
    """Return rows from the agency CSV, optionally filtered to one state.
    Skips rows without a resolved NSID (run --resolve-agencies first)."""
    import csv as _csv
    rows: list[dict] = []
    if not AGENCY_CSV.exists():
        return rows
    with open(AGENCY_CSV, encoding="utf-8") as f:
        for r in _csv.DictReader(f):
            if not (r.get("nsid") or "").strip():
                continue
            states = (r.get("states") or "").strip()
            if state and states not in ("*", "") and state not in states.split(","):
                continue
            rows.append(r)
    return rows


def flickr_find_username_nsid(username: str) -> str | None:
    """Resolve a Flickr screen name → NSID via flickr.people.findByUsername."""
    params = {
        "method":  "flickr.people.findByUsername",
        "api_key": FLICKR_KEY,
        "username": username,
        "format":  "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            d = json.loads(r.read())
        if d.get("stat") == "ok":
            return ((d.get("user") or {}).get("nsid")) or None
        print(f"  findByUsername failed for {username}: {d.get('message')}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  findByUsername exception for {username}: {e}", file=sys.stderr)
        return None


def resolve_agencies_command() -> int:
    """One-shot: read CSV, resolve any blank NSIDs, write back."""
    import csv as _csv
    if not AGENCY_CSV.exists():
        print(f"ERROR: {AGENCY_CSV} not found", file=sys.stderr); return 1
    with open(AGENCY_CSV, encoding="utf-8") as f:
        rows = list(_csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []
    n_resolved = 0
    for r in rows:
        if (r.get("nsid") or "").strip():
            continue
        un = (r.get("flickr_username") or "").strip()
        if not un:
            continue
        nsid = flickr_find_username_nsid(un)
        if nsid:
            r["nsid"] = nsid
            n_resolved += 1
            print(f"  ✔ {r['agency_name']:50s} {un} → {nsid}")
        else:
            print(f"  ✗ {r['agency_name']:50s} {un} (could not resolve)")
        time.sleep(1.0)  # be polite
    with open(AGENCY_CSV, "w", encoding="utf-8", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows)
    print(f"\nResolved {n_resolved} NSIDs. CSV updated.")
    return 0


def flickr_search_by_user(user_id: str, text: str, per_page: int = 20) -> list[dict]:
    """Search a specific user's CC photos for a text query.
    Used for agency-account fetching (e.g., WA State Parks @ wastateparks).
    No geo constraint — agency photos are often not geo-tagged but are
    well-titled."""
    params = {
        "method":   "flickr.photos.search",
        "api_key":  FLICKR_KEY,
        "user_id":  user_id,
        "text":     text,
        "license":  CC_LICENSES,
        "extras":   "license,owner_name,date_taken,geo,url_z,url_l,url_m,url_n,description",
        "sort":     "relevance",
        "media":    "photos",
        "per_page": str(per_page),
        "format":   "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            d = json.loads(r.read())
        if d.get("stat") != "ok":
            return []
        return (d.get("photos") or {}).get("photo") or []
    except Exception as e:
        print(f"  agency search ({user_id}): {e}", file=sys.stderr)
        return []


def flickr_search(text, lat, lng, radius_km=RADIUS_KM, per_page=20):
    """Geo-only search. The text+geo AND filter is too restrictive in practice
    (e.g. Boston-area Carson Beach returns 0 with 'Carson Beach Suffolk' but 10
    with no text) — text-matching photos are often ungeo'd and the geo filter
    drops them. Rely on geo + post-score: HARD_CUTOFF_M for proximity, the
    name-match and term-relevance signals for what to surface in the top N.
    The `text` arg is retained for callers but ignored.
    """
    _ = text  # intentionally unused — see docstring
    params = {
        "method":   "flickr.photos.search",
        "api_key":  FLICKR_KEY,
        "license":  CC_LICENSES,
        "lat":      f"{lat}",
        "lon":      f"{lng}",
        "radius":   f"{radius_km}",
        "radius_units": "km",
        "extras":   "license,owner_name,date_taken,geo,url_z,url_l,url_m,url_n",
        "sort":     "interestingness-desc",
        "media":    "photos",
        "per_page": str(per_page),
        "format":   "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "DogBeachScout/1.0"})
    # Backoff retry on stat=fail (code 201 = service rate-limit/outage). Same
    # pattern as the WC loader's _commons_call.
    delays = [3, 10, 30]
    for attempt, delay in enumerate([0, *delays]):
        if delay > 0:
            time.sleep(delay)
            print(f"  flickr backoff {delay}s (attempt {attempt}/{len(delays)})",
                  file=sys.stderr)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read())
            if d.get("stat") == "ok":
                return (d.get("photos") or {}).get("photo") or []
            # Retryable: code 201 (service not currently available) or 105 (rate limit)
            code = d.get("code")
            if code in (105, 201) and attempt < len(delays):
                continue
            print(f"  flickr error code={code}: {d.get('message')}", file=sys.stderr)
            return []
        except Exception as e:
            if attempt < len(delays): continue
            print(f"  flickr exception: {e}", file=sys.stderr)
            return []
    return []


def haversine_m(la1, lo1, la2, lo2):
    from math import radians, sin, cos, asin, sqrt
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return 2 * 6_371_000 * asin(sqrt(a))


def pick_best(photos, beach_lat, beach_lng, beach_name="", beach_meta=None, top_n=None):
    """Pick the best candidates per the unified v3 ingest filter.

    Refactored 2026-05-19 per Franz collapsed-architecture decision:
    the v3 criteria we developed for pre-vision rank are NOW the load-time
    filter. No second post-load filter. Source-side rules (negative regex,
    rare-keyword override, source weight, tier-aware cap, dog-loose radius)
    all live in _photo_filters.pre_vision_rank().

    Args:
      photos       — raw Flickr search response objects
      beach_lat/lng — beach centroid for distance computation
      beach_name   — used for legacy name-match score (kept in source_meta)
      beach_meta   — {scoring_tier, dogs_allowed}; required for tier cap
      top_n        — DEPRECATED; cap is now from beach_meta. If provided,
                     overrides the tier cap (back-compat for ad-hoc callers).

    Returns: subset of input photos (each enriched with _distance_m,
    _relevance, _name_match, _composite, _rank_score) in selection order.
    Geometry: ungeo'd photos still dropped (Flickr is a geo-search source).
    """
    from _photo_filters import pre_vision_rank  # local import to avoid cycle

    enriched = []
    for p in photos:
        try:
            plat = float(p.get("latitude") or 0)
            plng = float(p.get("longitude") or 0)
        except (TypeError, ValueError):
            plat = plng = 0
        if plat == 0 and plng == 0:
            continue   # Flickr ingest path requires geo (Wikimedia loader same)
        d = int(haversine_m(beach_lat, beach_lng, plat, plng))
        title = p.get("title") or ""
        rel    = _relevance_score(title)
        name_m = _name_match_score(beach_name, title)

        # Build normalized dict the shared scorer expects (pre_vision_rank
        # reads: source, distance_m, title_text, captured_at, curator_touched,
        # photographer). Keep the original Flickr fields too so replace_flickr
        # can build beach_photos rows from the same dicts.
        enriched.append({
            **p,
            "_distance_m":     d,
            "_relevance":      rel,
            "_name_match":     name_m,
            # Legacy composite kept for source_meta back-compat
            "_composite":      rel + name_m + (1.0 - d / max(HARD_CUTOFF_M, 1)) * 2.0,
            # Normalized keys for the shared scorer
            "source":          "flickr",
            "distance_m":      d,
            "title_text":      title + " " + (p.get("ownername") or ""),
            "captured_at":     None,                # parsed later if needed
            "curator_touched": False,
            "photographer":    p.get("ownername"),
        })

    # Backward compat: if top_n was passed explicitly, use it; else cap from beach tier
    if top_n is not None:
        bm = {"scoring_tier": "hourly", "dogs_allowed": "yes"}   # neutral → cap=15
        kept = pre_vision_rank(enriched, bm)[:top_n]
    else:
        bm = beach_meta or {"scoring_tier": None, "dogs_allowed": "yes"}
        kept = pre_vision_rank(enriched, bm)
    #   2. DISPLAY — re-sort the kept set by distance asc, name-match as
    #      tiebreak. Curator UX wants closest-first (Franz, 2026-05-11).
    kept.sort(key=lambda x: (x["_distance_m"], -x.get("_name_match", 0)))
    return kept


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_flickr(fid, photos, entity="beach"):
    # API transient = "nothing to replace with" — preserve existing rather
    # than wipe-then-fail-to-refill. Diagnosed 2026-05-19: fid 6017 went
    # 20 → 0 because Flickr search returned (none) on a re-run that
    # had returned 20 photos minutes earlier. The DELETE-then-INSERT
    # pattern assumed the API is authoritative-per-call; it isn't.
    if not photos: return
    ent = ENTITIES[entity]
    photo_table = ent["photo_table"]
    fk_col = ent["fk_col"]
    auto_curate_top = ent["auto_curate_top"]
    # Delete ONLY uncurated rows. Curated photos (sort_order set by the curator
    # via the admin-curate-beach edge function) are preserved across re-runs.
    # If a candidate from this batch matches a kept photo by external_id, the
    # unique constraint (<fk_col>, source, external_id) + ignore-duplicates
    # on the INSERT will silently drop the duplicate.
    supa(f"/rest/v1/{photo_table}", method="DELETE", params={
        fk_col:        f"eq.{fid}",
        "source":      "eq.flickr",
        "curated_at":  "is.null",
    }, prefer="return=minimal")
    rows = []
    for i, p in enumerate(photos):
        owner = p.get("ownername") or "Flickr user"
        license_code = str(p.get("license", ""))
        license_label = LICENSE_LABEL.get(license_code, f"CC-{license_code}")
        attribution = f"© {owner} / {license_label} via Flickr"
        page_url = f"https://www.flickr.com/photos/{p.get('owner')}/{p.get('id')}"
        # Pick the best size: url_l (1024) for image, url_m (medium 500) for thumb
        image_url = p.get("url_l") or p.get("url_z") or p.get("url_m")
        thumb_url = p.get("url_n") or p.get("url_m")
        # date_taken format: "2010-05-23 14:32:11"
        cap = p.get("datetaken")
        if cap and cap != "0000-00-00 00:00:00":
            try:
                cap = datetime.fromisoformat(cap.replace(" ", "T") + "+00:00").isoformat()
            except Exception:
                cap = None
        else:
            cap = None
        try:
            plat = float(p.get("latitude") or 0) or None
            plng = float(p.get("longitude") or 0) or None
        except Exception:
            plat = plng = None
        row = {
            fk_col:           fid,
            "source":         "flickr",
            "external_id":    str(p.get("id")),
            "image_url":      image_url,
            "thumb_url":      thumb_url,
            "attribution":    attribution,
            "license":        license_label,
            "captured_at":    cap,
            "lat":            plat,
            "lng":            plng,
            "distance_m":     p["_distance_m"] if p["_distance_m"] >= 0 else None,
            "sort_order":     50 + i,  # flickr ranked above mapillary (100+)
            "page_url":       page_url,
            "source_meta":    {
                "title":            p.get("title"),
                "license_code":     license_code,
                "artist":           owner[:200],
                "relevance_score":  round(p.get("_relevance", 0.0), 2),
                "name_match_score": round(p.get("_name_match", 0.0), 2),
                "composite_score":  round(p.get("_composite", 0.0), 2),
            },
        }
        # PostgREST batch INSERT requires all rows to share the same keys —
        # include curated/match fields on every row (null for non-auto-curated).
        if auto_curate_top:
            in_top = i < auto_curate_top
            row["curated_at"]    = datetime.now(timezone.utc).isoformat() if in_top else None
            row["curated_by"]    = "auto" if in_top else None
            row["match_quality"] = ("high" if p.get("_name_match", 0) >= 3 else "medium") if in_top else None
        rows.append(row)
    if rows:
        # PostgREST honors resolution=ignore-duplicates ONLY when on_conflict
        # is set; without it, dup-key raises 409 mid-batch and leaves the
        # earlier DELETE half-applied (data loss). Diagnosed 2026-05-19.
        supa(f"/rest/v1/{photo_table}", method="POST", body=rows,
             params={"on_conflict": f"{fk_col},source,external_id"},
             prefer="return=minimal,resolution=ignore-duplicates")


# ─── Main ─────────────────────────────────────────────────────────────────

def fetch_blocked_photographers():
    """Returns set of artist strings to skip on this run (Flickr source)."""
    try:
        rows = supa("/rest/v1/rpc/beach_photo_blocked_photographers",
                    method="POST", body={"p_source": "flickr"})
    except Exception:
        return set()
    if not rows: return set()
    return {(r["artist"] or "").lower() for r in rows if r.get("artist")}


def fetch_rejected_external_ids(fids: list[int]) -> dict[int, set[str]]:
    """Returns {fid: {external_id, ...}} of photos the curator has rejected
    for this source. Loaders filter candidates against this before scoring,
    so trashed photos don't come back next run.
    """
    if not fids: return {}
    try:
        rows = supa("/rest/v1/rpc/get_rejected_external_ids",
                    method="POST",
                    body={"p_fids": fids, "p_source": "flickr"})
    except Exception:
        return {}
    out: dict[int, set[str]] = {}
    for r in rows or []:
        out.setdefault(r["arena_group_id"], set()).add(r["external_id"])
    return out


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids",  help="comma-separated list of fids")
    grp.add_argument("--pilot", type=int, help="run on first N entities")
    grp.add_argument("--full",  action="store_true")
    grp.add_argument("--state", help="2-letter state code — process all active+scoreable in state")
    grp.add_argument("--resolve-agencies", action="store_true",
                     help="One-shot: resolve usernames in agency_flickr_accounts.csv "
                          "to NSIDs and write back. Run this once before --use-agencies.")
    ap.add_argument("--entity", default="beach", choices=["beach", "dog_park"],
                    help="Entity type to load photos for. Default: beach (back-compat).")
    ap.add_argument("--refresh", action="store_true", help="redo even if rows exist")
    ap.add_argument("--radius-km", type=float, default=None,
                    help="Override the default search RADIUS_KM. Use 2 for "
                         "loose-radius dog sweep (validated at Del Mar Dog "
                         "Beach 2026-05-12: +8 dog photos at 700-1500m).")
    ap.add_argument("--use-agencies", action="store_true",
                    help="In addition to geo search, query each applicable "
                         "agency Flickr account (per state) for beach-name "
                         "text matches. Requires --resolve-agencies first. "
                         "Beach-only.")
    args = ap.parse_args()
    if args.use_agencies and not ENTITIES[args.entity]["supports_agencies"]:
        print(f"ERROR: --use-agencies not supported for entity={args.entity}", file=sys.stderr)
        return 1

    if args.resolve_agencies:
        return resolve_agencies_command()
    if args.radius_km is not None:
        global RADIUS_KM
        RADIUS_KM = args.radius_km
        print(f"[override] RADIUS_KM = {RADIUS_KM}")

    targets = select_targets(args)
    print(f"Targets: {len(targets)} {args.entity}s  (radius {RADIUS_KM}km, "
          f"top {PER_BEACH} per entity, hard cutoff {HARD_CUTOFF_M}m)")

    ent = ENTITIES[args.entity]
    blocked = fetch_blocked_photographers() if ent["supports_curator_rpcs"] else set()
    if blocked:
        print(f"Photographer blocklist: {len(blocked)} flickr artists "
              f"(>=3 prior photos, mean relevance <= 0)")

    rejected_by_fid = fetch_rejected_external_ids([b["fid"] for b in targets]) \
        if ent["supports_curator_rpcs"] else {}
    if rejected_by_fid:
        total_rej = sum(len(s) for s in rejected_by_fid.values())
        print(f"Curator-rejected tombstones: {total_rej} external_ids "
              f"across {len(rejected_by_fid)} entities (will skip)")

    saved = errored = 0
    n_agency_cands_total = 0
    for i, b in enumerate(targets, 1):
        try:
            query_parts = [b["name"]]
            if "beach" not in b["name"].lower():
                query_parts.append("beach")
            if b.get("county"): query_parts.append(b["county"])
            text = " ".join(query_parts)
            cands = flickr_search(text, b["lat"], b["lng"])

            # ── Agency-account pass (Franz 2026-05-19 v3.9) ────────────
            if args.use_agencies:
                agencies = load_agency_accounts(state=b.get("state"))
                seen_ids = {str(c.get("id")) for c in cands}
                for ag in agencies:
                    ag_cands = flickr_search_by_user(ag["nsid"], b["name"])
                    for ac in ag_cands:
                        if str(ac.get("id")) in seen_ids:
                            continue
                        seen_ids.add(str(ac.get("id")))
                        cands.append(ac)
                        n_agency_cands_total += 1
                    time.sleep(0.5)  # polite per-agency

            # Filter blocked photographers before scoring
            if blocked:
                cands = [c for c in cands
                         if (c.get("ownername") or "").lower() not in blocked]
            # Filter curator-rejected external_ids — don't surface trashed photos
            rej = rejected_by_fid.get(b["fid"]) or set()
            if rej:
                cands = [c for c in cands if str(c.get("id")) not in rej]
            picked = pick_best(cands, b["lat"], b["lng"], beach_name=b["name"],
                               beach_meta={"scoring_tier": b.get("scoring_tier"),
                                           "dogs_allowed": b.get("dogs_allowed")})
            replace_flickr(b["fid"], picked, entity=args.entity)
            saved += len(picked)
            tag = f'{len(picked)} photos' if picked else '(none)'
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  {b['name'][:40]:40s}  {tag}")
            if picked:
                p = picked[0]
                print(f'      top: "{p.get("title", "")[:50]}"  {p.get("ownername")}  '
                      f'{p["_distance_m"]}m  rel={p.get("_relevance",0):.1f} '
                      f'nm={p.get("_name_match",0):.1f}')
        except Exception as e:
            errored += 1
            print(f"  [{i}/{len(targets)}] fid={b['fid']}  ERROR: {e}", file=sys.stderr)
        time.sleep(THROTTLE_S)

    print(f"\n=== TOTALS ===  saved={saved}  errors={errored}")


if __name__ == "__main__":
    sys.exit(main())
