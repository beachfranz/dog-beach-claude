"""walk_state_park_beaches.py — discover named beaches at state-park units.

Per-state handler-first walker. For each state-park unit in the state-parks
listing page, fetch the unit page and LLM-extract named beach/swim-area
features. Stages results in public.beach_discovery_queue.

Idempotent via UNIQUE(state, lower(parent_park_name), lower(beach_name),
source_handler) — re-running on a state is safe.

UT is the first handler (Franz directive 2026-05-27 — UT as proof point).
Add handlers as states need coverage.

Usage:
  python scripts/walk_state_park_beaches.py --state UT
  python scripts/walk_state_park_beaches.py --state UT --dry-run
  python scripts/walk_state_park_beaches.py --state UT --limit 3
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect  # noqa: E402
from scripts.common.llm import HAIKU, call_json  # noqa: E402


# ─── State-park-listing config ──────────────────────────────────────────

STATE_PARK_URLS_JSON = ROOT / "scripts" / "state_park_urls.json"


def state_park_listing_url(state: str) -> str | None:
    cfg = json.loads(STATE_PARK_URLS_JSON.read_text())
    return (cfg.get(state) or {}).get("url")


# ─── HTTP fetch ─────────────────────────────────────────────────────────

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def fetch_html(url: str, timeout: int = 30) -> str:
    """Plain requests fetch. Caller handles 403 fallback."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        # Strip simple HTML to keep LLM context tight
        return raw.decode("utf-8", errors="replace")


def html_to_text(html: str) -> str:
    """Crude HTML-to-text — strip tags, collapse whitespace, drop scripts/styles.
    Keeps the page small enough for cheap LLM context."""
    # Drop <script> + <style> bodies
    html = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<style\b[^>]*>.*?</style>", " ", html, flags=re.S | re.I)
    # Drop tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Decode common entities
    text = (text.replace("&nbsp;", " ")
                 .replace("&amp;", "&")
                 .replace("&#39;", "'")
                 .replace("&quot;", '"')
                 .replace("&rsquo;", "'")
                 .replace("&ldquo;", '"')
                 .replace("&rdquo;", '"'))
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ─── UT handler ─────────────────────────────────────────────────────────

UT_LISTING = "https://stateparks.utah.gov/parks/"
# Hrefs on stateparks.utah.gov are absolute. Capture the slug.
UT_PARK_URL_RE = re.compile(
    r'href=["\'](?:https?://stateparks\.utah\.gov)?/parks/([a-z0-9][a-z0-9\-]+)/?["\']',
    re.I)


def ut_enumerate_parks(listing_html: str) -> list[dict]:
    """Parse stateparks.utah.gov/parks/ HTML -> list of {name, url} dicts.
    UT uses /parks/<slug>/ pattern."""
    seen: dict[str, dict] = {}
    for m in UT_PARK_URL_RE.finditer(listing_html):
        slug = m.group(1).strip().lower()
        if not slug or slug in {"parks", ""}:
            continue
        # Slug -> Title Case name as fallback (overridden later by page <h1>)
        name = " ".join(w.capitalize() for w in slug.split("-"))
        url = f"https://stateparks.utah.gov/parks/{slug}/"
        if slug not in seen:
            seen[slug] = {"slug": slug, "name": name, "url": url}
    return list(seen.values())


# ─── LLM extraction ─────────────────────────────────────────────────────

EXTRACT_SYSTEM = """\
You extract NAMED on-the-ground beach / swim-area features from a US
state-park unit page. You receive ONE state park's page and must list
every named beach IN THAT PARK.

ENUMERATE THOROUGHLY. A park can have multiple named beaches; list them all.

═══ INCLUDE rule (ALL must be true) ═══

1. The name is a PROPER NOUN — capitalized name of a specific place,
   not a generic phrase. The name MUST contain at least one of these
   word stems: "Beach" / "Cove" / "Bay" / "Lagoon" / "Swim" /
   "Swimming" / "Sand" (only as a name segment, not as a descriptor).

2. The name is followed (or preceded) in the page by text confirming
   it is a swim/sand/beach access feature visitors go to.

3. The feature is INSIDE the state park the page is about. If the
   quote mentions a different park or reservoir as the location of the
   beach, REJECT IT — it belongs to that other park's row, not this one.

═══ EXCLUDE rule (any one triggers rejection) ═══

- Generic phrases: "sandy beaches", "the beach", "swim area", "beach
  access". (No proper noun.)
- Historical / closed / former / underwater (Lake Powell dam-flooded).
- Marinas / boat ramps / boat launches — UNLESS the proper noun
  literally contains "Beach" (e.g., "Marina Beach" OK, "North Marina"
  REJECT).
- Day-use areas / picnic areas / campgrounds — UNLESS the proper noun
  contains a beach-water word stem (Beach/Cove/Bay/etc.) AND the page
  describes it as a swim/sand spot.
- Sand dune / desert features (e.g., "Sand Mountain", "Sand Dunes").
  These are off-road / hike features, not water beaches.
- A reservoir or lake NAME itself (e.g., "Steinaker Reservoir",
  "Yuba Reservoir") — that is the water body, not a named beach
  feature ON that water body.
- Anything mentioned ONLY as a destination in another park.

═══ POSITIVE EXAMPLES (extract these shapes) ═══

  "Rendezvous Beach", "Cisco Beach", "Bridger Bay Beach",
  "Sunset Beach", "Sailboat Beach", "Sandy Beach Campground"
  (because "Sandy Beach" is the proper noun), "Hyrum Beach",
  "Madsen Bay" (Bay-suffixed named feature),
  "Sand Hollow Beach" (proper noun with Beach), "Swim Beach".

═══ NEGATIVE EXAMPLES (reject these) ═══

  "sandy beaches" (generic), "the beach" (generic), "Sand Mountain"
  (dune), "Cedar Point Day Use Area" (day-use, no beach-stem),
  "North Marina" (marina, no beach-stem), "Steinaker Reservoir"
  (water body name, not beach), "Quail Creek beaches" (generic).

═══ OUTPUT ═══

It is OK and common for a state park to have ZERO named beaches.
Return an empty array.

JSON ONLY (no markdown):
{
  "beaches": [
    {
      "name": "<proper-noun feature name, exactly as written in the
               page, including 'Beach'/'Bay'/'Cove' suffix or prefix>",
      "verbatim_quote": "<sentence from the page containing the name,
                          50-300 chars>",
      "has_dog_mention": <true if quote/page mentions dogs/pets at
                          this specific beach, else false>,
      "confidence": "high" | "medium" | "low"
    }
  ]
}
"""


def llm_extract_beaches(park_name: str, page_text: str) -> dict:
    # Trim page text to first 16k chars (Haiku has plenty of context, but
    # state-park pages are usually <8k after stripping).
    text_snippet = page_text[:16000]
    user = json.dumps({
        "state_park_unit": park_name,
        "page_text": text_snippet,
    }, ensure_ascii=False)
    r = call_json(EXTRACT_SYSTEM, user, model=HAIKU, max_tokens=1024)
    return r["parsed"]


# ─── PAD-US match (parent unit + centroid) ──────────────────────────────

def find_pad_us_match(cur, state: str, park_name: str) -> dict | None:
    """Best PAD-US match for park_name within state. Returns unit_id + centroid
    or None. Trigram-similarity-ranked.

    PAD-US schema: state is 2-letter, unit_name (not unit_nm), mng_type='STAT'
    AND mng_name='SPR' filters to actual state-park-rec units (vs State Land
    Board / Fish & Wildlife / DNR generic land)."""
    cur.execute("""
      SELECT unit_id, unit_name,
             ST_Y(ST_Centroid(geom))::float AS lat,
             ST_X(ST_Centroid(geom))::float AS lng,
             similarity(unit_name, %s) AS sim
        FROM public.pad_us_units
       WHERE state = %s
         AND mng_type = 'STAT'
         AND mng_name IN ('SPR','SDNR')
         AND (unit_name ILIKE %s OR similarity(unit_name, %s) > 0.4)
       ORDER BY similarity(unit_name, %s) DESC
       LIMIT 1
    """, (park_name, state, f"%{park_name}%", park_name, park_name))
    row = cur.fetchone()
    if row is None:
        return None
    return {"unit_id": str(row[0]), "unit_name": row[1],
            "lat": row[2], "lng": row[3], "similarity": float(row[4])}


# ─── Existing-arena dedup check ─────────────────────────────────────────

def find_best_arena_match(cur, beach_name: str, park_name: str,
                           lat: float | None, lng: float | None) -> dict | None:
    """Closest arena row by similarity + (if lat/lng) within 1000m. Returns
    fid + name + similarity or None."""
    if lat is not None and lng is not None:
        cur.execute("""
          SELECT a.fid, a.name,
                 GREATEST(similarity(coalesce(a.name,''), %s),
                          similarity(coalesce(a.park_name,''), %s)) AS sim,
                 ST_Distance(a.geom::geography,
                             ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)
                   AS dist_m
            FROM public.arena a
           WHERE a.geom IS NOT NULL
             AND ST_DWithin(a.geom::geography,
                            ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
                            1000)
           ORDER BY GREATEST(similarity(coalesce(a.name,''), %s),
                              similarity(coalesce(a.park_name,''), %s)) DESC
           LIMIT 1
        """, (beach_name, park_name, lng, lat, lng, lat, beach_name, park_name))
    else:
        cur.execute("""
          SELECT a.fid, a.name,
                 GREATEST(similarity(coalesce(a.name,''), %s),
                          similarity(coalesce(a.park_name,''), %s)) AS sim,
                 NULL::float AS dist_m
            FROM public.arena a
           ORDER BY GREATEST(similarity(coalesce(a.name,''), %s),
                              similarity(coalesce(a.park_name,''), %s)) DESC
           LIMIT 1
        """, (beach_name, park_name, beach_name, park_name))
    row = cur.fetchone()
    if row is None:
        return None
    return {"fid": int(row[0]), "name": row[1], "similarity": float(row[2]),
            "dist_m": float(row[3]) if row[3] is not None else None}


# ─── Main per-state walk ────────────────────────────────────────────────

HANDLERS = {
    "UT": {"enumerate": ut_enumerate_parks, "tag": "ut_stateparks_v1",
           "listing": UT_LISTING},
}


def walk_state(state: str, dry_run: bool = False, limit: int | None = None,
               sleep_s: float = 1.0) -> dict:
    handler = HANDLERS.get(state)
    if handler is None:
        raise SystemExit(f"No handler for state={state}. "
                         f"Add to HANDLERS in {__file__}.")

    listing_url = handler["listing"]
    print(f"[{state}] Fetching listing: {listing_url}", flush=True)
    listing_html = fetch_html(listing_url)

    parks = handler["enumerate"](listing_html)
    if limit:
        parks = parks[:limit]
    print(f"[{state}] Enumerated {len(parks)} parks", flush=True)

    stats = {"parks_seen": len(parks), "parks_with_beaches": 0,
             "beaches_found": 0, "rows_inserted": 0, "rows_dedup": 0,
             "llm_calls": 0, "errors": 0}

    if dry_run:
        for p in parks:
            print(f"  [{state}] {p['name']:<40} -> {p['url']}")
        return stats

    conn = connect(); conn.autocommit = False
    try:
        cur = conn.cursor()
        for i, p in enumerate(parks, 1):
            try:
                print(f"\n[{state}] {i}/{len(parks)} {p['name']} -> {p['url']}",
                      flush=True)
                park_html = fetch_html(p["url"])
                page_text = html_to_text(park_html)
                if not page_text or len(page_text) < 200:
                    print(f"  thin page ({len(page_text)} chars); skip")
                    continue

                extraction = llm_extract_beaches(p["name"], page_text)
                stats["llm_calls"] += 1
                beaches = extraction.get("beaches") or []
                if not beaches:
                    print(f"  no named beaches")
                    continue
                stats["parks_with_beaches"] += 1
                stats["beaches_found"] += len(beaches)

                pad_match = find_pad_us_match(cur, state, p["name"])
                parent_unit_id = pad_match["unit_id"] if pad_match else None
                centroid_lat = pad_match["lat"] if pad_match else None
                centroid_lng = pad_match["lng"] if pad_match else None
                if pad_match:
                    print(f"  PAD-US match: {pad_match['unit_name']} "
                          f"(sim={pad_match['similarity']:.2f})")

                for b in beaches:
                    name = (b.get("name") or "").strip()
                    quote = (b.get("verbatim_quote") or "").strip()
                    conf = (b.get("confidence") or "low").strip()
                    if not name:
                        continue
                    # Belt-and-suspenders post-LLM guards:
                    # 1. Verbatim quote must actually be in the page.
                    if quote and quote.lower() not in page_text.lower():
                        print(f"    [reject] '{name}' - quote not in page")
                        continue
                    # 2. Name must contain a water/beach stem (Beach, Cove,
                    #    Bay, Lagoon, Swim, Sand-as-prefix).
                    if not re.search(r"\b(beach|cove|bay|lagoon|swim|swimming|sand[ay]?)\b",
                                      name, re.I):
                        print(f"    [reject] '{name}' - no beach/cove/bay/swim stem")
                        continue
                    # 3. Name must not be lowercase-only (generic phrase).
                    if name == name.lower():
                        print(f"    [reject] '{name}' - lowercase / generic")
                        continue
                    # 4. Reject obvious non-beach features by name.
                    if re.search(r"\b(mountain|dune|ramp|marina|campground|"
                                  r"reservoir|lake|park)$", name, re.I):
                        # ...unless it has "Beach" right before that suffix
                        if not re.search(r"beach[\s\-]+(mountain|dune|ramp|marina|"
                                          r"campground|reservoir|lake|park)",
                                          name, re.I):
                            print(f"    [reject] '{name}' - non-beach feature suffix")
                            continue
                    # 5. Cross-park pollution check: if the verbatim quote
                    #    names a DIFFERENT state park, reject. Look for
                    #    "<other slug>" in the quote that doesn't match
                    #    this park's slug.
                    quote_lower = quote.lower()
                    if any(other["slug"].replace("-", " ") in quote_lower
                           and other["slug"] != p["slug"]
                           and other["name"].lower() in quote_lower
                           for other in parks):
                        # Likely cross-park pollution (e.g., Red Fleet page
                        # mentioning Steinaker)
                        print(f"    [reject] '{name}' - quote names a different park")
                        continue

                    dup = find_best_arena_match(cur, name, p["name"],
                                                centroid_lat, centroid_lng)
                    dup_status = None
                    if dup and dup["similarity"] > 0.7 and (
                        (dup["dist_m"] or 9999) < 300):
                        dup_status = "duplicate"
                        stats["rows_dedup"] += 1

                    try:
                        cur.execute("""
                          INSERT INTO public.beach_discovery_queue
                            (state, parent_park_name, parent_pad_us_unit_id,
                             parent_park_url, beach_name, verbatim_quote,
                             source_url, source_handler,
                             lat, lng, geocode_method,
                             best_similarity, best_match_fid, best_match_name,
                             status, queue_meta)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT
                            (state, lower(parent_park_name),
                             lower(beach_name), source_handler)
                          DO UPDATE SET
                            verbatim_quote = EXCLUDED.verbatim_quote,
                            source_url = EXCLUDED.source_url,
                            best_similarity = EXCLUDED.best_similarity,
                            best_match_fid = EXCLUDED.best_match_fid,
                            best_match_name = EXCLUDED.best_match_name,
                            updated_at = now()
                        """, (
                            state, p["name"], parent_unit_id, p["url"],
                            name, quote, p["url"], handler["tag"],
                            centroid_lat, centroid_lng,
                            "pad_us_centroid" if centroid_lat else None,
                            dup["similarity"] if dup else None,
                            dup["fid"] if dup else None,
                            dup["name"] if dup else None,
                            dup_status or "pending",
                            json.dumps({"confidence": conf,
                                       "has_dog_mention":
                                         bool(b.get("has_dog_mention")),
                                       "pad_match": pad_match}),
                        ))
                        stats["rows_inserted"] += 1
                        flag = "DUP" if dup_status == "duplicate" else "NEW"
                        print(f"    [{flag}] {name}  (conf={conf})")
                        if quote:
                            print(f"          quote: {quote[:120]}...")
                    except Exception as e:
                        print(f"    [insert error] {e}")
                        stats["errors"] += 1
                        conn.rollback()
                        continue
                conn.commit()
            except Exception as e:
                print(f"  [ERROR] {p['name']}: {e}")
                stats["errors"] += 1
                conn.rollback()
            time.sleep(sleep_s)
    finally:
        conn.close()

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="2-letter state code (e.g. UT)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Enumerate parks only, no fetch / no LLM / no writes")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process at most N parks (smoke-test)")
    args = ap.parse_args()
    stats = walk_state(args.state, dry_run=args.dry_run, limit=args.limit)
    print("\n=== SUMMARY ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
