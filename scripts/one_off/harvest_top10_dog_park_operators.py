"""Harvest dog-park listings from the top 10 CA operators' websites.

Goal: surface parks that operators publish but OSM hasn't tagged — the
"gap" parks. For each of the top 10 operators (ranked by # OSM dog parks
attributed via CPAD or TIGER city polygons), fetch the operator's dog-park
hub page, LLM-extract the structured park list, geocode the addresses,
then compare to osm_dog_parks within the state.

Output:
  tmp/dog_park_harvest_<date>.json         raw extracted records per operator
  tmp/dog_park_harvest_<date>_gaps.md      human-readable report:
                                             operator -> park -> OSM match status

Usage:
  python scripts/one_off/harvest_top10_dog_park_operators.py
  python scripts/one_off/harvest_top10_dog_park_operators.py --operator "City of San Diego"
  python scripts/one_off/harvest_top10_dog_park_operators.py --skip-geocode
  python scripts/one_off/harvest_top10_dog_park_operators.py --skip-fetch
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.llm import SONNET, call_json
from scripts.common.db import connect

HTTP_TIMEOUT = 30
# Some city sites 403 default Python user agents; use a Chrome string.
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
NOMINATIM_USER_AGENT = "dog-beach-scout-harvest/1.0 (franz@franzfunk.com)"
MAX_PAGE_TEXT = 30000
NOMINATIM_DELAY = 1.1

# ─── Top 10 operator configs ──────────────────────────────────────────

OPERATORS = [
    {"key": "san-diego-city",  "name": "City of San Diego",
     "state": "CA", "hub_url": "https://www.sandiego.gov/park-and-recreation/parks/dogs/leashfree"},
    {"key": "los-angeles",     "name": "City of Los Angeles Dept of Recreation and Parks",
     "state": "CA", "hub_url": "https://www.laparks.org/dogparks"},
    {"key": "san-francisco",   "name": "City and County of San Francisco — Rec and Parks",
     "state": "CA", "hub_url": "https://sfrecpark.org/457/Dog-Play-Areas"},
    {"key": "san-jose",        "name": "City of San Jose",
     "state": "CA", "hub_url": "https://www.sanjoseca.gov/Home/Components/FacilityDirectory/FacilityDirectory/2174/2028"},
    {"key": "davis",           "name": "City of Davis",
     "state": "CA", "hub_url": "https://www.cityofdavis.org/city-hall/parks-and-community-services/parks-and-open-space/dog-parks"},
    {"key": "sacramento",      "name": "City of Sacramento",
     "state": "CA", "hub_url": "http://www.cityofsacramento.org/parksandrec/parks/specialty-parks/dog-parks"},
    {"key": "irvine",          "name": "City of Irvine",
     "state": "CA", "hub_url": "https://cityofirvine.org/parks-facilities/central-bark-dog-park",
     "single_park": True},
    {"key": "san-diego-county","name": "County of San Diego — Parks and Recreation",
     "state": "CA", "hub_url": "https://www.sdparks.org/content/sdparks/en/AboutUs/FAQs.html"},
    {"key": "livermore-larpd", "name": "Livermore Area Recreation and Park District (LARPD)",
     "state": "CA", "hub_url": "https://www.larpd.org/dog-parks"},
    {"key": "san-ramon",       "name": "City of San Ramon",
     "state": "CA", "hub_url": "https://www.sanramon.ca.gov/our_city/departments_and_divisions/parks_community_services/parks_facilities/dog_parks/unleashed_dog_parks"},
]

# ─── LLM extraction prompt ────────────────────────────────────────────

EXTRACT_PROMPT = """\
You extract a list of DOG PARKS from an operator's web page.

A dog park is a designated off-leash facility (typically fenced) where dogs
are explicitly permitted off-leash. EXCLUDE:
  - beaches with dog rules (even if dogs allowed off-leash there)
  - trails or regular parks that just permit dogs on-leash
  - aggregate links ("All Dog Parks") or category navigation
  - generic rules pages without per-park enumeration

Output strict JSON; no prose. "parks" is an array:

{
  "parks": [
    {
      "name":                  "<exact name as listed on the page>",
      "address":               "<street + city if listed>" or null,
      "hours":                 "<as stated, e.g. 'dawn to dusk' or '6am-10pm'>" or null,
      "has_small_dog_area":    true|false|null,
      "has_water":             true|false|null,
      "has_double_gate":       true|false|null,
      "surface":               "grass" | "decomposed_granite" | "natural" | "concrete" | null,
      "has_lighting":          true|false|null,
      "amenity_notes":         "<brief free-form notes (acreage, special features)>" or null,
      "subpage_url":           "<URL of the per-park page if linked from the hub>" or null
    }
  ]
}

If the page is a single-park page (one dog park described in detail), return
that one park as a single-element list. If the page is rules-only with no
park enumeration AT ALL, return {"parks": []}. When in doubt about whether
something is a dog park, include it if the page treats it as one.
"""


# ─── HTTP helpers ─────────────────────────────────────────────────────

def fetch_html(url: str) -> tuple[str | None, str | None]:
    """Return (text_content, error). Strips scripts/style/nav."""
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return None, f"fetch_error: {e}"
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    return text[:MAX_PAGE_TEXT], None


def harvest_one(operator: dict) -> dict:
    """Fetch hub + run LLM extraction. Returns dict with parks + metadata."""
    print(f"\n  ── {operator['name']} ──")
    print(f"     {operator['hub_url']}")
    text, err = fetch_html(operator["hub_url"])
    if err:
        print(f"     FAIL: {err}")
        return {**operator, "parks": [], "error": err}
    try:
        resp = call_json(EXTRACT_PROMPT,
                         {"operator_name": operator["name"],
                          "page_url": operator["hub_url"],
                          "page_text": text,
                          "_hint": "single-park page" if operator.get("single_park") else None},
                         model=SONNET, max_tokens=8000)
        parks = resp["parsed"].get("parks", [])
        print(f"     extracted {len(parks)} parks")
        for p in parks[:5]:
            print(f"       - {p.get('name', '?')[:50]:<50}  {(p.get('address') or '—')[:40]}")
        if len(parks) > 5:
            print(f"       ... +{len(parks)-5} more")
        return {**operator, "parks": parks}
    except Exception as e:
        print(f"     LLM extract error: {e}")
        return {**operator, "parks": [], "error": str(e)}


# ─── Geocode pass ─────────────────────────────────────────────────────

def nominatim_geocode(query: str) -> tuple[float, float] | None:
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search",
                         params={"q": query, "format": "json", "limit": 1, "countrycodes": "us"},
                         headers={"User-Agent": NOMINATIM_USER_AGENT}, timeout=15)
        j = r.json()
    except Exception:
        return None
    if not j:
        return None
    return float(j[0]["lat"]), float(j[0]["lon"])


def geocode_all(harvest_data: dict) -> dict:
    print()
    print("Geocoding (Nominatim, 1 req/sec rate limit)...")
    n_ok = n_fail = 0
    for op in harvest_data["operators"]:
        for park in op["parks"]:
            if not park.get("address"):
                park["lat"] = None
                park["lng"] = None
                park["geocode_status"] = "no_address"
                continue
            q = park["address"]
            # Append operator's state if not present
            if op.get("state") and op["state"] not in q:
                q += ", " + op["state"]
            res = nominatim_geocode(q)
            if res:
                park["lat"], park["lng"] = res
                park["geocode_status"] = "ok"
                n_ok += 1
            else:
                park["lat"] = None
                park["lng"] = None
                park["geocode_status"] = "no_match"
                n_fail += 1
            time.sleep(NOMINATIM_DELAY)
    print(f"  geocoded {n_ok} OK, {n_fail} failed")
    return harvest_data


# ─── OSM comparison ───────────────────────────────────────────────────

def compare_to_osm(harvest_data: dict) -> dict:
    """For each extracted park, find best OSM match by trigram name + proximity."""
    print()
    print("Comparing to osm_dog_parks (CA-scoped)...")
    # Build flat list of (operator_key, park_name, lat, lng)
    candidates = []
    for op in harvest_data["operators"]:
        for park in op["parks"]:
            candidates.append({
                "op_key": op["key"],
                "op_name": op["name"],
                "park_name": park.get("name", "?"),
                "lat": park.get("lat"),
                "lng": park.get("lng"),
            })

    # Query OSM for each: best match by trigram + proximity
    conn = connect(application_name="harvest_compare_osm")
    matches: dict[tuple[str, str], dict] = {}
    with conn.cursor() as cur:
        for c in candidates:
            if c["lat"] is None:
                matches[(c["op_key"], c["park_name"])] = {"status": "no_geocode"}
                continue
            cur.execute("""
                SELECT odp.id, odp.osm_id, odp.name,
                       similarity(lower(%s), lower(coalesce(odp.name, ''))) AS trigram,
                       ST_Distance(
                         ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                         odp.geom::geography
                       ) AS dist_m
                FROM public.osm_dog_parks odp
                WHERE ST_DWithin(
                    ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                    odp.geom::geography, 2000)
                ORDER BY similarity(lower(%s), lower(coalesce(odp.name, ''))) DESC,
                         ST_Distance(
                           ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326)::geography,
                           odp.geom::geography
                         ) ASC
                LIMIT 1
            """, (c["park_name"], c["lng"], c["lat"], c["lng"], c["lat"],
                  c["park_name"], c["lng"], c["lat"]))
            row = cur.fetchone()
            if not row:
                matches[(c["op_key"], c["park_name"])] = {"status": "no_osm_within_2km"}
                continue
            osm_id, osm_way, osm_name, trigram, dist_m = row
            score = 0.6 * float(trigram) + 0.4 * max(0, 1 - float(dist_m) / 1000)
            status = "matched" if score >= 0.55 else "weak_match"
            matches[(c["op_key"], c["park_name"])] = {
                "status": status,
                "osm_id": osm_id,
                "osm_way": osm_way,
                "osm_name": osm_name,
                "trigram": round(float(trigram), 3),
                "dist_m": round(float(dist_m), 1),
                "score": round(score, 3),
            }

    # Stitch back into harvest data
    for op in harvest_data["operators"]:
        for park in op["parks"]:
            m = matches.get((op["key"], park.get("name", "?")), {})
            park["osm_match"] = m
    conn.close()
    return harvest_data


# ─── Report writer ────────────────────────────────────────────────────

def write_report(harvest_data: dict, md_path: Path) -> None:
    lines = ["# Top-10 CA Dog-Park Operator Harvest — OSM Comparison",
             "",
             f"Generated: {harvest_data['harvested_at']}",
             ""]
    # Summary table
    total_extracted = sum(len(op["parks"]) for op in harvest_data["operators"])
    matched = 0
    weak = 0
    no_osm = 0
    no_geo = 0
    for op in harvest_data["operators"]:
        for p in op["parks"]:
            s = (p.get("osm_match") or {}).get("status")
            if s == "matched": matched += 1
            elif s == "weak_match": weak += 1
            elif s == "no_osm_within_2km": no_osm += 1
            elif s == "no_geocode": no_geo += 1
    lines += [
        "## Summary", "",
        f"- **Total parks extracted:** {total_extracted}",
        f"- **Matched to OSM (score ≥ 0.55):** {matched}",
        f"- **Weak match (within 2km, score < 0.55):** {weak}",
        f"- **No OSM within 2km (gap candidates):** {no_osm}",
        f"- **Not geocoded:** {no_geo}",
        "",
    ]

    # Per-operator detail
    for op in harvest_data["operators"]:
        lines += [f"## {op['name']}", "", f"Hub: <{op['hub_url']}>", ""]
        if op.get("error"):
            lines += [f"_Error:_ `{op['error']}`", ""]
            continue
        if not op["parks"]:
            lines += ["_No parks extracted._", ""]
            continue
        lines += ["| Park | Address | OSM match | Score | Notes |",
                  "|---|---|---|---|---|"]
        for p in op["parks"]:
            m = p.get("osm_match") or {}
            status = m.get("status", "—")
            match_cell = ""
            if status == "matched":
                match_cell = f"✅ #{m.get('osm_id')} {(m.get('osm_name') or '')[:35]}"
            elif status == "weak_match":
                match_cell = f"⚠️ #{m.get('osm_id')} {(m.get('osm_name') or '')[:30]}"
            elif status == "no_osm_within_2km":
                match_cell = "❌ NOT IN OSM"
            elif status == "no_geocode":
                match_cell = "(no geocode)"
            score = m.get("score", "—")
            small = p.get("has_small_dog_area")
            small_str = "S/L" if small is True else "—" if small is None else ""
            lines.append(
                f"| {(p.get('name') or '?')[:40]} | "
                f"{(p.get('address') or '—')[:50]} | "
                f"{match_cell} | "
                f"{score} | "
                f"{small_str} {p.get('amenity_notes', '')[:30] if p.get('amenity_notes') else ''} |"
            )
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  wrote report: {md_path}")


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--operator", help="Run only this operator key")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="Skip the HTTP fetch + LLM extract; reuse existing JSON")
    ap.add_argument("--skip-geocode", action="store_true")
    ap.add_argument("--skip-osm", action="store_true")
    args = ap.parse_args()

    date_stamp = dt.datetime.now().strftime("%Y_%m_%d")
    json_path = ROOT / "tmp" / f"dog_park_harvest_{date_stamp}.json"
    md_path   = ROOT / "tmp" / f"dog_park_harvest_{date_stamp}_gaps.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)

    if args.skip_fetch and json_path.exists():
        harvest_data = json.loads(json_path.read_text(encoding="utf-8"))
        print(f"Reusing existing harvest: {json_path}")
    else:
        targets = OPERATORS
        if args.operator:
            targets = [op for op in OPERATORS if op["key"] == args.operator]
        print(f"Harvesting {len(targets)} operator(s)...")
        harvest_data = {
            "harvested_at": dt.datetime.now().isoformat(),
            "operators": [harvest_one(op) for op in targets],
        }
        json_path.write_text(json.dumps(harvest_data, indent=2), encoding="utf-8")
        print(f"\n  wrote raw harvest: {json_path}")

    if not args.skip_geocode:
        harvest_data = geocode_all(harvest_data)
        json_path.write_text(json.dumps(harvest_data, indent=2), encoding="utf-8")
        print(f"  updated with geocodes: {json_path}")

    if not args.skip_osm:
        harvest_data = compare_to_osm(harvest_data)
        json_path.write_text(json.dumps(harvest_data, indent=2), encoding="utf-8")
        print(f"  updated with OSM match: {json_path}")

    write_report(harvest_data, md_path)


if __name__ == "__main__":
    main()
