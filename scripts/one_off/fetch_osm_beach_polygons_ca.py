"""
fetch_osm_beach_polygons_ca.py
------------------------------
Refetch CA natural=beach features from Overpass with `out geom`.

Updated 2026-04-29: writes to public.osm_landing instead of UPDATE-ing
public.osm_features directly. The promote_osm_features_from_landing()
function then merges into osm_features, preserving downstream
enrichment (operator_id, admin_inactive, cleaning_status). One Overpass
fetch -> N landing rows -> 1 promote call -> osm_features.

Usage:
  python scripts/one_off/fetch_osm_beach_polygons_ca.py
"""

from __future__ import annotations
import json, os, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

# Public Overpass main server times out on this query (504). The kumi
# mirror is friendlier for heavy `out geom` workloads.
OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"
OUTPUT_DIR   = Path(__file__).parent.parent.parent / "share"
OUTPUT_FILE  = OUTPUT_DIR / "osm_beach_polygons_ca.json"

# Chunked by 4 CA quadrants — single-shot queries 504 even on the kumi
# mirror because `out geom` on 1.5k beach polygons is a big payload.
# Quadrant queries each return a few hundred features; safe.
# SE was originally one quadrant; split further (LA+OC+SD area is too
# dense for a single Overpass response, even with the kumi mirror).
CA_QUADRANTS = [
    ("NW",   37.0, 42.5, -125.0, -120.0),
    ("NE",   37.0, 42.5, -120.0, -114.0),
    ("SW",   32.0, 37.0, -125.0, -120.0),
    ("SE-N", 34.5, 37.0, -120.0, -114.0),  # mid coast
    ("SE-S", 32.0, 34.5, -120.0, -114.0),  # LA/OC/SD (densest)
]
QUERY_TPL = """
[out:json][timeout:180];
(
  way["natural"="beach"]({s},{w},{n},{e});
  relation["natural"="beach"]({s},{w},{n},{e});
);
out geom;
"""

QUADRANT_DELAY_S = 8


def overpass(query: str, attempts: int = 3) -> dict:
    """POST to Overpass with retry on 5xx + 429. Backs off 15s, 30s,
    60s between attempts."""
    last_err = None
    for i in range(attempts):
        if i > 0:
            wait = 15 * (2 ** (i - 1))
            print(f"    retry {i+1}/{attempts} after {wait}s…")
            time.sleep(wait)
        try:
            r = httpx.post(OVERPASS_URL, data={"data": query}, timeout=300.0,
                           headers={"User-Agent": "dog-beach-scout/1.0 (one-off OSM polygon ingest)"})
            if r.status_code in (429, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            last_err = str(e)
    raise RuntimeError(f"Overpass failed after {attempts} attempts: {last_err}")


def way_to_wkt(way: dict) -> str | None:
    """OSM way with `out geom` carries a 'geometry' array of {lat,lon}.
    For a beach polygon the way is closed (first==last). Build a
    POLYGON WKT. SRID=4326 is added at the SQL ingest layer."""
    geom = way.get("geometry") or []
    if len(geom) < 4: return None
    pts = [f"{p['lon']} {p['lat']}" for p in geom]
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return f"SRID=4326;POLYGON(({','.join(pts)}))"


def relation_to_wkt(rel: dict) -> str | None:
    """OSM multipolygon relation with `out geom` carries a 'members'
    list of ways with role outer/inner. We assemble all 'outer' rings
    into a MULTIPOLYGON. Inner rings (holes) are ignored — we only
    need the rough shape, not topological precision."""
    rings = []
    for m in (rel.get("members") or []):
        if m.get("type") != "way" or m.get("role") != "outer": continue
        coords = m.get("geometry") or []
        if len(coords) < 4: continue
        pts = [f"{p['lon']} {p['lat']}" for p in coords]
        if pts[0] != pts[-1]:
            pts.append(pts[0])
        rings.append(f"(({','.join(pts)}))")
    if not rings: return None
    return f"SRID=4326;MULTIPOLYGON({','.join(rings)})"


FETCHED_BY = "fetch_osm_beach_polygons_ca"


def land_row(el: dict, geom_full_wkt: str | None) -> bool:
    """INSERT one Overpass element into public.osm_landing.
    osm_landing has primary key (type, id, fetched_at) so re-fetches
    accumulate rows; promote function picks the latest per (type, id)."""
    type_ = el.get("type")
    id_   = el.get("id")
    if type_ not in ("node", "way", "relation") or id_ is None:
        return False
    body = {
        "type":       type_,
        "id":         id_,
        "fetched_by": FETCHED_BY,
        "tags":       el.get("tags") or {},
    }
    if type_ == "node":
        body["lat"] = el.get("lat")
        body["lon"] = el.get("lon")
    elif type_ == "way":
        body["geometry"] = el.get("geometry")
        if geom_full_wkt:
            body["geom_full"] = geom_full_wkt
    elif type_ == "relation":
        # Relations carry a `members` array (each with role/ref/type/geometry)
        # — store it in the dedicated members column. Leave geometry null.
        body["members"] = el.get("members")
        if geom_full_wkt:
            body["geom_full"] = geom_full_wkt
    url = f"{SUPABASE_URL}/rest/v1/osm_landing"
    headers = {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal,resolution=ignore-duplicates",
    }
    r = httpx.post(url, headers=headers, json=body, timeout=30.0)
    return r.is_success


def call_promote() -> dict:
    """Invoke public.promote_osm_features_from_landing() via PostgREST RPC."""
    url = f"{SUPABASE_URL}/rest/v1/rpc/promote_osm_features_from_landing"
    headers = {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
    }
    r = httpx.post(url, headers=headers, json={}, timeout=120.0)
    r.raise_for_status()
    return r.json()


def main() -> int:
    import functools
    globals()["print"] = functools.partial(print, flush=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching natural=beach in CA with out geom — chunked into 4 quadrants…")
    all_elements: list[dict] = []
    seen: set[tuple[str, int]] = set()
    for idx, (label, s, n, w, e) in enumerate(CA_QUADRANTS):
        if idx > 0:
            print(f"  …sleeping {QUADRANT_DELAY_S}s before next quadrant")
            time.sleep(QUADRANT_DELAY_S)
        q = QUERY_TPL.format(s=s, n=n, w=w, e=e)
        print(f"\nQuadrant {label} ({s},{w},{n},{e}):")
        t0 = time.monotonic()
        try:
            payload = overpass(q)
        except Exception as err:
            print(f"  ERR (skipping quadrant {label}): {err}", file=sys.stderr)
            continue
        elements = payload.get("elements", [])
        elapsed = time.monotonic() - t0
        new = 0
        for el in elements:
            key = (el.get("type"), el.get("id"))
            if key in seen: continue
            seen.add(key); all_elements.append(el); new += 1
        print(f"  {len(elements)} elements in {elapsed:.1f}s ({new} new)")

    OUTPUT_FILE.write_text(json.dumps({"elements": all_elements}, indent=2), encoding="utf-8")
    print(f"\n  total {len(all_elements)} unique elements; raw saved to {OUTPUT_FILE}")

    print("\nWriting to public.osm_landing…")
    landed = 0; failed = 0
    for e in all_elements:
        t = e.get("type")
        if t == "way":
            wkt = way_to_wkt(e)
        elif t == "relation":
            wkt = relation_to_wkt(e)
        else:
            wkt = None
        if land_row(e, wkt):
            landed += 1
        else:
            failed += 1
        if landed % 200 == 0 and landed > 0:
            print(f"  {landed} landed…")

    print(f"\nLanded: {landed}, failed: {failed}")
    if landed > 0:
        print("\nPromoting to public.osm_features…")
        result = call_promote()
        print(f"  inserted: {result.get('inserted', 0)}, updated: {result.get('updated', 0)}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
