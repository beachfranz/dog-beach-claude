"""
fetch_osm_coastline_oc.py
-------------------------
Fetch OSM natural=coastline ways inside Orange County's bbox with full
LineString geometry. Inserts into osm_features with feature_type =
'coastline'. Used to draw the actual waterline on the admin map for
sanity-checking how well our beach polygons follow the coast.

Usage:
  python scripts/one_off/fetch_osm_coastline_oc.py
"""

from __future__ import annotations
import json, os, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"
OUTPUT_DIR   = Path(__file__).parent.parent.parent / "share"
OUTPUT_FILE  = OUTPUT_DIR / "osm_coastline_oc.json"

# Orange County bbox from counties table, padded slightly.
S, W, N, E = 33.30, -118.20, 33.95, -117.40

QUERY = f"""
[out:json][timeout:120];
(
  way["natural"="coastline"]({S},{W},{N},{E});
);
out geom;
"""


def overpass(q: str, attempts: int = 3) -> dict:
    """Coastlines are heavy — retry across kumi + main mirrors with backoff."""
    last_err = None
    mirrors = [
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://overpass.openstreetmap.fr/api/interpreter",
    ]
    for i in range(attempts):
        if i > 0:
            wait = 15 * (2 ** (i - 1))
            print(f"    retry {i+1}/{attempts} after {wait}s…")
            time.sleep(wait)
        url = mirrors[i % len(mirrors)]
        print(f"    using {url}")
        try:
            r = httpx.post(url, data={"data": q}, timeout=240.0,
                           headers={"User-Agent": "dog-beach-scout/1.0 (coastline ingest)"})
            if r.status_code in (429, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"; continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            last_err = str(e)
    raise RuntimeError(f"All Overpass mirrors failed: {last_err}")


def way_to_linestring_wkt(way: dict) -> str | None:
    geom = way.get("geometry") or []
    if len(geom) < 2: return None
    pts = [f"{p['lon']} {p['lat']}" for p in geom]
    return f"SRID=4326;LINESTRING({','.join(pts)})"


def upsert_row(row: dict) -> tuple[bool, str]:
    url = f"{SUPABASE_URL}/rest/v1/osm_features"
    headers = {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }
    r = httpx.post(url, headers=headers, json=row, timeout=30.0)
    return r.is_success, "" if r.is_success else f"HTTP {r.status_code}: {r.text[:200]}"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Fetching natural=coastline in OC bbox ({S},{W},{N},{E})…")
    t0 = time.monotonic()
    payload = overpass(QUERY)
    elapsed = time.monotonic() - t0
    elements = payload.get("elements", [])
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  {len(elements)} ways in {elapsed:.1f}s; saved {OUTPUT_FILE}")

    rows = []
    for el in elements:
        if el.get("type") != "way": continue
        wkt = way_to_linestring_wkt(el)
        if not wkt: continue
        # Use first vertex as the centroid Point geom (a pragmatic stand-in).
        first = (el.get("geometry") or [{}])[0]
        lat = first.get("lat"); lon = first.get("lon")
        if lat is None or lon is None: continue
        tags = el.get("tags") or {}
        rows.append({
            "osm_type":     "way",
            "osm_id":       el["id"],
            "feature_type": "coastline",
            "name":         tags.get("name") or None,
            "latitude":     lat,
            "longitude":    lon,
            "geom":         f"SRID=4326;POINT({lon} {lat})",
            "geom_full":    wkt,
            "tags":         tags,
        })

    if not rows:
        print("Nothing to upsert.")
        return 0

    print(f"\nUpserting {len(rows)} coastline ways…")
    ok = err = 0
    for r in rows:
        success, msg = upsert_row(r)
        if success: ok += 1
        else: err += 1; print(f"  err {r['osm_type']}/{r['osm_id']}: {msg}", file=sys.stderr)
    print(f"  {ok} ok, {err} errors.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
