"""
fetch_osm_sand_shingle_laguna.py
--------------------------------
One-off test: fetch OSM natural=sand and natural=shingle features
inside the Laguna Beach city bbox with full polygon geometry, upsert
into public.osm_features. Bounded to the Laguna bbox to keep this a
focused experiment.

Usage:
  python scripts/one_off/fetch_osm_sand_shingle_laguna.py
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
OUTPUT_FILE  = OUTPUT_DIR / "osm_sand_shingle_laguna.json"

# Laguna Beach city bbox (from jurisdictions, padded a tad)
S, W, N, E = 33.48, -117.83, 33.62, -117.72

QUERY = f"""
[out:json][timeout:120];
(
  way["natural"="sand"]({S},{W},{N},{E});
  relation["natural"="sand"]({S},{W},{N},{E});
  way["natural"="shingle"]({S},{W},{N},{E});
  relation["natural"="shingle"]({S},{W},{N},{E});
);
out geom;
"""


def overpass(q: str) -> dict:
    r = httpx.post(OVERPASS_URL, data={"data": q}, timeout=180.0,
                   headers={"User-Agent": "dog-beach-scout/1.0 (sand/shingle ingest)"})
    r.raise_for_status()
    return r.json()


def way_to_wkt(way: dict) -> str | None:
    geom = way.get("geometry") or []
    if len(geom) < 4: return None
    pts = [f"{p['lon']} {p['lat']}" for p in geom]
    if pts[0] != pts[-1]: pts.append(pts[0])
    return f"SRID=4326;POLYGON(({','.join(pts)}))"


def relation_to_wkt(rel: dict) -> str | None:
    rings = []
    for m in (rel.get("members") or []):
        if m.get("type") != "way" or m.get("role") != "outer": continue
        coords = m.get("geometry") or []
        if len(coords) < 4: continue
        pts = [f"{p['lon']} {p['lat']}" for p in coords]
        if pts[0] != pts[-1]: pts.append(pts[0])
        rings.append(f"(({','.join(pts)}))")
    if not rings: return None
    return f"SRID=4326;MULTIPOLYGON({','.join(rings)})"


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


def el_to_row(el: dict) -> dict | None:
    osm_type = el.get("type")
    osm_id   = el.get("id")
    if not osm_type or osm_id is None: return None

    tags = el.get("tags") or {}
    natural = tags.get("natural") or ""
    if natural not in ("sand","shingle"): return None
    feature_type = natural  # 'sand' or 'shingle'

    if osm_type == "way":
        wkt_full = way_to_wkt(el)
    elif osm_type == "relation":
        wkt_full = relation_to_wkt(el)
    else:
        wkt_full = None
    if not wkt_full: return None

    # Compute a centroid for the centroid Point geom we store everywhere.
    geom_pts = el.get("geometry") or []
    if not geom_pts and osm_type == "relation":
        # collect from outer members
        for m in (el.get("members") or []):
            if m.get("type") == "way" and m.get("role") == "outer":
                geom_pts.extend(m.get("geometry") or [])
    if not geom_pts: return None
    avg_lat = sum(p["lat"] for p in geom_pts) / len(geom_pts)
    avg_lon = sum(p["lon"] for p in geom_pts) / len(geom_pts)

    return {
        "osm_type":     osm_type,
        "osm_id":       osm_id,
        "feature_type": feature_type,
        "name":         tags.get("name") or None,
        "latitude":     avg_lat,
        "longitude":    avg_lon,
        "geom":         f"SRID=4326;POINT({avg_lon} {avg_lat})",
        "geom_full":    wkt_full,
        "tags":         tags,
        "fee":          tags.get("fee"),
        "fence":        tags.get("fence"),
        "surface":      tags.get("surface"),
        "opening_hours":tags.get("opening_hours"),
        "website":      tags.get("website") or tags.get("contact:website"),
        "city":         tags.get("addr:city"),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Fetching natural=sand|shingle in Laguna Beach bbox ({S},{W},{N},{E})…")
    t0 = time.monotonic()
    payload = overpass(QUERY)
    elapsed = time.monotonic() - t0
    elements = payload.get("elements", [])
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  {len(elements)} elements in {elapsed:.1f}s; saved {OUTPUT_FILE}")

    by_natural = {}
    rows = []
    for el in elements:
        r = el_to_row(el)
        if not r: continue
        rows.append(r)
        by_natural[r["feature_type"]] = by_natural.get(r["feature_type"], 0) + 1
    for k, v in sorted(by_natural.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    if not rows:
        print("Nothing to upsert.")
        return 0

    print(f"\nUpserting {len(rows)} rows…")
    ok = err = 0
    for r in rows:
        success, msg = upsert_row(r)
        if success: ok += 1
        else: err += 1; print(f"  err {r['osm_type']}/{r['osm_id']}: {msg}", file=sys.stderr)
    print(f"  {ok} ok, {err} errors.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
