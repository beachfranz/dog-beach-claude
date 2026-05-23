"""
fetch_osm_dog_parks_ca.py
-------------------------
Pull every leisure=dog_park (node + way + relation) in California from
the OpenStreetMap Overpass API and upsert into public.osm_dog_parks.
Saves raw JSON to share/ alongside.

Overpass: https://overpass-api.de/api/interpreter
Tag scope: leisure=dog_park (the dedicated dog-park tag).
California scope: area["ISO3166-2"="US-CA"]

Usage:
  python scripts/one_off/fetch_osm_dog_parks_ca.py [--dry-run]
"""

from __future__ import annotations
import argparse, json, os, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUTPUT_DIR   = Path(__file__).parent.parent.parent / "share"
OUTPUT_FILE  = OUTPUT_DIR / "osm_dog_parks_ca.json"

QUERY = """
[out:json][timeout:120];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["leisure"="dog_park"](area.ca);
  way["leisure"="dog_park"](area.ca);
  relation["leisure"="dog_park"](area.ca);
);
out center tags;
"""

def to_row(el: dict) -> dict | None:
    """Map a single Overpass element into the osm_dog_parks row shape.
    Returns None for elements lacking a usable center point."""
    osm_type = el.get("type")
    osm_id   = el.get("id")
    if not osm_type or osm_id is None:
        return None

    # Nodes carry lat/lon directly; ways/relations carry center.{lat,lon}
    lat = el.get("lat")
    lon = el.get("lon")
    if lat is None or lon is None:
        c = el.get("center") or {}
        lat, lon = c.get("lat"), c.get("lon")
    if lat is None or lon is None:
        return None

    tags = el.get("tags") or {}
    return {
        "osm_type":      osm_type,
        "osm_id":        osm_id,
        "name":          (tags.get("name") or None),
        "latitude":      lat,
        "longitude":     lon,
        # PostGIS POINT in 4326. PostgREST accepts EWKT for geometry cols.
        "geom":          f"SRID=4326;POINT({lon} {lat})",
        "tags":          tags,
        "fee":           tags.get("fee"),
        "fence":         tags.get("fence"),
        "surface":       tags.get("surface"),
        "opening_hours": tags.get("opening_hours"),
        "website":       tags.get("website") or tags.get("contact:website"),
        "city":          tags.get("addr:city"),
    }


def upsert_batch(rows: list[dict]) -> tuple[int, str | None]:
    """Bulk upsert via PostgREST. Returns (count, error_message_or_None)."""
    url = f"{SUPABASE_URL}/rest/v1/osm_dog_parks"
    headers = {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }
    try:
        r = httpx.post(url, headers=headers, json=rows, timeout=120.0)
        if not r.is_success:
            return 0, f"HTTP {r.status_code}: {r.text[:300]}"
        return len(rows), None
    except httpx.HTTPError as e:
        return 0, str(e)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip the DB upsert. Still saves JSON + prints summary.")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Querying Overpass for leisure=dog_park in California…")
    t0 = time.monotonic()
    try:
        r = httpx.post(OVERPASS_URL, data={"data": QUERY},
                       timeout=180.0,
                       headers={"User-Agent": "dog-beach-scout/1.0 (one-off OSM ingest)"})
        r.raise_for_status()
    except httpx.HTTPError as e:
        print(f"ERR Overpass: {e}", file=sys.stderr)
        return 1
    elapsed = time.monotonic() - t0

    payload = r.json()
    elements = payload.get("elements", [])
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    by_type = {}
    named   = 0
    no_name = 0
    for el in elements:
        by_type[el.get("type", "?")] = by_type.get(el.get("type", "?"), 0) + 1
        name = (el.get("tags") or {}).get("name")
        if name and name.strip(): named += 1
        else: no_name += 1

    print(f"Fetched {len(elements)} elements in {elapsed:.1f}s")
    for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {t:<10s} {n}")
    print(f"  named:    {named}")
    print(f"  no_name:  {no_name}")
    print(f"Saved raw JSON: {OUTPUT_FILE}")

    print("\nFirst 8 named samples:")
    shown = 0
    for el in elements:
        tags = el.get("tags") or {}
        name = tags.get("name")
        if not name: continue
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        addr = " · ".join(filter(None, [
            tags.get("addr:city"),
            tags.get("addr:state"),
        ])) or "-"
        print(f"  {el['type']:<8s} {el['id']:>11d}  {name[:50]:50s}  ({lat:.4f},{lon:.4f})  {addr}")
        shown += 1
        if shown >= 8: break

    # ── Upsert into public.osm_dog_parks ────────────────────────────
    if args.dry_run:
        print("\n[dry-run] skipping upsert.")
        return 0

    rows = [r for r in (to_row(e) for e in elements) if r is not None]
    skipped = len(elements) - len(rows)
    print(f"\nUpserting {len(rows)} rows into public.osm_dog_parks "
          f"({skipped} skipped — missing center).")

    BATCH = 200
    written = 0
    errors  = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        n, err = upsert_batch(batch)
        if err:
            errors += 1
            print(f"  batch {i:>4d}–{i + len(batch):>4d}  ERR  {err[:200]}", file=sys.stderr)
        else:
            written += n
            print(f"  batch {i:>4d}–{i + len(batch):>4d}  ok   ({n} rows)")

    print(f"\nUpsert complete: {written} rows written, {errors} batch error(s).")
    return 0 if errors == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
