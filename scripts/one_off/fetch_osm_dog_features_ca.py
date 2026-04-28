"""
fetch_osm_dog_features_ca.py
----------------------------
Pull five classes of OSM features in California and upsert into
public.osm_features:

  1. leisure=dog_park                          (dedicated off-leash zones)
  2. leisure=park   with dog=yes|leashed|unleashed
  3. natural=beach  with dog=yes|leashed|unleashed
  4. leisure=park   (ALL CA parks; dog policy unknown)
  5. natural=beach  (ALL CA beaches; dog policy unknown)

Saves raw JSON for each class to share/ for inspection. Sleeps between
Overpass calls so we don't trigger 429 rate-limits.

Usage:
  python scripts/one_off/fetch_osm_dog_features_ca.py [--dry-run]
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

# Each class: (feature_type, label, Overpass QL, output filename).
QUERIES = [
    (
        "dog_park",
        "leisure=dog_park (dedicated off-leash)",
        """
[out:json][timeout:120];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["leisure"="dog_park"](area.ca);
  way["leisure"="dog_park"](area.ca);
  relation["leisure"="dog_park"](area.ca);
);
out center tags;
""",
        "osm_dog_parks_ca.json",
    ),
    (
        "dog_friendly_park",
        "leisure=park with dog=yes/leashed/unleashed",
        """
[out:json][timeout:120];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
  way["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
  relation["leisure"="park"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
);
out center tags;
""",
        "osm_dog_friendly_parks_ca.json",
    ),
    (
        "dog_friendly_beach",
        "natural=beach with dog=yes/leashed/unleashed",
        """
[out:json][timeout:120];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
  way["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
  relation["natural"="beach"]["dog"~"^(yes|leashed|unleashed)$"](area.ca);
);
out center tags;
""",
        "osm_dog_friendly_beaches_ca.json",
    ),
    (
        "park",
        "leisure=park (ALL CA parks; dog policy unknown)",
        """
[out:json][timeout:240];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["leisure"="park"](area.ca);
  way["leisure"="park"](area.ca);
  relation["leisure"="park"](area.ca);
);
out center tags;
""",
        "osm_parks_ca.json",
    ),
    (
        "beach",
        "natural=beach (ALL CA beaches; dog policy unknown)",
        """
[out:json][timeout:180];
area["ISO3166-2"="US-CA"]->.ca;
(
  node["natural"="beach"](area.ca);
  way["natural"="beach"](area.ca);
  relation["natural"="beach"](area.ca);
);
out center tags;
""",
        "osm_beaches_ca.json",
    ),
]

OVERPASS_DELAY_S = 30  # Overpass enforces strict per-IP rate limits and
                       # the all-parks query (~17k rows) is heavy — be
                       # generous between calls.


def overpass(query: str) -> dict:
    r = httpx.post(OVERPASS_URL, data={"data": query}, timeout=180.0,
                   headers={"User-Agent": "dog-beach-scout/1.0 (one-off OSM ingest)"})
    r.raise_for_status()
    return r.json()


def derive_dog_status(feature_type: str, tags: dict) -> str | None:
    """leisure=dog_park is implicitly off-leash. Otherwise read tags.dog."""
    raw = (tags.get("dog") or "").strip().lower() or None
    if feature_type == "dog_park":
        return raw if raw in ("yes", "leashed", "unleashed") else "unleashed"
    return raw if raw in ("yes", "leashed", "unleashed") else None


def to_row(el: dict, feature_type: str) -> dict | None:
    osm_type = el.get("type")
    osm_id   = el.get("id")
    if not osm_type or osm_id is None:
        return None

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
        "feature_type":  feature_type,
        "dog_status":    derive_dog_status(feature_type, tags),
        "name":          (tags.get("name") or None),
        "latitude":      lat,
        "longitude":     lon,
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
    url = f"{SUPABASE_URL}/rest/v1/osm_features"
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
    # Flush stdout per print so we can watch progress on long runs.
    import functools
    print_orig = print
    globals()["print"] = functools.partial(print_orig, flush=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip the DB upsert. Still saves JSON + prints summary.")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Queries run in priority order — most-specific first. An element
    # that already appeared in an earlier (more-specific) query is
    # skipped from the broader queries so its feature_type isn't
    # overwritten. Same idea as Tier 1 ranking elsewhere in the project.
    seen: set[tuple[str, int]] = set()
    all_rows: list[dict] = []
    for idx, (feature_type, label, query, outname) in enumerate(QUERIES):
        if idx > 0:
            print(f"\n  …sleeping {OVERPASS_DELAY_S}s before next Overpass call")
            time.sleep(OVERPASS_DELAY_S)
        print(f"\nFetching: {label}")
        t0 = time.monotonic()
        try:
            payload = overpass(query)
        except httpx.HTTPError as e:
            print(f"  ERR Overpass: {e}", file=sys.stderr)
            return 1
        elapsed = time.monotonic() - t0
        elements = payload.get("elements", [])
        (OUTPUT_DIR / outname).write_text(json.dumps(payload, indent=2), encoding="utf-8")

        named = sum(1 for e in elements if (e.get("tags") or {}).get("name"))
        rows = []
        skipped_dup = 0
        for e in elements:
            r = to_row(e, feature_type)
            if r is None:
                continue
            key = (r["osm_type"], r["osm_id"])
            if key in seen:
                skipped_dup += 1
                continue
            seen.add(key)
            rows.append(r)

        print(f"  {len(elements)} elements in {elapsed:.1f}s "
              f"({named} named, {len(rows)} new, {skipped_dup} dup-skipped)")
        print(f"  saved {OUTPUT_DIR / outname}")
        all_rows.extend(rows)

    print(f"\nTotal mappable rows: {len(all_rows)}")
    by_type = {}
    by_status = {}
    for r in all_rows:
        by_type[r["feature_type"]]     = by_type.get(r["feature_type"], 0) + 1
        by_status[r["dog_status"]]     = by_status.get(r["dog_status"], 0) + 1
    for k, v in sorted(by_type.items(),  key=lambda x: -x[1]): print(f"  {k:20s} {v}")
    for k, v in sorted(by_status.items(), key=lambda x: -x[1]): print(f"  status {str(k):14s} {v}")

    if args.dry_run:
        print("\n[dry-run] skipping upsert.")
        return 0

    print(f"\nUpserting into public.osm_features…")
    BATCH = 200
    written = 0
    errors  = 0
    for i in range(0, len(all_rows), BATCH):
        batch = all_rows[i:i + BATCH]
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
