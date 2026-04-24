"""
Seed the jurisdictions table from US Census data + Google Geocoding.

Strategy:
  1. Fetch all CA places from Census API (name, FIPS, incorporated/CDP status)
  2. For each unique city value in beaches_staging, resolve via:
       a. Exact Census name match
       b. Google Geocoding of the city string (for county)
       c. If still unresolved: geocode ZIPs associated with that city value in beaches_staging
  3. Upsert into jurisdictions table

Usage:
  python seed_jurisdictions.py --dry-run    # print rows, no DB writes
  python seed_jurisdictions.py              # seed the table
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict

import httpx
from dotenv import load_dotenv
import os

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GOOGLE_KEY   = os.environ["GOOGLE_MAPS_API_KEY"]

GEOCODE_URL  = "https://maps.googleapis.com/maps/api/geocode/json"
CENSUS_PLACES_URL = "https://api.census.gov/data/2020/dec/pl?get=NAME,GEO_ID&for=place:*&in=state:06"


def fetch_ca_places() -> dict[str, dict]:
    """Fetch all CA places from Census API. Returns {lower_name: {name, fips, place_type}}"""
    print("  Fetching CA places from Census API...")
    r = httpx.get(CENSUS_PLACES_URL, timeout=30)
    r.raise_for_status()
    data = r.json()

    places = {}
    for row in data[1:]:  # skip header
        name_raw, geo_id, state_fips, place_fips = row
        name = name_raw.replace(", California", "").strip()
        is_cdp = "CDP" in name
        for suffix in [" CDP", " city", " town", " village", " borough"]:
            name = name.replace(suffix, "")
        name = name.strip()
        place_type = "cdp" if is_cdp else "incorporated"
        places[name.lower()] = {
            "name":        name,
            "fips_place":  place_fips,
            "fips_state":  "06",
            "place_type":  place_type,
        }

    inc = sum(1 for p in places.values() if p["place_type"] == "incorporated")
    cdp = sum(1 for p in places.values() if p["place_type"] == "cdp")
    print(f"  {len(places)} CA places ({inc} incorporated, {cdp} CDPs)")
    return places


def fetch_unique_cities_with_zips() -> dict[str, list[str]]:
    """Fetch distinct city values from beaches_staging with associated ZIPs."""
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/beaches_staging",
        headers=headers, params="select=city,zip&state=eq.California&limit=2000")
    r.raise_for_status()

    city_zips: dict[str, set[str]] = defaultdict(set)
    for row in r.json():
        city = row.get("city", "") or ""
        zip_ = row.get("zip", "")  or ""
        if city:
            if zip_:
                city_zips[city].add(zip_.strip()[:5])  # normalize to 5-digit
            else:
                city_zips[city]  # ensure key exists

    result = {city: sorted(zips) for city, zips in city_zips.items()}
    print(f"  {len(result)} unique city values in beaches_staging")
    return result


def google_geocode(query: str) -> dict | None:
    """Geocode a query string -> {canonical_city, county, is_incorporated}."""
    r = httpx.get(GEOCODE_URL, params={
        "address": query,
        "key": GOOGLE_KEY,
        "components": "administrative_area:CA|country:US",
    })
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None

    result = data["results"][0]
    components = {
        c["types"][0]: c["long_name"]
        for c in result["address_components"] if c.get("types")
    }

    locality    = components.get("locality")
    sublocality = components.get("sublocality") or components.get("sublocality_level_1")
    county      = components.get("administrative_area_level_2", "").replace(" County", "").strip()

    canonical_city  = locality or sublocality
    is_incorporated = bool(locality)

    return {
        "canonical_city":  canonical_city,
        "county":          county,
        "is_incorporated": is_incorporated,
    }


def google_geocode_zip(zip_code: str) -> dict | None:
    """Geocode a ZIP code -> {canonical_city, county, is_incorporated}."""
    return google_geocode(f"{zip_code}, California")


def upsert_jurisdictions(rows: list[dict], dry_run: bool) -> None:
    if dry_run:
        print()
        for r in rows[:25]:
            print(f"  {r['name']:<35} {r['place_type']:<14} {r['county']}")
        if len(rows) > 25:
            print(f"  ... ({len(rows)} total)")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        r = httpx.post(f"{SUPABASE_URL}/rest/v1/jurisdictions",
            headers=headers, json=batch, timeout=30)
        if r.status_code not in (200, 201):
            print(f"  ERROR {r.status_code}: {r.text[:300]}")
        else:
            print(f"  Upserted rows {i+1}-{i+len(batch)}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    print("Fetching data...")
    census_places = fetch_ca_places()
    city_zips     = fetch_unique_cities_with_zips()
    unique_cities = sorted(city_zips.keys())

    print("\nResolving cities...")
    rows = []
    unresolved = []
    zip_resolved = []

    for i, city in enumerate(unique_cities):
        # Step 1: exact Census match (no fuzzy — prevents wrong corrections)
        census = census_places.get(city.lower().strip())

        # Step 2: Google geocode the city string for county
        geo = google_geocode(f"{city}, California")
        time.sleep(0.05)

        if geo and geo.get("county"):
            # We have a county — build the row
            canonical  = census["name"] if census else (geo["canonical_city"] or city)
            place_type = (census["place_type"] if census
                          else ("incorporated" if geo["is_incorporated"] else "cdp"))
            fips_place = census["fips_place"] if census else ""

            rows.append({
                "name":        canonical,
                "place_type":  place_type,
                "county":      geo["county"],
                "state":       "California",
                "fips_state":  "06",
                "fips_place":  fips_place,
                "fips_county": "",
                "_source":     "city_geocode",
            })
            continue

        # Step 3: ZIP-based disambiguation for unresolved cities
        zips = city_zips.get(city, [])
        resolved_via_zip = False
        for zip_code in zips[:3]:  # try up to 3 ZIPs per city
            zip_geo = google_geocode_zip(zip_code)
            time.sleep(0.05)
            if not zip_geo or not zip_geo.get("county"):
                continue

            zip_city = zip_geo["canonical_city"] or city
            # Validate the ZIP resolves to a known Census place
            zip_census = census_places.get(zip_city.lower().strip())

            canonical  = zip_census["name"] if zip_census else zip_city
            place_type = (zip_census["place_type"] if zip_census
                          else ("incorporated" if zip_geo["is_incorporated"] else "cdp"))
            fips_place = zip_census["fips_place"] if zip_census else ""

            rows.append({
                "name":        canonical,
                "place_type":  place_type,
                "county":      zip_geo["county"],
                "state":       "California",
                "fips_state":  "06",
                "fips_place":  fips_place,
                "fips_county": "",
                "_source":     f"zip:{zip_code}",
            })
            zip_resolved.append(f"{city!r} -> {canonical!r} via ZIP {zip_code}")
            resolved_via_zip = True
            break

        if not resolved_via_zip:
            unresolved.append(city)

    # Deduplicate by canonical name (last write wins — prefer city_geocode over zip)
    seen: dict[str, dict] = {}
    for r in rows:
        seen[r["name"].lower()] = r
    rows = list(seen.values())

    # Strip internal _source field before upsert
    for r in rows:
        r.pop("_source", None)

    print(f"\n{len(rows)} unique jurisdictions resolved")
    if zip_resolved:
        print(f"\n{len(zip_resolved)} resolved via ZIP:")
        for msg in zip_resolved:
            print(f"  {msg}")
    if unresolved:
        print(f"\n{len(unresolved)} unresolved: {unresolved}")

    tag = "[dry-run] " if args.dry_run else ""
    print(f"\n{tag}Upserting to Supabase...")
    upsert_jurisdictions(rows, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\nDone. {len(rows)} jurisdictions seeded.")


if __name__ == "__main__":
    main()
