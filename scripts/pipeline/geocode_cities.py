"""
Reverse-geocode every beach in beaches_staging to canonical city/municipality.

Uses Google Maps Geocoding API to resolve lat/lon → incorporated city name.
For unincorporated areas, falls back to sublocality, then county.

Outputs a CSV report of mismatches and optionally patches the DB.

Usage:
  python geocode_cities.py --report            # audit only, no DB writes
  python geocode_cities.py --apply             # write corrections to DB
  python geocode_cities.py --report --state California
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

GOOGLE_KEY   = os.environ["GOOGLE_MAPS_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
RATE_LIMIT_DELAY = 0.05  # 20 req/sec — well within Google's standard quota


@dataclass
class Beach:
    id: int
    display_name: str
    city: str
    county: str
    state: str
    latitude: float
    longitude: float


@dataclass
class GeoResult:
    canonical_city: Optional[str]        # most specific name: sublocality if present, else locality
    canonical_municipality: Optional[str] # always the incorporated city (locality)
    canonical_county: Optional[str]
    canonical_state: Optional[str]
    is_incorporated: bool                # True if within city limits
    place_types: list[str]              # raw Google types for reference


def fetch_beaches(state: Optional[str] = None) -> list[Beach]:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    params = "select=id,display_name,city,county,state,latitude,longitude&order=id&limit=1000"
    if state:
        from urllib.parse import quote
        params += f"&state=eq.{quote(state)}"

    r = httpx.get(f"{SUPABASE_URL}/rest/v1/beaches_staging", headers=headers, params=params)
    r.raise_for_status()
    return [
        Beach(
            id=row["id"],
            display_name=row.get("display_name") or "",
            city=row.get("city") or "",
            county=row.get("county") or "",
            state=row.get("state") or "",
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
        )
        for row in r.json()
        if row.get("latitude") and row.get("longitude")
    ]


def reverse_geocode(lat: float, lon: float) -> GeoResult:
    r = httpx.get(GEOCODE_URL, params={
        "latlng": f"{lat},{lon}",
        "key": GOOGLE_KEY,
        "result_type": "locality|sublocality|administrative_area_level_2",
    })
    r.raise_for_status()
    data = r.json()

    if data.get("status") != "OK" or not data.get("results"):
        return GeoResult(None, None, None, False, [])

    # Walk all results to extract the most specific usable components
    canonical_city = None
    canonical_locality = None  # always the incorporated city name
    canonical_county = None
    canonical_state = None
    is_incorporated = False
    all_types: list[str] = []

    for result in data["results"]:
        all_types.extend(result.get("types", []))
        components = {
            c["types"][0]: c["long_name"]
            for c in result.get("address_components", [])
            if c.get("types")
        }

        if not canonical_locality and "locality" in components:
            canonical_locality = components["locality"]

        if not canonical_city:
            # Prefer locality (incorporated city) over sublocality (neighborhood)
            if "locality" in components:
                canonical_city = components["locality"]
                is_incorporated = True
            elif "sublocality" in components:
                canonical_city = components["sublocality"]
            elif "sublocality_level_1" in components:
                canonical_city = components["sublocality_level_1"]

        if not canonical_county and "administrative_area_level_2" in components:
            canonical_county = components["administrative_area_level_2"]

        if not canonical_state and "administrative_area_level_1" in components:
            canonical_state = components["administrative_area_level_1"]

    return GeoResult(
        canonical_city=canonical_city,
        canonical_municipality=canonical_locality,
        canonical_county=canonical_county,
        canonical_state=canonical_state,
        is_incorporated=is_incorporated,
        place_types=list(set(all_types)),
    )


def patch_beach(beach_id: int, city: str, municipality: Optional[str]) -> bool:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    payload: dict = {"city": city}
    if municipality is not None:
        payload["municipality"] = municipality
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/beaches_staging?id=eq.{beach_id}",
        headers=headers,
        json=payload,
    )
    return r.status_code in (200, 204)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", action="store_true", help="Audit only, no DB writes")
    p.add_argument("--apply",  action="store_true", help="Write corrections to DB")
    p.add_argument("--state",  default="", help="Filter to one state")
    p.add_argument("--out",    default="city_audit.csv", help="Output CSV path")
    args = p.parse_args()

    if not args.report and not args.apply:
        args.report = True  # default to report-only

    print(f"Fetching beaches{' (' + args.state + ')' if args.state else ''}...")
    beaches = fetch_beaches(args.state or None)
    print(f"  {len(beaches)} beaches to process")
    print()

    rows = []
    mismatches = 0
    errors = 0

    for i, beach in enumerate(beaches):
        try:
            geo = reverse_geocode(beach.latitude, beach.longitude)
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"  ERROR id={beach.id}: {e}")
            errors += 1
            continue

        canonical     = geo.canonical_city or ""
        municipality  = geo.canonical_municipality or ""
        current       = beach.city or ""
        mismatch      = canonical.lower() != current.lower() and bool(canonical)

        status = "MISMATCH" if mismatch else ("NO_RESULT" if not canonical else "OK")
        if mismatch:
            mismatches += 1

        rows.append({
            "id":              beach.id,
            "display_name":    beach.display_name,
            "current_city":    current,
            "canonical_city":  canonical,
            "municipality":    municipality,
            "is_incorporated": geo.is_incorporated,
            "county":          beach.county,
            "state":           beach.state,
            "status":          status,
        })

        if mismatch:
            print(f"  [{status}] id={beach.id:>4}  {current!r:<30} → {canonical!r}  (municipality: {municipality!r})  ({beach.display_name})")

        if args.apply and mismatch and canonical:
            ok = patch_beach(beach.id, canonical, geo.canonical_municipality)
            print(f"    {'✓' if ok else '✗'} patched id={beach.id}")

        if (i + 1) % 50 == 0:
            print(f"  ... {i+1}/{len(beaches)} processed, {mismatches} mismatches so far")

    # Write CSV
    out = Path(args.out)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print()
    print(f"-- Summary {'':->50}")
    print(f"  Total processed : {len(rows)}")
    print(f"  Mismatches      : {mismatches} ({100*mismatches//max(len(rows),1)}%)")
    print(f"  No result       : {sum(1 for r in rows if r['status'] == 'NO_RESULT')}")
    print(f"  Errors          : {errors}")
    print(f"  Report saved to : {out}")


if __name__ == "__main__":
    main()
