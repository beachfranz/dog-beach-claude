"""
geocode_ca_beaches.py
---------------------
Filters US_beaches.csv to California records using lat/lng bounding box,
reverse geocodes each via Google Maps, and outputs a clean enriched CSV.

Usage:
    python geocode_ca_beaches.py --api-key YOUR_KEY [--dry-run]

Output:
    share/Dog_Beaches/ca_beaches_geocoded.csv

Cost estimate: ~835 candidate records → ~$4-5 at $5/1000 requests.
Progress is saved after each record so you can resume if interrupted.
"""

import csv
import json
import re
import time
import argparse
import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

# ── Paths ─────────────────────────────────────────────────────────────────────

INPUT_CSV   = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches.csv")
OUTPUT_CSV  = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\ca_beaches_geocoded.csv")
PROGRESS_FILE = OUTPUT_CSV.with_suffix(".progress.json")

# ── California bounding box (generous) ───────────────────────────────────────

CA_LAT_MIN, CA_LAT_MAX = 32.4,  42.1
CA_LON_MIN, CA_LON_MAX = -124.6, -114.0

# ── Output columns ────────────────────────────────────────────────────────────

OUTPUT_FIELDS = [
    "fid", "name",
    "latitude", "longitude",
    "formatted_address",
    "street_number", "route",
    "city", "county", "state", "zip", "country",
    "governing_jurisdiction",   # federal | state | county | municipal
    "governing_body",           # e.g. "California State Parks", "City of San Diego"
    "geocode_status",           # OK | ZERO_RESULTS | ERROR
    # Original raw fields preserved for reference
    "raw_addr1", "raw_addr2", "raw_addr3", "raw_addr4", "raw_addr5",
]


# ── WKT parser ────────────────────────────────────────────────────────────────

def parse_wkt(wkt: str):
    """Extract (lon, lat) from 'POINT (lon lat)'."""
    m = re.search(r"POINT\s*\(\s*([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\s*\)", wkt)
    if not m:
        return None, None
    return float(m.group(1)), float(m.group(2))  # lon, lat


# ── Google Maps reverse geocode ───────────────────────────────────────────────

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

def reverse_geocode(lat: float, lon: float, api_key: str) -> dict:
    """Call Google Maps reverse geocode. Returns parsed result dict."""
    resp = requests.get(GEOCODE_URL, params={
        "latlng": f"{lat},{lon}",
        "key":    api_key,
    }, timeout=10)
    resp.raise_for_status()
    return resp.json()


def extract_component(components: list, *types) -> str:
    """Pull long_name for the first component matching any of the given types."""
    for c in components:
        if any(t in c.get("types", []) for t in types):
            return c.get("long_name", "")
    return ""


def infer_jurisdiction(name: str, county: str, state: str) -> tuple[str, str]:
    """
    Infer governing jurisdiction from beach name and geocoded fields.
    Returns (jurisdiction_type, governing_body).
    """
    name_upper = name.upper()

    federal_keywords = [
        "NATIONAL SEASHORE", "NATIONAL PARK", "NATIONAL RECREATION",
        "NATIONAL MONUMENT", "NATIONAL WILDLIFE", "ARMY CORPS",
    ]
    state_keywords = [
        "STATE BEACH", "STATE PARK", "STATE RECREATION", "STATE RESERVE",
        "STATE MARINE", "STATE HISTORIC",
    ]
    county_keywords = ["COUNTY PARK", "COUNTY BEACH", "REGIONAL PARK", "REGIONAL BEACH"]

    for kw in federal_keywords:
        if kw in name_upper:
            return "federal", "National Park Service / Federal"

    for kw in state_keywords:
        if kw in name_upper:
            return "state", f"{state} State Parks"

    for kw in county_keywords:
        if kw in name_upper:
            return "county", f"{county} County"

    return "municipal", ""   # governing_body filled in later from city


def parse_result(name: str, lat: float, lon: float, geo_data: dict) -> dict:
    """Turn a Google geocode response into a flat result dict."""
    status = geo_data.get("status", "ERROR")

    if status != "OK" or not geo_data.get("results"):
        return {
            "geocode_status": status,
            "formatted_address": "", "street_number": "", "route": "",
            "city": "", "county": "", "state": "", "zip": "", "country": "",
            "governing_jurisdiction": "", "governing_body": "",
        }

    result     = geo_data["results"][0]
    components = result.get("address_components", [])

    street_number = extract_component(components, "street_number")
    route         = extract_component(components, "route")
    city          = extract_component(components, "locality", "sublocality", "neighborhood")
    county        = extract_component(components, "administrative_area_level_2")
    state         = extract_component(components, "administrative_area_level_1")
    zip_code      = extract_component(components, "postal_code")
    country       = extract_component(components, "country")

    jurisdiction, governing_body = infer_jurisdiction(name, county, state)
    if jurisdiction == "municipal" and city:
        governing_body = f"City of {city}"

    return {
        "geocode_status":       "OK",
        "formatted_address":    result.get("formatted_address", ""),
        "street_number":        street_number,
        "route":                route,
        "city":                 city,
        "county":               county,
        "state":                state,
        "zip":                  zip_code,
        "country":              country,
        "governing_jurisdiction": jurisdiction,
        "governing_body":       governing_body,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Geocode California beaches")
    parser.add_argument("--api-key", required=True, help="Google Maps API key")
    parser.add_argument("--dry-run", action="store_true",
                        help="Filter and count only — no API calls")
    parser.add_argument("--delay", type=float, default=0.05,
                        help="Seconds between API calls (default 0.05 = 20 req/s)")
    args = parser.parse_args()

    # ── Load input ────────────────────────────────────────────────────────────
    print(f"Reading {INPUT_CSV}...")
    with open(INPUT_CSV, encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    print(f"  {len(rows):,} total records")

    # ── Filter to CA bounding box ─────────────────────────────────────────────
    ca_candidates = []
    for r in rows:
        lon, lat = parse_wkt(r.get("WKT", ""))
        if lat is None:
            continue
        if CA_LAT_MIN <= lat <= CA_LAT_MAX and CA_LON_MIN <= lon <= CA_LON_MAX:
            ca_candidates.append((r, lat, lon))

    print(f"  {len(ca_candidates):,} records in California bounding box")
    cost_estimate = len(ca_candidates) * 0.005
    print(f"  Estimated API cost: ~${cost_estimate:.2f}")

    if args.dry_run:
        print("\nDry run — no API calls made.")
        for r, lat, lon in ca_candidates[:10]:
            print(f"  [{lat:.4f}, {lon:.4f}]  {r['NAME']}")
        return

    # ── Load progress (resume support) ───────────────────────────────────────
    done_fids: set = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done_fids = set(json.load(f))
        print(f"  Resuming — {len(done_fids)} already done")

    # ── Open output CSV ───────────────────────────────────────────────────────
    write_header = not OUTPUT_CSV.exists() or not done_fids
    out_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer   = csv.DictWriter(out_file, fieldnames=OUTPUT_FIELDS)
    if write_header:
        writer.writeheader()

    # ── Geocode loop ──────────────────────────────────────────────────────────
    total     = len(ca_candidates)
    processed = 0
    skipped   = 0
    errors    = 0

    try:
        for r, lat, lon in ca_candidates:
            fid = r.get("fid", "")

            if fid in done_fids:
                skipped += 1
                continue

            try:
                geo_data = reverse_geocode(lat, lon, args.api_key)
                parsed   = parse_result(r["NAME"], lat, lon, geo_data)
            except Exception as e:
                print(f"  ERROR [{r['NAME']}]: {e}")
                parsed = {
                    "geocode_status": "ERROR",
                    "formatted_address": "", "street_number": "", "route": "",
                    "city": "", "county": "", "state": "", "zip": "", "country": "",
                    "governing_jurisdiction": "", "governing_body": "",
                }
                errors += 1

            # Only keep confirmed California records (or failed geocodes for review)
            if parsed["state"] not in ("California", ""):
                processed += 1
                done_fids.add(fid)
                continue

            writer.writerow({
                "fid":                    fid,
                "name":                   r.get("NAME", ""),
                "latitude":               lat,
                "longitude":              lon,
                "raw_addr1":              r.get("ADDR1", ""),
                "raw_addr2":              r.get("ADDR2", ""),
                "raw_addr3":              r.get("ADDR3", ""),
                "raw_addr4":              r.get("ADDR4", ""),
                "raw_addr5":              r.get("ADDR5", ""),
                **parsed,
            })
            out_file.flush()

            processed += 1
            done_fids.add(fid)

            # Save progress
            with open(PROGRESS_FILE, "w") as pf:
                json.dump(list(done_fids), pf)

            if processed % 50 == 0:
                pct = (processed + skipped) / total * 100
                print(f"  {processed + skipped}/{total} ({pct:.0f}%)  errors: {errors}")

            time.sleep(args.delay)

    finally:
        out_file.close()

    print(f"\nDone. {processed} geocoded, {skipped} skipped, {errors} errors.")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
