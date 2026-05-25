"""Harvest stage: geocode extracted location addresses.

Reads operator_extracted_locations where lat IS NULL, runs Nominatim
(then Overpass fallback for sparse addresses), writes lat/lng back.

Usage:
    python -m scripts.harvest.geocode --content-type dog_park
    python -m scripts.harvest.geocode --content-type dog_park --operator-id 165
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect

NOMINATIM_DELAY = 1.1
NOMINATIM_HEADERS = {"User-Agent": "dog-beach-scout-harvest/1.0 (franz@franzfunk.com)"}


def nominatim_geocode(query: str):
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search",
                         params={"q": query, "format": "json", "limit": 1, "countrycodes": "us"},
                         headers=NOMINATIM_HEADERS, timeout=15)
        j = r.json()
    except Exception:
        return None
    if not j:
        return None
    return float(j[0]["lat"]), float(j[0]["lon"])


def overpass_name_search(name: str, state: str | None):
    """Fallback when address is sparse — search OSM by name within state bbox."""
    bboxes = {
        "CA": "(32.5,-124.5,42.0,-114.0)",
        "OR": "(42.0,-124.6,46.3,-116.4)",
        "WA": "(45.5,-124.8,49.0,-116.9)",
    }
    bbox = bboxes.get(state or "", "")
    if not bbox:
        return None
    escaped = name.replace('"', '').replace("\\", "")
    q = f'[out:json][timeout:60];nwr["name"~"{escaped}",i]{bbox};out center 1;'
    try:
        r = requests.get("https://overpass-api.de/api/interpreter",
                         params={"data": q}, headers=NOMINATIM_HEADERS, timeout=90)
        elements = r.json().get("elements", [])
    except Exception:
        return None
    if not elements:
        return None
    el = elements[0]
    center = el.get("center") or {"lat": el.get("lat"), "lon": el.get("lon")}
    if center.get("lat") is None:
        return None
    return float(center["lat"]), float(center["lon"])


def fetch_pending(conn, content_type: str, operator_id: int | None):
    sql = """
      SELECT id, name, address, address_city, address_state,
             (SELECT op.web_url FROM public.operator op WHERE op.id = oel.operator_id) AS op_url
        FROM public.operator_extracted_locations oel
       WHERE content_type = %s
         AND extraction_status = 'success'
         AND lat IS NULL
         AND (%s::int IS NULL OR operator_id = %s)
       ORDER BY operator_id, id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (content_type, operator_id, operator_id))
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--content-type", required=True)
    ap.add_argument("--operator-id", type=int)
    args = ap.parse_args()

    conn = connect(application_name=f"harvest_geocode_{args.content_type}")
    targets = fetch_pending(conn, args.content_type, args.operator_id)

    if not targets:
        print("No pending geocode rows.")
        return

    print(f"Geocoding {len(targets)} {args.content_type} rows...")
    n_ok = n_overpass = n_fail = 0
    t0 = time.time()

    for i, (row_id, name, addr, city, state, op_url) in enumerate(targets, 1):
        parts = [p for p in [addr, city, state] if p]
        q = ", ".join(parts) if parts else f"{name}, {city or ''} {state or ''}".strip()

        # Address-quality gate: sparse addresses (intersection-style, no
        # street number) yield bogus Nominatim results. Prefer Overpass
        # name-search first when the address lacks a street number.
        has_street_num = bool(addr and any(c.isdigit() for c in addr.split(",")[0])) \
                         and len(addr.split(",")[0].split()) >= 2
        prefer_overpass = (not has_street_num) and bool(name)

        lat = lng = None
        method = None

        if prefer_overpass:
            res = overpass_name_search(name, state)
            if res:
                lat, lng = res
                method = "overpass_first"
                n_overpass += 1
                time.sleep(2)

        if lat is None:
            res = nominatim_geocode(q)
            if res:
                lat, lng = res
                method = "success"
                n_ok += 1
            time.sleep(NOMINATIM_DELAY)

        if lat is None and not prefer_overpass and name:
            res = overpass_name_search(name, state)
            if res:
                lat, lng = res
                method = "overpass_fallback"
                n_overpass += 1
                time.sleep(2)

        with conn.cursor() as cur:
            if lat is None:
                cur.execute("""
                    UPDATE public.operator_extracted_locations
                       SET geocode_status = 'no_match'
                     WHERE id = %s
                """, (row_id,))
                n_fail += 1
                print(f"  [{i}/{len(targets)}] FAIL  {name[:35]:<35}  q={q[:50]}")
            else:
                cur.execute("""
                    UPDATE public.operator_extracted_locations
                       SET lat = %s, lng = %s, geocode_status = %s
                     WHERE id = %s
                """, (lat, lng, method, row_id))
                print(f"  [{i}/{len(targets)}] {method:<18}  {name[:35]:<35}  ({lat:.5f}, {lng:.5f})")
        conn.commit()

    print(f"\nDone in {time.time()-t0:.1f}s. nominatim={n_ok}, overpass={n_overpass}, fail={n_fail}")


if __name__ == "__main__":
    main()
