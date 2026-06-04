"""Harvest stage: geocode extracted location addresses (parallel).

Reads operator_extracted_locations where lat IS NULL. Multi-strategy
geocoding per row, in order:

  1. Google Places API (searchText) — primary. Best at "name + city"
     queries; returns place_id + formattedAddress + types + opening
     hours + website + phone + rating + Google Maps URI. Full response
     stored as JSONB in google_places_data.
  2. Nominatim with augmented query — free fallback. Always appends
     operator-city + state. For intersections ("A and B"), rewrite to
     "<A> & <B>".
  3. Overpass name-search — last resort. Finds the park polygon even
     when OSM tags it as leisure=park, not leisure=dog_park.
  4. If all three fail, mark geocode_status='no_match'.

Parallelization (per Franz 2026-05-24 LATE):
  - ThreadPoolExecutor with max_workers=6 (under pgbouncer 15-cap)
  - Per-thread DB connection via scripts.common.db.thread_conn
  - Nominatim has a strict 1 req/sec global rate; threads serialize on a
    shared Lock + 1.1s sleep after each Nominatim call.
  - Overpass calls run truly in parallel (no global rate cap on the
    public instance for low-volume queries; 30-90s per call dominates
    runtime, so parallelism is real speedup here).

Usage:
    python -m scripts.harvest.geocode --content-type dog_park
    python -m scripts.harvest.geocode --content-type dog_park --workers 4
    python -m scripts.harvest.geocode --content-type dog_park --retry-failed
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect, thread_conn

# Google Maps Platform key from scripts/pipeline/.env (loaded by scripts.common.db)
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

NOMINATIM_DELAY = 1.1
NOMINATIM_HEADERS = {"User-Agent": "dog-beach-scout-harvest/1.0 (franz@franzfunk.com)"}

# Serializes Nominatim calls across all threads. Public Nominatim
# enforces 1 req/sec across YOUR account, not per connection.
_NOMINATIM_LOCK = threading.Lock()


# ─── Query augmentation ──────────────────────────────────────────────

_CITY_OF_RX = re.compile(r"^city of (.+?)(?:\s*,.*)?$", re.IGNORECASE)
_COUNTY_OF_RX = re.compile(r"^(.+?)\s+county$|^county of (.+?)$", re.IGNORECASE)


def derive_city_state(operator_name: str) -> tuple[str | None, str]:
    """From 'City of San Diego' → ('San Diego', 'CA'). Best-effort.

    Operators in the canonical table are mostly CA today, so default state='CA'.
    Returns (city_or_None, state).
    """
    m = _CITY_OF_RX.match(operator_name.strip())
    if m:
        return m.group(1).strip(), "CA"
    # 'San Francisco, City and County of' → 'San Francisco'
    if ", city and county of" in operator_name.lower():
        return operator_name.split(",")[0].strip(), "CA"
    # County operators — no specific city; geocode without one
    return None, "CA"


# Detect "A and B" / "A at B" / "A & B" intersection patterns. Capture
# the two streets so we can reformat as Nominatim's preferred syntax.
_INTERSECTION_RX = re.compile(
    r"^\s*(?P<a>[A-Z][A-Za-z0-9\.\s'-]+?)\s+(?:and|at|&)\s+(?P<b>[A-Z][A-Za-z0-9\.\s'-]+?)\s*(?:,|$)",
    re.IGNORECASE
)


def augment_query(address: str | None, address_city: str | None,
                  derived_city: str | None, state: str) -> str | None:
    """Build a Nominatim-friendly query string.

    Augments with the operator's derived city when the LLM-extracted
    address_city is missing or is a neighborhood. Reformats intersection
    addresses as "A & B, City, State".

    Returns None if nothing usable.
    """
    if not address:
        return None

    addr = address.strip()
    # Intersection rewrite: "Felspar Street and Soledad Mountain Road, Pacific Beach"
    #                    → "Felspar Street & Soledad Mountain Road, San Diego, CA"
    m = _INTERSECTION_RX.match(addr)
    if m:
        a, b = m.group("a").strip(), m.group("b").strip()
        # Prefer derived_city over neighborhood-name address_city
        city = derived_city or address_city
        parts = [f"{a} & {b}"]
        if city:
            parts.append(city)
        parts.append(state)
        return ", ".join(parts)

    # Non-intersection: strip any neighborhood suffix and append derived city + state
    # Keep address as-is; APPEND city + state if not already present.
    addr_lower = addr.lower()
    parts = [addr]
    # If derived_city not already in addr, append it
    if derived_city and derived_city.lower() not in addr_lower:
        parts.append(derived_city)
    if state.lower() not in addr_lower:
        parts.append(state)
    return ", ".join(parts)


def google_places_searchtext(query: str) -> dict | None:
    """Google Places API (New) — searchText endpoint.

    Returns the first place from the response, with all useful fields,
    or None on miss. Caller can extract lat/lng from the 'location' field
    AND persist the full dict as JSONB for downstream consumers.

    Field mask is REQUIRED — you only pay for fields you request.
    """
    if not GOOGLE_MAPS_API_KEY:
        return None
    field_mask = (
        "places.id,"
        "places.displayName,"
        "places.formattedAddress,"
        "places.shortFormattedAddress,"
        "places.addressComponents,"
        "places.location,"
        "places.types,"
        "places.primaryType,"
        "places.primaryTypeDisplayName,"
        "places.websiteUri,"
        "places.googleMapsUri,"
        "places.nationalPhoneNumber,"
        "places.regularOpeningHours,"
        "places.businessStatus,"
        "places.rating,"
        "places.userRatingCount,"
        "places.priceLevel,"
        "places.iconMaskBaseUri,"
        "places.iconBackgroundColor,"
        "places.viewport,"
        "places.utcOffsetMinutes"
    )
    try:
        r = requests.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
                "X-Goog-FieldMask": field_mask,
            },
            json={"textQuery": query},
            timeout=20,
        )
        if r.status_code != 200:
            return None
        j = r.json()
    except Exception:
        return None
    places = j.get("places") or []
    return places[0] if places else None


def nominatim_geocode(query: str):
    """Single Nominatim call. Caller is responsible for honoring the
    1 req/sec global rate via _NOMINATIM_LOCK (do not call this directly
    from a worker without holding the lock + sleep)."""
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


def nominatim_throttled(query: str):
    """Lock-protected Nominatim call. Releases the lock after the
    1.1s sleep so the next thread can fire immediately."""
    with _NOMINATIM_LOCK:
        res = nominatim_geocode(query)
        time.sleep(NOMINATIM_DELAY)
    return res


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


def fetch_pending(conn, content_type: str, operator_id: int | None, retry_failed: bool):
    sql = """
      SELECT oel.id, oel.name, oel.address, oel.address_city, oel.address_state,
             op.canonical_name AS operator_name
        FROM public.operator_extracted_locations oel
        JOIN public.operators op
          ON op.id = oel.operator_id
         AND op.is_canonical = true
       WHERE oel.content_type = %s
         AND oel.extraction_status = 'success'
         AND (oel.lat IS NULL OR (%s AND oel.geocode_status = 'no_match'))
         AND (%s::int IS NULL OR oel.operator_id = %s)
       ORDER BY oel.operator_id, oel.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (content_type, retry_failed, operator_id, operator_id))
        return cur.fetchall()


def geocode_one(row: tuple):
    """Worker: geocode one row. Returns a dict with:
      row_id, status ('ok'/'fail'), method, lat, lng,
      google_place_id (optional), google_places_data (optional)
    """
    row_id, name, addr, city, state_col, op_name = row
    derived_city, derived_state = derive_city_state(op_name or "")
    state = state_col or derived_state or "CA"

    # Build the natural-language query for Google Places (name + city +
    # state is the high-recall shape; Places searchText handles this well).
    places_query_parts = [name]
    if addr:
        places_query_parts.append(addr)
    if derived_city:
        places_query_parts.append(derived_city)
    places_query_parts.append(state)
    places_query = ", ".join(p for p in places_query_parts if p)

    result = {"row_id": row_id, "status": "fail", "method": None,
              "lat": None, "lng": None,
              "google_place_id": None, "google_places_data": None}

    # Strategy 1: Google Places searchText (primary)
    if GOOGLE_MAPS_API_KEY:
        place = google_places_searchtext(places_query)
        if place and place.get("location"):
            loc = place["location"]
            result.update({
                "status": "ok",
                "method": "google_places",
                "lat": float(loc["latitude"]),
                "lng": float(loc["longitude"]),
                "google_place_id": place.get("id"),
                "google_places_data": place,
            })
            return result

    # Strategy 2: Nominatim (rate-limited via shared lock)
    q = augment_query(addr, city, derived_city, state)
    if q is None:
        q = f"{name}, {derived_city or ''}, {state}".strip(", ")
    if q:
        res = nominatim_throttled(q)
        if res:
            result.update({"status": "ok", "method": "success",
                           "lat": res[0], "lng": res[1]})
            return result

    # Strategy 3: Overpass name search (parallel)
    if name:
        res = overpass_name_search(name, state)
        if res:
            result.update({"status": "ok", "method": "overpass_fallback",
                           "lat": res[0], "lng": res[1]})
            return result

    return result


def write_result(result: dict) -> None:
    """Persist one geocode result via per-thread connection."""
    conn = thread_conn()
    with conn.cursor() as cur:
        if result["status"] == "fail":
            cur.execute("""
                UPDATE public.operator_extracted_locations
                   SET geocode_status = 'no_match'
                 WHERE id = %s
            """, (result["row_id"],))
        else:
            places_data = result.get("google_places_data")
            cur.execute("""
                UPDATE public.operator_extracted_locations
                   SET lat = %s, lng = %s, geocode_status = %s,
                       google_place_id = %s,
                       google_places_data = %s::jsonb
                 WHERE id = %s
            """, (result["lat"], result["lng"], result["method"],
                  result.get("google_place_id"),
                  json.dumps(places_data) if places_data else None,
                  result["row_id"]))
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--content-type", required=True)
    ap.add_argument("--operator-id", type=int)
    ap.add_argument("--retry-failed", action="store_true",
                    help="Also re-geocode rows previously marked no_match")
    ap.add_argument("--workers", type=int, default=6,
                    help="Parallel workers (default 6; cap below pgbouncer 15)")
    args = ap.parse_args()

    conn = connect(application_name=f"harvest_geocode_{args.content_type}")
    targets = fetch_pending(conn, args.content_type, args.operator_id, args.retry_failed)
    conn.close()  # workers use thread_conn

    if not targets:
        print("No pending geocode rows.")
        return

    print(f"Geocoding {len(targets)} {args.content_type} rows with {args.workers} workers...")
    n_ok = n_overpass = n_fail = 0
    t0 = time.time()

    n_places = 0
    # Submit all rows to the executor; Nominatim is throttled inside
    # the worker via _NOMINATIM_LOCK. Google Places has high QPS so
    # parallelism helps a lot when it's the primary path.
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(geocode_one, t): t for t in targets}
        for i, fut in enumerate(as_completed(futures), 1):
            target = futures[fut]
            result = fut.result()
            name = target[1]
            try:
                write_result(result)
            except Exception as e:
                print(f"  [{i}/{len(targets)}] WRITE-FAIL  {(name or '?')[:35]:<35}  {e}")
                continue
            status, method, lat, lng = (result["status"], result["method"],
                                         result["lat"], result["lng"])
            if status == "fail":
                n_fail += 1
                print(f"  [{i}/{len(targets)}] FAIL  {(name or '?')[:35]}")
            elif method == "google_places":
                n_places += 1
                print(f"  [{i}/{len(targets)}] places       {(name or '?')[:35]:<35}  ({lat:.5f}, {lng:.5f})")
            elif method == "overpass_fallback":
                n_overpass += 1
                print(f"  [{i}/{len(targets)}] overpass     {(name or '?')[:35]:<35}  ({lat:.5f}, {lng:.5f})")
            else:
                n_ok += 1
                print(f"  [{i}/{len(targets)}] nominatim    {(name or '?')[:35]:<35}  ({lat:.5f}, {lng:.5f})")

    print(f"\nDone in {time.time()-t0:.1f}s. places={n_places}, nominatim={n_ok}, "
          f"overpass={n_overpass}, fail={n_fail}")


if __name__ == "__main__":
    main()
