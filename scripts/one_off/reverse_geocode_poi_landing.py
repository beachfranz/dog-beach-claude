"""
reverse_geocode_poi_landing.py
------------------------------
Reverse-geocodes poi_landing rows whose CSV-parsed address didn't make
it to validation_status = 'ok'. Uses Google Maps reverse geocoding API.

Cost: ~$5 per 1,000 requests. ~2,800 rows below 'ok' = ~$14 to run.

Updates per row:
  - address_street/city/state/zip parsed from Google's components
  - address_full = canonical re-assembly
  - address_validation = 'ok' if Google returned a usable result;
    'reverse_geocode_failed' if no results
  - address_source = 'reverse_geocode'

Idempotent: skips rows where address_source is already 'reverse_geocode'
unless --force is passed.

Usage:
  python scripts/one_off/reverse_geocode_poi_landing.py [--limit N] [--dry-run]
"""

from __future__ import annotations
import argparse
import os
import time
import urllib.parse
from pathlib import Path
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432,
          user=p.username, password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

GOOGLE_KEY = os.environ["GOOGLE_MAPS_API_KEY"]
GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

REVERSE_OK_TYPES = {"street_address", "premise", "subpremise"}


def reverse_geocode(lat: float, lng: float) -> dict | None:
    """Returns parsed dict or None on no results."""
    r = httpx.get(GOOGLE_URL,
                  params={"latlng": f"{lat},{lng}", "key": GOOGLE_KEY},
                  timeout=20.0)
    r.raise_for_status()
    data = r.json()
    if data.get("status") not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Google API status={data.get('status')}: {data.get('error_message')}")
    if not data.get("results"):
        return None
    # Pick the result with the most specific type
    best = sorted(
        data["results"],
        key=lambda r: -sum(t in REVERSE_OK_TYPES for t in r.get("types", []))
    )[0]
    out = {"formatted": best.get("formatted_address")}
    for c in best.get("address_components", []):
        types = c.get("types") or []
        if "street_number" in types:        out["street_number"] = c["short_name"]
        elif "route" in types:              out["route"] = c["short_name"]
        elif "locality" in types:           out["city"] = c["short_name"]
        elif "administrative_area_level_3" in types and "city" not in out:
                                            out["city"] = c["short_name"]
        elif "administrative_area_level_2" in types and "city" not in out:
                                            out["city"] = c["short_name"]
        elif "administrative_area_level_1" in types: out["state"] = c["short_name"]
        elif "postal_code" in types:        out["zip"]   = c["long_name"]
        elif "country" in types:            out["country"] = c["long_name"]
    out["street"] = (
        ((out.get("street_number") or "") + " " + (out.get("route") or "")).strip()
        or None
    )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="Re-process rows already marked address_source=reverse_geocode")
    args = ap.parse_args()

    where = "where address_validation in ('partial_no_street','minimal_state_only','missing') and geom is not null"
    if not args.force:
        where += " and (address_source is null or address_source <> 'reverse_geocode')"

    sql_select = f"""
      select fid, fetched_at, ST_X(geom::geometry) as lng, ST_Y(geom::geometry) as lat
        from public.poi_landing
        {where}
        order by fid
        {('limit ' + str(args.limit)) if args.limit else ''}
    """

    with psycopg2.connect(**PG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_select)
            rows = cur.fetchall()

    print(f"{len(rows)} rows to reverse-geocode")
    if args.dry_run:
        for r in rows[:5]:
            print(f"  fid={r['fid']}  {r['lat']:.5f}, {r['lng']:.5f}")
        return

    n_ok = n_fail = 0
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        for i, r in enumerate(rows, 1):
            try:
                parsed = reverse_geocode(r["lat"], r["lng"])
            except Exception as e:
                print(f"  fid={r['fid']}: API error {e}")
                n_fail += 1
                continue

            if parsed is None:
                cur.execute("""
                    update public.poi_landing
                       set address_validation = 'reverse_geocode_failed',
                           address_source = 'reverse_geocode'
                     where fid = %s and fetched_at = %s
                """, (r["fid"], r["fetched_at"]))
                n_fail += 1
            else:
                full = parsed.get("formatted")
                cur.execute("""
                    update public.poi_landing
                       set address_street = %s,
                           address_city = %s,
                           address_state = %s,
                           address_zip = %s,
                           address_country = %s,
                           address_full = %s,
                           address_validation = 'ok',
                           address_source = 'reverse_geocode'
                     where fid = %s and fetched_at = %s
                """, (parsed.get("street"), parsed.get("city"),
                      parsed.get("state"), parsed.get("zip"),
                      parsed.get("country"), full,
                      r["fid"], r["fetched_at"]))
                n_ok += 1

            if i % 50 == 0:
                conn.commit()
                print(f"  {i}/{len(rows)}: ok={n_ok} fail={n_fail}")
        conn.commit()

    print(f"\nDone. ok={n_ok} failed={n_fail}")


if __name__ == "__main__":
    main()
