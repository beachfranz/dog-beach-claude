"""
seed_arena_beach.py — manually seed one beach all the way to scored.

Use this when a beach we care about isn't in beaches_gold (e.g., it
came in under a non-beach OSM tag like Fiesta Island's leisure=dog_park,
or it's a CCC-only access point that arena's pipeline didn't promote).

What it does, in order:

  1. INSERT a row into public.arena with source_code='manual',
     name + lat/lon supplied via args. group_id=fid (its own head).
  2. INSERT a matching row into public.beaches_gold with state
     derived from county_name and scoring columns populated (NOAA
     station from the canonical CA list, timezone, open/close).
  3. INSERT a public.beaches row tying location_id (slug) ↔ arena_group_id
     so legacy queries + chat keep working.

Stops short of scoring on purpose — not every seeded beach is worth
the API spend. Pass --score to also fire daily-beach-refresh and
generate 7 days of forecasts.

Idempotent: skips any step whose target row already exists.

Usage:
  python scripts/seed_arena_beach.py \
    --name "Fiesta Island" \
    --county "San Diego" \
    --lat 32.780292 \
    --lon -117.219177 \
    --state CA

Optional:
  --slug fiesta-island-san-diego   (default: auto from name + county)
  --noaa-station 9410230            (default: nearest from data file)
  --timezone America/Los_Angeles    (default by state)
  --open 05:00 --close 22:00        (defaults)
  --score                           (also fire daily-beach-refresh now)
"""
from __future__ import annotations
import argparse
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

NOAA_STATIONS_PATH = ROOT / "scripts" / "data" / "ca_noaa_stations.json"
STATE_TZ = {"CA": "America/Los_Angeles", "OR": "America/Los_Angeles",
            "WA": "America/Los_Angeles"}


def slugify(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unnamed"


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dLon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def nearest_noaa(lat: float, lon: float, max_km: float = 50.0) -> str | None:
    """Find the nearest CA NOAA reference (type=R) station within max_km."""
    raw = json.loads(NOAA_STATIONS_PATH.read_text(encoding="utf-8"))
    stations = [s for s in raw if s.get("type") == "R"]
    best, best_km = None, None
    for s in stations:
        km = haversine_km(lat, lon, s["lat"], s["lng"])
        if best_km is None or km < best_km:
            best, best_km = s, km
    if best and best_km <= max_km:
        return best["id"]
    return None


def trigger_refresh(location_id: str) -> dict:
    """POST to daily-beach-refresh for one beach via the admin gate."""
    url = (os.environ.get("SUPABASE_URL") or "").rstrip("/") + "/functions/v1/daily-beach-refresh"
    anon = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
    admin_secret = os.environ.get("ADMIN_SECRET")
    if not admin_secret:
        return {"skipped": True, "reason": "ADMIN_SECRET not set"}
    body = json.dumps({"location_ids": [location_id]}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type":   "application/json",
            "Authorization":  f"Bearer {anon}",
            "apikey":         anon,
            "x-admin-secret": admin_secret,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=240) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed a beach into arena → beaches_gold → public.beaches → score.")
    ap.add_argument("--name",     required=True)
    ap.add_argument("--county",   required=True)
    ap.add_argument("--lat",      required=True, type=float)
    ap.add_argument("--lon",      required=True, type=float)
    ap.add_argument("--state",    default="CA", choices=["CA", "OR", "WA"])
    ap.add_argument("--slug")
    ap.add_argument("--noaa-station")
    ap.add_argument("--timezone")
    ap.add_argument("--open",     default="05:00")
    ap.add_argument("--close",    default="22:00")
    ap.add_argument("--park-name")
    ap.add_argument("--score", action="store_true",
                    help="Also fire daily-beach-refresh after seeding (default: don't)")
    args = ap.parse_args()

    slug     = args.slug or f"{slugify(args.name)}-{slugify(args.county)}"
    noaa     = args.noaa_station or nearest_noaa(args.lat, args.lon)
    timezone = args.timezone or STATE_TZ[args.state]
    source_id = f"manual/{slug}"

    print(f"Seeding: {args.name}")
    print(f"  slug:      {slug}")
    print(f"  coords:    ({args.lat}, {args.lon})")
    print(f"  county:    {args.county}, state: {args.state}")
    print(f"  noaa:      {noaa or '(none — inland or out of range)'}")
    print(f"  timezone:  {timezone}")
    print()

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. arena row (idempotent on source_id)
    cur.execute("SELECT fid FROM public.arena WHERE source_id = %s", (source_id,))
    existing = cur.fetchone()
    if existing:
        arena_fid = existing["fid"]
        print(f"  [arena]   already exists at fid={arena_fid}")
    else:
        cur.execute("""
            INSERT INTO public.arena
                (name, lat, lon, county_name, source_code, source_id,
                 nav_lat, nav_lon, nav_source, name_source, is_active)
            VALUES
                (%s, %s, %s, %s, 'manual', %s,
                 %s, %s, 'manual_seed', 'manual_seed', true)
            RETURNING fid;
        """, (args.name, args.lat, args.lon, args.county, source_id,
              args.lat, args.lon))
        arena_fid = cur.fetchone()["fid"]
        cur.execute("UPDATE public.arena SET group_id = fid WHERE fid = %s",
                    (arena_fid,))
        print(f"  [arena]   inserted fid={arena_fid}, group_id={arena_fid}")

    # 2. beaches_gold row (idempotent on fid)
    cur.execute("SELECT fid FROM public.beaches_gold WHERE fid = %s", (arena_fid,))
    if cur.fetchone():
        print(f"  [gold]    already exists at fid={arena_fid}")
    else:
        cur.execute("""
            INSERT INTO public.beaches_gold
                (fid, name, lat, lon, county_name, source_code, source_id,
                 group_id, nav_lat, nav_lon, nav_source, name_source,
                 park_name, state, promoted_from, is_active,
                 noaa_station_id, timezone, open_time, close_time)
            VALUES
                (%s, %s, %s, %s, %s, 'manual', %s,
                 %s, %s, %s, 'manual_seed', 'manual_seed',
                 %s, %s, 'manual_seed_v1', true,
                 %s, %s, %s, %s);
        """, (arena_fid, args.name, args.lat, args.lon, args.county,
              source_id, arena_fid, args.lat, args.lon,
              args.park_name, args.state,
              noaa, timezone, args.open, args.close))
        print(f"  [gold]    inserted fid={arena_fid}")

    # 3. public.beaches row (idempotent on location_id)
    cur.execute("SELECT location_id FROM public.beaches WHERE location_id = %s",
                (slug,))
    if cur.fetchone():
        print(f"  [beaches] already exists at location_id={slug}")
    else:
        cur.execute("""
            INSERT INTO public.beaches
                (location_id, display_name, latitude, longitude,
                 noaa_station_id, timezone, open_time, close_time,
                 is_active, arena_group_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true, %s);
        """, (slug, args.name, args.lat, args.lon,
              noaa, timezone, args.open, args.close, arena_fid))
        print(f"  [beaches] inserted location_id={slug}, arena_group_id={arena_fid}")

    conn.commit()

    # 4. Trigger daily-beach-refresh (opt-in via --score)
    if args.score:
        if not noaa:
            print(f"\n  [refresh] skipped: no NOAA station — beach has no tide signal")
        else:
            # Flip is_scoreable so the nightly daily-beach-refresh picks
            # this beach up going forward (not just the one-off run below).
            cur.execute(
                "UPDATE public.beaches_gold SET is_scoreable = true WHERE fid = %s",
                (arena_fid,)
            )
            conn.commit()
            print(f"  [score]   beaches_gold.is_scoreable = true")
            print(f"\n  Triggering daily-beach-refresh for {slug}...")
            result = trigger_refresh(slug)
            if "error" in result:
                print(f"  [refresh] error: {result['error']}")
            elif result.get("skipped"):
                print(f"  [refresh] skipped: {result['reason']}")
            else:
                r = (result.get("results") or [{}])[0]
                print(f"  [refresh] {r.get('locationId')}: ok={r.get('ok')} days={r.get('daysProcessed')}")
                if r.get("phases"):
                    print(f"            phases: {r['phases']}")
    else:
        print(f"\n  [refresh] skipped (default). Pass --score to generate forecasts now,")
        print(f"            or run `python scripts/score_one_beach.py --location-id {slug}` later.")

    print()
    print(f"DONE. Try: file:///C:/Users/beach/Documents/dog-beach-claude/detail.html?fid={arena_fid}&date=$(date +%Y-%m-%d)")
    print(f"  arena.fid          = {arena_fid}")
    print(f"  beaches_gold.fid   = {arena_fid}")
    print(f"  public.beaches.location_id = {slug}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
