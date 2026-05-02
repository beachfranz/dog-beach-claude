"""
bulk_promote_socal.py — promote all SoCal coastal beaches to scoreable.

Backfills slug + nearest NOAA station + timezone + open/close on
beaches_gold for every active CA coastal beach in the SB → Mexico
corridor (5 counties: Santa Barbara, Ventura, Los Angeles, Orange,
San Diego). Sets is_scoreable=true. Then chunked daily-beach-refresh
to seed forecasts.

Usage:
  python scripts/bulk_promote_socal.py            # dry-run
  python scripts/bulk_promote_socal.py --apply    # update + score
  python scripts/bulk_promote_socal.py --apply --skip-score   # update only
"""
from __future__ import annotations
import argparse
import json
import math
import os
import re
import sys
import time
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
NOAA_PATH = ROOT / "scripts" / "data" / "ca_noaa_stations.json"

SOCAL_COUNTIES = ["Santa Barbara", "Ventura", "Los Angeles", "Orange", "San Diego"]
TIMEZONE = "America/Los_Angeles"
OPEN_T   = "05:00"
CLOSE_T  = "22:00"
CHUNK    = 25  # beaches per daily-beach-refresh invocation


def slugify(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unnamed"


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = math.radians(lat2 - lat1); dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2)
    return 2 * R * math.asin(math.sqrt(a))


def nearest_noaa(lat, lon, max_km=50.0) -> str | None:
    raw = json.loads(NOAA_PATH.read_text(encoding="utf-8"))
    stations = [s for s in raw if s.get("type") == "R"]
    best, best_km = None, None
    for s in stations:
        km = haversine_km(lat, lon, s["lat"], s["lng"])
        if best_km is None or km < best_km:
            best, best_km = s, km
    return best["id"] if best and best_km <= max_km else None


def trigger_refresh_chunk(loc_ids: list[str]) -> dict:
    url = (os.environ.get("SUPABASE_URL") or "").rstrip("/") + "/functions/v1/daily-beach-refresh"
    anon = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
    secret = os.environ.get("ADMIN_SECRET")
    body = json.dumps({"location_ids": loc_ids}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {anon}",
            "apikey": anon,
            "x-admin-secret": secret or "",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=240) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def ensure_unique_slug(cur, base_slug: str, fid: int) -> str:
    """If base_slug already taken, suffix with -<fid>."""
    cur.execute("SELECT 1 FROM public.beaches_gold WHERE location_id = %s AND fid <> %s",
                (base_slug, fid))
    if cur.fetchone():
        return f"{base_slug}-{fid}"
    return base_slug


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--skip-score", action="store_true",
                    help="Update metadata + flip is_scoreable but don't fire daily-beach-refresh")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. Pull SoCal coastal beaches needing metadata
    cur.execute(f"""
        SELECT fid, name, county_name, lat, lon,
               location_id, noaa_station_id, timezone, is_scoreable
          FROM public.beaches_gold
         WHERE is_active = true AND state = 'CA'
           AND county_name IN %s
         ORDER BY county_name, name
    """, (tuple(SOCAL_COUNTIES),))
    beaches = cur.fetchall()
    print(f"SoCal coastal active in gold: {len(beaches)}")

    # 2. Plan updates with two-pass slug uniqueness:
    #    pass A — propose base slugs, count within-batch duplicates
    #    pass B — also check existing DB slugs (held by beaches outside the batch)
    #    pass C — for any slug colliding within batch OR with DB, suffix -<fid>
    proposals = []
    for b in beaches:
        base = b["location_id"] or f"{slugify(b['name'])}-{slugify(b['county_name'])}"
        proposals.append((b, base))
    # within-batch duplicate count
    from collections import Counter
    base_counts = Counter(slug for _, slug in proposals)
    # existing DB slugs not held by this batch
    batch_fids = {b["fid"] for b, _ in proposals}
    cur.execute("SELECT location_id, fid FROM public.beaches_gold WHERE location_id IS NOT NULL")
    db_slug_to_fid = {r["location_id"]: r["fid"] for r in cur.fetchall()}
    db_external = {s for s, fid in db_slug_to_fid.items() if fid not in batch_fids}

    updates = []
    for b, base in proposals:
        if b["location_id"]:
            slug = b["location_id"]  # keep what's set
        elif base_counts[base] > 1 or base in db_external:
            slug = f"{base}-{b['fid']}"
        else:
            slug = base
        noaa = b["noaa_station_id"] or nearest_noaa(b["lat"], b["lon"])
        tz   = b["timezone"] or TIMEZONE
        updates.append({
            "fid": b["fid"], "name": b["name"], "county": b["county_name"],
            "slug": slug, "noaa": noaa, "tz": tz,
            "needs_change": (
                b["location_id"] != slug or
                b["noaa_station_id"] != noaa or
                b["timezone"] != tz or
                not b["is_scoreable"]
            ),
        })

    n_change   = sum(1 for u in updates if u["needs_change"])
    n_no_noaa  = sum(1 for u in updates if u["noaa"] is None)
    print(f"  rows needing update: {n_change}")
    print(f"  rows with no NOAA within 50km (will skip scoring): {n_no_noaa}")

    if not args.apply:
        print("\n(dry-run; rerun with --apply)")
        return 0

    # 3. Apply per-row UPDATEs
    print(f"\nApplying...")
    for i, u in enumerate(updates, 1):
        cur.execute("""
            UPDATE public.beaches_gold
               SET location_id     = COALESCE(location_id, %s),
                   noaa_station_id = COALESCE(noaa_station_id, %s),
                   timezone        = COALESCE(timezone, %s),
                   open_time       = COALESCE(open_time, %s),
                   close_time      = COALESCE(close_time, %s),
                   is_scoreable    = true
             WHERE fid = %s
        """, (u["slug"], u["noaa"], u["tz"], OPEN_T, CLOSE_T, u["fid"]))
        if i % 50 == 0:
            print(f"  ... {i}/{len(updates)}")
    conn.commit()
    print(f"  applied {len(updates)} updates.")

    if args.skip_score:
        print("\n(--skip-score; not triggering daily-beach-refresh)")
        return 0

    # 4. Chunked daily-beach-refresh trigger
    scoreable = [u for u in updates if u["noaa"]]
    print(f"\nTriggering daily-beach-refresh in chunks of {CHUNK}...")
    print(f"  total to score: {len(scoreable)}")
    ok_total = 0; err_total = 0
    for i in range(0, len(scoreable), CHUNK):
        chunk = scoreable[i:i+CHUNK]
        slugs = [u["slug"] for u in chunk]
        t0 = time.time()
        result = trigger_refresh_chunk(slugs)
        dt = time.time() - t0
        if "error" in result:
            err_total += len(chunk)
            print(f"  chunk {i//CHUNK+1}: ERROR after {dt:.0f}s — {result['error'][:120]}")
        else:
            results = result.get("results", [])
            ok = sum(1 for r in results if r.get("ok"))
            err = len(results) - ok
            ok_total += ok; err_total += err
            print(f"  chunk {i//CHUNK+1}: {ok}/{len(chunk)} ok in {dt:.0f}s")
    print(f"\nFinal: {ok_total} scored, {err_total} errored.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
