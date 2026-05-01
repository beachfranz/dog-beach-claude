"""
sync_beaches_gold_to_beaches.py — Phase A dry-run.

Plans the auto-promotion of beaches_gold rows into public.beaches so the
scoring pipeline (daily-beach-refresh) covers every catalog beach.

Scope:
  - State hardcoded to 'CA' (matches the populator pattern; copy this
    file per state when expanding)
  - For each beaches_gold row (CA, is_active), produce a candidate
    public.beaches row.
  - Skip if an existing public.beaches row sits within 100m AND has a
    similar display_name (the 14 hand-curated beaches stay untouched).
  - Auto-fill: location_id (slug + county/fid suffix for collisions),
    display_name, latitude, longitude, nearest NOAA tide station,
    timezone (America/Los_Angeles for CA), open_time, close_time,
    is_active=true, besttime_venue_id=NULL (path 1: skip crowd).
  - Default mode: write _sync_preview.json + summary, NO DB changes.
  - --apply: actually INSERT (Phase B; not used yet).

Usage:
  python scripts/sync_beaches_gold_to_beaches.py            # dry-run
  python scripts/sync_beaches_gold_to_beaches.py --apply    # Phase B
"""
from __future__ import annotations
import json
import os
import re
import sys
import urllib.parse
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

# ── HARDCODED PER STATE ───────────────────────────────────────────────
STATE         = "CA"
TIMEZONE      = "America/Los_Angeles"
DEFAULT_OPEN  = "05:00:00"
DEFAULT_CLOSE = "22:00:00"

# Active CA NOAA CO-OPS tide stations. Hand-picked subset; expand if a
# beach falls > 50km from all of these (would surface in the dry-run).
NOAA_CA_STATIONS = [
    ("9419750", "Crescent City",      41.7456, -124.1844),
    ("9418767", "North Spit",         40.7669, -124.2172),
    ("9416841", "Arena Cove",         38.9145, -123.7110),
    ("9415020", "Point Reyes",        37.9942, -122.9736),
    ("9414750", "Alameda",            37.7717, -122.3000),
    ("9414290", "San Francisco",      37.8063, -122.4659),
    ("9413450", "Monterey",           36.6053, -121.8884),
    ("9412110", "Port San Luis",      35.1681, -120.7553),
    ("9411340", "Santa Barbara",      34.4080, -119.6857),
    ("9410840", "Santa Monica",       34.0083, -118.5000),
    ("9410660", "Los Angeles",        33.7197, -118.2728),
    ("9410647", "Long Beach",         33.7660, -118.1900),
    ("9410580", "Newport Harbor",     33.6033, -117.8833),
    ("9410230", "La Jolla",           32.8669, -117.2575),
    ("9410170", "San Diego",          32.7142, -117.1736),
]

EARTH_KM = 6371.0
# Match against existing public.beaches by (close OR (medium-close + similar name))
MATCH_TIGHT_M     = 200.0    # auto-match regardless of name
MATCH_LOOSE_M     = 2000.0   # match only if name_sim >= MATCH_NAME_SIM
MATCH_NAME_SIM    = 0.40
NOAA_MAX_KM       = 50.0

OUT_PATH = ROOT / "scripts" / "_sync_preview.json"


def slugify(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[’']", "", s)              # strip apostrophes
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unnamed"


def haversine_km(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    a = sin(dLat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dLon/2)**2
    return 2 * EARTH_KM * asin(sqrt(a))


def nearest_noaa(lat, lon):
    best_id, best_name, best_km = None, None, None
    for sid, sname, slat, slon in NOAA_CA_STATIONS:
        km = haversine_km(lat, lon, slat, slon)
        if best_km is None or km < best_km:
            best_id, best_name, best_km = sid, sname, km
    if best_km is not None and best_km <= NOAA_MAX_KM:
        return best_id, best_name, round(best_km, 1)
    return None, None, round(best_km, 1) if best_km is not None else None


def fetch_gold(cur):
    cur.execute("""
        SELECT fid, name, lat, lon, county_name, source_code, source_id, park_name
          FROM public.beaches_gold
         WHERE state = %s AND is_active = true
         ORDER BY fid
    """, (STATE,))
    return cur.fetchall()


def fetch_existing_beaches(cur):
    cur.execute("""
        SELECT location_id, display_name, latitude, longitude
          FROM public.beaches
         WHERE is_active = true OR is_active IS NULL
    """)
    return cur.fetchall()


def name_similarity(a: str | None, b: str | None) -> float:
    """Trigram-ish similarity using set-of-3-grams Jaccard. Cheap, OK for
    catching 'Dog Beach' ~ 'Huntington Beach Dog Beach' (0.4ish)."""
    def grams(s):
        s = (s or "").lower()
        s = re.sub(r"[^a-z0-9]+", "", s)
        return {s[i:i+3] for i in range(len(s) - 2)} if len(s) >= 3 else {s}
    A, B = grams(a), grams(b)
    if not A or not B:
        return 0.0
    return len(A & B) / max(len(A | B), 1)


def find_existing_match(gold_row, existing):
    """Return location_id of an existing public.beaches row that the gold
    row maps to. Match if (a) within MATCH_TIGHT_M regardless of name, or
    (b) within MATCH_LOOSE_M AND name similarity >= MATCH_NAME_SIM."""
    if gold_row["lat"] is None or gold_row["lon"] is None:
        return None
    best = None  # (location_id, dist_m)
    for b in existing:
        if b["latitude"] is None or b["longitude"] is None:
            continue
        km = haversine_km(gold_row["lat"], gold_row["lon"],
                          float(b["latitude"]), float(b["longitude"]))
        m = km * 1000
        if m <= MATCH_TIGHT_M:
            if best is None or m < best[1]:
                best = (b["location_id"], m)
            continue
        if m <= MATCH_LOOSE_M:
            sim = name_similarity(gold_row["name"], b["display_name"])
            if sim >= MATCH_NAME_SIM:
                if best is None or m < best[1]:
                    best = (b["location_id"], m)
    return best[0] if best else None


def main() -> int:
    apply = "--apply" in sys.argv
    if apply:
        print("--apply not implemented in Phase A; aborting.")
        return 2

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    gold = fetch_gold(cur)
    existing = fetch_existing_beaches(cur)
    print(f"beaches_gold (CA, active): {len(gold)} rows")
    print(f"public.beaches existing  : {len(existing)} rows")
    print()

    # First pass: build candidate slug + match status + NOAA assignment
    candidates = []
    for g in gold:
        existing_loc = find_existing_match(g, existing)
        slug_base = slugify(g["name"])
        county_slug = slugify(g["county_name"]) if g["county_name"] else "unknown"
        proposed_slug = f"{slug_base}-{county_slug}"

        if existing_loc is not None:
            candidates.append({
                "fid": g["fid"], "name": g["name"], "match": existing_loc,
                "action": "skip_existing",
            })
            continue

        if g["lat"] is None or g["lon"] is None:
            candidates.append({
                "fid": g["fid"], "name": g["name"],
                "action": "skip_no_coords",
            })
            continue

        noaa_id, noaa_name, noaa_km = nearest_noaa(g["lat"], g["lon"])
        candidates.append({
            "fid":            g["fid"],
            "arena_fid":      g["fid"],
            "name":           g["name"],
            "county":         g["county_name"],
            "lat":            g["lat"],
            "lon":            g["lon"],
            "park_name":      g["park_name"],
            "source_code":    g["source_code"],
            "source_id":      g["source_id"],
            "proposed_slug":  proposed_slug,
            "noaa_id":        noaa_id,
            "noaa_name":      noaa_name,
            "noaa_dist_km":   noaa_km,
            "tide_capable":   noaa_id is not None,
            "timezone":       TIMEZONE,
            "open_time":      DEFAULT_OPEN,
            "close_time":     DEFAULT_CLOSE,
            "is_active":      True,
            "besttime_venue_id": None,
            "action":         "would_insert",
        })

    # Resolve slug collisions among would_insert rows
    seen: dict[str, list[dict]] = {}
    for c in candidates:
        if c.get("action") != "would_insert":
            continue
        seen.setdefault(c["proposed_slug"], []).append(c)
    for slug_key, rows in seen.items():
        if len(rows) > 1:
            for r in rows:
                r["proposed_slug"] = f"{slug_key}-{r['fid']}"
                r["slug_collision"] = True
                r["slug_collision_count"] = len(rows)

    inserts = [c for c in candidates if c.get("action") == "would_insert"]
    skips_match    = [c for c in candidates if c.get("action") == "skip_existing"]
    skips_nocoords = [c for c in candidates if c.get("action") == "skip_no_coords"]
    inland         = [c for c in inserts if not c["tide_capable"]]
    collisions     = [c for c in inserts if c.get("slug_collision")]

    # Coverage by NOAA station
    by_station: dict[str, int] = {}
    for c in inserts:
        by_station[c["noaa_id"] or "(none)"] = by_station.get(c["noaa_id"] or "(none)", 0) + 1

    print("─── Summary ──────────────────────────────────────────")
    print(f"  WOULD INSERT       : {len(inserts):4}")
    print(f"  skip (existing)    : {len(skips_match):4}")
    print(f"  skip (no coords)   : {len(skips_nocoords):4}")
    print(f"  TOTAL gold         : {len(gold):4}")
    print()
    print(f"  inland (no NOAA)   : {len(inland)}")
    print(f"  slug collisions    : {len(collisions)}  ({len(collisions)//2 if len(collisions) > 1 else 0} groups; suffix=-<fid>)")
    print()
    print("  NOAA station distribution (would-insert):")
    for sid, n in sorted(by_station.items(), key=lambda kv: -kv[1]):
        nm = next((s[1] for s in NOAA_CA_STATIONS if s[0] == sid), sid)
        print(f"    {sid:>10}  {nm:25}  {n:4}")
    print()

    # Examples
    print("─── Examples ─────────────────────────────────────────")
    print("  matched to existing beach (skipped):")
    for c in skips_match[:5]:
        print(f"    fid={c['fid']:5}  {c['name'][:40]:40}  → {c['match']}")
    if len(skips_match) > 5:
        print(f"    ... ({len(skips_match) - 5} more)")
    print()
    print("  would-insert (sample):")
    for c in inserts[:8]:
        flag = "INLAND" if not c["tide_capable"] else f"NOAA {c['noaa_id']} ({c['noaa_dist_km']}km)"
        col  = "  COLLISION" if c.get("slug_collision") else ""
        print(f"    fid={c['fid']:5}  slug={c['proposed_slug']:50}  {flag}{col}")
    if len(inserts) > 8:
        print(f"    ... ({len(inserts) - 8} more)")
    print()
    print("  slug collisions (resolved with -<fid>):")
    for c in collisions[:6]:
        print(f"    fid={c['fid']:5}  → {c['proposed_slug']}")
    if len(collisions) > 6:
        print(f"    ... ({len(collisions) - 6} more)")
    print()
    print("  inland beaches (no NOAA station within 50km):")
    for c in inland[:8]:
        print(f"    fid={c['fid']:5}  {c['name'][:35]:35}  county={c['county']}  nearest_dist={c['noaa_dist_km']}km")
    if len(inland) > 8:
        print(f"    ... ({len(inland) - 8} more)")

    OUT_PATH.write_text(json.dumps(candidates, indent=2, default=str), encoding="utf-8")
    print(f"\n  full preview → {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
