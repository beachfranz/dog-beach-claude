"""match_beaches_to_osm_dog_parks.py — snap dog-beach pins/polys to OSM.

Per Franz 2026-05-19 — designated dog-beach carve-outs (Coronado Dog
Beach, Rosie's Dog Beach, HBDB, Del Mar Dog Beach) often have a real
polygon in OSM as `leisure=dog_park`. Loaded into `osm_dog_parks`.

For beaches whose name strongly matches a nearby OSM dog park
polygon, this script:

  1. Snaps the beach pin (`beaches_gold.geom`) to the OSM polygon
     centroid (so PIP-cascade lands the right polygons)
  2. Replaces the auto-buffer polygon in `beach_polygons` with the
     real OSM polygon (source_tier='osm')
  3. Inserts a row into `dog_policy_zones` tagged as off-leash
     carve-out — the codify cascade can use this as a signal that
     the beach IS the carve-out (suppressing the surrounding
     baseline rule from zone_rules)

**Tight match criteria** (avoids merging unrelated dog parks):
  - Beach name + OSM dog park name share a distinctive token after
    stripping common words (beach, dog, park, state, the, of, area)
  - Distance < 1.5 km
  - OR beach pin is already inside the OSM polygon

Run:
  python scripts/match_beaches_to_osm_dog_parks.py --dry-run
  python scripts/match_beaches_to_osm_dog_parks.py --apply
  python scripts/match_beaches_to_osm_dog_parks.py --fid 6202 --apply
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import argparse
import json
import os
import re
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect():
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


# Common words that don't help discriminate beach ↔ dog-park identity.
# "Coronado Dog Beach" + "Coronado Dog Beach" → distinctive token = "coronado"
# "Alameda Point Shoreline" + "Alameda Large Dog Park" → "alameda" is a city
#   name; matching only on it is too loose, so we ALSO require the beach
#   name to contain "dog" OR the pin to be inside the polygon below.
STOP = {"beach", "beaches", "dog", "park", "parks", "state", "city", "the",
        "of", "and", "a", "an", "area", "memorial", "point",
        "north", "south", "east", "west", "small", "large"}


def _tokens(name: str) -> set[str]:
    toks = re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).split()
    return {t for t in toks if t and t not in STOP and len(t) >= 4}


def discover(conn, state: str | None = None, fid: int | None = None) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT g.fid, g.name AS beach_name, g.state, g.county_name,
               g.scoring_tier, ST_X(g.geom) AS pin_lng, ST_Y(g.geom) AS pin_lat,
               o.osm_id, o.name AS osm_name,
               ST_X(ST_Centroid(o.geom)) AS osm_cx, ST_Y(ST_Centroid(o.geom)) AS osm_cy,
               o.area_m2 AS osm_area_m2,
               ST_Distance(g.geom::geography, o.geom::geography) AS dist_m,
               ST_Within(g.geom, o.geom) AS pin_inside,
               ST_AsGeoJSON(o.geom) AS osm_geojson
        FROM beaches_gold g
        JOIN osm_dog_parks o
          ON ST_DWithin(g.geom::geography, o.geom::geography, 2000)
        WHERE g.is_active
          AND (%s::text IS NULL OR g.state = %s)
          AND (%s::int IS NULL OR g.fid = %s)
    """, (state, state, fid, fid))
    rows = cur.fetchall()

    matches = []
    for r in rows:
        b_toks = _tokens(r["beach_name"])
        o_toks = _tokens(r["osm_name"])
        overlap = b_toks & o_toks
        # Must share ≥1 distinctive token AND name look related (both contain
        # "dog" or pin is inside the polygon)
        beach_has_dog = "dog" in (r["beach_name"] or "").lower()
        osm_has_dog   = "dog" in (r["osm_name"] or "").lower()
        related = (beach_has_dog and osm_has_dog) or r["pin_inside"]
        if overlap and related and r["dist_m"] <= 1500:
            matches.append(dict(r))
        elif r["pin_inside"]:
            matches.append(dict(r))

    # If a beach matches multiple OSM polys, prefer the one with the pin
    # inside; else the closest; else the largest token overlap
    by_fid: dict[int, dict] = {}
    for m in matches:
        prev = by_fid.get(m["fid"])
        if (prev is None
            or (m["pin_inside"] and not prev["pin_inside"])
            or (m["pin_inside"] == prev["pin_inside"] and m["dist_m"] < prev["dist_m"])):
            by_fid[m["fid"]] = m
    return list(by_fid.values())


def apply_one(conn, m: dict) -> dict:
    """Snap pin + replace polygon + upsert dog_policy_zones row. Returns
    a dict describing what changed."""
    cur = conn.cursor()
    changes = {"fid": m["fid"], "name": m["beach_name"], "osm_id": m["osm_id"]}

    # 1. Snap pin to OSM centroid (only if not already inside)
    if not m["pin_inside"]:
        cur.execute("""
            UPDATE beaches_gold SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
             WHERE fid = %s
        """, (m["osm_cx"], m["osm_cy"], m["fid"]))
        changes["pin_snapped"] = {"from": [m["pin_lng"], m["pin_lat"]],
                                    "to":   [m["osm_cx"], m["osm_cy"]],
                                    "moved_m": int(round(m["dist_m"]))}
    else:
        changes["pin_snapped"] = "no_change (already inside polygon)"

    # 2. Replace beach_polygons (upsert). source_osm_id is bigint.
    cur.execute("""
        INSERT INTO beach_polygons (fid, source_tier, source_osm_id, geom, area_m2,
                                     is_split_loser, computed_at)
        SELECT %s, 'osm', %s::bigint, o.geom, ST_Area(o.geom::geography), false, now()
          FROM osm_dog_parks o WHERE o.osm_id = %s
        ON CONFLICT (fid) DO UPDATE SET
          source_tier   = EXCLUDED.source_tier,
          source_osm_id = EXCLUDED.source_osm_id,
          geom          = EXCLUDED.geom,
          area_m2       = EXCLUDED.area_m2,
          is_split_loser = false,
          computed_at   = now()
    """, (m["fid"], m["osm_id"], m["osm_id"]))
    changes["polygon_replaced"] = {"new_area_m2": int(m["osm_area_m2"])}

    # 3. Upsert dog_policy_zones row tagging this OSM polygon as an
    # off-leash carve-out (signals to future codify cascade work that the
    # beach IS the carve-out — surrounding baseline rules don't apply).
    cur.execute("""
        INSERT INTO dog_policy_zones (category, name, source_agency, source_url,
                                       effective_dates, notes, geom, area_m2,
                                       loaded_at)
        SELECT 'pet_allowed_carveout', o.name, 'osm', %s,
               NULL, %s, o.geom, ST_Area(o.geom::geography), now()
          FROM osm_dog_parks o WHERE o.osm_id = %s
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (f"https://www.openstreetmap.org/way/{m['osm_id']}",
          f"Imported from osm_dog_parks for beach fid {m['fid']} ({m['beach_name']})",
          m["osm_id"]))
    z = cur.fetchone()
    changes["dog_policy_zone_id"] = z[0] if z else "exists"

    conn.commit()
    return changes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", help="Limit to one state")
    ap.add_argument("--fid", type=int, help="Limit to a single beach fid")
    ap.add_argument("--apply", action="store_true",
                    help="Actually write. Default is dry-run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Alias for default (no writes)")
    args = ap.parse_args()
    apply_mode = args.apply and not args.dry_run

    conn = _connect()
    try:
        matches = discover(conn, state=args.state, fid=args.fid)
        print(f"Tight matches: {len(matches)}\n")
        for m in matches:
            inside = "✓ pin inside" if m["pin_inside"] else f"{int(m['dist_m'])}m off"
            print(f"  fid={m['fid']:<10} {m['state']} {m['scoring_tier']:<8} "
                  f"{m['beach_name'][:32]:<34} ↔ {m['osm_name'][:30]:<32} "
                  f"({inside}, osm_area={int(m['osm_area_m2'])}m²)")

        if not apply_mode:
            print("\n[dry-run] no writes. Re-run with --apply.")
            return 0

        print("\n=== APPLYING ===")
        for m in matches:
            changes = apply_one(conn, m)
            print(f"\n  fid {changes['fid']} ({changes['name']}):")
            for k, v in changes.items():
                if k in ("fid","name"): continue
                print(f"    {k}: {v}")
        print(f"\nApplied to {len(matches)} beaches.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
