#!/usr/bin/env python3
"""spatial_filter_foreign_regions.py — geocode + distance pass for the
zone_rules cross-beach residual (2026-06-30).

The name-based guard (_region_is_foreign_to_beach, mig 20260630d) catches
SHARED-source contamination (a region carried on >=2 beaches) but misses
SINGLE-beach contamination — a multi-place ordinance attached to ONE beach
(Otter Cove -> Berwick Park; Rosemont -> Bellevue Botanical Garden; Capitola ->
Jade Street Park). Those regions are unique-to-one-beach + not self-named, so
the name guard keeps them; only geometry can tell a foreign named place from a
genuine sub-feature.

For each candidate region (non-self-named, name-guard-kept), geocode the place
name (Google Places Text Search, biased to the beach) and, when the resolved
place's NAME matches the region AND it sits clearly far from the beach point
(--far-m), set beach_policy_source.region_is_foreign = true (mig 20260630e),
then re-promote the beach so the injector drops it.

CONSERVATIVE: descriptive / regulatory sub-zones ("roped-off nesting areas",
"Ocean Shore State Recreation Area - prohibition zone", "between X and Y") don't
name-match a geocoded place, so they are never flagged.

Usage:
  python scripts/spatial_filter_foreign_regions.py             # dry-run
  python scripts/spatial_filter_foreign_regions.py --apply
  python scripts/spatial_filter_foreign_regions.py --far-m 1500 --limit 20
"""
from __future__ import annotations
import sys, os, re, argparse, time
from math import radians, sin, cos, asin, sqrt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests  # noqa: E402
import psycopg2.extras  # noqa: E402
from scripts.common.db import connect  # noqa: E402  (also loads pipeline/.env)

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")


# Oregon's "Ocean Shore State Recreation Area" is the statewide ocean-shore
# designation on every OR beach (the governing rule, working fine). It geocodes
# to one arbitrary far point, so the spatial test can't handle it — skip it
# entirely and don't touch it (Franz 2026-06-30).
LEGIT_REGULATORY_RE = re.compile(r'ocean shore', re.I)


def _head(region_name: str) -> str:
    """The place-name part, before any descriptive qualifier."""
    for sep in ('—', '–', ' - ', '(', ','):
        if sep in region_name:
            region_name = region_name.split(sep, 1)[0]
    return region_name.strip()


def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9 ]', ' ', s.lower()).strip()


def places_search(query: str, lat: float, lng: float):
    if not API_KEY:
        return None
    try:
        r = requests.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": API_KEY,
                "X-Goog-FieldMask": "places.displayName,places.location,places.formattedAddress,places.viewport",
            },
            json={
                "textQuery": query,
                "locationBias": {"circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": 50000.0}},
            },
            timeout=20,
        )
        if r.status_code != 200:
            return None
        places = (r.json() or {}).get("places") or []
        return places[0] if places else None
    except Exception:
        return None


def name_matches(head: str, display: str) -> bool:
    """Confirm the geocoded place IS the named region (guards against geocoding
    a descriptive phrase to some unrelated place)."""
    h, d = _norm(head), _norm(display)
    if len(h) < 5 or len(d) < 3:
        return False
    if h in d or d in h:
        return True
    ht = [t for t in h.split() if len(t) > 2]
    if not ht:
        return False
    dt = set(d.split())
    overlap = sum(1 for t in ht if t in dt) / len(ht)
    return overlap >= 0.8


def haversine_m(lat1, lng1, lat2, lng2):
    R = 6371000.0
    dlat = radians(lat2 - lat1); dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    return 2 * R * asin(sqrt(a))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="set region_is_foreign + re-promote (else dry-run)")
    ap.add_argument("--far-m", type=float, default=1500.0, help="distance (m) beyond which a name-matched place is foreign")
    ap.add_argument("--max-span-deg", type=float, default=0.05,
                    help="skip places whose geocoded viewport spans more than this (deg ~ 0.05=5.5km) — "
                         "filters statewide/diffuse designations (Oregon Ocean Shore SRA) that geocode to a far point")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    if not API_KEY:
        print("GOOGLE_MAPS_API_KEY missing", file=sys.stderr)
        return 1

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT g.fid, coalesce(g.display_name_override,g.name) AS beach,
               ST_Y(g.geom::geometry) AS lat, ST_X(g.geom::geometry) AS lng,
               e->>'name' AS region_name
          FROM beach_dog_policy bdp
          JOIN beaches_gold g ON g.fid=bdp.arena_group_id
          CROSS JOIN LATERAL jsonb_array_elements(bdp.zone_rules->'regions') e
         WHERE g.is_active AND g.scoring_tier IN ('daily','hourly') AND g.state IN ('CA','OR','WA')
           AND e->>'name' IS NOT NULL
           AND NOT public._region_is_foreign_to_beach(e->>'name', g.fid)
           AND lower(e->>'name') NOT LIKE '%'||lower(coalesce(g.display_name_override,g.name))||'%'
         ORDER BY g.fid
    """)
    rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]
    print(f"candidates: {len(rows)} (apply={args.apply}, far_m={args.far_m})", flush=True)

    flagged: list[tuple[int, str]] = []
    n_foreign = n_kept = n_nogeo = n_regulatory = 0
    for r in rows:
        # Never flag a governing regulatory designation (e.g. Oregon Ocean Shore).
        if LEGIT_REGULATORY_RE.search(r["region_name"]):
            n_regulatory += 1
            continue
        head = _head(r["region_name"])
        pl = places_search(head, r["lat"], r["lng"])
        time.sleep(0.05)
        if not pl:
            n_nogeo += 1
            continue
        disp = (pl.get("displayName") or {}).get("text") or ""
        if not name_matches(head, disp):
            n_kept += 1
            continue
        loc = pl.get("location") or {}
        if loc.get("latitude") is None:
            n_kept += 1
            continue
        # Discrete-place gate: a foreign place is a specific facility (small
        # viewport). Statewide/diffuse designations (Oregon Ocean Shore SRA)
        # have a huge viewport and geocode to a far representative point — skip
        # them. Missing viewport → can't confirm discrete → skip (conservative).
        vp = pl.get("viewport") or {}
        lo, hi = vp.get("low") or {}, vp.get("high") or {}
        if lo.get("latitude") is None or hi.get("latitude") is None:
            n_kept += 1
            continue
        span = max(abs(hi["latitude"] - lo["latitude"]), abs(hi["longitude"] - lo["longitude"]))
        if span > args.max_span_deg:
            n_kept += 1
            continue
        dist = haversine_m(r["lat"], r["lng"], loc["latitude"], loc["longitude"])
        if dist > args.far_m:
            n_foreign += 1
            flagged.append((r["fid"], r["region_name"]))
            print(f"  FOREIGN  [{r['beach'][:20]:20}] {head[:30]:30} -> {disp[:24]:24} {dist/1000:5.1f}km span={span:.3f}", flush=True)
        else:
            n_kept += 1

    print(f"\nforeign={n_foreign}  kept={n_kept}  no_geocode={n_nogeo}  regulatory_skipped={n_regulatory}", flush=True)

    if args.apply and flagged:
        for fid, rn in flagged:
            cur.execute("UPDATE public.beach_policy_source SET region_is_foreign=true "
                        "WHERE beach_fid=%s AND lower(region_name)=lower(%s)", (fid, rn))
        fids = sorted({fid for fid, _ in flagged})
        for fid in fids:
            cur.execute("SELECT public._promote_zone_rules_for_fid(%s)", (fid,))
        conn.commit()
        print(f"applied: flagged {len(flagged)} regions, re-promoted {len(fids)} beaches", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
