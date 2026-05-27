"""ingest_beach_discovery_queue.py — promote pending beach_discovery_queue rows
into public.arena, then call promote_to_gold() to land them in beaches_gold.

Mirrors scripts/ingest_dog_park_discovery_queue.py for the beach side.

Each new arena row:
  source_code = 'state_park_walk_v1' (or whatever source_handler suggests)
  source_id   = '<state>:<park-slug>:<beach-slug>'
  group_id    = own fid (single-row group; no upstream cluster)
  park_name   = parent_park_name from queue
  is_active   = true; admin_approved_inactive = false

Then promote_to_gold(ARRAY[<new fids>], p_score=false, p_publish=true)
lands them in beaches_gold with scoring_tier='none', is_scoreable=false
(per the existing promote_to_gold contract).

Idempotent — already-ingested rows are skipped (queue.status='ingested').

Usage:
  python scripts/ingest_beach_discovery_queue.py --state UT
  python scripts/ingest_beach_discovery_queue.py --state UT --dry-run
  python scripts/ingest_beach_discovery_queue.py --all
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect  # noqa: E402
from scripts.harvest.geocode import google_places_searchtext  # noqa: E402


# State bbox sanity-check for geocoded coordinates. If the place Google
# returns lands outside the state bbox, it's likely the wrong place
# (common when beach names are reused across states: "Sunset Beach" exists
# in CA, OR, FL, NC, etc.).
STATE_BBOX = {
    "AL": (30.14, -88.47, 35.01, -84.89),
    "AK": (54.5, -180.0, 71.5, -129.0),
    "AZ": (31.33, -114.82, 37.00, -109.05),
    "AR": (33.00, -94.62, 36.50, -89.65),
    "CA": (32.5, -124.5, 42.1, -114.0),
    "CO": (36.99, -109.06, 41.00, -102.04),
    "CT": (40.98, -73.73, 42.05, -71.79),
    "DE": (38.45, -75.79, 39.84, -75.05),
    "DC": (38.79, -77.12, 38.99, -76.91),
    "FL": (24.40, -87.64, 31.00, -80.03),
    "GA": (30.36, -85.61, 35.00, -80.84),
    "HI": (18.91, -160.25, 22.24, -154.81),
    "ID": (41.99, -117.24, 49.00, -111.04),
    "IL": (36.97, -91.51, 42.51, -87.50),
    "IN": (37.77, -88.10, 41.76, -84.78),
    "IA": (40.38, -96.64, 43.50, -90.14),
    "KS": (36.99, -102.05, 40.00, -94.59),
    "KY": (36.50, -89.57, 39.15, -81.96),
    "LA": (28.93, -94.04, 33.02, -88.82),
    "ME": (42.97, -71.10, 47.46, -66.94),
    "MD": (37.91, -79.49, 39.72, -75.05),
    "MA": (41.18, -73.51, 42.89, -69.86),
    "MI": (41.70, -90.42, 48.31, -82.41),
    "MN": (43.50, -97.24, 49.38, -89.49),
    "MS": (30.17, -91.66, 35.01, -88.10),
    "MO": (35.99, -95.77, 40.61, -89.10),
    "MT": (44.36, -116.05, 49.00, -104.04),
    "NE": (39.99, -104.05, 43.00, -95.31),
    "NV": (35.00, -120.01, 42.00, -114.04),
    "NH": (42.70, -72.56, 45.31, -70.61),
    "NJ": (38.93, -75.56, 41.36, -73.89),
    "NM": (31.33, -109.05, 37.00, -103.00),
    "NY": (40.50, -79.76, 45.02, -71.78),
    "NC": (33.84, -84.33, 36.59, -75.46),
    "ND": (45.94, -104.05, 49.00, -96.55),
    "OH": (38.40, -84.82, 41.98, -80.52),
    "OK": (33.62, -103.00, 37.00, -94.43),
    "OR": (41.99, -124.57, 46.30, -116.46),
    "PA": (39.72, -80.52, 42.27, -74.69),
    "RI": (41.13, -71.91, 42.02, -71.12),
    "SC": (32.03, -83.36, 35.22, -78.55),
    "SD": (42.48, -104.06, 45.95, -96.44),
    "TN": (34.98, -90.31, 36.68, -81.65),
    "TX": (25.84, -106.65, 36.50, -93.51),
    "UT": (36.99, -114.07, 42.00, -109.04),
    "VT": (42.73, -73.44, 45.02, -71.46),
    "VA": (36.54, -83.68, 39.47, -75.24),
    "WA": (45.54, -124.85, 49.00, -116.92),
    "WV": (37.20, -82.64, 40.64, -77.72),
    "WI": (42.49, -92.89, 47.08, -86.81),
    "WY": (40.99, -111.06, 45.01, -104.05),
}

STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"Washington DC",
    "FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois",
    "IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
    "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
    "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey",
    "NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota",
    "OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island",
    "SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas",
    "UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
}


def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def geocode_via_places(state: str, beach_name: str,
                       parent_park_name: str | None) -> dict | None:
    """Google Places searchText geocode. Returns dict with lat/lng/place_name
    if hit lands within the state bbox; None otherwise.

    Builds a query like '<beach_name> <parent_park_name> <STATE_FULL_NAME>'.
    The parent often disambiguates: 'Sunset Beach Great Salt Lake Utah' picks
    the UT one over the dozen other 'Sunset Beach' places nationwide."""
    state_name = STATE_NAMES.get(state, state)
    parent = parent_park_name or ""
    parent_token = parent if parent and "County" not in parent else ""
    parts = [beach_name]
    if parent_token:
        parts.append(parent_token)
    parts.append(state_name)
    query = " ".join(parts)

    place = google_places_searchtext(query)
    if not place:
        return None
    loc = place.get("location") or {}
    lat = loc.get("latitude")
    lng = loc.get("longitude")
    if lat is None or lng is None:
        return None

    bbox = STATE_BBOX.get(state)
    if bbox:
        s_lat, s_lng, n_lat, n_lng = bbox
        if not (s_lat <= lat <= n_lat and s_lng <= lng <= n_lng):
            # Hit is in a different state — wrong place. Reject.
            return {"lat": lat, "lng": lng,
                    "place_name": (place.get("displayName") or {}).get("text"),
                    "out_of_state": True}

    return {"lat": float(lat), "lng": float(lng),
            "place_name": (place.get("displayName") or {}).get("text"),
            "formatted_address": place.get("formattedAddress"),
            "place_id": place.get("id"),
            "out_of_state": False}


def ingest(state: str | None, dry_run: bool = False) -> dict:
    stats = {"pending": 0, "inserted_arena": 0, "promoted_gold": 0,
             "marked_duplicate": 0, "marked_needs_review": 0, "errors": 0}

    where_state = ""
    params: list = []
    if state:
        where_state = "AND state = %s"
        params.append(state)

    conn = connect(); conn.autocommit = False
    new_fids: list[int] = []
    try:
        cur = conn.cursor()
        # Bump statement timeout — the CPAD spatial join inside promote_to_gold
        # → build_beach_evidence → refresh_beach_polygon_membership can take
        # 30-90s for large coastal states (CA has 17k CPAD units; 50 new
        # beaches × 17k = 850k ST_DWithin ops).
        cur.execute("SET LOCAL statement_timeout = '300s'")

        cur.execute(f"""
          SELECT id, state, parent_park_name, parent_pad_us_unit_id,
                 parent_park_url, beach_name, source_url, source_handler,
                 lat, lng, best_similarity, best_match_fid, queue_meta
            FROM public.beach_discovery_queue
           WHERE status = 'pending' {where_state}
           ORDER BY id
        """, params)
        rows = cur.fetchall()
        stats["pending"] = len(rows)

        print(f"[ingest_beach_queue] pending rows: {len(rows)}", flush=True)
        if dry_run:
            for r in rows:
                print(f"  [dry] q_id={r[0]} {r[1]} | {r[2]} | {r[5]} | "
                      f"lat={r[8]} lng={r[9]} match_fid={r[11]}")
            return stats

        for r in rows:
            (qid, st, park_name, pad_unit, park_url, beach_name, source_url,
             source_handler, lat, lng, best_sim, best_match_fid, qmeta) = r

            # Final dedup check — re-evaluate against arena now (queue's
            # best_similarity could be stale).
            #
            # Dedup ONLY on BEACH-NAME similarity. Park-name matching would
            # collapse different beaches that share a state-park parent
            # (e.g., Cisco Beach + Rendezvous Beach both at Bear Lake SP).
            arena_match_fid = None
            if lat is not None and lng is not None:
                cur.execute("""
                  SELECT fid, name, similarity(coalesce(name,''), %s) AS sim,
                         ST_Distance(geom::geography,
                                     ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)
                           AS dist_m
                    FROM public.arena
                   WHERE geom IS NOT NULL
                     AND ST_DWithin(geom::geography,
                                    ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
                                    300)
                   ORDER BY similarity(coalesce(name,''), %s) DESC
                   LIMIT 1
                """, (beach_name, lng, lat, lng, lat, beach_name))
                m = cur.fetchone()
                if m and float(m[2]) > 0.70:
                    arena_match_fid = int(m[0])

            if arena_match_fid:
                cur.execute("""
                  UPDATE public.beach_discovery_queue
                     SET status = 'duplicate',
                         best_match_fid = %s,
                         updated_at = now()
                   WHERE id = %s
                """, (arena_match_fid, qid))
                stats["marked_duplicate"] += 1
                print(f"  [DUP] q_id={qid} {beach_name} -> arena fid={arena_match_fid}")
                continue

            # Google Places fallback when handler didn't supply coords
            # (operator catalog walker + Wikipedia walker both rely on this).
            geocode_source = "queue"
            if lat is None or lng is None:
                geo = geocode_via_places(st, beach_name, park_name)
                if geo is None:
                    cur.execute("""
                      UPDATE public.beach_discovery_queue
                         SET status = 'needs_review',
                             review_notes = 'google_places_no_match',
                             updated_at = now()
                       WHERE id = %s
                    """, (qid,))
                    stats["marked_needs_review"] += 1
                    stats["geocode_miss"] = stats.get("geocode_miss", 0) + 1
                    print(f"  [no-geo] q_id={qid} {beach_name}: Places no match")
                    continue
                if geo.get("out_of_state"):
                    cur.execute("""
                      UPDATE public.beach_discovery_queue
                         SET status = 'needs_review',
                             review_notes = 'google_places_out_of_state',
                             queue_meta = coalesce(queue_meta,'{}'::jsonb)
                                          || jsonb_build_object(
                                               'gp_place', %s::text,
                                               'gp_lat', %s::float,
                                               'gp_lng', %s::float),
                             updated_at = now()
                       WHERE id = %s
                    """, (geo.get("place_name"), geo["lat"], geo["lng"], qid))
                    stats["marked_needs_review"] += 1
                    stats["geocode_out_of_state"] = stats.get("geocode_out_of_state", 0) + 1
                    print(f"  [oos]  q_id={qid} {beach_name}: Places -> "
                          f"{geo.get('place_name')} but out of {st} bbox")
                    continue
                lat, lng = geo["lat"], geo["lng"]
                geocode_source = "google_places"
                stats["geocode_hit"] = stats.get("geocode_hit", 0) + 1
                print(f"  [geo] q_id={qid} {beach_name}: Places -> "
                      f"{geo.get('place_name')} @ ({lat:.5f},{lng:.5f})")
                # Re-run arena dedup with the freshly geocoded coords.
                cur.execute("""
                  SELECT fid, name, similarity(coalesce(name,''), %s) AS sim,
                         ST_Distance(geom::geography,
                                     ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)
                           AS dist_m
                    FROM public.arena
                   WHERE geom IS NOT NULL
                     AND ST_DWithin(geom::geography,
                                    ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
                                    300)
                   ORDER BY similarity(coalesce(name,''), %s) DESC
                   LIMIT 1
                """, (beach_name, lng, lat, lng, lat, beach_name))
                m2 = cur.fetchone()
                if m2 and float(m2[2]) > 0.70:
                    cur.execute("""
                      UPDATE public.beach_discovery_queue
                         SET status = 'duplicate',
                             best_match_fid = %s,
                             updated_at = now()
                       WHERE id = %s
                    """, (int(m2[0]), qid))
                    stats["marked_duplicate"] += 1
                    print(f"  [DUP-post-geo] q_id={qid} -> arena fid={int(m2[0])}")
                    continue

            # INSERT into arena. fid via sequence.
            source_id = f"{st}:{slugify(park_name)}:{slugify(beach_name)}"
            try:
                cur.execute("""
                  INSERT INTO public.arena
                    (name, address, lat, lon, source_code, source_id,
                     geom, is_active, admin_approved_inactive, park_name,
                     name_source, nav_lat, nav_lon, nav_source,
                     created_at, updated_at)
                  VALUES (%s, NULL, %s, %s, %s, %s,
                          ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                          true, false, %s,
                          %s, %s, %s, %s,
                          now(), now())
                  RETURNING fid
                """, (beach_name, lat, lng, "state_park_walk_v1", source_id,
                      lng, lat, park_name,
                      source_handler, lat, lng, source_handler))
                new_fid = cur.fetchone()[0]
                # Single-row group (no cluster).
                cur.execute("UPDATE public.arena SET group_id = %s WHERE fid = %s",
                            (new_fid, new_fid))
                # County PIP so promote_to_gold can derive state via
                # _infer_state_from_county_fips.
                cur.execute("""
                  UPDATE public.arena
                     SET county_fips = c.geoid,
                         county_name = c.name
                    FROM public.counties c
                   WHERE arena.fid = %s
                     AND ST_Contains(c.geom, arena.geom)
                """, (new_fid,))
                stats["inserted_arena"] += 1
                new_fids.append(new_fid)

                cur.execute("""
                  UPDATE public.beach_discovery_queue
                     SET status = 'ingested',
                         best_match_fid = %s,
                         geocode_method = %s,
                         queue_meta = coalesce(queue_meta,'{}'::jsonb)
                                      || jsonb_build_object('arena_fid', %s::bigint),
                         updated_at = now()
                   WHERE id = %s
                """, (new_fid, geocode_source, new_fid, qid))
                print(f"  [NEW] q_id={qid} {beach_name} -> arena.fid={new_fid}")
            except Exception as e:
                print(f"  [ERROR] q_id={qid} {beach_name}: {e}")
                stats["errors"] += 1
                conn.rollback()
                continue

        # Commit arena INSERTs BEFORE the expensive promote_to_gold so
        # that a downstream timeout doesn't roll back the discovered rows.
        conn.commit()

        # Promote in batches of 10 — each promote_to_gold call triggers
        # build_beach_evidence + refresh_beach_polygon_membership, which
        # does an N-beaches × M-CPAD-units spatial join. Per-batch keeps
        # the join below statement_timeout for large coastal states.
        BATCH = 10
        for i in range(0, len(new_fids), BATCH):
            chunk = new_fids[i:i + BATCH]
            try:
                cur.execute("SET LOCAL statement_timeout = '300s'")
                cur.execute("""
                  SELECT * FROM public.promote_to_gold(%s::bigint[],
                                                        p_score => false,
                                                        p_publish => true)
                """, (chunk,))
                cur.fetchall()
                stats["promoted_gold"] = stats.get("promoted_gold", 0) + len(chunk)
                conn.commit()
                print(f"[promote_to_gold] batch {i//BATCH+1} OK ({len(chunk)} fids)")
            except Exception as e:
                conn.rollback()
                stats["promote_errors"] = stats.get("promote_errors", 0) + len(chunk)
                print(f"[promote_to_gold] batch {i//BATCH+1} FAILED ({len(chunk)} fids): {e}")
                continue
    finally:
        conn.close()

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", help="Limit to one state (e.g. UT)")
    ap.add_argument("--all",   action="store_true",
                    help="Process all pending across states")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.state and not args.all:
        ap.error("specify --state <STATE> or --all")
    state = args.state if not args.all else None
    stats = ingest(state, dry_run=args.dry_run)
    print("\n=== SUMMARY ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
