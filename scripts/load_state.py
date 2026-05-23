"""load_state.py — Phase A of the state-launch pipeline.

Single-entry-point loader for a state's external data + arena promote.

Usage:
  python scripts/load_state.py --state OR [--records 20] [--skip-overpass]
                                          [--skip-pad-us] [--no-publish]

Steps:
  1. Load PAD-US for the state via external_sources.py (registry-driven loader)
  2. Run Overpass query for natural=beach in the state → osm_landing
  3. Promote landing → arena (calls SQL function promote_osm_landing_to_arena)
  4. Run SQL run_full_pipeline_maintenance(state) — clusters, promotes to gold,
     enqueues extractions, publishes
  5. Optionally drain the extraction queue immediately (--drain-queue)

The loader is deliberately thin — it composes existing scripts and SQL
functions. Each step prints its result. Idempotent end-to-end.

State expansion to OR/WA/etc is now: install state in state_config (for
state_fp lookup), then `python scripts/load_state.py --state OR`. The
existing 5 OR seeds will be joined by any new beaches found.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import urllib.parse
import urllib.request
import json
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.common.ssl_compat import get_ssl_context  # noqa: E402

# Python 3.13+ strictness — see scripts/common/ssl_compat.py
_SSL_CTX = get_ssl_context()

load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# 2-letter → 2-digit FIPS for all 50 states + DC + PR/GU/VI.
# Used by fetch_overpass_beaches to clip elements to the state polygon.
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "PR": "72", "GU": "66", "VI": "78",
}

# State bbox catalog. Expand as needed; OR comes from the existing
# OR seeds' rough envelope.
STATE_BBOX = {
    "CA": (32.50, -124.50, 42.00, -114.10),
    "OR": (41.99, -124.60, 46.30, -116.45),
    "WA": (45.50, -124.80, 49.00, -116.90),
    "RI": (41.14, -71.91, 42.02, -71.10),
    "MA": (41.20, -73.50, 42.90, -69.90),
    "DE": (38.45, -75.80, 39.85, -75.05),
}


def step(msg: str):
    print(f"\n=== {msg} ===")


def run_noaa_stations(state: str):
    """Phase A.0 — load NOAA tide-prediction stations for the state.
    Idempotent edge-function call."""
    step(f"Phase A.0 - NOAA stations load for {state}")
    url = os.environ["SUPABASE_URL"].rstrip("/") + "/functions/v1/admin-load-noaa-stations"
    body = json.dumps({"state": state}).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "x-admin-secret": os.environ["ADMIN_SECRET"],
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        },
    )
    try:
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=120) as resp:
            print(f"  {resp.read().decode()}")
    except urllib.error.HTTPError as e:
        print(f"  NOAA loader failed: {e.code} {e.read().decode()[:200]}")
    except Exception as e:
        print(f"  NOAA loader error: {e}")


def run_padus(state: str):
    step(f"Phase A.1 - PAD-US load for {state}")
    cmd = ["python", str(ROOT / "scripts" / "external_sources.py"),
           "load", "pad_us", "--state", state]
    subprocess.run(cmd, check=False)


def fetch_overpass_beaches(state: str, max_records: int | None = None):
    """Run Overpass query for natural=beach in state bbox and insert into osm_landing."""
    step(f"Phase A.2 — Overpass natural=beach for {state}")
    if state not in STATE_BBOX:
        print(f"  no bbox catalog for {state}; skipping Overpass")
        return 0
    s, w, n, e = STATE_BBOX[state]
    bbox = f"{s},{w},{n},{e}"
    query = f"""
[out:json][timeout:120];
(
  node["natural"="beach"]({bbox});
  way["natural"="beach"]({bbox});
  relation["natural"="beach"]({bbox});
);
out body geom;
"""
    print(f"  bbox={bbox}")
    data = urllib.parse.urlencode({"data": query}).encode()
    req = urllib.request.Request(
        OVERPASS_URL, data=data, method="POST",
        headers={
            "User-Agent": "dog-beach-scout/1.0 (load_state.py)",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=130) as resp:
            payload = json.loads(resp.read())
    except Exception as e:
        # RE-RAISE (2026-05-22 LATE): formerly swallowed and returned 0,
        # which made the outer bulk_load_overpass.py write
        # external_source_status='ok' with rows=0 — masking SSL/network
        # failures as silent successes. Now the outer loader sees the
        # exception and correctly records 'failed'.
        print(f"  Overpass error: {e}")
        raise

    elements = payload.get("elements", [])
    if max_records:
        elements = elements[:max_records]
    print(f"  fetched {len(elements)} OSM elements")

    # Insert into osm_landing — append only, ON CONFLICT DO NOTHING
    # Clip to state polygon: bbox can bleed across state lines (Overpass
    # bbox=(s,w,n,e) is rectangular; OR's east edge -116.45 catches Snake
    # River basin in ID, and -124.6 west edge catches CA Imperial).
    inserted = 0
    skipped_oos = 0
    with psycopg2.connect(**PG) as conn:
        conn.autocommit = True
        # Resolve state geom once. Counties union as fallback when states is missing.
        # 2026-05-22 LATE: was {CA/OR/WA-only} dict; expanded to all 50 + DC
        # so polygon clipping works for any state (not silently skipped, which
        # would cause out-of-state OSM elements to leak into osm_landing).
        state_fp = STATE_FIPS.get(state)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                select st_union(geom) as geom from public.counties where state_fp = %s
            """, (state_fp,))
            row = cur.fetchone()
            state_geom = row["geom"] if row else None
        with conn.cursor() as cur:
            for el in elements:
                etype = el.get("type")
                eid = el.get("id")
                if not etype or eid is None:
                    continue
                tags = el.get("tags") or {}
                # `name` is a generated column on osm_landing (tags->>'name'); skip it.
                geom_wkt = None
                lat_v = lon_v = None
                if etype == "node":
                    if "lat" in el and "lon" in el:
                        lat_v, lon_v = el["lat"], el["lon"]
                        geom_wkt = f"POINT({lon_v} {lat_v})"
                else:
                    geom_arr = el.get("geometry", [])
                    if geom_arr:
                        coords = ",".join(f"{p['lon']} {p['lat']}" for p in geom_arr)
                        if etype == "way":
                            if geom_arr[0] == geom_arr[-1]:
                                geom_wkt = f"POLYGON(({coords}))"
                            else:
                                geom_wkt = f"LINESTRING({coords})"
                if not geom_wkt:
                    continue
                # State polygon clip: skip elements outside the requested state.
                if state_geom is not None:
                    try:
                        cur.execute(
                            "select st_contains(%s::geometry, ST_SetSRID(ST_GeomFromText(%s), 4326))",
                            (state_geom, geom_wkt),
                        )
                        if not cur.fetchone()[0]:
                            skipped_oos += 1
                            continue
                    except Exception as e:
                        print(f"  containment check failed for {etype}/{eid}: {e}")
                        conn.rollback()
                        continue
                try:
                    cur.execute("""
                        insert into public.osm_landing
                          (type, id, lat, lon, tags, geom_full, fetched_at, fetched_by, is_active)
                        values (%s, %s, %s, %s, %s::jsonb,
                                ST_SetSRID(ST_GeomFromText(%s), 4326),
                                now(), 'load_state.py', true)
                        on conflict (type, id, fetched_at) do nothing
                    """, (etype, eid, lat_v, lon_v, json.dumps(tags), geom_wkt))
                    inserted += cur.rowcount
                except Exception as e:
                    print(f"  insert failed for {etype}/{eid}: {e}")
                    conn.rollback()
    if skipped_oos:
        print(f"  skipped {skipped_oos} out-of-state elements (bbox bleed)")
    print(f"  inserted {inserted} new osm_landing rows")
    return inserted


def fetch_overpass_landscape_features(state: str, max_records: int | None = None):
    """Fetch beach-adjacent landscape features + dog-friendly POIs from
    Overpass into osm_landing. Replaces live Overpass calls from the
    description generator (generate_beach_descriptions.py was hitting
    Overpass 504s mid-run on these exact tags).

    Tag classes (all within state bbox, state-polygon clipped on insert):
      - natural=cliff|reef|peninsula|cape|bay  (landscape backdrops)
      - man_made=pier|jetty|breakwater|groyne|lighthouse  (named structures)
      - waterway=stream|river (named only)  (creek-mouth anchors)
      - leisure=marina  (harbor-adjacent context)
      - amenity=cafe|restaurant|pub|fast_food + dog=yes  (make-a-day-of-it extensions)
      - leisure=dog_park (named)  (catches some not loaded by bulk_load_dog_amenities)

    All written to osm_landing with full geometry per Franz's pipeline
    principle (out body geom). Idempotent — ON CONFLICT DO NOTHING.
    """
    step(f"Phase A.3 — Overpass landscape + dog-friendly POIs for {state}")
    if state not in STATE_BBOX:
        print(f"  no bbox catalog for {state}; skipping landscape fetch")
        return 0
    s, w, n, e = STATE_BBOX[state]
    bbox = f"{s},{w},{n},{e}"
    query = f"""
[out:json][timeout:180];
(
  way["natural"~"^(cliff|reef|peninsula|cape|bay|arch|stack|sand_dunes|dune|cave_entrance|wetland)$"]({bbox});
  node["natural"~"^(arch|stack|cave_entrance)$"]({bbox});
  way["man_made"~"^(pier|jetty|breakwater|groyne|lighthouse|bridge)$"]["name"]({bbox});
  node["man_made"~"^(pier|jetty|lighthouse)$"]({bbox});
  way["historic"="lighthouse"]({bbox});
  node["historic"="lighthouse"]({bbox});
  way["waterway"~"^(stream|river)$"]["name"]({bbox});
  way["leisure"~"^(marina|swimming_area|fishing)$"]({bbox});
  node["tourism"="viewpoint"]["name"]({bbox});
  node["amenity"~"^(cafe|restaurant|pub|fast_food)$"]["dog"="yes"]({bbox});
  way["amenity"~"^(cafe|restaurant|pub|fast_food)$"]["dog"="yes"]({bbox});
  way["leisure"="dog_park"]["name"]({bbox});
  way["highway"="cycleway"]["name"]({bbox});
);
out body geom;
"""
    print(f"  bbox={bbox}")
    data = urllib.parse.urlencode({"data": query}).encode()
    req = urllib.request.Request(
        OVERPASS_URL, data=data, method="POST",
        headers={
            "User-Agent": "dog-beach-scout/1.0 (load_state.py landscape)",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=200) as resp:
            payload = json.loads(resp.read())
    except Exception as e:
        # Re-raise per same fix as fetch_overpass_beaches; see comment there.
        print(f"  Overpass error: {e}")
        raise

    elements = payload.get("elements", [])
    if max_records:
        elements = elements[:max_records]
    print(f"  fetched {len(elements)} OSM elements")

    # NOTE: per-row state-polygon containment check removed (Franz, 2026-05-12).
    # MA bbox alone produced 95k OSM elements; per-row ST_Intersects round-trip
    # was the bottleneck. Downstream queries via get_physical_features_for_beach
    # and get_dog_friendly_pois_for_beach filter with ST_DWithin against a
    # specific beach's geom, so out-of-state bbox-bleed pollution is
    # functionally invisible. Provenance: fetched_by labels the loader.
    #
    # INSERTs are batched via execute_values (page_size=1000) instead of per-row
    # — drops 95k-row insert from ~80 min to ~1-2 min across the Supabase pooler.
    rows: list[tuple] = []
    for el in elements:
        etype = el.get("type"); eid = el.get("id")
        if not etype or eid is None: continue
        tags = el.get("tags") or {}
        geom_wkt = None; lat_v = lon_v = None
        if etype == "node":
            if "lat" in el and "lon" in el:
                lat_v, lon_v = el["lat"], el["lon"]
                geom_wkt = f"POINT({lon_v} {lat_v})"
        else:
            geom_arr = el.get("geometry", [])
            if geom_arr:
                coords = ",".join(f"{p['lon']} {p['lat']}" for p in geom_arr)
                if etype == "way":
                    if geom_arr[0] == geom_arr[-1] and len(geom_arr) >= 4:
                        geom_wkt = f"POLYGON(({coords}))"
                    else:
                        geom_wkt = f"LINESTRING({coords})"
        if not geom_wkt: continue
        rows.append((etype, eid, lat_v, lon_v, json.dumps(tags), geom_wkt))

    print(f"  prepared {len(rows)} rows for batch insert")
    inserted = 0
    with psycopg2.connect(**PG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = '600s'")
            try:
                psycopg2.extras.execute_values(cur, """
                    insert into public.osm_landing
                      (type, id, lat, lon, tags, geom_full, fetched_at, fetched_by, is_active)
                    values %s
                    on conflict (type, id, fetched_at) do nothing
                """, rows,
                template="(%s, %s, %s, %s, %s::jsonb, "
                         "ST_SetSRID(ST_GeomFromText(%s), 4326), "
                         "now(), 'load_state.py:landscape', true)",
                page_size=1000)
                inserted = cur.rowcount
            except Exception as e:
                print(f"  batch insert failed: {e}")
    print(f"  inserted {inserted} new osm_landing rows")
    return inserted


def call_sql(fn_call: str):
    with psycopg2.connect(**PG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"select * from {fn_call}")
            return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="Two-letter state code (CA, OR, WA)")
    ap.add_argument("--records", type=int, default=None,
                    help="Cap Overpass to N records for testing (default: all)")
    ap.add_argument("--skip-pad-us", action="store_true")
    ap.add_argument("--skip-overpass", action="store_true")
    ap.add_argument("--skip-noaa", action="store_true")
    ap.add_argument("--no-publish", action="store_true",
                    help="Stop after extract; skip Phase D consumer-promote")
    ap.add_argument("--drain-queue", action="store_true",
                    help="After load, drain the extraction queue inline")
    ap.add_argument("--queue-budget", type=float, default=5.00)
    args = ap.parse_args()

    state = args.state.upper()

    # Phase A - data sources
    if not args.skip_noaa:
        run_noaa_stations(state)
    if not args.skip_pad_us:
        run_padus(state)
    if not args.skip_overpass:
        fetch_overpass_beaches(state, args.records)

    # Phase B - promote landing -> arena -> gold (init only, no publish if --no-publish)
    step(f"Phase B - promote landing -> arena -> gold for {state}")
    poi_rec  = call_sql("public.promote_poi_landing_to_arena()")[0]
    osm_rec  = call_sql("public.promote_osm_landing_to_arena()")[0]
    name_rec = call_sql("public.refresh_arena_names_from_osm_landing()")[0]
    print(f"  POI promoted: {poi_rec.get('promoted')}")
    print(f"  OSM promoted: {osm_rec.get('promoted')}")
    print(f"  Names refreshed: {name_rec.get('arena_rows_updated')}")

    # Run cluster + extras + promote_to_gold for THIS state's fids only.
    # run_full_pipeline_maintenance also calls refire_missing_bep across all
    # states' backlog, which can timeout when launching a new state. Stick to
    # the state-scoped orchestrator here; the nightly maintenance cron handles
    # cross-state cleanup separately.
    rec = call_sql(f"public.run_pipeline_for_state('{state}')")[0]
    if args.no_publish:
        print(f"  --no-publish: state-scoped orchestrator already stops after BEP enrichment")

    print(f"  Pipeline result: {dict(rec)}")

    # Phase C — drain queue if requested
    if args.drain_queue:
        step("Phase C — drain extraction queue")
        cmd = ["python", str(ROOT / "scripts" / "process_extraction_queue.py"),
               "--max-fids", str(args.records or 50),
               "--budget", str(args.queue_budget)]
        subprocess.run(cmd, check=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
