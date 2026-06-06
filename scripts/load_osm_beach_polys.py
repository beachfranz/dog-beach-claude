"""load_osm_beach_polys.py — load OSM natural=beach polygons by state.

Pulls every way + relation tagged natural=beach within a state's bbox
from the Overpass API, assembles geometry, and UPSERTs into
public.osm_beach_polys. Multi-state: accept --states CA,OR,WA,... .
Idempotent — UPSERT on (osm_type, osm_id); running for a new state adds
its rows; re-running for an existing state refreshes them.

Why this exists: our existing osm_features table loaded beach features as
POINT centroids only — zero polygons. We need actual beach footprints
for area_m2 (parity with dog_parks_gold) and for consumer maps so beach
extents render correctly (vs uniform-circle markers losing the "100m
cove vs 5km strip" signal). See spec memo
project_zone_rules_design_locked.md catalog-vs-zones rule for how this
relates to gold-fid attribution.

Usage:
  python scripts/load_osm_beach_polys.py                  # default: CA
  python scripts/load_osm_beach_polys.py --states OR
  python scripts/load_osm_beach_polys.py --states CA,OR,WA,MA,NH,...
  python scripts/load_osm_beach_polys.py --states ALL_MVP # all MVP+ coastal+lake states
  python scripts/load_osm_beach_polys.py --states CA --dry-run

~1-3 minutes runtime per state depending on Overpass load.
"""

from __future__ import annotations
import argparse
import io
import json
import os
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

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_MIRROR = "https://overpass.kumi.systems/api/interpreter"

# Per-state bboxes: south,west,north,east (rough envelopes that include
# coast + inland lakes). Overpass natural=beach picks up both.
BBOXES = {
    "AL":     "30.2,-88.5,35.0,-84.9",
    "CA":     "32.5,-124.5,42.0,-114.1",
    "DE":     "38.4,-75.8,39.8,-75.0",
    "FL":     "24.5,-87.6,31.0,-80.0",
    "MA":     "41.2,-73.5,42.9,-69.9",
    "MD":     "37.9,-79.5,39.7,-75.0",
    "MI":     "41.7,-90.4,48.3,-82.4",
    "NH":     "42.7,-72.6,45.3,-70.6",
    "OH":     "38.4,-84.8,42.0,-80.5",
    "OR":     "41.9,-124.6,46.3,-116.5",
    "RI":     "41.1,-71.9,42.0,-71.1",
    "UT":     "36.9,-114.1,42.0,-109.0",
    "VA":     "36.5,-83.7,39.5,-75.2",
    "WA":     "45.5,-124.8,49.1,-116.9",
    # Test slices
    "SoCal":  "32.5,-121.5,35.0,-117.0",
    "Marin":  "37.8,-123.0,38.3,-122.4",
}

# MVP+ states (catalog has active beaches) — used by --states ALL_MVP.
ALL_MVP_STATES = ["AL", "CA", "DE", "FL", "MA", "MD", "MI", "NH",
                  "OH", "OR", "RI", "UT", "VA", "WA"]


def overpass_query(bbox: str) -> str:
    """Query for all natural=beach ways and relations within bbox.

    `out geom` includes inline coordinates per node so we don't need a
    second roundtrip to resolve node IDs.
    """
    return f"""
[out:json][timeout:300];
(
  way["natural"="beach"]({bbox});
  relation["natural"="beach"]({bbox});
);
out geom;
""".strip()


def fetch_overpass(query: str, url: str = OVERPASS_URL, max_attempts: int = 4) -> dict:
    """Overpass POST with simple retry/backoff on 429/504. ~60s between attempts."""
    import time as _time
    data = urllib.parse.urlencode({"data": query}).encode("utf-8")
    last_err = None
    for attempt in range(1, max_attempts + 1):
        print(f"  POST {url}  ({len(query)} chars query) attempt {attempt}/{max_attempts}", flush=True)
        try:
            req = urllib.request.Request(url, data=data,
                                         headers={"User-Agent": "dog-beach-scout/1.0"})
            with urllib.request.urlopen(req, timeout=600) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 503, 504) and attempt < max_attempts:
                wait = 30 * attempt  # 30s, 60s, 90s
                print(f"  HTTP {e.code} — backing off {wait}s")
                _time.sleep(wait)
                continue
            raise
    raise last_err if last_err else RuntimeError("overpass fetch failed")


def way_to_geojson_polygon(way: dict) -> dict | None:
    """An OSM way with closed geometry becomes a Polygon. Open ways are skipped."""
    geom = way.get("geometry") or []
    if len(geom) < 4:  # need at least 3 distinct points + closing
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geom]
    if coords[0] != coords[-1]:
        coords.append(coords[0])  # close the ring if OSM didn't
    return {"type": "Polygon", "coordinates": [coords]}


def relation_to_geojson_multipolygon(rel: dict) -> dict | None:
    """OSM multipolygon relation -> GeoJSON MultiPolygon.

    Strategy: collect 'outer' member ways, treat each as a separate ring;
    'inner' members become holes in the closest containing outer (we use
    the simple heuristic of attaching all inners to the largest outer).
    For v1 we ignore complex multi-ring relations and just stitch outers.
    Ways from members come pre-resolved with `out geom`.
    """
    outers = []
    inners = []
    for m in rel.get("members") or []:
        if m.get("type") != "way":
            continue
        coords = [[pt["lon"], pt["lat"]] for pt in (m.get("geometry") or [])]
        if len(coords) < 4:
            continue
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        if m.get("role") == "inner":
            inners.append(coords)
        else:
            outers.append(coords)
    if not outers:
        return None
    polys = [[outer] for outer in outers]
    # Naive: all inners attached to first outer. Most beach relations are
    # single-outer anyway; complex ones can be revisited later.
    if inners and polys:
        polys[0].extend(inners)
    return {"type": "MultiPolygon", "coordinates": polys}


def features_from_overpass(raw: dict) -> list[dict]:
    out = []
    for el in raw.get("elements") or []:
        t = el.get("type")
        if t == "way":
            geom = way_to_geojson_polygon(el)
        elif t == "relation":
            geom = relation_to_geojson_multipolygon(el)
        else:
            continue
        if not geom:
            continue
        tags = el.get("tags") or {}
        out.append({
            "osm_id": el["id"],
            "osm_type": t,
            "name": tags.get("name"),
            "name_short": tags.get("short_name"),
            "tags": tags,
            "geom_geojson": geom,
        })
    return out


def ensure_table(conn):
    """Create osm_beach_polys + indexes if not present (idempotent)."""
    with conn.cursor() as cur:
        cur.execute("""
            create table if not exists public.osm_beach_polys (
              id          bigserial primary key,
              osm_id      bigint not null,
              osm_type    text   not null check (osm_type in ('way','relation')),
              name        text,
              name_short  text,
              tags        jsonb not null,
              geom        geometry(Geometry, 4326) not null,
              area_m2     double precision,
              loaded_at   timestamptz default now(),
              unique (osm_type, osm_id)
            );
            create index if not exists osm_beach_polys_geom_gix
              on public.osm_beach_polys using gist (geom);
            create index if not exists osm_beach_polys_name_idx
              on public.osm_beach_polys (lower(name));
        """)
        conn.commit()


def upsert_features(features: list[dict], dry_run: bool = False):
    if dry_run:
        print(f"  --dry-run: not writing. {len(features)} features ready.")
        return

    with psycopg2.connect(**PG) as conn:
        ensure_table(conn)
        # The osm_beach_polys after-change trigger calls refresh_beach_polygons()
        # which creates `_split_losers` as a temp table. Per-row firing during a
        # batch insert causes a name collision after the first row. Disable the
        # trigger for the load, then fire refresh_beach_polygons() once at the
        # end so the downstream beach_polygons table stays consistent.
        with conn.cursor() as cur:
            cur.execute("alter table public.osm_beach_polys disable trigger tg_after_change_refresh_polygons;")
            conn.commit()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, """
                    insert into public.osm_beach_polys
                      (osm_id, osm_type, name, name_short, tags, geom, area_m2, loaded_at)
                    values (
                      %(osm_id)s, %(osm_type)s, %(name)s, %(name_short)s,
                      %(tags)s::jsonb,
                      st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326)),
                      st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom)s), 4326))::geography),
                      now()
                    )
                    on conflict (osm_type, osm_id) do update
                      set name      = excluded.name,
                          tags      = excluded.tags,
                          geom      = excluded.geom,
                          area_m2   = excluded.area_m2,
                          loaded_at = excluded.loaded_at
                """, [{
                    "osm_id": f["osm_id"],
                    "osm_type": f["osm_type"],
                    "name": f["name"],
                    "name_short": f["name_short"],
                    "tags": json.dumps(f["tags"]),
                    "geom": json.dumps(f["geom_geojson"]),
                } for f in features], page_size=100)
                conn.commit()
        finally:
            with conn.cursor() as cur:
                cur.execute("alter table public.osm_beach_polys enable trigger tg_after_change_refresh_polygons;")
                conn.commit()

        with conn.cursor() as cur:
            cur.execute("select count(*), count(name), avg(area_m2)::int from public.osm_beach_polys")
            n, n_named, avg_area = cur.fetchone()
            print(f"  table total: {n} rows ({n_named} named) avg area {avg_area:,} m2")


def _write_status(state: str, row_count: int, status: str = 'ok', notes: str = ''):
    """Record per-state load in external_source_status so the launch
    pipeline's `ensure_osm_beach_polys` phase can skip a freshly-loaded state."""
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("""
            insert into public.external_source_status
              (source, state, last_loaded_at, row_count, status, notes)
            values ('osm_beach_polys', %s, now(), %s, %s, %s)
            on conflict (source, state) do update
              set last_loaded_at = excluded.last_loaded_at,
                  row_count      = excluded.row_count,
                  status         = excluded.status,
                  notes          = excluded.notes
        """, (state, row_count, status, notes))
        conn.commit()


def fetch_and_upsert_state(state: str, dry_run: bool = False, mirror: bool = False):
    """Fetch + upsert OSM beach polygons for one state."""
    bbox = BBOXES.get(state)
    if not bbox:
        print(f"[{state}] no bbox configured; skipping")
        return
    print(f"[{state}] fetching natural=beach for bbox={bbox}")
    url = OVERPASS_MIRROR if mirror else OVERPASS_URL
    raw = fetch_overpass(overpass_query(bbox), url=url)
    print(f"[{state}]   Overpass returned {len(raw.get('elements') or [])} elements")

    features = features_from_overpass(raw)
    n_way = sum(1 for f in features if f["osm_type"] == "way")
    n_rel = sum(1 for f in features if f["osm_type"] == "relation")
    n_named = sum(1 for f in features if f["name"])
    print(f"[{state}]   built {len(features)} polygons ({n_way} ways, {n_rel} relations, {n_named} named)")

    if not features:
        print(f"[{state}]   nothing to load")
        if not dry_run:
            _write_status(state, 0, status='skipped',
                          notes='Overpass returned no polygons in this bbox')
        return

    upsert_features(features, dry_run=dry_run)
    if not dry_run:
        _write_status(state, len(features), status='ok')


def main():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", default="CA",
                    help="Comma-separated state codes (e.g. CA,OR,WA) or ALL_MVP")
    ap.add_argument("--bbox", default=None,
                    help="DEPRECATED: legacy single-bbox arg (use --states)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--mirror", action="store_true",
                    help="Use overpass.kumi.systems mirror (sometimes faster)")
    args = ap.parse_args()

    if args.bbox is not None:
        states = [args.bbox]
    elif args.states == "ALL_MVP":
        states = ALL_MVP_STATES
    else:
        states = [s.strip() for s in args.states.split(",") if s.strip()]

    import time as _time
    print(f"Loading OSM beach polygons for {len(states)} state(s): {','.join(states)}")
    for i, st in enumerate(states):
        if i > 0:
            print("  (pause 20s between states to be polite to Overpass)")
            _time.sleep(20)
        fetch_and_upsert_state(st, dry_run=args.dry_run, mirror=args.mirror)

    # Single end-of-run refresh of the downstream beach_polygons table.
    # Per-row firing was disabled during the bulk load to avoid temp-table
    # collisions; this catches up to the final state in one statement.
    if not args.dry_run:
        print("Firing refresh_beach_polygons() to sync downstream beach_polygons...")
        with psycopg2.connect(**PG) as conn:
            with conn.cursor() as cur:
                cur.execute("select public.refresh_beach_polygons();")
                conn.commit()
        print("  done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
