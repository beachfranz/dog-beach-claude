"""external_sources.py — unified registry-driven loader for external GIS sources.

Polymorphic fetcher dispatch supports multiple upstream protocols:
  * fetcher_kind='arcgis_rest' — ArcGIS REST FeatureServer (PAD-US, USFWS Critical Habitat)
  * fetcher_kind='overpass'    — OSM Overpass API (osm_amenities, future OSM-driven sources)

Tier 1 sources (national, scriptable, well-defined):
  * pad_us                  — federal/state/local protected area boundaries
  * usfws_critical_habitat  — designated critical habitat for all federally listed species
  * osm_amenities           — OSM amenity nodes/ways near beaches (parking, toilets, showers, etc.)

Each source declares: protocol-specific endpoint config, target table, ID column(s),
schema mapper, TTL. Loader behavior:
  * Cache check: skip if max(loaded_at) for the scope is within TTL (override with --force).
  * Idempotent upsert by source-native ID(s).
  * Per-state when applicable; national or other-scope when not.

Usage:
  python scripts/external_sources.py list
  python scripts/external_sources.py status
  python scripts/external_sources.py load pad_us --state CA
  python scripts/external_sources.py load usfws_critical_habitat
  python scripts/external_sources.py load --all
  python scripts/external_sources.py load pad_us --state CA --force

Database connection: same pooler-url + SUPABASE_DB_PASSWORD pattern as
the rest of scripts/.
"""
from __future__ import annotations
import argparse
import io
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ============================================================================
# Setup
# ============================================================================
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

SSL_CTX = ssl.create_default_context()


# ============================================================================
# Fetcher: ArcGIS REST FeatureServer (paged GET)
# ============================================================================
PAGE_SIZE = 2000


def fetch_count_arcgis(endpoint: str, where: str) -> int:
    qs = urllib.parse.urlencode({"where": where, "returnCountOnly": "true", "f": "json"})
    url = f"{endpoint}/query?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "dog-beach-scout/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as r:
        return json.loads(r.read())["count"]


def fetch_page_arcgis(endpoint: str, where: str, offset: int,
                      page_size: int = PAGE_SIZE,
                      max_offset_deg: float | None = None) -> dict:
    """One paged GET against an ArcGIS FeatureServer query endpoint."""
    params = {
        "where": where, "outFields": "*", "f": "geojson", "outSR": 4326,
        "resultOffset": offset, "resultRecordCount": page_size,
    }
    if max_offset_deg is not None:
        params["maxAllowableOffset"] = max_offset_deg
    qs = urllib.parse.urlencode(params)
    url = f"{endpoint}/query?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "dog-beach-scout/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=180) as r:
        return json.loads(r.read())


# ============================================================================
# Fetcher: OSM Overpass API (single POST per scope)
# ============================================================================

def fetch_overpass(overpass_url: str, query: str, timeout_s: int = 600) -> list[dict]:
    """Execute an Overpass QL query. Returns list of OSM elements (nodes/ways/relations).

    Caller must construct a query that uses `out tags center;` so each way
    has a `center` field with lat/lon — we don't reconstruct geometry from
    member nodes here.
    """
    body = ("data=" + urllib.parse.quote(query)).encode("ascii")
    req = urllib.request.Request(
        overpass_url, method="POST", data=body,
        headers={
            "User-Agent": "dog-beach-scout/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout_s) as r:
        payload = json.loads(r.read())
    return payload.get("elements", []) or []


# ============================================================================
# Schema mappers — feature -> row dict
# ============================================================================

def _epoch_to_iso(v: Any) -> str | None:
    """ArcGIS dates come as epoch ms. Convert to ISO timestamp or None."""
    if v is None:
        return None
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def map_pad_us(feat: dict, scope: dict) -> dict | None:
    geom = feat.get("geometry"); props = feat.get("properties", {}) or {}
    if not geom or props.get("OBJECTID") is None:
        return None
    return {
        "unit_id":     props["OBJECTID"],
        "feat_class":  props.get("FeatClass"),
        "category":    props.get("Category"),
        "own_type":    props.get("Own_Type"),
        "own_name":    props.get("Own_Name"),
        "loc_own":     props.get("Loc_Own"),
        "mng_type":    props.get("Mang_Type"),
        "mng_name":    props.get("Mang_Name"),
        "loc_mang":    props.get("Loc_Mang"),
        "des_tp":      props.get("Des_Tp"),
        "loc_ds":      props.get("Loc_Ds"),
        "unit_name":   props.get("Unit_Nm"),
        "loc_name":    props.get("Loc_Nm"),
        "state":       props.get("State_Nm") or scope.get("state"),
        "agg_src":     props.get("Agg_Src"),
        "raw_attrs":   psycopg2.extras.Json(props),
        "geom_geojson": json.dumps(geom),
    }


def map_usfws_critical_habitat(feat: dict, scope: dict) -> dict | None:
    geom = feat.get("geometry"); props = feat.get("properties", {}) or {}
    if not geom or props.get("OBJECTID") is None:
        return None
    return {
        "unit_id":     props["OBJECTID"],
        "comname":     props.get("comname"),
        "sciname":     props.get("sciname"),
        "spcode":      props.get("spcode"),
        "vipcode":     props.get("vipcode"),
        "unit":        props.get("unit"),
        "subunit":     props.get("subunit"),
        "unitname":    props.get("unitname"),
        "subunitname": props.get("subunitname"),
        "status":      props.get("status"),
        "pubdate":     _epoch_to_iso(props.get("pubdate")),
        "effectdate":  _epoch_to_iso(props.get("effectdate")),
        "source_url":  ("https://www.fws.gov/program/endangered-species/"
                        "critical-habitat-finder"),
        "raw_attrs":   psycopg2.extras.Json(props),
        "geom_geojson": json.dumps(geom),
    }


# ============================================================================
# Source registry
# ============================================================================
@dataclass
class Source:
    name: str
    description: str
    fetcher_kind: str           # 'arcgis_rest' | 'overpass'
    requires_state: bool
    target_table: str
    id_columns: list[str]       # PK column(s) — single for arcgis, ≥1 for compound (osm)
    columns: list[str]          # all non-PK + non-geom data columns
    mapper: Callable[[dict, dict], dict | None]
    ttl_days: int
    # ArcGIS REST options
    endpoint: str = ""          # FeatureServer base URL
    where_template: str = "1=1" # SQL-style WHERE; supports {state} template var
    page_size: int = PAGE_SIZE
    max_offset_deg: float | None = None  # server-side Douglas-Peucker simplification
    # Overpass options
    overpass_url: str = "https://overpass-api.de/api/interpreter"
    overpass_query_template: str = ""    # supports {bbox} template var (south,west,north,east)
    state_bbox: dict[str, tuple[float, float, float, float]] = None  # type: ignore
    # PostGIS column shape
    geom_column: str = "geom"
    geom_kind: str = "MultiPolygon"  # 'MultiPolygon' or 'Point'


PAD_US_COLUMNS = [
    "unit_id", "feat_class", "category", "own_type", "own_name", "loc_own",
    "mng_type", "mng_name", "loc_mang", "des_tp", "loc_ds",
    "unit_name", "loc_name", "state", "agg_src", "raw_attrs",
]
USFWS_COLUMNS = [
    "unit_id", "comname", "sciname", "spcode", "vipcode",
    "unit", "subunit", "unitname", "subunitname", "status",
    "pubdate", "effectdate", "source_url", "raw_attrs",
]
OSM_AMENITIES_COLUMNS = [
    "osm_id", "osm_kind", "amenity_type", "raw_amenity", "raw_tags",
    "name", "state",
]


# Map OSM tag → our canonical amenity_type. Multiple OSM tags can map to the
# same canonical type. None means "ignore".
def _osm_canonical_type(tags: dict) -> str | None:
    a = (tags.get("amenity") or "").lower()
    leis = (tags.get("leisure") or "").lower()
    tour = (tags.get("tourism") or "").lower()
    emer = (tags.get("emergency") or "").lower()
    if a in ("parking", "parking_space"):           return "parking"
    if a == "toilets":                               return "toilets"
    if a == "shower":                                return "shower"
    if a == "drinking_water":                        return "drinking_water"
    if a in ("restaurant", "cafe", "fast_food"):    return "food"
    if leis in ("picnic_table",) or tour == "picnic_site": return "picnic_area"
    if leis == "fire_pit":                           return "fire_pit"
    if emer == "lifeguard_tower":                    return "lifeguard"
    return None


def map_osm_amenity(elem: dict, scope: dict) -> dict | None:
    """OSM Overpass element → osm_amenities row.
    Nodes carry lat/lon directly; ways carry `center` from `out center;`.
    """
    tags = elem.get("tags") or {}
    osm_kind = elem.get("type")  # 'node' | 'way' | 'relation'
    osm_id   = elem.get("id")
    if osm_kind not in ("node", "way", "relation") or osm_id is None:
        return None
    amenity_type = _osm_canonical_type(tags)
    if amenity_type is None:
        return None

    if osm_kind == "node":
        lat = elem.get("lat"); lon = elem.get("lon")
    else:
        center = elem.get("center") or {}
        lat = center.get("lat"); lon = center.get("lon")
    if lat is None or lon is None:
        return None

    raw_amenity = (tags.get("amenity") or tags.get("leisure")
                   or tags.get("tourism") or tags.get("emergency"))
    return {
        "osm_id":       osm_id,
        "osm_kind":     osm_kind,
        "amenity_type": amenity_type,
        "raw_amenity":  raw_amenity,
        "raw_tags":     psycopg2.extras.Json(tags),
        "name":         tags.get("name"),
        "state":        scope.get("state", "CA"),
        "geom_geojson": json.dumps({"type": "Point", "coordinates": [lon, lat]}),
    }


# State bboxes for Overpass scope-by-state (south, west, north, east).
# Priority 1 = coastal + Great Lakes states. Add Priority 2 (interior) as
# needed. Coordinates lifted from scripts/one_off/bulk_load_*.py — keep in
# sync with those.
_STATE_BBOX = {
    "CA": (32.5,  -124.5, 42.1,  -114.0),
    "OR": (41.99, -124.57, 46.30, -116.46),
    "WA": (45.54, -124.85, 49.00, -116.92),
    # Priority 1 — coastal + Great Lakes
    "AK": (54.5,  -180.0,  71.5,  -129.0),
    "ME": (42.97, -71.10,  47.46, -66.94),
    "NH": (42.70, -72.56,  45.31, -70.61),
    "MA": (41.18, -73.51,  42.89, -69.86),
    "RI": (41.13, -71.91,  42.02, -71.12),
    "CT": (40.98, -73.73,  42.05, -71.79),
    "NY": (40.50, -79.76,  45.02, -71.78),
    "NJ": (38.93, -75.56,  41.36, -73.89),
    "DE": (38.45, -75.79,  39.84, -75.05),
    "MD": (37.91, -79.49,  39.72, -75.05),
    "VA": (36.54, -83.68,  39.47, -75.24),
    "NC": (33.84, -84.33,  36.59, -75.46),
    "SC": (32.03, -83.36,  35.22, -78.55),
    "GA": (30.36, -85.61,  35.00, -80.84),
    "FL": (24.40, -87.64,  31.00, -80.03),
    "AL": (30.14, -88.47,  35.01, -84.89),
    "MS": (30.17, -91.66,  35.01, -88.10),
    "LA": (28.93, -94.04,  33.02, -88.82),
    "TX": (25.84, -106.65, 36.50, -93.51),
    "HI": (18.91, -160.25, 22.24, -154.81),
    "MI": (41.70, -90.42,  48.31, -82.41),
    "MN": (43.50, -97.24,  49.38, -89.49),
    "WI": (42.49, -92.89,  47.08, -86.81),
    "IL": (36.97, -91.51,  42.51, -87.50),
    "IN": (37.77, -88.10,  41.76, -84.78),
    "OH": (38.40, -84.82,  41.98, -80.52),
    "PA": (39.72, -80.52,  42.27, -74.69),
}

OSM_AMENITIES_QUERY_TEMPLATE = """
[out:json][timeout:600];
(
  node["amenity"~"^(parking|parking_space|toilets|shower|drinking_water|restaurant|cafe|fast_food)$"]({bbox});
  way["amenity"~"^(parking|parking_space|toilets|shower|drinking_water|restaurant|cafe|fast_food)$"]({bbox});
  node["leisure"~"^(picnic_table|fire_pit)$"]({bbox});
  way["tourism"="picnic_site"]({bbox});
  node["emergency"="lifeguard_tower"]({bbox});
);
out tags center;
""".strip()

SOURCES: dict[str, Source] = {
    "pad_us": Source(
        name="pad_us",
        description="USGS PAD-US protected-area boundaries (per-state).",
        fetcher_kind="arcgis_rest",
        endpoint=("https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services/"
                  "Manager_Name/FeatureServer/0"),
        where_template="State_Nm='{state}'",
        requires_state=True,
        target_table="pad_us_units",
        id_columns=["unit_id"],
        columns=PAD_US_COLUMNS,
        mapper=map_pad_us,
        ttl_days=90,
    ),
    # usfws_critical_habitat retired 2026-05-07 (see project_nesting_zones_gis_load.md).
    # Endpoint + mapper preserved in version control; re-add this entry
    # if priorities change. Endpoint:
    # https://services.arcgis.com/QVENGdaPbd4LUkLV/ArcGIS/rest/services/USFWS_Critical_Habitat/FeatureServer/0
    "osm_amenities": Source(
        name="osm_amenities",
        description="OSM amenity nodes/ways (parking, toilets, showers, picnic, lifeguard, food, etc.) per-state via Overpass.",
        fetcher_kind="overpass",
        requires_state=True,
        target_table="osm_amenities",
        id_columns=["osm_id", "osm_kind"],
        columns=OSM_AMENITIES_COLUMNS,
        mapper=map_osm_amenity,
        ttl_days=180,
        geom_kind="Point",
        overpass_query_template=OSM_AMENITIES_QUERY_TEMPLATE,
        state_bbox=_STATE_BBOX,
    ),
    # nps_park_boundaries: REST endpoint not yet validated. PAD-US covers
    # NPS via Mang_Name='NPS' so not load-bearing.
}


# ============================================================================
# Cache check — skip if recently loaded unless --force
# ============================================================================
def last_loaded_at(src: Source, scope: dict) -> datetime | None:
    where = ""
    args: list[Any] = []
    if src.requires_state:
        where = " WHERE state = %s"
        args.append(scope["state"])
    sql = f"SELECT max(loaded_at) FROM public.{src.target_table}{where}"
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()[0]


def needs_refresh(src: Source, scope: dict, force: bool) -> tuple[bool, str]:
    if force:
        return True, "force"
    last = last_loaded_at(src, scope)
    if last is None:
        return True, "never_loaded"
    age = datetime.now(timezone.utc) - last
    if age > timedelta(days=src.ttl_days):
        return True, f"stale ({age.days}d > {src.ttl_days}d ttl)"
    return False, f"fresh ({age.days}d < {src.ttl_days}d ttl, last={last.date()})"


# ============================================================================
# Upsert — generic INSERT ON CONFLICT (handles compound PK + geom_kind)
# ============================================================================
def build_upsert_sql(src: Source) -> str:
    cols = src.columns + [src.geom_column]
    placeholders = ["%s"] * len(src.columns)
    if src.geom_kind == "Point":
        placeholders.append(
            "ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)::geometry(Point, 4326)"
        )
    else:
        placeholders.append(
            "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geometry(MultiPolygon, 4326)"
        )
    pk = ", ".join(src.id_columns)
    update_set = ",\n  ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in src.id_columns
    )
    return (
        f"INSERT INTO public.{src.target_table} ({', '.join(cols)})\n"
        f"VALUES ({', '.join(placeholders)})\n"
        f"ON CONFLICT ({pk}) DO UPDATE SET\n"
        f"  {update_set},\n"
        f"  loaded_at = now()"
    )


def row_to_args(src: Source, row: dict) -> list:
    return [row.get(c) for c in src.columns] + [row["geom_geojson"]]


# ============================================================================
# Loader (dispatches on fetcher_kind)
# ============================================================================
def load_source(src: Source, scope: dict, force: bool = False, dry_run: bool = False) -> dict:
    scope_label = scope.get("state", "national")
    refresh, reason = needs_refresh(src, scope, force)
    if not refresh:
        print(f"  [{src.name} / {scope_label}] skipping: {reason}")
        return {"skipped": True, "reason": reason}
    print(f"  [{src.name} / {scope_label}] {reason}; querying via {src.fetcher_kind}...")

    if src.fetcher_kind == "arcgis_rest":
        return _load_arcgis(src, scope, dry_run)
    if src.fetcher_kind == "overpass":
        return _load_overpass(src, scope, dry_run)
    raise ValueError(f"Unknown fetcher_kind: {src.fetcher_kind}")


def _load_arcgis(src: Source, scope: dict, dry_run: bool) -> dict:
    where = src.where_template.format(**scope) if src.requires_state else src.where_template
    total = fetch_count_arcgis(src.endpoint, where)
    print(f"  [{src.name}] {total} features available")
    if dry_run: return {"skipped": False, "total": total, "loaded": 0, "dry_run": True}
    if total == 0: return {"skipped": False, "total": 0, "loaded": 0}

    upsert_sql = build_upsert_sql(src)
    n_done = 0; t0 = time.time()
    with psycopg2.connect(**PG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            offset = 0
            while offset < total:
                page = fetch_page_arcgis(src.endpoint, where, offset,
                                         src.page_size, src.max_offset_deg)
                feats = page.get("features", []) or []
                if not feats: break
                rows = [row_to_args(src, m)
                        for m in (src.mapper(f, scope) for f in feats) if m]
                if rows:
                    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=200)
                    conn.commit()
                n_done += len(rows)
                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (total - n_done) / rate if rate > 0 else 0
                pct = (n_done * 100 // total) if total else 100
                print(f"    {n_done:>6} / {total} ({pct}%)  rate={rate:5.0f}/s  eta={eta:4.0f}s")
                offset += src.page_size
    return {"skipped": False, "total": total, "loaded": n_done,
            "elapsed_s": round(time.time() - t0, 1)}


def _load_overpass(src: Source, scope: dict, dry_run: bool) -> dict:
    state = scope.get("state")
    if not state or not src.state_bbox or state not in src.state_bbox:
        raise ValueError(f"Overpass source {src.name} requires state with known bbox; got {state}")
    bbox = ",".join(str(x) for x in src.state_bbox[state])
    query = src.overpass_query_template.format(bbox=bbox)
    print(f"  [{src.name} / {state}] Overpass bbox={bbox}; running single query...")
    if dry_run:
        return {"skipped": False, "total": -1, "loaded": 0, "dry_run": True,
                "note": "Overpass returns the count + data in one shot — no pre-count phase."}

    t0 = time.time()
    elements = fetch_overpass(src.overpass_url, query)
    fetch_s = time.time() - t0
    print(f"  [{src.name} / {state}] fetched {len(elements)} OSM elements in {fetch_s:.0f}s")

    upsert_sql = build_upsert_sql(src)
    rows = [row_to_args(src, m)
            for m in (src.mapper(e, scope) for e in elements) if m]
    if not rows:
        return {"skipped": False, "total": len(elements), "loaded": 0,
                "elapsed_s": round(fetch_s, 1)}
    with psycopg2.connect(**PG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
            conn.commit()
    print(f"  [{src.name} / {state}] upserted {len(rows)} rows")
    return {"skipped": False, "total": len(elements), "loaded": len(rows),
            "elapsed_s": round(time.time() - t0, 1)}


# ============================================================================
# CLI
# ============================================================================
def cmd_list():
    print("Available sources:")
    for name, src in SOURCES.items():
        scope = "per-state" if src.requires_state else "national"
        print(f"  {name:30s} {scope:10s} ttl={src.ttl_days}d   {src.description}")


def cmd_status():
    print(f"{'source':30s} {'scope':12s} {'last_loaded':20s} {'rows':>10s}")
    for name, src in SOURCES.items():
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            if src.requires_state:
                cur.execute(
                    f"SELECT state, max(loaded_at), count(*) "
                    f"FROM public.{src.target_table} "
                    f"GROUP BY state ORDER BY state"
                )
                rows = cur.fetchall()
                if not rows:
                    print(f"  {name:30s} {'(empty)':12s}")
                    continue
                for state, last, n in rows:
                    last_s = last.strftime("%Y-%m-%d %H:%M") if last else "-"
                    print(f"  {name:30s} {state or '?':12s} {last_s:20s} {n:>10d}")
            else:
                cur.execute(
                    f"SELECT max(loaded_at), count(*) FROM public.{src.target_table}"
                )
                last, n = cur.fetchone()
                last_s = last.strftime("%Y-%m-%d %H:%M") if last else "(never)"
                print(f"  {name:30s} {'national':12s} {last_s:20s} {n:>10d}")


# Two-letter US state codes — for --all-states
ALL_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR","VI","GU","AS","MP",
]


def cmd_load(args):
    targets: list[tuple[str, dict]] = []
    if args.all_sources:
        for name, src in SOURCES.items():
            if src.requires_state:
                states = args.states or ["CA"]
                for s in states:
                    targets.append((name, {"state": s.upper()}))
            else:
                targets.append((name, {}))
    elif args.source in SOURCES:
        src = SOURCES[args.source]
        if src.requires_state:
            states = args.states or ["CA"]
            for s in states:
                targets.append((args.source, {"state": s.upper()}))
        else:
            targets.append((args.source, {}))
    else:
        print(f"Unknown source: {args.source}", file=sys.stderr)
        print("Run `python scripts/external_sources.py list` for available sources.", file=sys.stderr)
        return 2

    print(f"=== {len(targets)} target(s) ===")
    summary = []
    failed = []
    for name, scope in targets:
        src = SOURCES[name]
        try:
            r = load_source(src, scope, force=args.force, dry_run=args.dry_run)
            summary.append((name, scope, r))
        except Exception as e:
            print(f"  [{name}] FAIL: {type(e).__name__}: {e}")
            failed.append((name, scope, str(e)))

    print("\n=== TOTALS ===")
    for name, scope, r in summary:
        scope_label = scope.get("state", "national")
        if r.get("skipped"):
            tag = f"skipped: {r['reason']}"
        elif r.get("dry_run"):
            tag = f"dry: {r['total']} would-load"
        else:
            tag = f"loaded {r['loaded']:,} / {r['total']:,} in {r.get('elapsed_s','?')}s"
        print(f"  {name:30s} {scope_label:12s} {tag}")
    if failed:
        print(f"\nFAILED ({len(failed)}):")
        for name, scope, err in failed:
            print(f"  {name} {scope}: {err}")
        return 1
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("list", help="List available sources")
    sub.add_parser("status", help="Show last-loaded date + row counts per source")

    p_load = sub.add_parser("load", help="Load one or more sources")
    p_load.add_argument("source", nargs="?", help="Source name (run `list` to see)")
    p_load.add_argument("--all", dest="all_sources", action="store_true",
                        help="Load every source")
    p_load.add_argument("--state", dest="states", action="append",
                        help="State code(s) for per-state sources (default CA). Repeat for multiple.")
    p_load.add_argument("--all-states", action="store_const", const=ALL_STATES, dest="states",
                        help="Load every state + territory for per-state sources")
    p_load.add_argument("--force", action="store_true", help="Bypass TTL check")
    p_load.add_argument("--dry-run", action="store_true", help="Count features, don't insert")

    args = ap.parse_args()
    if args.cmd == "list":
        cmd_list()
        return 0
    if args.cmd == "status":
        cmd_status()
        return 0
    if args.cmd == "load":
        if not args.source and not args.all_sources:
            ap.error("specify a source name or --all")
        return cmd_load(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
