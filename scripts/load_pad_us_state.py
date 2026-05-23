"""load_pad_us_state.py — load PAD-US polygons for one state from the
Esri Living Atlas REST FeatureServer into public.pad_us_units.

ScienceBase's per-state .gdb zip downloads are gated by a JS-driven SPA
with no working unauthenticated direct-download path. Living Atlas hosts
the same data as a queryable REST FeatureServer that pages cleanly.

Endpoint:
  https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services
    /Manager_Name/FeatureServer/0/query

Usage:
  python scripts/load_pad_us_state.py CA            # one state
  python scripts/load_pad_us_state.py CA --dry-run  # count only, no insert
  python scripts/load_pad_us_state.py CA OR WA      # multiple states
  python scripts/load_pad_us_state.py --all         # every state code

Idempotent via ON CONFLICT (unit_id) DO UPDATE.
Exits non-zero if any state fails.
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
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.common.ssl_compat import get_ssl_context  # noqa: E402

# Force UTF-8 stdout on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

ENDPOINT = ("https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/"
            "services/Manager_Name/FeatureServer/0/query")
PAGE_SIZE = 2000  # FeatureServer maxRecordCount

SSL_CTX = get_ssl_context()  # see scripts/common/ssl_compat.py


def fetch_page(state: str, offset: int) -> dict:
    qs = urllib.parse.urlencode({
        "where": f"State_Nm='{state}'",
        "outFields": "*",
        "f": "geojson",
        "outSR": 4326,
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    })
    url = f"{ENDPOINT}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "dog-beach-scout/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=120) as r:
        return json.loads(r.read())


def count_state(state: str) -> int:
    qs = urllib.parse.urlencode({
        "where": f"State_Nm='{state}'",
        "returnCountOnly": "true",
        "f": "json",
    })
    url = f"{ENDPOINT}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "dog-beach-scout/1.0"})
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as r:
        return json.loads(r.read())["count"]


def feature_to_row(feat: dict, state_filter: str) -> tuple | None:
    """Map a GeoJSON feature to a tuple of pad_us_units columns.
    Returns None if geometry can't be coerced to MultiPolygon."""
    props = feat.get("properties", {}) or {}
    geom = feat.get("geometry")
    if not geom:
        return None

    # ArcGIS GeoJSON returns MultiPolygon or Polygon for these features.
    # ST_Multi(ST_GeomFromGeoJSON(...)) handles both at the DB side.
    geom_str = json.dumps(geom)

    return (
        props.get("OBJECTID"),
        props.get("FeatClass"),
        props.get("Category"),
        props.get("Own_Type"),
        props.get("Own_Name"),
        props.get("Loc_Own"),
        props.get("Mang_Type"),
        props.get("Mang_Name"),
        props.get("Loc_Mang"),
        props.get("Des_Tp"),
        props.get("Loc_Ds"),
        props.get("Unit_Nm"),
        props.get("Loc_Nm"),
        props.get("State_Nm") or state_filter,
        props.get("Agg_Src"),
        psycopg2.extras.Json(props),
        geom_str,
        geom_str,  # geom_geog uses the same GeoJSON — cast happens server-side
    )


INSERT_SQL = """
insert into public.pad_us_units (
  unit_id, feat_class, category, own_type, own_name, loc_own,
  mng_type, mng_name, loc_mang, des_tp, loc_ds,
  unit_name, loc_name, state, agg_src, raw_attrs, geom, geom_geog
) values (
  %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s,
  ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geometry(MultiPolygon, 4326),
  -- geom_geog mirrors geom; required by populate_pad_us_containment_gold
  ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geometry(MultiPolygon, 4326)::geography
)
on conflict (unit_id) do update set
  feat_class = excluded.feat_class,
  category   = excluded.category,
  own_type   = excluded.own_type,
  own_name   = excluded.own_name,
  loc_own    = excluded.loc_own,
  mng_type   = excluded.mng_type,
  mng_name   = excluded.mng_name,
  loc_mang   = excluded.loc_mang,
  des_tp     = excluded.des_tp,
  loc_ds     = excluded.loc_ds,
  unit_name  = excluded.unit_name,
  loc_name   = excluded.loc_name,
  state      = excluded.state,
  agg_src    = excluded.agg_src,
  raw_attrs  = excluded.raw_attrs,
  geom       = excluded.geom,
  geom_geog  = excluded.geom_geog,
  loaded_at  = now()
"""


def load_state(state: str, dry_run: bool = False) -> tuple[int, int]:
    """Returns (rows_processed, total_for_state)."""
    total = count_state(state)
    print(f"\n== {state}: {total} polygons")
    if dry_run or total == 0:
        return (0, total)

    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    n_done = 0
    t0 = time.time()
    try:
        with conn.cursor() as cur:
            offset = 0
            while offset < total:
                page = fetch_page(state, offset)
                feats = page.get("features", [])
                if not feats:
                    break
                rows = [r for r in (feature_to_row(f, state) for f in feats) if r]
                psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=200)
                conn.commit()
                n_done += len(rows)
                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (total - n_done) / rate if rate > 0 else 0
                print(f"  {n_done:>6} / {total} ({n_done*100//total}%)  "
                      f"rate={rate:5.0f}/s  eta={eta:4.0f}s")
                offset += PAGE_SIZE
    finally:
        conn.close()
    return (n_done, total)


# Two-letter US state + territory codes
ALL_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR","VI","GU","AS","MP",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("states", nargs="*", help="2-char state codes (CA, OR, ...)")
    ap.add_argument("--all", action="store_true", help="Load every state + territory")
    ap.add_argument("--dry-run", action="store_true", help="Count only, don't insert")
    args = ap.parse_args()

    if args.all:
        states = ALL_STATES
    elif args.states:
        states = [s.upper() for s in args.states]
    else:
        ap.error("specify state codes or --all")

    summary = []
    failed = []
    t0 = time.time()
    for state in states:
        try:
            done, total = load_state(state, args.dry_run)
            summary.append((state, done, total))
        except Exception as e:
            print(f"  FAIL: {type(e).__name__}: {e}")
            failed.append((state, str(e)))

    print("\n=== TOTALS ===")
    grand_done = sum(d for _, d, _ in summary)
    grand_total = sum(t for _, _, t in summary)
    print(f"  states processed: {len(summary)}")
    print(f"  rows loaded:      {grand_done:,} / {grand_total:,}")
    print(f"  elapsed:          {time.time()-t0:.0f}s")
    if failed:
        print(f"  FAILED ({len(failed)}):")
        for s, err in failed:
            print(f"    {s}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
