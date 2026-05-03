"""
promote_arena_to_beaches_gold.py — per-state populator.

Reads active OSM-anchored heads from public.arena for one hardcoded
STATE and merges them into public.beaches_gold.

Round 1 criterion (locked):
    a.is_active = true
    AND a.fid = a.group_id                       -- head-of-group
    AND a.source_code IN ('osm', 'poi')          -- both anchor types
    -- (arena's dedup already collapsed POI<->OSM matches; surviving
    --  POI heads are deliberately distinct beaches.)

The state is HARDCODED inside this script (Franz's call: "1. HARDCODE").
For each new state, copy this file, change STATE + COUNTY_FIPS_PREFIX,
keep the rest identical.

Smart-merge semantics:
    UPSERT every row that should be in beaches_gold for STATE
    DELETE rows in beaches_gold for STATE that are NOT in the upsert set
        (= beaches removed/deactivated in arena since last run)
    Rows in OTHER states are untouched.

Default mode prints a preview diff + counts and exits without writing.
Pass --apply to execute the changes inside a single transaction.
Pass --rollback after --apply to do a quick test commit-then-rollback.

Usage:
    python scripts/promote_arena_to_beaches_gold.py            # preview only
    python scripts/promote_arena_to_beaches_gold.py --apply    # write
"""
from __future__ import annotations
import os
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
STATE                = "CA"
COUNTY_FIPS_PREFIX   = "06"   # CA county FIPS all begin with 06
PROMOTED_FROM        = "all_heads_v1"
# ──────────────────────────────────────────────────────────────────────

SOURCE_QUERY = f"""
    SELECT
      a.fid, a.name, a.address, a.lat, a.lon, a.geom,
      a.county_fips, a.county_name,
      a.source_code, a.source_id, a.cpad_unit_id,
      a.group_id, a.nav_lat, a.nav_lon, a.nav_source,
      a.name_source, a.park_name
    FROM public.arena a
    WHERE a.is_active = true
      AND a.fid = a.group_id
      AND a.source_code IN ('osm', 'poi')
      AND a.county_fips LIKE '{COUNTY_FIPS_PREFIX}%'
"""


def fetch_source(cur):
    cur.execute(SOURCE_QUERY)
    return cur.fetchall()


def fetch_existing(cur):
    cur.execute("""
        SELECT fid, name, source_id, promoted_at
          FROM public.beaches_gold
         WHERE state = %s
    """, (STATE,))
    return {r["fid"]: dict(r) for r in cur.fetchall()}


def diff(source_rows, existing):
    src_fids = {r["fid"] for r in source_rows}
    ex_fids  = set(existing.keys())
    to_insert = [r for r in source_rows if r["fid"] not in ex_fids]
    to_update = [r for r in source_rows if r["fid"] in ex_fids]
    to_delete = sorted(ex_fids - src_fids)
    return to_insert, to_update, to_delete


def upsert_sql(rows):
    """Build a single multi-row INSERT...ON CONFLICT statement."""
    if not rows:
        return None, []
    cols = ["fid","name","address","lat","lon","geom",
            "county_fips","county_name","source_code","source_id",
            "cpad_unit_id","group_id","nav_lat","nav_lon","nav_source",
            "name_source","park_name","state","promoted_from"]
    values_template = "(" + ",".join(["%s"]*len(cols)) + ")"
    template = ", ".join([values_template]*len(rows))
    flat = []
    for r in rows:
        flat += [
            r["fid"], r["name"], r["address"], r["lat"], r["lon"], r["geom"],
            r["county_fips"], r["county_name"], r["source_code"], r["source_id"],
            r["cpad_unit_id"], r["group_id"], r["nav_lat"], r["nav_lon"], r["nav_source"],
            r["name_source"], r["park_name"], STATE, PROMOTED_FROM,
        ]
    sql = f"""
        INSERT INTO public.beaches_gold ({", ".join(cols)})
        VALUES {template}
        ON CONFLICT (fid) DO UPDATE SET
          name        = EXCLUDED.name,
          address     = EXCLUDED.address,
          lat         = EXCLUDED.lat,
          lon         = EXCLUDED.lon,
          geom        = EXCLUDED.geom,
          county_fips = EXCLUDED.county_fips,
          county_name = EXCLUDED.county_name,
          source_code = EXCLUDED.source_code,
          source_id   = EXCLUDED.source_id,
          cpad_unit_id= EXCLUDED.cpad_unit_id,
          group_id    = EXCLUDED.group_id,
          nav_lat     = EXCLUDED.nav_lat,
          nav_lon     = EXCLUDED.nav_lon,
          nav_source  = EXCLUDED.nav_source,
          name_source = EXCLUDED.name_source,
          park_name   = EXCLUDED.park_name,
          state       = EXCLUDED.state,
          promoted_from = EXCLUDED.promoted_from,
          promoted_at   = now(),
          is_active     = true,
          inactive_reason = null
    """
    return sql, flat


def main():
    apply = "--apply" in sys.argv

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Confirm beaches_gold exists; if not, abort with a clear message
    cur.execute("""
        SELECT 1 FROM information_schema.tables
         WHERE table_schema='public' AND table_name='beaches_gold' LIMIT 1
    """)
    if cur.fetchone() is None:
        print("ERROR: public.beaches_gold does not exist.")
        print("Apply supabase/migrations/20260501_beaches_gold.sql first "
              "(via Supabase dashboard SQL editor).")
        return 1

    src   = fetch_source(cur)
    exist = fetch_existing(cur)
    to_insert, to_update, to_delete = diff(src, exist)

    print(f"STATE: {STATE}")
    print(f"  source rows  (active OSM heads from arena, {COUNTY_FIPS_PREFIX}xxx counties): {len(src)}")
    print(f"  existing rows in beaches_gold for state {STATE}:                          {len(exist)}")
    print()
    print(f"  to INSERT:  {len(to_insert)}")
    print(f"  to UPDATE:  {len(to_update)}")
    print(f"  to DELETE:  {len(to_delete)}")
    print()

    if to_insert:
        print("  Sample INSERT:")
        for r in to_insert[:5]:
            print(f"    fid={r['fid']:6}  group_id={r['group_id']:6}  source_id={r['source_id']:25}  name={r['name']}")
        if len(to_insert) > 5:
            print(f"    ... ({len(to_insert) - 5} more)")
        print()

    if to_delete:
        print("  Sample DELETE (in beaches_gold but not in current arena source):")
        for fid in to_delete[:5]:
            ex = exist[fid]
            print(f"    fid={fid:6}  name={ex.get('name')}  source_id={ex.get('source_id')}")
        if len(to_delete) > 5:
            print(f"    ... ({len(to_delete) - 5} more)")
        print()

    if not apply:
        print("(preview only; pass --apply to execute)")
        return 0

    # ── Execute ─────────────────────────────────────────────────────
    upsert_rows = to_insert + to_update
    sql, flat = upsert_sql(upsert_rows)
    if sql:
        cur.execute(sql, flat)
        print(f"  upserted {len(upsert_rows)} rows.")
    if to_delete:
        cur.execute(
            "DELETE FROM public.beaches_gold WHERE state = %s AND fid = ANY(%s)",
            (STATE, to_delete),
        )
        print(f"  deleted {len(to_delete)} rows.")

    conn.commit()
    print("COMMIT.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
