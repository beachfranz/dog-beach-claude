"""Build public.beach_relevant_pad_us (filtered PAD-US) state-by-state.

The single-transaction materialized-view approach times out at the
Supabase pooler (likely 2-min hard ceiling, ignoring SET LOCAL). This
script chunks by state: each per-state INSERT is its own transaction
and finishes well under the cap.

Output: a regular table (not materialized view) populated by per-state
inserts. Refresh = re-run the script (drops + rebuilds).

Filter: PAD-US polygons within 500m of a beach (beaches_gold) or dog
park (osm_dog_parks). Reduces PIP cost ~99% vs the full pad_us_units.

Usage:
  python scripts/build_beach_relevant_pad_us.py            # priority states
  python scripts/build_beach_relevant_pad_us.py --all      # every loaded state
  python scripts/build_beach_relevant_pad_us.py --states WA,OR  # specific
  python scripts/build_beach_relevant_pad_us.py --dry-run  # just plan
  python scripts/build_beach_relevant_pad_us.py --no-setup --states NY,PA  # append
                                                            # (skip DROP/CREATE;
                                                            # safe to run in
                                                            # batches across the
                                                            # existing table)
"""

from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import argparse
import os
import time
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

# 5 priority states for the codify/cascade work
PRIORITY_STATES = ["CA", "WA", "OR", "MI", "MA"]


def list_pad_us_states(cur) -> list[str]:
    """All states present in pad_us_units."""
    cur.execute("SELECT DISTINCT state FROM public.pad_us_units WHERE state IS NOT NULL ORDER BY 1")
    return [r[0] for r in cur.fetchall()]


def setup_table(cur):
    """Create the target table mirroring pad_us_units + two diagnostic flags."""
    cur.execute("""
        DROP TABLE IF EXISTS public.beach_relevant_pad_us CASCADE;

        CREATE TABLE public.beach_relevant_pad_us (
          unit_id    bigint PRIMARY KEY,
          feat_class text,
          category   text,
          own_type   text,
          own_name   text,
          loc_own    text,
          mng_type   text,
          mng_name   text,
          loc_mang   text,
          des_tp     text,
          loc_ds     text,
          unit_name  text,
          loc_name   text,
          state      text,
          agg_src    text,
          raw_attrs  jsonb,
          geom       geometry(MultiPolygon, 4326),
          loaded_at  timestamptz default now(),
          geom_geog  geography(MultiPolygon, 4326),
          near_beach    boolean,
          near_dog_park boolean
        );

        CREATE INDEX brpu_geom_gix          ON public.beach_relevant_pad_us USING gist (geom);
        CREATE INDEX brpu_geom_geog_gix     ON public.beach_relevant_pad_us USING gist (geom_geog);
        CREATE INDEX brpu_state_idx         ON public.beach_relevant_pad_us(state);
        CREATE INDEX brpu_mng_type_idx      ON public.beach_relevant_pad_us(mng_type);
        CREATE INDEX brpu_own_type_idx      ON public.beach_relevant_pad_us(own_type);
        CREATE INDEX brpu_mng_name_idx      ON public.beach_relevant_pad_us(mng_name);
        CREATE INDEX brpu_near_beach_idx    ON public.beach_relevant_pad_us(near_beach)    WHERE near_beach;
        CREATE INDEX brpu_near_dog_park_idx ON public.beach_relevant_pad_us(near_dog_park) WHERE near_dog_park;

        COMMENT ON TABLE public.beach_relevant_pad_us IS
          'PAD-US polygons within 500m of a beach (beaches_gold) or dog park (osm_dog_parks). '
          'Built state-by-state by scripts/build_beach_relevant_pad_us.py. '
          'Refresh = re-run the script. Reduces PIP cost ~99% vs full pad_us_units.';
    """)


def insert_for_state(cur, state: str) -> tuple[int, int, int, float]:
    """Returns (kept, near_beach_count, near_dog_park_count, seconds).

    Splits into TWO simpler INSERTs to keep each query under the pooler
    timeout — beach-hits first (typically smaller set, cheaper), then
    dog-park hits with UPSERT on near_dog_park flag for overlap."""
    t0 = time.time()
    # Step A — beach hits
    cur.execute("""
        INSERT INTO public.beach_relevant_pad_us
        SELECT
          pu.unit_id, pu.feat_class, pu.category, pu.own_type, pu.own_name,
          pu.loc_own, pu.mng_type, pu.mng_name, pu.loc_mang, pu.des_tp,
          pu.loc_ds, pu.unit_name, pu.loc_name, pu.state, pu.agg_src,
          pu.raw_attrs, pu.geom, pu.loaded_at, pu.geom_geog,
          TRUE  AS near_beach,
          FALSE AS near_dog_park
        FROM public.pad_us_units pu
        WHERE pu.state = %s
          AND EXISTS (
            SELECT 1 FROM public.beaches_gold g
            WHERE g.is_active AND g.state = %s
              AND ST_DWithin(pu.geom_geog, g.geom::geography, 500)
          )
        ON CONFLICT (unit_id) DO NOTHING
    """, (state, state))
    n_beach = cur.rowcount

    # Step B — dog-park hits (UPSERT updates near_dog_park flag for overlap).
    # Uses osm_dog_parks.geom_geog (added + indexed 2026-05-18) — NOT a cast,
    # so both sides hit GIST indexes.
    cur.execute("""
        INSERT INTO public.beach_relevant_pad_us
        SELECT
          pu.unit_id, pu.feat_class, pu.category, pu.own_type, pu.own_name,
          pu.loc_own, pu.mng_type, pu.mng_name, pu.loc_mang, pu.des_tp,
          pu.loc_ds, pu.unit_name, pu.loc_name, pu.state, pu.agg_src,
          pu.raw_attrs, pu.geom, pu.loaded_at, pu.geom_geog,
          FALSE AS near_beach,
          TRUE  AS near_dog_park
        FROM public.pad_us_units pu
        WHERE pu.state = %s
          AND EXISTS (
            SELECT 1 FROM public.osm_dog_parks odp
            WHERE ST_DWithin(pu.geom_geog, odp.geom_geog, 500)
          )
        ON CONFLICT (unit_id) DO UPDATE SET near_dog_park = TRUE
    """, (state,))
    n_dog = cur.rowcount

    cur.execute("SELECT count(*), count(*) FILTER (WHERE near_beach), count(*) FILTER (WHERE near_dog_park) "
                "FROM public.beach_relevant_pad_us WHERE state = %s", (state,))
    kept, nb, ndp = cur.fetchone()
    return kept, nb, ndp, time.time() - t0


def main() -> int:
    ap = argparse.ArgumentParser(description="Build beach_relevant_pad_us state-by-state")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--all", action="store_true", help="every state in pad_us_units")
    g.add_argument("--states", help="comma-separated state codes (e.g. WA,OR)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-setup", action="store_true",
                    help="skip DROP/CREATE; append to the existing table "
                         "(use when running in batches across multiple "
                         "invocations)")
    args = ap.parse_args()

    with psycopg2.connect(**PG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            available = list_pad_us_states(cur)
            print(f"PAD-US has {len(available)} states loaded")

        if args.all:
            target_states = available
        elif args.states:
            target_states = [s.strip().upper() for s in args.states.split(",")]
        else:
            target_states = [s for s in PRIORITY_STATES if s in available]
        print(f"Target: {len(target_states)} state(s) — {', '.join(target_states)}")

        if args.dry_run:
            print("--dry-run: no DB changes")
            return 0

        # Setup (drops + recreates the table — its own transaction).
        # Skipped under --no-setup so subsequent batch runs APPEND to the
        # existing table (ON CONFLICT (unit_id) DO NOTHING/UPDATE handles
        # any natural dupes inside a batch).
        if args.no_setup:
            print("--no-setup: skipping DROP/CREATE; appending to existing table")
        else:
            with conn.cursor() as cur:
                setup_table(cur)
            conn.commit()
            print("Table beach_relevant_pad_us created")

        # Per-state inserts (each its own transaction)
        totals = {"kept": 0, "elapsed_total": 0.0}
        for state in target_states:
            with conn.cursor() as cur:
                kept, nb, ndp, elapsed = insert_for_state(cur, state)
            conn.commit()
            both = max(0, nb + ndp - kept)  # rough: rows counted in both
            print(f"  {state:<4}  kept={kept:>5}  near_beach={nb:>5}  near_dog_park={ndp:>5}  "
                  f"({elapsed:.1f}s)")
            totals["kept"] += kept
            totals["elapsed_total"] += elapsed

        print(f"\n=== TOTALS ===")
        print(f"  states:            {len(target_states)}")
        print(f"  rows kept:         {totals['kept']}")
        print(f"  elapsed (build):   {totals['elapsed_total']:.1f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
