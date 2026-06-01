"""Populate pad_us_units_buf200m using simplified-geom buffer.

Performance fix vs the original geography ST_Buffer: simplify the
polygon (tolerance 0.0001° ≈ 11m, well below the 200m buffer's
precision) and use quad_segs=4 (vs default 8) for the buffer. ~10-50×
faster while still producing a buffer that captures coastal-edge
beach pins sitting <200m offshore of the source polygon.

Idempotent: each invocation processes only MISSING unit_ids for the
state. ON CONFLICT DO UPDATE so re-runs are cheap.

Usage:
  python scripts/one_off/populate_pad_us_units_buf200m.py --state CA
  python scripts/one_off/populate_pad_us_units_buf200m.py --all
"""

from __future__ import annotations
import sys
import os
import argparse
import time
import traceback

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect


def log(msg: str) -> None:
    print(msg, flush=True)


def populate_state(conn, state: str, batch: int) -> int:
    cur = conn.cursor()
    total_added = 0
    while True:
        t0 = time.time()
        cur.execute("""
            WITH buf AS (
              SELECT p.unit_id,
                     ST_Multi(ST_Buffer(
                       COALESCE(NULLIF(ST_Simplify(p.geom, 0.0001), 'POLYGON EMPTY'::geometry), p.geom)::geography,
                       200,
                       'quad_segs=4'
                     )::geometry)::geometry(MultiPolygon, 4326) AS geom
                FROM (
                  SELECT unit_id FROM public.pad_us_units
                   WHERE state = %s AND geom IS NOT NULL
                     AND NOT EXISTS (
                       SELECT 1 FROM public.pad_us_units_buf200m b
                        WHERE b.unit_id = pad_us_units.unit_id
                     )
                   ORDER BY unit_id LIMIT %s
                ) sub
                JOIN public.pad_us_units p ON p.unit_id = sub.unit_id
            )
            INSERT INTO public.pad_us_units_buf200m (unit_id, geom)
            SELECT unit_id, geom FROM buf
             WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
            ON CONFLICT (unit_id) DO UPDATE SET geom = EXCLUDED.geom
        """, (state, batch))
        n = cur.rowcount
        conn.commit()
        if n == 0:
            break
        total_added += n
        log(f"  {state}: +{n} in {time.time()-t0:.1f}s (state total this run: {total_added})")
    cur.close()
    return total_added


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default=None)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args()

    if not args.state and not args.all:
        ap.error("specify --state X or --all")

    log("Connecting...")
    conn = connect(application_name="pad_us_buf200m_simplify")
    cur = conn.cursor()

    if args.state:
        states = [args.state]
    else:
        cur.execute("""
            SELECT p.state
              FROM public.pad_us_units p
              LEFT JOIN public.pad_us_units_buf200m b ON b.unit_id = p.unit_id
             WHERE p.state IS NOT NULL AND p.geom IS NOT NULL
             GROUP BY p.state
            HAVING COUNT(*) FILTER (WHERE b.unit_id IS NULL) > 0
             ORDER BY COUNT(*) FILTER (WHERE b.unit_id IS NULL) DESC
        """)
        states = [r[0] for r in cur.fetchall()]
        log(f"States with missing rows: {states}")

    total = 0
    for i, st in enumerate(states, 1):
        log(f"[{i}/{len(states)}] {st} starting...")
        try:
            n = populate_state(conn, st, args.batch)
            total += n
            log(f"[{i}/{len(states)}] {st} done: +{n} (running total: {total})")
        except Exception:
            log(f"[{i}/{len(states)}] {st} FAIL")
            traceback.print_exc()
            try:
                conn.rollback()
            except Exception:
                pass

    cur.execute("SELECT COUNT(*) FROM public.pad_us_units_buf200m")
    done = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.pad_us_units WHERE state IS NOT NULL AND geom IS NOT NULL")
    target = cur.fetchone()[0]
    log(f"\nFinal: {done}/{target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
