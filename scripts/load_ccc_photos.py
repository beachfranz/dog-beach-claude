"""Migrate CCC photos into beach_photos.

CCC access points have up to 4 photo URLs each. We join CCC points to
beaches by 200m proximity (per project_buffer_convention.md), filter
to non-whitespace URLs, and write each beach's nearest CCC point's
photos into beach_photos.

CCC photos are highest-confidence photos we have because:
  - Each access point is bound to a specific known location
  - CCC staff captured them on a known date
  - Stable hosting on coastal.ca.gov

Sort order: 10-19 (above Unsplash 25+ / Pexels 40+) — CCC ranks first.

Usage:
  python scripts/load_ccc_photos.py --fids 6212,6202,8337,218
  python scripts/load_ccc_photos.py --pilot 50
  python scripts/load_ccc_photos.py --full
"""

from __future__ import annotations
import argparse, json, os, sys, urllib.parse, urllib.request, urllib.error

# scripts.common loads .env + injects truststore at package init.
from scripts.common.supa import supa


def db_query(sql):
    """Run a one-shot SQL via the Supabase pgmeta-style RPC. We use a
    helper RPC `_one_off_select` defined inline; if not available we'll
    fall back to PostgREST queries."""
    raise NotImplementedError("not used; we use REST queries below")


def select_target_fids(args) -> list[int]:
    if args.fids:
        return [int(s) for s in args.fids.split(",")]
    if args.pilot:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid", "is_active": "eq.true",
                            "is_scoreable": "eq.true", "order": "fid.asc",
                            "limit": str(int(args.pilot))})
        return [r["fid"] for r in (rows or [])]
    if args.full:
        rows = supa("/rest/v1/beaches_gold",
                    params={"select": "fid", "is_active": "eq.true",
                            "state": "eq.CA",  # CCC is CA-only
                            "order": "fid.asc"})
        return [r["fid"] for r in (rows or [])]
    print("ERROR: provide --fids, --pilot N, or --full", file=sys.stderr); sys.exit(1)


# ─── Persistence ──────────────────────────────────────────────────────────

def replace_ccc_for_fids(fids):
    """Wipe existing CCC rows for the target fids, then bulk-insert from
    a single SQL spatial join. Returns (rows_inserted, beaches_with_photos).
    """
    # 1. Wipe
    in_clause = ",".join(map(str, fids))
    supa("/rest/v1/beach_photos", method="DELETE", params={
        "arena_group_id": f"in.({in_clause})", "source": "eq.ccc",
    }, prefer="return=minimal")

    # 2. Bulk-insert via the supabase CLI db-query (we don't have a single
    # RPC for this so we run the SQL directly)
    # Loosened 2026-05-07: 400m -> 600m radius, dropped sandy/rocky
    # filter. The curator validates each photo by hand now, so we
    # pull more candidates (including some parking-lot/museum photos
    # that the curator can quickly trash). The sandy/rocky tag is
    # still passed through as source_meta so the curator can see it.
    sql = f"""
    insert into public.beach_photos
      (arena_group_id, source, external_id, image_url, thumb_url,
       attribution, license, lat, lng, distance_m, sort_order, page_url, source_meta)
    select
      g.fid,
      'ccc',
      ccc.objectid::text || ':' || photo_n::text,
      photo_url,
      photo_url,
      coalesce('CCC access point: ' || ccc.name, 'CCC California Coastal Commission'),
      'state-govt',
      st_y(ccc.geom),
      st_x(ccc.geom),
      round(st_distance(g.geom::geography, ccc.geom::geography))::int,
      10 + photo_n - 1,
      'https://www.coastal.ca.gov/access/',
      jsonb_build_object('access_point_name', ccc.name, 'photo_n', photo_n,
                         'sandy_beach', ccc.sandy_beach, 'rocky_shore', ccc.rocky_shore)
    from public.beaches_gold g
    join public.ccc_access_points ccc
      on st_dwithin(g.geom::geography, ccc.geom::geography, 600)
    cross join lateral (
      values (1, ccc.photo_1), (2, ccc.photo_2), (3, ccc.photo_3), (4, ccc.photo_4)
    ) as p(photo_n, photo_url)
    where g.fid in ({in_clause})
      and length(trim(p.photo_url)) > 10
    on conflict (arena_group_id, source, external_id) do nothing
    returning arena_group_id;
    """
    # Run via supabase CLI
    import subprocess
    cli = os.path.expanduser("~/scoop/shims/supabase")
    r = subprocess.run([cli, "db", "query", "--linked", sql],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"SQL insert failed: {r.stderr[:500]}")
    # Parse the JSON output to count rows
    import re
    rows = re.findall(r'"arena_group_id":\s*(\d+)', r.stdout)
    return len(rows), len(set(rows))


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids");  grp.add_argument("--pilot", type=int)
    grp.add_argument("--full", action="store_true")
    args = ap.parse_args()

    fids = select_target_fids(args)
    print(f"Targets: {len(fids)} beaches (CCC photos within 200m)")
    inserted, beaches = replace_ccc_for_fids(fids)
    print(f"\n=== TOTALS ===")
    print(f"  rows inserted:        {inserted}")
    print(f"  beaches with photos:  {beaches}")
    print(f"  beaches with no CCC:  {len(fids) - beaches}")


if __name__ == "__main__":
    sys.exit(main())
