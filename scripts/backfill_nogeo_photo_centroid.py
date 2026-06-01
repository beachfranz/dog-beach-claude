"""Centroid-attribution backfill for non-geo photo sources.

Websearch (Tavily) and unsplash photo loaders historically inserted
rows without lat/lng/distance_m, leaving them ineligible for
get_beach_photos_diverse (which gates on lat IS NOT NULL OR curated_at
IS NOT NULL). Result: 668 VA beach photos uncuratable on first sweep.

This script reads photos missing lat/lng, fetches the entity's name +
centroid, and stamps lat/lng/distance_m=0 ONLY when the photo's text
(description + page_url + host) contains all distinctive tokens of the
entity name (tight_name_match). Off-topic web-search hits stay
NULL-coord — correct outcome for "Pinterest pin generically titled
'beach'" type rows.

Usage:
  python scripts/backfill_nogeo_photo_centroid.py --entity beach --state VA
  python scripts/backfill_nogeo_photo_centroid.py --entity dog_park --state OR
  python scripts/backfill_nogeo_photo_centroid.py --entity beach --full
  python scripts/backfill_nogeo_photo_centroid.py --entity beach --state VA --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect
from scripts._photo_filters import ENTITIES, tight_name_match


SOURCES = ("websearch", "unsplash")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", required=True, choices=["beach", "dog_park"])
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--state", help="2-letter state code")
    grp.add_argument("--full", action="store_true")
    grp.add_argument("--fids", help="Comma-separated entity fids")
    ap.add_argument("--strictness", default="all", choices=["all", "majority"],
                    help="tight_name_match strictness. 'all' (default, every "
                         "distinctive token must appear) or 'majority' (>N/2 "
                         "tokens). Use 'majority' for dog-biased fid-scoped "
                         "loads per [[no-unilateral-architectural-decisions]] "
                         "+ [[this-is-a-dog-app]].")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report counts; no UPDATE")
    args = ap.parse_args()

    ent = ENTITIES[args.entity]
    gold = ent["table"]
    fk = ent["fk_col"]
    photo_table = ent["photo_table"]
    # Beach uses nav_lat/nav_lon (point-on-surface); dog_park uses lat/lon.
    lat_col = "nav_lat" if args.entity == "beach" else "lat"
    lon_col = "nav_lon" if args.entity == "beach" else "lon"

    where_state = ""
    params: list = []
    if args.state:
        where_state = f" AND g.state = %s"
        params.append(args.state)
    elif args.fids:
        ids = [int(s) for s in args.fids.split(",") if s.strip()]
        where_state = f" AND bp.{fk} = ANY(%s)"
        params.append(ids)

    sql = f"""
        SELECT bp.id, bp.{fk} AS fid, bp.source, bp.page_url,
               bp.source_meta,
               COALESCE(g.display_name_override, g.name) AS name,
               g.{lat_col} AS clat, g.{lon_col} AS clon
          FROM public.{photo_table} bp
          JOIN public.{gold} g ON g.fid = bp.{fk}
         WHERE bp.lat IS NULL
           AND bp.source = ANY(%s)
           {where_state}
    """
    params = [list(SOURCES)] + params

    conn = connect(); conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    print(f"scope: {len(rows)} {args.entity} photos with NULL lat in source={SOURCES}", flush=True)

    matched = skipped_nomatch = skipped_nocoord = 0
    by_source = {s: {"matched": 0, "skipped": 0} for s in SOURCES}
    upd_cur = conn.cursor()
    for pid, fid, source, page_url, source_meta, name, clat, clon in rows:
        if clat is None or clon is None:
            skipped_nocoord += 1
            continue
        meta = source_meta or {}
        desc = meta.get("description") or ""
        host = meta.get("host") or ""
        haystack = " ".join(filter(None, [desc, page_url or "", host]))
        if not tight_name_match(name, haystack, strictness=args.strictness):
            skipped_nomatch += 1
            by_source[source]["skipped"] += 1
            continue
        if not args.dry_run:
            upd_cur.execute(
                f"UPDATE public.{photo_table} "
                f"   SET lat = %s, lng = %s, distance_m = 0 "
                f" WHERE id = %s",
                (clat, clon, pid),
            )
        matched += 1
        by_source[source]["matched"] += 1

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n=== SUMMARY ({'DRY-RUN' if args.dry_run else 'APPLIED'}) ===")
    print(f"  matched (centroid stamped): {matched}")
    print(f"  skipped (no name match):    {skipped_nomatch}")
    print(f"  skipped (no entity coord):  {skipped_nocoord}")
    print(f"  by source:")
    for s, d in by_source.items():
        if d["matched"] or d["skipped"]:
            print(f"    {s:12} matched={d['matched']:4}  skipped={d['skipped']:4}")


if __name__ == "__main__":
    sys.exit(main())
