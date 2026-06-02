"""Strict cross-source beach photo curate.

Replaces auto_curate.py in the state-launch pipeline per Franz directive
2026-06-02. The diverse-selector path (get_beach_photos_diverse +
Tier 2 keep_prob gate) is retired; the new pattern is:

  for each beach fid:
    pick the top-N (default 6) uncurated unhidden photos with
    lat IS NOT NULL, ranked by sort_order. Stamp curated_by='Curated:AI'.

Why this works (per the MVP+ gap-beach pilot 2026-06-02):
  - lat IS NOT NULL is the attribution signal across ALL sources:
      * Flickr/Wikimedia/CDPR/NPS/OPRD/WSPRC photos always have lat
        from their source's real GPS (or polygon-via-membership centroid).
      * websearch (Tavily) photos have lat only when the load-time
        tight_name_match gate passed.
  - sort_order is each source's own relevance rank (Tavily relevance,
    Flickr keyword-bias position, agency-page order).
  - No keep_prob / vision-tag gating. Cross-source mix is by-construction.

Preserves prior curations:
  - Curated:Human is never overwritten (only operates on curated_at IS NULL).
  - Existing Curated:AI / vision-auto / loader-bias picks are preserved.

Usage:
  python scripts/curate_beach_photos_strict.py --fids 6411,8347,...
  python scripts/curate_beach_photos_strict.py --state CA --limit 100
  python scripts/curate_beach_photos_strict.py --full              # all active scoreable
  python scripts/curate_beach_photos_strict.py --n 8 --fids ...    # top 8 per beach
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse, os, time, urllib.parse
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect_pg():
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def strict_curate_for_fids(conn, fids: list[int], n: int) -> dict:
    """Pick top-N per fid where lat IS NOT NULL, stamp Curated:AI."""
    if not fids:
        return {"selected": 0, "selected_photos": 0, "beaches_with_picks": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH existing_count AS (
              SELECT arena_group_id, count(*) AS n_curated
                FROM public.beach_photos
               WHERE arena_group_id = ANY(%s)
                 AND curated_at IS NOT NULL AND hidden_at IS NULL
               GROUP BY 1
            ),
            needed AS (
              SELECT f.fid, %s - COALESCE(ec.n_curated, 0) AS need
                FROM unnest(%s::bigint[]) AS f(fid)
                LEFT JOIN existing_count ec ON ec.arena_group_id = f.fid
               WHERE %s - COALESCE(ec.n_curated, 0) > 0
            ),
            ranked AS (
              SELECT p.id, p.arena_group_id,
                     ROW_NUMBER() OVER (PARTITION BY p.arena_group_id
                                        ORDER BY p.sort_order ASC NULLS LAST, p.id ASC) AS rk
                FROM public.beach_photos p
                JOIN needed n ON n.fid = p.arena_group_id
               WHERE p.curated_at IS NULL
                 AND p.hidden_at IS NULL
                 AND p.lat IS NOT NULL
            )
            UPDATE public.beach_photos p
               SET curated_at = now(),
                   curated_by = 'Curated:AI'
              FROM ranked r JOIN needed n ON n.fid = r.arena_group_id
             WHERE p.id = r.id AND r.rk <= n.need
             RETURNING p.arena_group_id
            """,
            (fids, n, fids, n),
        )
        rows = cur.fetchall()
    conn.commit()
    beaches = {r[0] for r in rows}
    return {
        "selected_photos": len(rows),
        "beaches_with_picks": len(beaches),
    }


def fids_for_state(conn, state: str, tier12_only: bool = True) -> list[int]:
    sql = """
      SELECT DISTINCT b.fid
        FROM public.beaches_gold b
       WHERE b.state = %s AND b.is_active
         AND b.scoring_tier IN ('hourly', 'daily')
    """
    if tier12_only:
        sql += """
           AND EXISTS (
             SELECT 1 FROM public.beach_dog_policy bdp
              WHERE bdp.arena_group_id = b.fid
                AND public.beach_location_tier(
                      bdp.dogs_allowed, bdp.has_off_leash,
                      bdp.has_on_leash, bdp.dogs_prohibited_start::text)
                    IN ('1_off-leash', '2_on-leash')
           )
        """
    sql += " ORDER BY b.fid"
    with conn.cursor() as cur:
        cur.execute(sql, (state,))
        return [r[0] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser(description="Strict cross-source beach photo curate")
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids", help="Comma-separated beach fids")
    grp.add_argument("--state", help="Two-letter state code")
    grp.add_argument("--full", action="store_true", help="All active scoreable beaches")
    ap.add_argument("--n", type=int, default=6, help="Top-N per beach (default 6)")
    ap.add_argument("--limit", type=int, default=None, help="Cap fid count (debug)")
    args = ap.parse_args()

    conn = _connect_pg()
    try:
        if args.fids:
            fids = [int(x) for x in args.fids.split(",") if x.strip()]
        elif args.state:
            fids = fids_for_state(conn, args.state.upper())
        elif args.full:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT fid FROM public.beaches_gold "
                    " WHERE is_active AND scoring_tier IN ('hourly','daily') ORDER BY fid"
                )
                fids = [r[0] for r in cur.fetchall()]
        else:
            ap.error("expected --fids / --state / --full")
            return 1

        if args.limit:
            fids = fids[: args.limit]

        print(f"Strict curate scope: {len(fids)} beaches  n={args.n}", flush=True)

        t0 = time.time()
        # Chunk DB calls so we don't try to UPDATE thousands of rows in
        # one transaction. 100 fids/chunk × top-6 = ~600 row updates max.
        CHUNK = 100
        agg = {"selected_photos": 0, "beaches_with_picks": 0}
        seen_beaches = set()
        for i in range(0, len(fids), CHUNK):
            batch = fids[i : i + CHUNK]
            res = strict_curate_for_fids(conn, batch, args.n)
            agg["selected_photos"] += res["selected_photos"]
            # Track unique beaches across chunks (needed_beaches in this batch)
        # Recompute the per-state distinct beach count for the SUMMARY
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(DISTINCT arena_group_id)
                  FROM public.beach_photos
                 WHERE arena_group_id = ANY(%s)
                   AND curated_at IS NOT NULL AND hidden_at IS NULL
                """,
                (fids,),
            )
            beaches_with_curated = cur.fetchone()[0]
        elapsed = time.time() - t0

        print("\n=== SUMMARY ===", flush=True)
        print(f"  beaches processed:       {len(fids)}", flush=True)
        print(f"  new picks:               {agg['selected_photos']}", flush=True)
        print(f"  beaches with curated:    {beaches_with_curated}", flush=True)
        print(f"  elapsed:                 {elapsed:.1f}s", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
