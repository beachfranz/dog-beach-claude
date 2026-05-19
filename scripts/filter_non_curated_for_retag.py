"""Apply the unified v3 ingest filter retroactively to non-curated photos.

The 7,106 non-curated MVP+ photos in beach_photos were ingested by the
OLD per-source filters. Before paying to vision-tag them at v3, apply
our current `_photo_filters.pre_vision_rank()` and mark the rejects so
the tagger skips them.

Marks rejected rows with `source_meta.v3_skipped = true`. The vision
tagger's WHERE clause excludes these.

Usage:
  python scripts/filter_non_curated_for_retag.py             # dry-run summary
  python scripts/filter_non_curated_for_retag.py --apply     # mark v3_skipped on rejects
  python scripts/filter_non_curated_for_retag.py --apply --states CA,OR,WA

Idempotent — re-runs only re-evaluate photos not yet marked.
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import os
import time
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
sys.path.insert(0, str(ROOT / "scripts"))
from _photo_filters import pre_vision_rank  # noqa: E402


def _connect_pg():
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Mark rejects with source_meta.v3_skipped (default: dry-run)")
    ap.add_argument("--states", default="CA,OR,WA",
                    help="Comma-sep states to scope MVP+ (default CA,OR,WA)")
    args = ap.parse_args()
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    import psycopg2.extras
    conn = _connect_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("set statement_timeout = '180s'")

        # Pull non-curated MVP+ photos missing v3 (and not already marked v3_skipped).
        # Include scoring_tier + dogs_allowed per beach for the cap rule.
        cur.execute("""
            SELECT bp.id, bp.arena_group_id AS fid, bp.source, bp.distance_m,
                   bp.attribution, bp.captured_at, bp.curated_at, bp.source_meta,
                   g.scoring_tier, p.dogs_allowed
              FROM public.beach_photos bp
              JOIN public.beaches_gold g ON g.fid = bp.arena_group_id
              LEFT JOIN public.beach_dog_policy p ON p.arena_group_id = g.fid
             WHERE bp.hidden_at IS NULL
               AND (bp.curated_at IS NULL OR bp.curated_by LIKE 'auto:%%')
               AND (bp.source_meta -> 'vision' ->> 'schema_version' IS NULL
                    OR bp.source_meta -> 'vision' ->> 'schema_version' != 'v3')
               AND coalesce(bp.source_meta ->> 'v3_skipped', 'false') != 'true'
               AND g.state = ANY(%s)
               AND g.scoring_tier IN ('daily','hourly')
               AND g.is_active = true
             ORDER BY bp.arena_group_id, bp.id
        """, (states,))
        rows = cur.fetchall()
        if not rows:
            print("Nothing to evaluate. (All non-curated MVP+ photos already tagged or marked.)")
            return 0

        # Group by beach
        per_beach: dict[int, list[dict]] = {}
        for r in rows:
            per_beach.setdefault(r["fid"], []).append(dict(r))

        def title_text(p):
            sm = p.get("source_meta") or {}
            parts = [p.get("attribution") or ""]
            for k in ("title", "caption", "description"):
                v = sm.get(k)
                if isinstance(v, str): parts.append(v)
            return " ".join(parts)

        n_kept = 0
        n_skipped = 0
        skipped_ids: list[int] = []
        for fid, photos in per_beach.items():
            for p in photos:
                p["title_text"]      = title_text(p)
                p["curator_touched"] = False   # all non-curated by definition
                p["photographer"]    = p.get("attribution")
            beach_meta = {
                "scoring_tier": photos[0].get("scoring_tier"),
                "dogs_allowed": photos[0].get("dogs_allowed"),
            }
            selected = pre_vision_rank(photos, beach_meta)
            sel_ids = {p["id"] for p in selected}
            for p in photos:
                if p["id"] in sel_ids:
                    n_kept += 1
                else:
                    n_skipped += 1
                    skipped_ids.append(p["id"])

        total = n_kept + n_skipped
        pct = 100.0 * n_skipped / max(total, 1)
        print(f"\n=== FILTER PASS ({'APPLY' if args.apply else 'DRY-RUN'}) ===")
        print(f"  Scope: non-curated MVP+ photos missing v3, states={states}")
        print(f"  Beaches: {len(per_beach)}")
        print(f"  Photos evaluated: {total}")
        print(f"  Kept (would re-tag): {n_kept}")
        print(f"  Skipped (would mark v3_skipped): {n_skipped}  ({pct:.1f}%)")
        print(f"  Re-tag cost projection: ${n_kept * 0.003:.2f}  (vs ${total * 0.003:.2f} unfiltered)")

        if not args.apply:
            print("\n  Dry-run — no writes. Re-run with --apply to mark rejects.")
            return 0

        upd = conn.cursor()
        for i in range(0, len(skipped_ids), 500):
            batch = skipped_ids[i:i+500]
            upd.executemany("""
                UPDATE public.beach_photos
                   SET source_meta = coalesce(source_meta, '{}'::jsonb)
                                     || jsonb_build_object('v3_skipped', true,
                                                           'v3_skipped_at', now()::text,
                                                           'v3_skip_reason', 'unified_filter')
                 WHERE id = %s
            """, [(sid,) for sid in batch])
            conn.commit()
            print(f"    marked {min(i+500, len(skipped_ids))}/{len(skipped_ids)} as v3_skipped", flush=True)
        print(f"\n  ✔ marked {len(skipped_ids)} rows as v3_skipped")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
