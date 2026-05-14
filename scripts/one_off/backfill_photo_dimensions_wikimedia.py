"""backfill_photo_dimensions_wikimedia.py

Backfill beach_photos.width / .height for source='wikimedia' rows by
calling the Commons API in batches of 50 titles per request, paced 1.5s
between requests per WMF robots policy.

Per the chunked-subprocess pin (chunk by meaningful dimension; >5min
must chunk): chunks are 50-photo API batches. Each batch commits its
DB update independently — failure of one batch doesn't lose the others.

Usage:
  python scripts/one_off/backfill_photo_dimensions_wikimedia.py
  python scripts/one_off/backfill_photo_dimensions_wikimedia.py --limit 200  # dry sample
"""

import argparse
import os
import time
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=6543, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = (
    "DogBeachScout/2.0 (https://beachfranz.github.io; contact@franzfunk.com) "
    "dimension-backfill"
)
BATCH = 50
PACE_SEC = 1.5


def fetch_batch(titles: list[str]) -> dict[str, tuple[int, int]]:
    """Returns {title: (width, height)} for the input titles."""
    params = {
        "action": "query", "format": "json", "prop": "imageinfo",
        "iiprop": "size", "titles": "|".join(titles),
    }
    resp = requests.get(API, params=params,
                        headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    out: dict[str, tuple[int, int]] = {}
    pages = (data.get("query") or {}).get("pages") or {}
    for _, page in pages.items():
        title = page.get("title")
        info = (page.get("imageinfo") or [{}])[0]
        w = info.get("width")
        h = info.get("height")
        if title and w and h:
            out[title] = (int(w), int(h))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="cap total photos updated (for dry sampling)")
    ap.add_argument("--re-fetch", action="store_true",
                    help="also re-fetch rows that already have width/height")
    args = ap.parse_args()

    print(f"connecting…", flush=True)
    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        where_extra = "" if args.re_fetch else "AND width IS NULL"
        sql = (
            "SELECT id, source_meta->>'title' AS title "
            "  FROM public.beach_photos "
            " WHERE source='wikimedia' "
            "   AND source_meta ? 'title' "
            f"  {where_extra}"
        )
        if args.limit:
            sql += f" LIMIT {int(args.limit)}"
        cur.execute(sql)
        rows = cur.fetchall()
    conn.commit()
    print(f"{len(rows)} wikimedia photos to backfill", flush=True)
    if not rows:
        return

    title_to_id: dict[str, list[int]] = {}
    for r in rows:
        title_to_id.setdefault(r["title"], []).append(int(r["id"]))

    titles = list(title_to_id.keys())
    total = len(titles)
    updated = 0
    api_errs = 0
    db_errs = 0
    for i in range(0, total, BATCH):
        batch = titles[i:i + BATCH]
        try:
            dims = fetch_batch(batch)
        except Exception as e:
            api_errs += 1
            print(f"  batch {i//BATCH+1}: API error {e}", flush=True)
            time.sleep(PACE_SEC)
            continue

        # Build update tuple list: (id, width, height)
        update_rows: list[tuple[int, int, int]] = []
        for title, (w, h) in dims.items():
            for pid in title_to_id.get(title, []):
                update_rows.append((pid, w, h))

        if update_rows:
            try:
                with psycopg2.connect(**PG) as c, c.cursor() as cur:
                    cur.execute("SET statement_timeout = '60s'")
                    psycopg2.extras.execute_values(
                        cur,
                        "UPDATE public.beach_photos AS bp "
                        "   SET width=v.w, height=v.h "
                        "  FROM (VALUES %s) AS v(id, w, h) "
                        " WHERE bp.id = v.id",
                        update_rows, template="(%s, %s, %s)"
                    )
                    c.commit()
                updated += len(update_rows)
            except Exception as e:
                db_errs += 1
                print(f"  batch {i//BATCH+1}: DB error {e}", flush=True)

        pct = round(100 * (i + BATCH) / total)
        print(
            f"  batch {i//BATCH+1}/{(total+BATCH-1)//BATCH}: "
            f"{len(batch)} titles → {len(dims)} sized → {len(update_rows)} rows updated "
            f"(cumulative: {updated}; ~{min(pct,100)}%)",
            flush=True,
        )
        time.sleep(PACE_SEC)

    print(
        f"DONE: {updated} rows updated, "
        f"{api_errs} API errors, {db_errs} DB errors",
        flush=True,
    )


if __name__ == "__main__":
    main()
