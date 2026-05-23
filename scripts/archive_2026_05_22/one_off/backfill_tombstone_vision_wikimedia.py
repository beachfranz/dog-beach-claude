"""Backfill vision_tags on legacy Wikimedia tombstones.

For each beach_photo_rejected row where source='wikimedia',
vision_tags is NULL, and title (File:...) is captured, construct
the Special:FilePath URL from the title, run two-pass Haiku vision
tagging, write to vision_tags.

Doesn't need the chunked subprocess pattern (~43 rows × ~5s each =
~4 min total, comfortably under the 10-min hard rule). Reuses
tag_photo and _image_source from load_photo_vision_tags.py — the
Wikimedia robot-policy compliance (Lock + 1.5s pacing) is built in.

Idempotent: skips tombstones that already have vision_tags.
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

sys.path.insert(0, str(ROOT / "scripts"))
from load_photo_vision_tags import tag_photo  # noqa: E402


def wikimedia_url_from_title(title: str) -> str | None:
    """File:Rock Harbor just click with sent - panoramio.jpg
       → https://commons.wikimedia.org/wiki/Special:FilePath/Rock%20Harbor%20just%20click%20with%20sent%20-%20panoramio.jpg
    Title without 'File:' prefix is the filename. Special:FilePath
    redirects to upload.wikimedia.org but our _image_source handles
    the base64 fetch with the robot-policy lock."""
    if not title or not title.startswith("File:"):
        return None
    filename = title[len("File:"):]
    return ("https://commons.wikimedia.org/wiki/Special:FilePath/"
            + urllib.parse.quote(filename))


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("set statement_timeout = '120s'")

    cur.execute("""
        select r.arena_group_id, r.source, r.external_id, r.title,
               coalesce(g.display_name_override, g.name) as beach_name
          from public.beach_photo_rejected r
          join public.beaches_gold g on g.fid = r.arena_group_id
         where r.source = 'wikimedia'
           and r.vision_tags is null
           and r.title is not null
           and g.is_active
         order by r.rejected_at desc
    """)
    targets = cur.fetchall()
    print(f"Wikimedia tombstones to backfill: {len(targets)}", flush=True)
    if not targets:
        print("Nothing to do.")
        return

    upd_cur = conn.cursor()
    ok = url_fail = vision_fail = 0
    in_tok = out_tok = 0
    t0 = time.time()

    for i, t in enumerate(targets, 1):
        url = wikimedia_url_from_title(t["title"])
        if not url:
            url_fail += 1
            print(f"  [{i}/{len(targets)}] no URL for title={t['title']!r}", flush=True)
            continue
        try:
            tags, usage = tag_photo(url, t["beach_name"] or "a beach")
            in_tok += usage["input_tokens"]
            out_tok += usage["output_tokens"]
            ok += 1
        except Exception as e:
            vision_fail += 1
            print(f"  [{i}/{len(targets)}] vision fail ext_id={t['external_id']}: {str(e)[:140]}",
                  flush=True)
            continue

        upd_cur.execute("""
            update public.beach_photo_rejected
               set vision_tags = %s::jsonb
             where arena_group_id = %s and source = %s and external_id = %s
        """, (json.dumps(tags), t["arena_group_id"], t["source"], t["external_id"]))
        if i % 10 == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = i / elapsed
            eta = (len(targets) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(targets)}] ok={ok} url_fail={url_fail} "
                  f"vision_fail={vision_fail} {rate:.1f}/s eta={eta:.0f}s",
                  flush=True)

    conn.commit()
    actual = in_tok * 1e-6 + out_tok * 5e-6
    print(f"\nDone in {time.time()-t0:.0f}s")
    print(f"  ok:           {ok}")
    print(f"  url_fail:     {url_fail}")
    print(f"  vision_fail:  {vision_fail}")
    print(f"  est cost:     ${actual:.2f}")


if __name__ == "__main__":
    main()
