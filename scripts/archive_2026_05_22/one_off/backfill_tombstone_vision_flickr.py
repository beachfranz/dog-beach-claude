"""Backfill vision_tags on legacy Flickr tombstones.

For each beach_photo_rejected row where source='flickr' and vision_tags
is NULL, call flickr.photos.getInfo(photo_id), rebuild the image URL,
run two-pass Claude Haiku vision tagging (reusing tag_photo from
scripts/load_photo_vision_tags.py), and write the result to
beach_photo_rejected.vision_tags.

Wikimedia tombstones deferred until the active wikimedia vision
backfill finishes (shared robot-policy budget — running both
concurrently triggers IP-level 429s).

Idempotent: skips tombstones where vision_tags is already populated.
Tombstones whose Flickr photo has been deleted/made-private are
permanently un-backfillable; we leave their vision_tags NULL and
training-side SQL filters them out.
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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

FLICKR_KEY = os.environ.get("FLICKR_API_KEY") or os.environ.get("FLICKR_KEY")
UA = "DogBeachScout/1.0 tombstone-vision-backfill"

# Reuse the vision tagger from the main loader
sys.path.insert(0, str(ROOT / "scripts"))
from load_photo_vision_tags import tag_photo  # noqa: E402


def flickr_image_url(photo_id: str) -> str | None:
    """getInfo → reconstruct https://live.staticflickr.com/<server>/<id>_<secret>_b.jpg"""
    params = {
        "method": "flickr.photos.getInfo",
        "api_key": FLICKR_KEY,
        "photo_id": photo_id,
        "format": "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for attempt, delay in enumerate([0, 3, 10, 30]):
        if delay > 0: time.sleep(delay)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read())
            if d.get("stat") != "ok":
                code = d.get("code")
                if code in (105, 201) and attempt < 3: continue
                return None
            p = d["photo"]
            return (f"https://live.staticflickr.com/"
                    f"{p['server']}/{photo_id}_{p['secret']}_b.jpg")
        except Exception:
            if attempt < 3: continue
            return None
    return None


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("set statement_timeout = '120s'")

    cur.execute("""
        select r.arena_group_id, r.source, r.external_id,
               coalesce(g.display_name_override, g.name) as beach_name
          from public.beach_photo_rejected r
          join public.beaches_gold g on g.fid = r.arena_group_id
         where r.source = 'flickr'
           and r.vision_tags is null
           and g.is_active
         order by r.rejected_at desc
    """)
    targets = cur.fetchall()
    print(f"Flickr tombstones to backfill: {len(targets)}", flush=True)

    upd_cur = conn.cursor()
    state = {"ok": 0, "url_fail": 0, "vision_fail": 0, "done": 0,
             "in": 0, "out": 0}
    import threading
    lock = threading.Lock()
    t0 = time.time()

    def process(t):
        url = flickr_image_url(str(t["external_id"]))
        time.sleep(2.5)        # gentle flickr throttle (per loader convention)
        if not url:
            with lock:
                state["url_fail"] += 1; state["done"] += 1
            return None
        try:
            tags, usage = tag_photo(url, t["beach_name"] or "a beach")
        except Exception as e:
            with lock:
                state["vision_fail"] += 1; state["done"] += 1
            print(f"  vision fail id={t['external_id']}: {str(e)[:120]}", flush=True)
            return None
        with lock:
            state["ok"] += 1
            state["in"] += usage["input_tokens"]
            state["out"] += usage["output_tokens"]
            state["done"] += 1
        return (t["arena_group_id"], t["source"], t["external_id"], tags,
                state["done"])

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(process, t): t for t in targets}
        for fut in as_completed(futures):
            r = fut.result()
            if r is None: continue
            fid, src, ext, tags, d = r
            upd_cur.execute("""
                update public.beach_photo_rejected
                   set vision_tags = %s::jsonb
                 where arena_group_id = %s and source = %s and external_id = %s
            """, (json.dumps(tags), fid, src, ext))
            if d % 20 == 0:
                conn.commit()
                elapsed = time.time() - t0
                rate = d / elapsed
                eta = (len(targets) - d) / rate if rate > 0 else 0
                print(f"  [{d}/{len(targets)}] ok={state['ok']} "
                      f"url_fail={state['url_fail']} vision_fail={state['vision_fail']} "
                      f"{rate:.1f}/s eta={eta:.0f}s "
                      f"tok in={state['in']:,} out={state['out']:,}",
                      flush=True)

    conn.commit()
    actual = state["in"] * 1e-6 + state["out"] * 5e-6
    print(f"\nDone in {time.time()-t0:.0f}s")
    print(f"  ok:           {state['ok']}")
    print(f"  url_fail:     {state['url_fail']} (photo deleted/private on Flickr)")
    print(f"  vision_fail:  {state['vision_fail']}")
    print(f"  est cost:     ${actual:.2f}")


if __name__ == "__main__":
    main()
