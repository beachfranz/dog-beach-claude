"""Backfill features on legacy beach_photo_rejected tombstones.

The beach_photo_rejected table was extended 2026-05-12 with feature
columns (title, artist, distance_m, relevance_score, name_match_score,
composite_score, license) so the Tier 2 photo-quality model can train
on feature-parity labels.

The ~946 legacy tombstones have NULL feature columns. This script
re-fetches features from the original source where possible:
  - Flickr: flickr.photos.getInfo(photo_id) -> title, ownername
  - Wikimedia: commons imageinfo(pageid) -> title, artist, license
  - CCC: not re-fetchable through this script (would need re-running
    the loader); skip and leave NULL.
  - Unsplash: 52 tombstones from the bulk-reject during the dog-park
    photo pilot. They had no useful metadata anyway; skip.

Distance is re-computed from photo coords (when re-fetch yields geo)
and the current beach centroid.

Run: python scripts/one_off/backfill_tombstone_features.py
"""
from __future__ import annotations
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from math import asin, cos, radians, sin, sqrt
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

FLICKR_KEY = (os.environ.get("FLICKR_API_KEY") or os.environ.get("FLICKR_KEY"))
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
UA = "DogBeachScout/1.0 tombstone-backfill"

# Reuse term lists from the Flickr loader for scoring consistency
sys.path.insert(0, str(ROOT / "scripts"))
from load_flickr_photos import (  # noqa: E402
    POSITIVE_TERMS_DOG, POSITIVE_TERMS_GENERIC, NEGATIVE_TERMS,
    _relevance_score, _name_match_score,
)


def hav(la1, lo1, la2, lo2):
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return int(2*6371000*asin(sqrt(a)))


# ─── Flickr re-fetch ──────────────────────────────────────────────────────

def flickr_photo_info(photo_id: str) -> dict | None:
    """Returns {title, ownername, lat, lng, license_code} or None.
    Gentle: 3-step backoff retry (3s/10s/30s) on stat=fail with code
    201/105 (service unavailable / rate-limit) — matches the loader's
    pattern that survived the post-burst throttle 2026-05-11.
    """
    if not FLICKR_KEY: return None
    params = {
        "method": "flickr.photos.getInfo",
        "api_key": FLICKR_KEY,
        "photo_id": photo_id,
        "format": "json",
        "nojsoncallback": "1",
    }
    url = "https://api.flickr.com/services/rest/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    delays = [3, 10, 30]
    for attempt, delay in enumerate([0, *delays]):
        if delay > 0:
            time.sleep(delay)
            print(f"  flickr backoff {delay}s (attempt {attempt}/{len(delays)})", flush=True)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read())
            if d.get("stat") == "ok":
                p = d.get("photo", {})
                loc = p.get("location") or {}
                return {
                    "title": (p.get("title") or {}).get("_content") or "",
                    "artist": (p.get("owner") or {}).get("username") or "",
                    "lat": float(loc["latitude"]) if loc.get("latitude") else None,
                    "lng": float(loc["longitude"]) if loc.get("longitude") else None,
                    "license_code": str(p.get("license", "")),
                }
            code = d.get("code")
            if code in (105, 201) and attempt < len(delays):
                continue
            return None  # non-retryable fail (photo deleted, private, etc.)
        except Exception:
            if attempt < len(delays): continue
            return None
    return None


# ─── Wikimedia re-fetch ──────────────────────────────────────────────────

def wikimedia_imageinfo(pageid: str) -> dict | None:
    """Returns {title, artist, lat, lng, license_short} or None."""
    params = {
        "action": "query", "prop": "imageinfo|coordinates",
        "pageids": str(pageid),
        "iiprop": "extmetadata",
        "iiextmetadatafilter": "License|LicenseShortName|Artist",
        "format": "json",
    }
    url = COMMONS_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        pages = (d.get("query") or {}).get("pages") or {}
        page = pages.get(str(pageid)) or {}
        title = page.get("title", "")
        info = (page.get("imageinfo") or [{}])[0]
        ext = info.get("extmetadata") or {}
        def meta(key):
            v = ext.get(key, {})
            if isinstance(v, dict):
                s = v.get("value", "")
                return re.sub(r"<[^>]+>", "", s).strip() if isinstance(s, str) else None
            return None
        coords = (page.get("coordinates") or [{}])[0]
        return {
            "title": title,
            "artist": meta("Artist"),
            "lat": coords.get("lat"),
            "lng": coords.get("lon"),
            "license_short": meta("LicenseShortName") or meta("License"),
        }
    except Exception:
        return None


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("set statement_timeout = '120s'")

    # Find legacy tombstones missing features
    cur.execute("""
        select r.arena_group_id fid, r.source, r.external_id,
               g.lat beach_lat, g.lon beach_lon, g.name beach_name
          from public.beach_photo_rejected r
          join public.beaches_gold g on g.fid = r.arena_group_id
         where r.title is null
           and r.source in ('flickr', 'wikimedia')
         order by r.source, r.rejected_at
    """)
    targets = cur.fetchall()
    print(f"Legacy tombstones to backfill: {len(targets)}")
    by_source = {}
    for t in targets: by_source.setdefault(t["source"], []).append(t)
    for s, lst in by_source.items(): print(f"  {s}: {len(lst)}")

    updates = []
    t0 = time.time()
    for i, t in enumerate(targets, 1):
        if i % 50 == 0:
            print(f"  [{i}/{len(targets)}]  ok={len(updates)}  elapsed={time.time()-t0:.0f}s",
                  flush=True)

        if t["source"] == "flickr":
            info = flickr_photo_info(str(t["external_id"]))
            if not info:
                time.sleep(1.5); continue
            title = info["title"] or ""
            artist = info["artist"] or ""
            license_code = info.get("license_code", "")
            dist_m = None
            if info["lat"] is not None and t["beach_lat"] is not None:
                dist_m = hav(t["beach_lat"], t["beach_lon"], info["lat"], info["lng"])
            rel = _relevance_score(title)
            name_match = _name_match_score(t["beach_name"], title)
            composite = rel + name_match + (1.0 - (dist_m or 450) / 450.0) * 2.0 if dist_m is not None else rel + name_match
            updates.append((title, artist, dist_m, rel, name_match, composite,
                            f"CC-{license_code}" if license_code else None,
                            t["fid"], t["source"], t["external_id"]))
            time.sleep(2.5)   # gentle Flickr throttle (matches loader 2026-05-11)

        elif t["source"] == "wikimedia":
            info = wikimedia_imageinfo(str(t["external_id"]))
            if not info:
                time.sleep(0.4); continue
            title = info["title"] or ""
            artist = info["artist"] or ""
            license_short = info.get("license_short")
            dist_m = None
            if info["lat"] is not None and t["beach_lat"] is not None:
                dist_m = hav(t["beach_lat"], t["beach_lon"], info["lat"], info["lng"])
            rel = _relevance_score(title, "")
            name_match = _name_match_score(t["beach_name"], title)
            composite = rel + name_match + (1.0 - (dist_m or 500) / 500.0) * 2.0 if dist_m is not None else rel + name_match
            updates.append((title, artist, dist_m, rel, name_match, composite,
                            license_short,
                            t["fid"], t["source"], t["external_id"]))
            time.sleep(0.4)

    print(f"\nBatch-updating {len(updates)} tombstones ...")
    upd_cur = conn.cursor()
    for u in updates:
        upd_cur.execute("""
            update public.beach_photo_rejected
               set title=%s, artist=%s, distance_m=%s,
                   relevance_score=%s, name_match_score=%s, composite_score=%s,
                   license=%s
             where arena_group_id=%s and source=%s and external_id=%s
        """, u)
    conn.commit()
    print(f"  done. {len(updates)} of {len(targets)} backfilled "
          f"({100*len(updates)/max(len(targets),1):.0f}%).")


if __name__ == "__main__":
    main()
