"""load_beach_wikipedia.py — fetch Wikipedia content for scoreable
beaches that have an OSM landing within 200m carrying a wikidata QID
(or a direct wikipedia tag).

Flow per beach:
  1. Find the nearest osm_landing within 200m with a wikidata tag.
  2. Resolve the QID to the en.wikipedia sitelink via the Wikidata
     entity API.
  3. Fetch /api/rest_v1/page/summary/<title> for the article first
     paragraph + thumbnail + coords.
  4. Upsert into public.beach_wikipedia.

Idempotent: re-running overwrites the existing row.

Usage:
  python scripts/one_off/load_beach_wikipedia.py --state RI
  python scripts/one_off/load_beach_wikipedia.py --state CA --limit 5
"""
from __future__ import annotations
import argparse, json, os, time, urllib.parse, urllib.request
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

UA = 'dog-beach-claude wikipedia-loader (franz@franzfunk.com)'
WIKIDATA_API = 'https://www.wikidata.org/wiki/Special:EntityData'
WIKI_REST = 'https://en.wikipedia.org/api/rest_v1/page/summary'


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def http_json(url):
    req = urllib.request.Request(url, headers={
        'User-Agent': UA, 'Accept': 'application/json',
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def resolve_qid_to_title(qid: str) -> str | None:
    """Wikidata entity → en.wikipedia article title."""
    try:
        data = http_json(f'{WIKIDATA_API}/{qid}.json')
    except Exception as e:
        log(f'    qid resolve failed {qid}: {e}')
        return None
    entity = data.get('entities', {}).get(qid, {})
    sitelinks = entity.get('sitelinks', {})
    enwiki = sitelinks.get('enwiki')
    return enwiki.get('title') if enwiki else None


def fetch_summary(title: str) -> dict | None:
    enc = urllib.parse.quote(title.replace(' ', '_'))
    try:
        data = http_json(f'{WIKI_REST}/{enc}')
    except Exception as e:
        log(f'    summary fetch failed "{title}": {e}')
        return None
    return data


def find_candidates(state: str, limit: int):
    """Return [{fid, name, qid_from_osm, wp_from_osm, dist_m}] for
    scoreable beaches in `state` with a tagged OSM landing within 200m."""
    sql = """
      with tagged as (
        select id, type, geom_full, tags
          from public.osm_landing
         where is_active and (tags ? 'wikidata' or tags ? 'wikipedia')
      ),
      matched as (
        select g.fid,
               coalesce(g.display_name_override, g.name) as name,
               t.tags->>'wikidata'  as qid,
               t.tags->>'wikipedia' as wp_raw,
               st_distance(g.geom::geography, t.geom_full::geography)::int as dist_m
          from public.beaches_gold g
          join tagged t
            on g.is_active and g.is_scoreable
           and st_dwithin(g.geom::geography, t.geom_full::geography, 200)
         where g.state = %s
      ),
      ranked as (
        select *, row_number() over (partition by fid order by dist_m) rn
          from matched
      )
      select fid, name, qid, wp_raw, dist_m
        from ranked where rn = 1
        order by name
        limit %s
    """
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("set statement_timeout = '300s'")
        cur.execute(sql, (state, limit))
        return cur.fetchall()


def upsert(fid, qid, summary, match_method, dist_m):
    """Upsert beach_wikipedia row from the REST summary blob."""
    title = summary.get('title')
    url = summary.get('content_urls', {}).get('desktop', {}).get('page')
    extract = summary.get('extract', '')
    description = summary.get('description', '')
    thumb = summary.get('thumbnail') or {}
    coords = summary.get('coordinates') or {}
    coord_pt = None
    if coords.get('lat') is not None and coords.get('lon') is not None:
        coord_pt = f'({coords["lon"]},{coords["lat"]})'

    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""
          insert into public.beach_wikipedia
            (arena_group_id, qid, title, url, summary, description,
             thumbnail_url, thumbnail_w, thumbnail_h, coordinates,
             match_method, match_dist_m, raw, refreshed_at)
          values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::point,
                  %s, %s, %s::jsonb, now())
          on conflict (arena_group_id) do update
            set qid = excluded.qid,
                title = excluded.title,
                url = excluded.url,
                summary = excluded.summary,
                description = excluded.description,
                thumbnail_url = excluded.thumbnail_url,
                thumbnail_w = excluded.thumbnail_w,
                thumbnail_h = excluded.thumbnail_h,
                coordinates = excluded.coordinates,
                match_method = excluded.match_method,
                match_dist_m = excluded.match_dist_m,
                raw = excluded.raw,
                refreshed_at = now()
        """, (fid, qid, title, url, extract, description,
              thumb.get('source'), thumb.get('width'), thumb.get('height'),
              coord_pt, match_method, dist_m, json.dumps(summary)))
        c.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--state', required=True)
    ap.add_argument('--limit', type=int, default=500)
    args = ap.parse_args()

    candidates = find_candidates(args.state, args.limit)
    log(f'{len(candidates)} candidate beaches in {args.state}')
    ok = miss = skipped = 0
    for fid, name, qid, wp_raw, dist_m in candidates:
        # Prefer wikidata QID (canonical bridge); fall back to wikipedia tag
        title = None
        method = None
        if qid:
            title = resolve_qid_to_title(qid)
            if title:
                method = 'osm_wikidata'
        if not title and wp_raw:
            # wp_raw format: 'en:Article Title' or 'Article Title'
            if ':' in wp_raw:
                lang, t = wp_raw.split(':', 1)
                if lang == 'en':
                    title = t
            else:
                title = wp_raw
            if title:
                method = 'osm_wikipedia'
        if not title:
            log(f'  fid={fid:<6} {name[:30]:<30}  no en.wikipedia article (qid={qid})')
            miss += 1
            continue

        time.sleep(0.4)
        summary = fetch_summary(title)
        if not summary or not summary.get('extract'):
            log(f'  fid={fid:<6} {name[:30]:<30}  summary empty for "{title}"')
            miss += 1
            continue

        try:
            upsert(fid, qid or '', summary, method, dist_m)
            ok += 1
            log(f'  fid={fid:<6} {name[:30]:<30}  -> {summary["title"][:40]} ({len(summary["extract"])} chars)')
        except Exception as e:
            log(f'  fid={fid:<6} ERR upsert: {e}')
            miss += 1
        time.sleep(0.4)

    log(f'Done. ok={ok} miss={miss}')


if __name__ == '__main__':
    main()
