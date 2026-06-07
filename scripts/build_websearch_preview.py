"""Build a stratified HTML preview of recent websearch beach photos.

Used to visually QA the Tavily websearch loader's bias quality + spot host
pollution that should be added to load_websearch_photos.HOST_BLOCKLIST.

Sampling: NTILE(5) per state by per-beach photo count (descending), one
beach per stratum. Defaults to the 9 states fixed in the 2026-06-06 entity-
refactor sweep; pass --states A,B,C to override.

Usage:
  .venv-pipeline/Scripts/python.exe scripts/build_websearch_preview.py
  .venv-pipeline/Scripts/python.exe scripts/build_websearch_preview.py \\
      --load-after '2026-06-06 18:00' --out admin/preview_websearch_20260607.html
"""
from __future__ import annotations
import argparse
import html as html_lib
import os
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import psycopg2


DEFAULT_STATES = ['HI', 'MA', 'WA', 'MI', 'OH', 'NH', 'RI', 'AL', 'DE']
DEFAULT_LOAD_AFTER = '2026-06-06 18:00'
DEFAULT_OUT = 'admin/preview_websearch_20260607.html'

SQL = """
WITH bp_per_beach AS (
  SELECT b.fid, b.name, b.state, COUNT(*) AS n
  FROM public.beaches_gold b
  JOIN public.beach_photos bp ON bp.arena_group_id = b.fid
  WHERE bp.source = 'websearch'
    AND bp.loaded_at > %(load_after)s
    AND b.state = ANY(%(states)s)
  GROUP BY 1, 2, 3
),
strat AS (
  SELECT *, NTILE(5) OVER (PARTITION BY state ORDER BY n DESC, fid) AS stratum
  FROM bp_per_beach
),
picked AS (
  SELECT DISTINCT ON (state, stratum) state, stratum, fid, name
  FROM strat
  ORDER BY state, stratum, md5(fid::text)
)
SELECT p.state, p.stratum, p.fid, p.name,
       bp.image_url, bp.page_url, bp.source_meta->>'host' AS host,
       bp.source_meta->>'description' AS descr,
       (bp.source_meta->>'rank')::int AS rk
FROM picked p
JOIN public.beach_photos bp
  ON bp.arena_group_id = p.fid AND bp.source = 'websearch'
WHERE bp.loaded_at > %(load_after)s
ORDER BY p.state, p.stratum, p.fid, rk;
"""

STYLE = """body{font-family:system-ui;max-width:1400px;margin:24px auto;padding:0 16px;background:#f5f5f5;color:#222}
h1{margin:0 0 8px 0}h2{margin-top:32px;border-bottom:2px solid #ccc;padding-bottom:6px}
h3{margin:24px 0 8px 0;font-size:16px}
.beach{background:white;border-radius:8px;padding:14px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.photos{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-top:8px}
.photo{border:1px solid #eee;border-radius:6px;overflow:hidden;background:#fafafa;font-size:12px}
.photo img{width:100%;height:160px;object-fit:cover;display:block;background:#ccc}
.photo .meta{padding:6px 8px}
.photo .host{color:#666;font-size:11px;margin-bottom:4px}
.photo .descr{color:#333;line-height:1.35}
.toc a{display:inline-block;margin:0 8px 6px 0;padding:3px 8px;background:white;border:1px solid #ddd;border-radius:14px;text-decoration:none;color:#06c;font-size:13px}
.stratum{color:#888;font-weight:normal;font-size:12px}"""


def _load_env() -> None:
    for line in Path('scripts/pipeline/.env').read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip().strip("'\"")


def _pooler_url() -> str:
    base = Path('supabase/.temp/pooler-url').read_text().strip()
    pw = quote(os.environ['SUPABASE_DB_PASSWORD'])
    return base.replace('@aws', f':{pw}@aws').replace('6543', '5432')


def _fetch_rows(states: list[str], load_after: str) -> list[tuple]:
    with psycopg2.connect(_pooler_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, {'states': states, 'load_after': load_after})
            return cur.fetchall()


def _build_html(rows: list[tuple], states: list[str], notes: str) -> str:
    by_state = defaultdict(lambda: defaultdict(list))
    fid_meta: dict[int, tuple[str, int]] = {}
    for state, stratum, fid, name, image_url, page_url, host, descr, rk in rows:
        by_state[state][fid].append({
            'image_url': image_url, 'page_url': page_url,
            'host': host, 'descr': descr, 'rk': rk,
        })
        fid_meta[fid] = (name, stratum)

    parts: list[str] = []
    parts.append('<!doctype html><html><head><meta charset="utf-8">')
    parts.append('<title>Websearch sweep preview</title>')
    parts.append(f'<style>{STYLE}</style></head><body>')
    parts.append('<h1>Websearch sweep preview</h1>')
    if notes:
        parts.append(f'<p>{html_lib.escape(notes)}</p>')

    parts.append('<div class="toc">')
    for st in states:
        n_b = len(by_state[st])
        n_p = sum(len(v) for v in by_state[st].values())
        parts.append(f'<a href="#{st}">{st} ({n_b} beaches, {n_p} photos)</a>')
    parts.append('</div>')

    for st in states:
        if not by_state[st]:
            continue
        parts.append(f'<h2 id="{st}">{st}</h2>')
        for fid in sorted(by_state[st].keys(), key=lambda f: fid_meta[f][1]):
            name, stratum = fid_meta[fid]
            photos = by_state[st][fid]
            parts.append('<div class="beach">')
            parts.append(
                f'<h3>{html_lib.escape(name)} '
                f'<span class="stratum">— fid {fid} · stratum {stratum} · '
                f'{len(photos)} photos</span></h3>'
            )
            parts.append('<div class="photos">')
            for p in photos:
                img = html_lib.escape(p['image_url'] or '')
                page = html_lib.escape(p['page_url'] or '#')
                host = html_lib.escape(p['host'] or '')
                descr = html_lib.escape((p['descr'] or '')[:200])
                parts.append(
                    f'<div class="photo">'
                    f'<a href="{page}" target="_blank">'
                    f'<img src="{img}" loading="lazy" alt=""></a>'
                    f'<div class="meta"><div class="host">{host}</div>'
                    f'<div class="descr">{descr}</div></div></div>'
                )
            parts.append('</div></div>')
    parts.append('</body></html>')
    return '\n'.join(parts)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--states', default=','.join(DEFAULT_STATES))
    ap.add_argument('--load-after', default=DEFAULT_LOAD_AFTER)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--notes', default='')
    args = ap.parse_args()

    _load_env()
    states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    rows = _fetch_rows(states, args.load_after)
    html_doc = _build_html(rows, states, args.notes)
    Path(args.out).write_text(html_doc, encoding='utf-8')
    total_beaches = len({(s, f) for s, _, f, *_ in rows})
    print(f'Wrote {args.out}  beaches={total_beaches} photos={len(rows)}')


if __name__ == '__main__':
    main()
