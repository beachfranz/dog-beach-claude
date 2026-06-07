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

# Entity-specific table + FK shapes. dog_park doesn't have a load_after
# filter applied — most dog park websearch data is older; we sample from
# the whole catalog instead.
ENTITY_META = {
    'beach': {
        'gold_table': 'public.beaches_gold',
        'photo_table': 'public.beach_photos',
        'fk_col': 'arena_group_id',
    },
    'dog_park': {
        'gold_table': 'public.dog_parks_gold',
        'photo_table': 'public.dog_park_photos',
        'fk_col': 'dog_park_fid',
    },
}


def _build_sql(entity: str, strata_mode: str, strata: int, use_load_after: bool) -> str:
    """Compose the stratified-sample SQL for the chosen entity + strata mode."""
    meta = ENTITY_META[entity]
    if strata_mode == 'state':
        partition = 'PARTITION BY state '
        order_outer = 'state, stratum, md5(fid::text)'
        distinct_on = '(state, stratum)'
        select_strata_cols = 'state, stratum, fid, name'
    else:  # global
        partition = ''
        order_outer = 'stratum, md5(fid::text)'
        distinct_on = '(stratum)'
        select_strata_cols = "''::text AS state, stratum, fid, name"

    state_filter = 'AND e.state = ANY(%(states)s)' if strata_mode == 'state' else ''
    load_filter = "AND bp.loaded_at > %(load_after)s" if use_load_after else ''

    return f"""
    WITH per_entity AS (
      SELECT e.fid, e.name, e.state, COUNT(*) AS n
      FROM {meta['gold_table']} e
      JOIN {meta['photo_table']} bp ON bp.{meta['fk_col']} = e.fid
      WHERE bp.source = 'websearch'
        AND e.is_active
        {load_filter}
        {state_filter}
      GROUP BY 1, 2, 3
    ),
    strat AS (
      SELECT *, NTILE({strata}) OVER ({partition}ORDER BY n DESC, fid) AS stratum
      FROM per_entity
    ),
    picked AS (
      SELECT DISTINCT ON {distinct_on} {select_strata_cols}
      FROM strat
      ORDER BY {order_outer}
    )
    SELECT p.state, p.stratum, p.fid, p.name,
           bp.image_url, bp.page_url, bp.source_meta->>'host' AS host,
           bp.source_meta->>'description' AS descr,
           (bp.source_meta->>'rank')::int AS rk
    FROM picked p
    JOIN {meta['photo_table']} bp
      ON bp.{meta['fk_col']} = p.fid AND bp.source = 'websearch'
    {('WHERE bp.loaded_at > %(load_after)s' if use_load_after else '')}
    ORDER BY p.state NULLS FIRST, p.stratum, p.fid, rk;
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


def _fetch_rows(entity: str, strata_mode: str, strata: int,
                states: list[str], load_after: str | None) -> list[tuple]:
    use_load_after = bool(load_after)
    sql = _build_sql(entity, strata_mode, strata, use_load_after)
    params: dict = {'states': states}
    if use_load_after:
        params['load_after'] = load_after
    with psycopg2.connect(_pooler_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _build_html(rows: list[tuple], states: list[str], notes: str,
                strata_mode: str = 'state') -> str:
    by_state = defaultdict(lambda: defaultdict(list))
    fid_meta: dict[int, tuple[str, int, str]] = {}
    for state, stratum, fid, name, image_url, page_url, host, descr, rk in rows:
        by_state[state][fid].append({
            'image_url': image_url, 'page_url': page_url,
            'host': host, 'descr': descr, 'rk': rk,
        })
        fid_meta[fid] = (name, stratum, state)

    parts: list[str] = []
    parts.append('<!doctype html><html><head><meta charset="utf-8">')
    parts.append('<title>Websearch sweep preview</title>')
    parts.append(f'<style>{STYLE}</style></head><body>')
    parts.append('<h1>Websearch sweep preview</h1>')
    if notes:
        parts.append(f'<p>{html_lib.escape(notes)}</p>')

    if strata_mode == 'global':
        # Flat layout: just stratum-ordered list, no per-state grouping
        all_fids = sorted(fid_meta.keys(), key=lambda f: fid_meta[f][1])
        parts.append(f'<p><b>{len(all_fids)} entities, '
                     f'{sum(len(v) for v in by_state[""].values())} photos</b></p>')
        for fid in all_fids:
            name, stratum, st = fid_meta[fid]
            photos = by_state[''][fid] if '' in by_state else by_state[st][fid]
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
    else:
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
                name, stratum, _ = fid_meta[fid]
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
    ap.add_argument('--entity', choices=['beach', 'dog_park'], default='beach')
    ap.add_argument('--strata-mode', choices=['state', 'global'], default='state',
                    help='state: NTILE per state (default, 5 strata per state). '
                         'global: NTILE across the whole catalog (e.g. 50 total).')
    ap.add_argument('--strata', type=int, default=5,
                    help='Strata count. For state mode this is per-state (default 5); '
                         'for global it is the total sample size.')
    ap.add_argument('--states', default=','.join(DEFAULT_STATES))
    ap.add_argument('--load-after', default=DEFAULT_LOAD_AFTER,
                    help='Only photos loaded after this timestamp. Pass empty string '
                         'to disable (sample the whole catalog).')
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--notes', default='')
    args = ap.parse_args()

    _load_env()
    states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    load_after = args.load_after.strip() or None
    rows = _fetch_rows(args.entity, args.strata_mode, args.strata, states, load_after)
    html_doc = _build_html(rows, states, args.notes, strata_mode=args.strata_mode)
    Path(args.out).write_text(html_doc, encoding='utf-8')
    total = len({f for _, _, f, *_ in rows})
    print(f'Wrote {args.out}  {args.entity}s={total} photos={len(rows)}')


if __name__ == '__main__':
    main()
