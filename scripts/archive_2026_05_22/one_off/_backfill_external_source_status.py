"""One-off: backfill public.external_source_status from current row counts in
pad_us_units / osm_landing / osm_amenities. Safe to re-run — uses upsert and
only marks states with > threshold rows as 'ok'.

Idempotent: re-running just refreshes last_loaded_at and row_count from
current data.
"""
from __future__ import annotations
import os, urllib.parse
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')


def q(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []


def upsert_many(rows: list[dict]) -> None:
    if not rows:
        return
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                insert into public.external_source_status
                  (source, state, last_loaded_at, row_count, status, notes)
                values (%(source)s, %(state)s, now(), %(row_count)s, 'ok',
                        'backfilled from existing rows')
                on conflict (source, state) do update set
                  last_loaded_at = excluded.last_loaded_at,
                  row_count      = excluded.row_count,
                  status         = excluded.status,
                  notes          = excluded.notes
            """, rows)


def main():
    # PAD-US: per-state counts. Threshold 100 (matches bulk_load_pad_us.py).
    pad_us = q("""
        select state, count(*) c
          from public.pad_us_units
         where state is not null
         group by 1
        having count(*) > 100
    """)
    print(f'PAD-US: {len(pad_us)} states with >100 rows')
    upsert_many([{'source': 'pad_us', 'state': r['state'], 'row_count': r['c']}
                 for r in pad_us])

    # osm_landing: bbox is the only state attribution for now. Use the
    # state column if present; otherwise skip (bulk_load_overpass will
    # backfill on next pass).
    osm_l_cols = q("""select column_name from information_schema.columns
                       where table_schema='public' and table_name='osm_landing'""")
    has_state = any(c['column_name'] == 'state' for c in osm_l_cols)
    if has_state:
        osm_landing = q("""
            select state, count(*) c
              from public.osm_landing
             where state is not null
             group by 1
            having count(*) > 50
        """)
        print(f'osm_landing: {len(osm_landing)} states with >50 rows')
        upsert_many([{'source': 'osm_landing', 'state': r['state'], 'row_count': r['c']}
                     for r in osm_landing])
    else:
        print('osm_landing: no state column — skipping backfill (will be filled on next overpass run)')

    # osm_amenities: state column exists per migration.
    osm_amen = q("""
        select state, count(*) c
          from public.osm_amenities
         where state is not null
         group by 1
        having count(*) > 1000
    """)
    print(f'osm_amenities: {len(osm_amen)} states with >1000 rows')
    upsert_many([{'source': 'osm_amenities', 'state': r['state'], 'row_count': r['c']}
                 for r in osm_amen])

    # Summary
    print('\nFinal external_source_status:')
    for r in q("select source, state, row_count, status, last_loaded_at "
               "from public.external_source_status "
               "order by source, state"):
        rc_disp = f"{r['row_count']:>7}" if r['row_count'] is not None else '   None'
        print(f"  {r['source']:<14} {r['state']:<3} rows={rc_disp} {r['status']}")


if __name__ == '__main__':
    main()
