"""backfill_agency_aliases.py — populate agency_aliases from PAD-US + OSM.

For every distinct loc_mang/loc_own value in pad_us_units and every
distinct operator tag in osm_landing, attempt to resolve to an existing
operator via public.resolve_agency() and INSERT an alias row. Skipped
on ON CONFLICT.

Runs out-of-band (long-running connection, paginated) because doing it
inside a migration's 120s statement_timeout fails on the cross-join
lateral cardinality.

Idempotent — re-running picks up where it left off via ON CONFLICT DO
NOTHING. Each chunk commits independently.

Usage:
  python scripts/one_off/backfill_agency_aliases.py
  python scripts/one_off/backfill_agency_aliases.py --chunk-size 100
"""
import argparse, os, sys, time, urllib.parse
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


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def open_conn():
    c = psycopg2.connect(**PG)
    c.autocommit = False
    with c.cursor() as cur:
        # Long timeout for chunk inserts
        cur.execute("set statement_timeout = '600s'")
    return c


def backfill_chunk(c, source_label, distinct_sql, chunk_size, offset):
    """Insert aliases for one chunk. Returns rows inserted."""
    with c.cursor() as cur:
        cur.execute(f"""
            insert into public.agency_aliases
              (alias, alias_normalized, alias_source, operator_id,
               match_method, confidence, notes)
            select
              alias_text,
              public._normalize_agency_text(alias_text),
              %s as alias_source,
              r.operator_id,
              case when r.confidence >= 0.95 then 'exact'
                   when r.confidence >= 0.85 then 'normalized_exact'
                   when r.confidence >= 0.78 then 'normalized_stripped'
                   else 'fuzzy' end,
              r.confidence,
              'auto-backfilled by backfill_agency_aliases.py'
            from (
              {distinct_sql}
              order by 1
              offset %s limit %s
            ) src
            cross join lateral public.resolve_agency(src.alias_text, null, null, 0.65) r
            on conflict (alias, alias_source) do nothing
            returning 1
        """, (source_label, offset, chunk_size))
        rows = cur.rowcount
    c.commit()
    return rows


def total_count(c, distinct_sql):
    with c.cursor() as cur:
        cur.execute(f"select count(*) from ({distinct_sql}) t")
        return cur.fetchone()[0]


def run_backfill(c, source_label, distinct_sql, chunk_size):
    total = total_count(c, distinct_sql)
    log(f'  {source_label}: {total} distinct candidates')
    if total == 0: return
    inserted = 0
    offset = 0
    t0 = time.time()
    while offset < total:
        chunk_t0 = time.time()
        rows = backfill_chunk(c, source_label, distinct_sql, chunk_size, offset)
        inserted += rows
        offset += chunk_size
        elapsed = time.time() - t0
        chunk_elapsed = time.time() - chunk_t0
        log(f'    [{min(offset, total)}/{total}] ({offset/total*100:.0f}%) '
            f'inserted={inserted} chunk={chunk_elapsed:.1f}s total_elapsed={elapsed:.0f}s')
    log(f'  {source_label}: DONE — {inserted} rows inserted in {time.time()-t0:.0f}s')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--chunk-size', type=int, default=200)
    args = ap.parse_args()

    log('agency_aliases backfill starting')
    c = open_conn()

    # PAD-US Loc_Mang values
    run_backfill(c, 'pad_us_loc_mang',
        """select distinct nullif(trim(loc_mang),'') as alias_text
             from public.pad_us_units
            where loc_mang is not null and trim(loc_mang) <> ''""",
        args.chunk_size)

    # PAD-US Loc_Own values
    run_backfill(c, 'pad_us_loc_own',
        """select distinct nullif(trim(loc_own),'') as alias_text
             from public.pad_us_units
            where loc_own is not null and trim(loc_own) <> ''""",
        args.chunk_size)

    # OSM operator tags
    run_backfill(c, 'osm_operator',
        """select distinct nullif(trim(tags->>'operator'),'') as alias_text
             from public.osm_landing
            where tags ? 'operator' and trim(tags->>'operator') <> ''""",
        args.chunk_size)

    # Final stats
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""select alias_source, match_method, count(*) c
                         from public.agency_aliases group by 1,2 order by 1,2""")
        log('Final agency_aliases distribution:')
        for r in cur.fetchall():
            log(f"  {r['alias_source']:<22} {r['match_method']:<22} {r['c']}")

    c.close()
    log('done')


if __name__ == '__main__':
    main()
