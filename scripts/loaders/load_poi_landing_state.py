"""load_poi_landing_state.py — state-scoped POI loader for poi_landing.

Reads share/Dog_Beaches/US_beaches_with_state.csv (STATE column = 2-letter)
and inserts rows for one state into public.poi_landing. Records
external_source_status so the pipeline's _ensure_loader gate works.

Closes the gap surfaced by 2026-05-22 DE virgin-state test:
  - bulk_load_overpass.py auto-pulls OSM beaches per state ✓
  - load_poi_landing.py was a national-CSV one-shot, no per-state path ✗
  - For DE: stripping POI dropped beaches_gold from 49 → 6 (POI is
    load-bearing; OSM natural=beach alone is sparse for most coastal states)

Idempotent: skips state if poi_landing already has ≥1 row for it.

Usage:
  python scripts/loaders/load_poi_landing_state.py --states DE
  python scripts/loaders/load_poi_landing_state.py --states DE,MS,NH
  python scripts/loaders/load_poi_landing_state.py --states DE --force
"""
from __future__ import annotations
import argparse
import csv
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding='utf-8')

from psycopg2.extras import execute_values
from scripts.common.db import connect

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _external_source_status import record_status  # noqa: E402

CSV_PATH = ROOT / 'share' / 'Dog_Beaches' / 'US_beaches_with_state.csv'
FETCHED_BY = 'load_poi_landing_state'


def log(msg: str):
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


def count_state(state: str) -> int:
    c = connect()
    c.set_session(autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute("select count(*) from public.poi_landing where state=%s", (state,))
            return cur.fetchone()[0]
    finally:
        c.close()


def load_state(state: str, force: bool) -> tuple[int, str]:
    """Returns (rows_inserted, status)."""
    existing = count_state(state)
    if existing > 0 and not force:
        log(f'  {state}: {existing} poi_landing rows already; skipping (--force to override)')
        return 0, 'skipped'

    rows = []
    with CSV_PATH.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if (r.get('STATE') or '').strip().upper() != state:
                continue
            rows.append((
                FETCHED_BY,
                int(r['fid']),
                r['WKT'] or None,
                (r.get('NAME') or '').strip() or None,
                (r.get('COUNTRY') or '').strip() or None,
                (r.get('ADDR1') or '').strip() or None,
                (r.get('ADDR2') or '').strip() or None,
                (r.get('ADDR3') or '').strip() or None,
                (r.get('ADDR4') or '').strip() or None,
                (r.get('ADDR5') or '').strip() or None,
                (r.get('CAT_MOD') or '').strip() or None,
                state,         # state column (CSV is authoritative)
                r['WKT'],      # geom — parsed by SQL template
                True,          # is_active
                True,          # is_dog_beach_signal — CSV is curated
            ))

    log(f'  {state}: {len(rows)} POI rows from CSV')
    if not rows:
        return 0, 'ok'  # genuinely no POIs for this state

    # Self-sufficient INSERT: parse WKT inline via ST_GeomFromText + set
    # state + flag dog-beach signal. Originally relied on
    # trg_poi_landing_enrich BEFORE INSERT trigger to fill these, but that
    # trigger is DISABLED in the current DB (diagnosed 2026-05-22 LATE
    # during DE virgin-state test — geom=NULL, state=NULL,
    # is_dog_beach_signal=FALSE all blocked promote_poi_landing_to_arena).
    # is_dog_beach_signal=TRUE is correct: US_beaches.csv is curated for
    # exactly this signal (cat_mod=travel_and_leisure.beach throughout).
    sql = """
        insert into public.poi_landing
          (fetched_by, fid, raw_wkt, name, country,
           addr1, addr2, addr3, addr4, addr5, cat_mod, state,
           geom, is_active, is_dog_beach_signal)
        values %s
        on conflict (fid, fetched_at) do nothing
    """
    # Custom template: 12 plain %s, then ST_SetSRID(ST_GeomFromText(%s),4326), then 2 plain %s
    template = ("(" + ",".join(["%s"] * 12) + ","
                "ST_SetSRID(ST_GeomFromText(%s),4326),%s,%s)")
    c = connect()
    c.set_session(autocommit=True)
    try:
        with c.cursor() as cur:
            execute_values(cur, sql, rows, template=template, page_size=500)
            inserted = cur.rowcount
            log(f'  {state}: inserted {inserted} poi_landing rows')

            # Explicit PIP enrichment (originally done by disabled trigger):
            # fill county_geoid / county_name / place_fips / place_name /
            # cpad_unit_id from spatial containment. Without these,
            # promote_poi_landing_to_arena leaves county_fips NULL on the
            # arena rows, and the promote→gold join `c.geoid=a.county_fips`
            # silently filters them out. Discovered 2026-05-22 LATE.
            cur.execute("""
                update public.poi_landing pl
                   set county_geoid = sub.geoid,
                       county_name  = sub.name
                  from (select pl2.fid, c.geoid, c.name
                          from public.poi_landing pl2
                          join public.counties c on st_contains(c.geom, pl2.geom)
                         where pl2.state = %s and pl2.county_geoid is null) sub
                 where pl.fid = sub.fid
                   and pl.state = %s
                   and pl.county_geoid is null
            """, (state, state))
            log(f'    PIP enrichment (counties): updated {cur.rowcount} rows')

            cur.execute("""
                update public.poi_landing pl
                   set place_fips = sub.fips_place,
                       place_name = sub.name,
                       place_type = sub.place_type
                  from (select pl2.fid, j.fips_place, j.name, j.place_type
                          from public.poi_landing pl2
                          join public.jurisdictions j on st_contains(j.geom, pl2.geom)
                         where pl2.state = %s and pl2.place_fips is null) sub
                 where pl.fid = sub.fid
                   and pl.state = %s
                   and pl.place_fips is null
            """, (state, state))
            log(f'    PIP enrichment (jurisdictions): updated {cur.rowcount} rows')

            return inserted, 'ok'
    finally:
        c.close()


PG_FOR_STATUS = None
def _get_pg():
    """external_source_status helper needs PG dict; build lazily."""
    global PG_FOR_STATUS
    if PG_FOR_STATUS is not None:
        return PG_FOR_STATUS
    import urllib.parse
    pooler = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
    p = urllib.parse.urlparse(pooler)
    PG_FOR_STATUS = dict(host=p.hostname, port=p.port or 5432, user=p.username,
                         password=os.environ['SUPABASE_DB_PASSWORD'],
                         dbname=(p.path or '/postgres').lstrip('/'), sslmode='require')
    return PG_FOR_STATUS


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', required=True, help='Comma-separated 2-letter codes')
    ap.add_argument('--force', action='store_true',
                    help='Re-load even if state already has rows')
    args = ap.parse_args()

    states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    if not CSV_PATH.exists():
        log(f'ERR: CSV not found at {CSV_PATH}')
        return 1

    log(f'Plan: load POI landing for {len(states)} states from {CSV_PATH.name}')
    log(f'States: {", ".join(states)}')

    loaded, skipped, failed = 0, 0, 0
    for st in states:
        log(f'{st}: loading')
        t0 = time.time()
        try:
            inserted, status = load_state(st, args.force)
            elapsed = time.time() - t0
            log(f'  {st}: done — {status} (rows={inserted}, {elapsed:.1f}s)')
            record_status(_get_pg(), 'poi_landing', st, status, inserted,
                          f'loaded from {CSV_PATH.name} in {elapsed:.1f}s')
            if status == 'ok':
                loaded += 1
            else:
                skipped += 1
        except Exception as e:
            elapsed = time.time() - t0
            log(f'  {st}: FAILED — {e}')
            try:
                record_status(_get_pg(), 'poi_landing', st, 'failed', None, str(e)[:500])
            except Exception:
                pass
            failed += 1

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
