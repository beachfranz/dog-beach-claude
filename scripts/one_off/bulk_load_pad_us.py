"""bulk_load_pad_us.py — pre-load PAD-US for all (or priority) states.

Idempotent + resumable + storage-gated. Designed for autonomous runs
when Franz is AFK. Pauses if DB size approaches the Nano-tier ceiling.

Usage:
  python scripts/one_off/bulk_load_pad_us.py
  python scripts/one_off/bulk_load_pad_us.py --priority 1   # coastal only
  python scripts/one_off/bulk_load_pad_us.py --states AK,ME,FL  # custom
  python scripts/one_off/bulk_load_pad_us.py --storage-cap-mb 5000

Per-state loader is the existing scripts/external_sources.py. Each
state is its own ArcGIS REST pull, takes 1-3 min, exits cleanly. A
failure on one state doesn't poison the rest.

Resume: if pad_us_units already has >100 rows for a state, skip it.
Storage gate: stop if pg_database_size approaches --storage-cap-mb.

Logs every step to stdout (also tee to tmp/bulk_pad_us.log for review).
"""
from __future__ import annotations
import argparse, os, subprocess, sys, time, urllib.parse
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

# Priority order. CA + OR + WA already loaded.
PRIORITY_1 = [  # coastal + Great Lakes
    'AK','ME','NH','MA','RI','CT','NY','NJ','DE','MD','VA','NC','SC','GA',
    'FL','AL','MS','LA','TX','HI',
    'MI','MN','WI','IL','IN','OH','PA',
]
PRIORITY_2 = [  # major-river / interior recreational
    'ID','NV','AZ','UT','NM','CO','WY','MT','ND','SD','NE','KS','OK','AR',
    'MO','IA','TN','KY','WV','VT',
]


def q(sql, args=None):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []


def pad_us_count(state: str) -> int:
    rs = q("select count(*) c from public.pad_us_units where state=%s", (state,))
    return rs[0]['c']


def db_size_mb() -> int:
    rs = q("select pg_database_size(current_database())/1024/1024 as mb")
    return int(rs[0]['mb'])


def log(msg: str):
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--priority', type=int, choices=[1,2,3], default=1,
                    help='1=coastal+great lakes, 2=interior+rivers, 3=both')
    ap.add_argument('--states', help='Comma-sep list to override priority (e.g., AK,ME,FL)')
    ap.add_argument('--storage-cap-mb', type=int, default=6500,
                    help='Pause if DB size approaches this (default 6.5GB; Nano is ~8GB)')
    ap.add_argument('--rest-seconds', type=int, default=5,
                    help='Sleep between states (be polite to Living Atlas)')
    ap.add_argument('--dry-run', action='store_true', help='List what would run, exit')
    args = ap.parse_args()

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    elif args.priority == 1:
        states = PRIORITY_1
    elif args.priority == 2:
        states = PRIORITY_2
    else:
        states = PRIORITY_1 + PRIORITY_2

    log(f'Plan: load PAD-US for {len(states)} states (priority {args.priority})')
    log(f'States: {", ".join(states)}')
    log(f'Storage cap: {args.storage_cap_mb} MB')

    initial_size = db_size_mb()
    log(f'Current DB size: {initial_size} MB')

    if args.dry_run:
        for st in states:
            n = pad_us_count(st)
            tag = 'SKIP (already loaded)' if n > 100 else 'WOULD LOAD'
            log(f'  {st}: {n:>6} pad_us rows  {tag}')
        return

    loaded, skipped, failed = 0, 0, 0
    for st in states:
        n = pad_us_count(st)
        if n > 100:
            log(f'{st}: {n} pad_us rows already; skipping')
            skipped += 1
            continue

        size_mb = db_size_mb()
        if size_mb > args.storage_cap_mb:
            log(f'STORAGE GATE: {size_mb} MB > {args.storage_cap_mb} MB cap; stopping')
            break

        log(f'{st}: loading PAD-US (DB at {size_mb} MB)')
        t0 = time.time()
        result = subprocess.run(
            ['python', str(ROOT / 'scripts' / 'external_sources.py'),
             'load', 'pad_us', '--state', st],
            cwd=ROOT, capture_output=True, text=True, timeout=900,
        )
        elapsed = time.time() - t0
        post_n = pad_us_count(st)
        if result.returncode == 0 and post_n > 100:
            log(f'  {st}: loaded {post_n} rows in {elapsed:.0f}s (DB now {db_size_mb()} MB)')
            loaded += 1
        else:
            log(f'  {st}: FAILED rc={result.returncode} elapsed={elapsed:.0f}s')
            log(f'    stderr tail: {result.stderr[-500:]}')
            failed += 1

        time.sleep(args.rest_seconds)

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')
    log(f'DB size delta: {initial_size} MB -> {db_size_mb()} MB')


if __name__ == '__main__':
    main()
