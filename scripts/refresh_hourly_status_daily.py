"""refresh_hourly_status_daily.py — daily-cron-ready wrapper that
refreshes beach_section_hour_status for MVP+ beaches.

`beach_section_hour_status` depends on current_date (sunrise/sunset +
seasonal windows). The pipeline's hourly_status_refresh phase runs
once per pipeline invocation; if pipeline doesn't run daily, the
materialized table drifts.

This wrapper is intended for daily-cron registration. It calls the
existing `refresh_beach_section_hour_status.py --all` script in chunks
of 100 fids for politeness + connection reuse.

Usage (manual):
  python scripts/refresh_hourly_status_daily.py
  python scripts/refresh_hourly_status_daily.py --states CA,OR,WA

Cron registration (Windows Task Scheduler):
  Run daily at 03:00 local. No special args needed.
"""
from __future__ import annotations
import sys, os, time, argparse, urllib.parse, subprocess
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')


def _connect():
    pp = urllib.parse.urlparse((ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip())
    return psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='CA,OR,WA')
    ap.add_argument('--chunk', type=int, default=100)
    args = ap.parse_args()
    states = [s.strip() for s in args.states.split(',') if s.strip()]

    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
      SELECT fid FROM beaches_gold
       WHERE is_active = true
         AND scoring_tier IN ('daily','hourly')
         AND state = ANY(%s)
       ORDER BY fid
    """, (states,))
    fids = [r[0] for r in cur.fetchall()]
    print(f'Refreshing beach_section_hour_status for {len(fids)} MVP+ beaches in {states}')

    t0 = time.time()
    n_chunks = (len(fids) + args.chunk - 1) // args.chunk
    for i in range(0, len(fids), args.chunk):
        chunk = fids[i:i + args.chunk]
        rc = subprocess.run(
            [sys.executable, 'scripts/refresh_beach_section_hour_status.py',
             '--fids', ','.join(map(str, chunk))],
            cwd=str(ROOT), capture_output=True, text=True, timeout=600,
        )
        idx = i // args.chunk + 1
        if rc.returncode == 0:
            print(f'  chunk {idx}/{n_chunks} ok ({len(chunk)} fids)')
        else:
            print(f'  chunk {idx}/{n_chunks} FAIL: {rc.stderr[-160:]}')

    print(f'\nDone in {time.time()-t0:.0f}s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
