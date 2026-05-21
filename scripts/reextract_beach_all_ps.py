"""reextract_beach_all_ps.py — re-extract ALL policy sources for one or
more beaches via v2 area-first prompt. Wraps reextract_area_first.py.

Per Franz 2026-05-21. When a beach has bps rows from multiple
policy_sources (e.g. SD city policy + SD county code + state parks
agency), re-running just one ps leaves the others' regions in place.
This helper iterates all ps's per beach.

Usage:
  python scripts/reextract_beach_all_ps.py --fid 8540
  python scripts/reextract_beach_all_ps.py --fid 8540 8452 6054
  python scripts/reextract_beach_all_ps.py --fids-file scope.txt
  python scripts/reextract_beach_all_ps.py --fid 8540 --refresh-hourly
"""
from __future__ import annotations
import sys, os, argparse, urllib.parse, subprocess
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
import truststore; truststore.inject_into_ssl()
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


def fetch_ps_for_fid(cur, fid: int) -> list[int]:
    cur.execute("""SELECT DISTINCT policy_source_id FROM beach_policy_source
                    WHERE beach_fid = %s ORDER BY policy_source_id""", (fid,))
    return [r[0] for r in cur.fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--fid', type=int, nargs='+', help='one or more beach fids')
    g.add_argument('--fids-file', type=str, help='file with one fid per line')
    ap.add_argument('--refresh-hourly', action='store_true',
                     help='also refresh beach_section_hour_status for each fid')
    ap.add_argument('--skip-manual-curator', action='store_true', default=True,
                     help='skip beaches with source=manual_curator (default)')
    args = ap.parse_args()

    if args.fid:
        fids = args.fid
    else:
        fids = [int(l.strip()) for l in Path(args.fids_file).read_text().splitlines() if l.strip()]

    conn = _connect()
    cur = conn.cursor()

    totals = {'beach_skip_manual': 0, 'beach_done': 0,
              'ps_runs': 0, 'ps_failed': 0, 'hourly_refreshed': 0}

    for i, fid in enumerate(fids, 1):
        # Skip manual_curator beaches (their zone_rules are preserved)
        cur.execute("""SELECT source FROM beach_dog_policy WHERE arena_group_id = %s""", (fid,))
        row = cur.fetchone()
        src = (row[0] if row else None)
        if args.skip_manual_curator and src == 'manual_curator':
            print(f'[{i}/{len(fids)}] fid={fid} SKIP (manual_curator)')
            totals['beach_skip_manual'] += 1
            continue

        ps_ids = fetch_ps_for_fid(cur, fid)
        if not ps_ids:
            print(f'[{i}/{len(fids)}] fid={fid} (no bps rows)')
            continue
        print(f'[{i}/{len(fids)}] fid={fid}: {len(ps_ids)} policy_sources → {ps_ids}')

        for ps in ps_ids:
            try:
                # Invoke the per-(fid, ps) script in a subprocess so each
                # gets a fresh DB connection (pgbouncer-friendly).
                result = subprocess.run(
                    ['python', 'scripts/reextract_area_first.py', '--fid', str(fid), '--ps-id', str(ps)],
                    cwd=str(ROOT), capture_output=True, text=True, encoding='utf-8', errors='replace',
                )
                # Parse the last "inserted: N" line
                inserted = '?'
                for line in (result.stdout or '').splitlines():
                    if 'inserted:' in line:
                        inserted = line.split('inserted:')[-1].strip()
                if result.returncode == 0:
                    print(f'    ps={ps}: inserted={inserted}')
                    totals['ps_runs'] += 1
                else:
                    print(f'    ps={ps}: FAIL rc={result.returncode}')
                    if result.stderr: print(f'      stderr: {result.stderr[:200]}')
                    totals['ps_failed'] += 1
            except Exception as e:
                print(f'    ps={ps}: EXC {e}')
                totals['ps_failed'] += 1

        if args.refresh_hourly:
            try:
                subprocess.run(['python', 'scripts/refresh_beach_section_hour_status.py',
                                '--fid', str(fid)],
                               cwd=str(ROOT), check=True, capture_output=True, text=True)
                totals['hourly_refreshed'] += 1
            except Exception as e:
                print(f'    hourly-refresh fail: {e}')

        totals['beach_done'] += 1

    print()
    print('=== SUMMARY ===')
    print(f'  beaches done:        {totals["beach_done"]}')
    print(f'  beaches skip manual: {totals["beach_skip_manual"]}')
    print(f'  ps runs ok:          {totals["ps_runs"]}')
    print(f'  ps runs fail:        {totals["ps_failed"]}')
    print(f'  hourly refreshed:    {totals["hourly_refreshed"]}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
