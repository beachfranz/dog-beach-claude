"""scripts/orchestrator/fire_scoring_chunked.py -- POST daily-beach-refresh
in chunks of N location_ids so we don't blow the 150s edge-fn timeout.

Usage (from repo root):
  python -m scripts.orchestrator.fire_scoring_chunked --all-scoreable
  python -m scripts.orchestrator.fire_scoring_chunked --fids 34,107,213
  python -m scripts.orchestrator.fire_scoring_chunked --phases 04,05 --chunk 50
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]


def supabase_query(sql: str) -> list[dict]:
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False,
                                     encoding='utf-8') as f:
        f.write(sql); tmp = f.name
    try:
        out = subprocess.check_output(
            ['supabase', 'db', 'query', '--linked', '-f', tmp],
            text=True, cwd=str(REPO_ROOT))
    finally:
        os.unlink(tmp)
    idx, end = out.find('{'), out.rfind('}')
    return json.loads(out[idx:end+1]).get('rows', []) if idx >= 0 else []


def fire_chunk(location_ids: list[str], url: str, anon: str, secret: str) -> dict:
    body = json.dumps({'location_ids': location_ids}).encode()
    req = urllib.request.Request(
        url, method='POST', data=body,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {anon}',
            'apikey': anon,
            'x-admin-secret': secret,
        })
    ctx = ssl.create_default_context()
    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=300) as r:
            return {'ok': True, 'data': json.loads(r.read())}
    except urllib.error.HTTPError as e:
        return {'ok': False, 'status': e.code, 'body': e.read()[:500].decode()}
    except Exception as e:
        return {'ok': False, 'error': f'{type(e).__name__}: {e}'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--all-scoreable', action='store_true',
                    help='Fire scoring for every is_scoreable=true active CA beach')
    ap.add_argument('--phases', help='Comma-separated phase codes to filter '
                                     '(e.g. "04,05")')
    ap.add_argument('--fids', help='Comma-separated fid list')
    ap.add_argument('--chunk', type=int, default=50,
                    help='Beaches per fan-out batch (default: 50)')
    ap.add_argument('--sleep', type=float, default=2.0,
                    help='Seconds to sleep between chunks (default: 2)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Print plan without firing')
    args = ap.parse_args()

    # Resolve location_ids
    where_parts = ['is_active', 'is_scoreable']
    if args.fids:
        where_parts.append(f"fid in ({args.fids})")
    elif args.phases:
        phase_csv = ",".join(f"'{p.strip()}'" for p in args.phases.split(','))
        where_parts.append(f"phase in ({phase_csv})")
    elif not args.all_scoreable:
        ap.error('specify --all-scoreable, --phases, or --fids')

    rows = supabase_query(f"""
        select fid, location_id
          from public.beach_pipeline_status
         where {' and '.join(where_parts)}
           and location_id is not null
         order by fid
    """)
    location_ids = [r['location_id'] for r in rows]
    if not location_ids:
        print('No matching beaches.'); return 0

    n = len(location_ids)
    chunks = [location_ids[i:i+args.chunk] for i in range(0, n, args.chunk)]
    print(f'== {n} beaches -> {len(chunks)} chunks of {args.chunk}')

    if args.dry_run:
        for i, c in enumerate(chunks, 1):
            print(f'  chunk {i}/{len(chunks)}: {len(c)} beaches')
        return 0

    dotenv.load_dotenv(str(REPO_ROOT / 'scripts/pipeline/.env'))
    url = os.environ['SUPABASE_URL'] + '/functions/v1/daily-beach-refresh'
    secret = os.environ['ADMIN_SECRET']
    anon = 'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'

    t0 = time.time()
    n_ok, n_fail = 0, 0
    for i, c in enumerate(chunks, 1):
        elapsed = time.time() - t0
        print(f'\n[{i}/{len(chunks)}] firing {len(c)} beaches  '
              f'(elapsed {elapsed:.0f}s)')
        result = fire_chunk(c, url, anon, secret)
        if result['ok']:
            refreshed = result['data'].get('refreshed', '?')
            print(f'    OK -- refreshed {refreshed}')
            n_ok += 1
        else:
            print(f'    FAILED: {result}')
            n_fail += 1
        if i < len(chunks):
            time.sleep(args.sleep)

    elapsed = time.time() - t0
    print(f'\n== done in {elapsed:.0f}s  chunks_ok={n_ok}/{len(chunks)}  '
          f'failed={n_fail}')
    return 0 if n_fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
