"""scripts/orchestrator/worker.py -- phase advancer.

Takes a list of fids; for each one, queries beach_pipeline_status and
advances the beach by one phase using whatever tooling exists today.

Phases:
  01_no_evidence         needs extraction (no per-fid extractor wired yet -- see TODO)
  02/03 evidence/canonical_pending  trigger should auto-handle; falls back to manual chain
  04_dogs_allowed_only   needs zone_rules text-repass (wired)
  05_zone_rules_no_score needs scoring run (wired via daily-beach-refresh edge fn)
  06_complete            done

Usage (from repo root):
  python -m scripts.orchestrator.worker --fids 34,107,213
  python -m scripts.orchestrator.worker --fids 34,107,213 --max-passes 3
  python -m scripts.orchestrator.worker --auto                # all stuck SoCal beaches

Each pass advances each beach by one phase. With --max-passes >1, the
worker re-queries status and advances again. Stops when all listed
beaches reach 06_complete or there's no more advancement possible.

Logs each invocation to pipeline_runs via subprocess to scripts.orchestrator.run.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import ssl
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def supabase_query(sql: str) -> list[dict]:
    """Run SQL via supabase CLI; return rows."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False,
                                     encoding='utf-8') as f:
        f.write(sql)
        tmp = f.name
    try:
        out = subprocess.check_output(
            ['supabase', 'db', 'query', '--linked', '-f', tmp],
            text=True, cwd=str(REPO_ROOT),
        )
    finally:
        os.unlink(tmp)
    idx = out.find('{')
    end = out.rfind('}')
    if idx < 0 or end <= idx:
        return []
    return json.loads(out[idx:end+1]).get('rows', [])


def query_phase(fid: int) -> dict:
    rows = supabase_query(f"""
        select s.fid, s.name, s.phase, s.location_id,
               s.has_bep_dogs, s.has_policy_row, s.has_dogs_allowed,
               s.has_zone_rules, s.has_recent_score
          from public.beach_pipeline_status s
         where s.fid = {fid}
    """)
    return rows[0] if rows else {}


def fire_zone_rules_repass(fids: list[int]) -> int:
    """Run scripts/repass_zone_rules.py --fids X -- wraps via the
    orchestrator runner so it ends up in pipeline_runs."""
    fid_str = ','.join(str(f) for f in fids)
    print(f'  -> zone_rules repass for fid(s) {fid_str}')
    proc = subprocess.run(
        [sys.executable, 'scripts/repass_zone_rules.py', '--fids', fid_str],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
        encoding='utf-8', errors='replace',
    )
    if proc.returncode != 0:
        print(f'    FAILED (exit {proc.returncode}): {proc.stderr[-500:]}')
    else:
        print(f'    OK')
    return proc.returncode


def fire_score(location_ids: list[str]) -> int:
    """POST to daily-beach-refresh edge function for these location_ids."""
    import dotenv
    dotenv.load_dotenv(str(REPO_ROOT / 'scripts/pipeline/.env'))
    url = os.environ['SUPABASE_URL'] + '/functions/v1/daily-beach-refresh'
    admin_secret = os.environ['ADMIN_SECRET']
    anon = 'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'
    print(f'  -> daily-beach-refresh for location_id(s) {location_ids}')

    body = json.dumps({'location_ids': location_ids}).encode()
    req = urllib.request.Request(
        url, method='POST', data=body,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {anon}',
            'apikey': anon,
            'x-admin-secret': admin_secret,
        },
    )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=300) as r:
            data = json.loads(r.read())
        print(f'    OK -- refreshed {data.get("refreshed", "?")} beaches')
        return 0
    except urllib.error.HTTPError as e:
        print(f'    FAILED ({e.code}): {e.read()[:500].decode()}')
        return 1
    except Exception as e:
        print(f'    FAILED: {type(e).__name__}: {e}')
        return 1


def advance_one(fid: int) -> str:
    """Advance one beach by one phase. Returns a status string."""
    p = query_phase(fid)
    if not p:
        return f'fid {fid} not found in beaches_gold'
    name = p.get('name', f'fid {fid}')
    phase = p.get('phase', 'unknown')
    print(f'\nfid={fid}  {name}  phase={phase}')

    if phase in ('00_inactive', '00_not_scoreable', '06_complete'):
        return f'{phase} -- nothing to do'

    if phase == '01_no_evidence':
        return ('NEEDS_EXTRACTION -- no per-fid extraction script wired. '
                'Add --fids support to extract_beach_policies.py or build '
                'extract_for_fid.py to advance beaches at this phase.')

    if phase in ('02_evidence_only', '03_canonical_pending'):
        # Trigger should have fired; force the chain manually.
        supabase_query(f"""
            select public.compute_beach_field_consensus({fid});
            select public._resolve_dogs_gold({fid});
            select public.promote_canonical_dogs_to_beach_dog_policy({fid}, 0.5);
        """)
        return 'kicked dogs-chain manually'

    if phase == '04_dogs_allowed_only':
        rc = fire_zone_rules_repass([fid])
        return 'zone_rules repassed' if rc == 0 else 'zone_rules FAILED'

    if phase == '05_zone_rules_no_score':
        loc = p.get('location_id')
        if not loc:
            return 'no location_id -- cannot fire scoring'
        rc = fire_score([loc])
        return 'score fired' if rc == 0 else 'score FAILED'

    return f'unhandled phase {phase}'


def find_stuck_fids(limit: int = 25) -> list[int]:
    """Return fids in is_scoreable + active + non-complete phases."""
    rows = supabase_query(f"""
        select fid from public.beach_pipeline_status
         where is_active and is_scoreable
           and phase not in ('06_complete', '00_inactive', '00_not_scoreable')
         order by phase, fid
         limit {limit}
    """)
    return [r['fid'] for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fids', help='Comma-separated list of fids to advance')
    ap.add_argument('--auto', action='store_true',
                    help='Advance up to N stuck SoCal beaches (default 25)')
    ap.add_argument('--limit', type=int, default=25,
                    help='Used with --auto')
    ap.add_argument('--max-passes', type=int, default=1,
                    help='Re-query phases and advance again up to N times')
    args = ap.parse_args()

    if args.auto:
        fids = find_stuck_fids(args.limit)
        print(f'Auto mode: {len(fids)} stuck beaches')
    elif args.fids:
        fids = [int(f) for f in args.fids.split(',') if f.strip()]
    else:
        ap.error('specify --fids or --auto')

    if not fids:
        print('No fids to process.')
        return 0

    for pass_n in range(1, args.max_passes + 1):
        if args.max_passes > 1:
            print(f'\n=== pass {pass_n}/{args.max_passes} ===')
        all_done = True
        for fid in fids:
            result = advance_one(fid)
            print(f'  -> {result}')
            if 'nothing to do' not in result and 'NEEDS' not in result:
                all_done = False
        if all_done:
            print('\nAll beaches at terminal phase or out of advancement.')
            break
        if pass_n < args.max_passes:
            print(f'\nSleeping 5s before next pass…')
            time.sleep(5)
    return 0


if __name__ == '__main__':
    sys.exit(main())
