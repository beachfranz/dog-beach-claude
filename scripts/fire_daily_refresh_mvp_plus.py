"""fire_daily_refresh_mvp_plus.py — fire daily-beach-refresh edge fn
for MVP+ daily/hourly tier beaches across CA + OR + WA.

Parallel-but-polite: --workers concurrent batches, each batch is up to
--batch-size beaches sent in one POST. The edge fn processes a batch
sequentially internally, so concurrency = parallel batches.

API call profile per beach: 1 Open-Meteo forecast + 1 NOAA tide
(+ BestTime skipped via skip_besttime=true body flag). At workers=4
batch=25, peak rate ≈ 100 in-flight beaches → ~5-10 Open-Meteo
calls/sec (well under OM's free-tier 600/min cap).

Usage:
  python scripts/fire_daily_refresh_mvp_plus.py                          # CA+OR+WA daily+hourly
  python scripts/fire_daily_refresh_mvp_plus.py --states CA              # one state
  python scripts/fire_daily_refresh_mvp_plus.py --workers 6 --batch-size 30
  python scripts/fire_daily_refresh_mvp_plus.py --dry-run                # scope only
"""
from __future__ import annotations
import sys, os, time, argparse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect


def pick_scope(cur, states: list[str]) -> list[str]:
    cur.execute("""
      SELECT location_id FROM beaches_gold
       WHERE is_active = true
         AND scoring_tier IN ('daily','hourly')
         AND state = ANY(%s)
       ORDER BY scoring_tier DESC, state, fid
    """, (states,))
    return [r[0] for r in cur.fetchall()]


def fire_one_batch(url: str, headers: dict, batch: list[str],
                   batch_idx: int) -> tuple[int, int, str]:
    """POST one batch. Returns (batch_idx, n_beaches_ok, status)."""
    try:
        r = httpx.post(url, headers=headers,
                       json={'location_ids': batch,
                             'tide_window_days': 7,
                             'skip_besttime': True,
                             'skip_recent_hours': 12},
                       timeout=600.0)
        if r.is_success:
            return batch_idx, len(batch), 'ok'
        return batch_idx, 0, f'HTTP {r.status_code}: {r.text[:160]}'
    except Exception as e:
        return batch_idx, 0, f'EXC {str(e)[:160]}'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='CA,OR,WA')
    ap.add_argument('--batch-size', type=int, default=25)
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    states = [s.strip() for s in args.states.split(',') if s.strip()]
    conn = connect()
    cur = conn.cursor()
    ids = pick_scope(cur, states)
    n = len(ids)
    n_batches = (n + args.batch_size - 1) // args.batch_size
    print(f'Scope: {n} beaches across {states}  →  {n_batches} batches of ≤{args.batch_size}')
    print(f'Parallelism: {args.workers} concurrent batches')
    print(f'BestTime: SKIPPED (skip_besttime=true)')
    print(f'skip_recent_hours: 12 (skip beaches refreshed <12h ago)')
    if args.dry_run:
        print('[dry-run] no calls executed'); return 0

    url = os.environ['SUPABASE_URL'].rstrip('/') + '/functions/v1/daily-beach-refresh'
    headers = {
        'Authorization': f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        'apikey':         os.environ['SUPABASE_SERVICE_KEY'],
        'x-admin-secret': os.environ['ADMIN_SECRET'],
        'Content-Type':   'application/json',
    }

    batches = [ids[i:i+args.batch_size] for i in range(0, n, args.batch_size)]
    t0 = time.time()
    n_ok_beaches = 0
    n_fail_batches = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(fire_one_batch, url, headers, b, i)
                    for i, b in enumerate(batches, 1)]
        for fut in as_completed(futures):
            batch_idx, n_ok, status = fut.result()
            n_ok_beaches += n_ok
            if status == 'ok':
                elapsed = time.time() - t0
                if batch_idx % 5 == 0 or batch_idx == n_batches:
                    print(f'  batch {batch_idx}/{n_batches} ok  '
                          f'cum_beaches={n_ok_beaches}/{n}  ({elapsed:.0f}s)')
            else:
                n_fail_batches += 1
                print(f'  batch {batch_idx}/{n_batches} FAIL: {status}')

    elapsed = time.time() - t0
    print()
    print('=== SUMMARY ===')
    print(f'  beaches refreshed:  {n_ok_beaches}/{n}')
    print(f'  failed batches:     {n_fail_batches}/{n_batches}')
    print(f'  total time:         {elapsed:.0f}s ({elapsed/60:.1f} min)')
    print(f'  rate:               {n_ok_beaches/max(1,elapsed):.1f} beaches/sec')
    return 0 if n_fail_batches == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
