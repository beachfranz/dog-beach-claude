"""fire_descriptions_mvp_plus.py — regen descriptions for all MVP+
daily+hourly beaches across CA + OR + WA with balanced parallelism.

Each subprocess invokes generate_beach_descriptions.py with a chunk of
fids; the per-beach script handles the input_hash cache check (now
prompt-aware), so already-cached beaches skip cheaply.

Defaults: 35 fids/chunk × 4 parallel subprocesses. Adjust via flags.

Cost (post-prompt-edit, full regen): ~1045 beaches × $0.018 ≈ $19.
Once cached against the new hash, subsequent runs near $0.

Usage:
  python scripts/fire_descriptions_mvp_plus.py                       # all MVP+ CA+OR+WA
  python scripts/fire_descriptions_mvp_plus.py --states CA           # one state
  python scripts/fire_descriptions_mvp_plus.py --workers 6 --chunk 25
  python scripts/fire_descriptions_mvp_plus.py --dry-run             # scope only
"""
from __future__ import annotations
import sys, time, argparse, subprocess
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent


def pick_scope(cur, states: list[str]) -> list[int]:
    cur.execute("""
      SELECT fid FROM beaches_gold
       WHERE is_active = true
         AND scoring_tier IN ('daily','hourly')
         AND state = ANY(%s)
       ORDER BY scoring_tier DESC, state, fid
    """, (states,))
    return [r[0] for r in cur.fetchall()]


def run_chunk(chunk: list[int], chunk_idx: int, extra: list[str]) -> tuple[int, int, str]:
    """Invoke the generator on one chunk. Returns (idx, n_done, status)."""
    try:
        r = subprocess.run(
            [sys.executable, 'scripts/generate_beach_descriptions.py',
             '--fids', ','.join(map(str, chunk))] + extra,
            cwd=str(ROOT), timeout=1800, capture_output=True,
            text=True, encoding='utf-8', errors='replace',
        )
        if r.returncode == 0:
            return chunk_idx, len(chunk), 'ok'
        return chunk_idx, 0, f'rc={r.returncode}: {r.stderr[-200:] if r.stderr else r.stdout[-200:]}'
    except subprocess.TimeoutExpired:
        return chunk_idx, 0, 'TIMEOUT after 1800s'
    except Exception as e:
        return chunk_idx, 0, f'EXC {str(e)[:200]}'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='CA,OR,WA')
    ap.add_argument('--chunk', type=int, default=35)
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--no-photos', action='store_true',
                     help='pass --no-photos to the generator (omit photo_descriptions from input)')
    args = ap.parse_args()
    extra = ['--no-photos'] if args.no_photos else []

    states = [s.strip() for s in args.states.split(',') if s.strip()]
    conn = connect()
    cur = conn.cursor()
    fids = pick_scope(cur, states)
    n = len(fids)
    chunks = [fids[i:i+args.chunk] for i in range(0, n, args.chunk)]
    print(f'Scope: {n} MVP+ beaches across {states} → {len(chunks)} chunks of ≤{args.chunk}')
    print(f'Parallelism: {args.workers} concurrent subprocesses')
    print(f'Expected: ~{n * 0.018:.0f}$ on full regen (post prompt edit), ~$0 once cached')
    if args.dry_run:
        print('[dry-run] no calls executed'); return 0

    t0 = time.time()
    n_done_beaches = 0
    n_fail_chunks = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(run_chunk, ch, i, extra)
                   for i, ch in enumerate(chunks, 1)]
        for fut in as_completed(futures):
            idx, n_ok, status = fut.result()
            n_done_beaches += n_ok
            elapsed = time.time() - t0
            if status == 'ok':
                if idx % 5 == 0 or idx == len(chunks):
                    print(f'  chunk {idx}/{len(chunks)} ok  cum={n_done_beaches}/{n}  ({elapsed:.0f}s)')
            else:
                n_fail_chunks += 1
                print(f'  chunk {idx}/{len(chunks)} FAIL: {status}')

    elapsed = time.time() - t0
    print()
    print('=== SUMMARY ===')
    print(f'  beaches scope:        {n}')
    print(f'  chunks ok:            {len(chunks) - n_fail_chunks}/{len(chunks)}')
    print(f'  chunks failed:        {n_fail_chunks}')
    print(f'  total time:           {elapsed:.0f}s ({elapsed/60:.1f} min)')
    return 0 if n_fail_chunks == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
