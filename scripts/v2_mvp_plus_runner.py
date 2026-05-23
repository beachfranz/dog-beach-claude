"""v2_mvp_plus_runner.py — auto-batch v2 re-extract across all active MVP+ beaches.

Per Franz 2026-05-21 (AFK). Loops batches of N=50 fids, parallelized
internally via subprocess.Popen pool. After each batch, evaluates
region distribution + flags outliers. Auto-pauses if thresholds
exceeded; otherwise continues until exhausted.

Scope: beaches_gold rows where
  is_active = true
  scoring_tier IN ('hourly','daily')
  beach_dog_policy.source <> 'manual_curator'
  beach has at least one beach_policy_source row with region_name IS NOT NULL
  (i.e. has data v2 can reshape)
EXCLUDES fids touched today (extracted_at >= current_date).

Log: writes a running summary to share/v2_mvp_plus_runner.log
(timestamped batch entries, region distribution, flagged fids).

Auto-pause thresholds (per batch):
  flagged_pct >= 20%  (beaches with >=4 regions post-run)
  empty_pct   >=  5%  (beaches with 0 regions post-run — possibly
                       Marina-style data loss)

Usage:
  python scripts/v2_mvp_plus_runner.py                # run unattended
  python scripts/v2_mvp_plus_runner.py --batch-size 30
  python scripts/v2_mvp_plus_runner.py --workers 8
  python scripts/v2_mvp_plus_runner.py --dry-run      # just report scope
"""
from __future__ import annotations
import sys, time, argparse, subprocess
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
from datetime import datetime

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = ROOT / 'share' / 'v2_mvp_plus_runner.log'
LOG_PATH.parent.mkdir(exist_ok=True)


def log(msg: str):
    line = f'[{datetime.now().strftime("%H:%M:%S")}] {msg}'
    print(line)
    existing = LOG_PATH.read_text(encoding='utf-8') if LOG_PATH.exists() else ''
    LOG_PATH.write_text(existing + line + '\n', encoding='utf-8')


def pick_next_batch(cur, batch_size: int) -> list[int]:
    """Return up to batch_size MVP+ fids not yet v2'd today."""
    cur.execute("""
      SELECT DISTINCT bg.fid
        FROM beaches_gold bg
        JOIN beach_dog_policy bdp ON bdp.arena_group_id = bg.fid
        JOIN beach_policy_source bps ON bps.beach_fid = bg.fid
       WHERE bg.is_active = true
         AND bg.scoring_tier IN ('hourly','daily')
         AND bdp.source <> 'manual_curator'
         AND NOT EXISTS (
           SELECT 1 FROM beach_policy_source bps2
            WHERE bps2.beach_fid = bg.fid
              AND bps2.extracted_at >= current_date
         )
       ORDER BY bg.fid
       LIMIT %s
    """, (batch_size,))
    return [r[0] for r in cur.fetchall()]


def remaining_scope(cur) -> int:
    cur.execute("""
      SELECT COUNT(DISTINCT bg.fid)
        FROM beaches_gold bg
        JOIN beach_dog_policy bdp ON bdp.arena_group_id = bg.fid
        JOIN beach_policy_source bps ON bps.beach_fid = bg.fid
       WHERE bg.is_active = true
         AND bg.scoring_tier IN ('hourly','daily')
         AND bdp.source <> 'manual_curator'
         AND NOT EXISTS (
           SELECT 1 FROM beach_policy_source bps2
            WHERE bps2.beach_fid = bg.fid
              AND bps2.extracted_at >= current_date
         )
    """)
    return cur.fetchone()[0]


def run_batch_parallel(fids: list[int], workers: int) -> None:
    """Spawn `workers` subprocess.Popen instances, each handles one fid;
    rotate as they finish."""
    pending = list(fids)
    active: list[tuple[int, subprocess.Popen]] = []
    while pending or active:
        while pending and len(active) < workers:
            fid = pending.pop(0)
            p = subprocess.Popen(
                ['python', 'scripts/reextract_beach_all_ps.py', '--fid', str(fid), '--refresh-hourly'],
                cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            active.append((fid, p))
        time.sleep(0.5)
        finished = [(f, p) for f, p in active if p.poll() is not None]
        for f, p in finished:
            active.remove((f, p))


def evaluate_batch(cur, fids: list[int]) -> dict:
    """Returns distribution + flagged + empty counts."""
    buckets = {}
    flagged, empty = [], []
    for fid in fids:
        cur.execute("SELECT zone_rules FROM beach_dog_policy WHERE arena_group_id = %s", (fid,))
        row = cur.fetchone()
        if not row or not row[0]:
            empty.append(fid); continue
        regs = row[0].get('regions') or []
        n = len(regs)
        buckets[n] = buckets.get(n, 0) + 1
        if n >= 4:
            flagged.append((fid, n))
    return {'buckets': buckets, 'flagged': flagged, 'empty': empty}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--batch-size', type=int, default=50)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--flagged-threshold', type=float, default=0.20,
                     help='auto-pause if flagged fraction >= this')
    ap.add_argument('--empty-threshold',   type=float, default=0.05,
                     help='auto-pause if empty fraction >= this')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    conn = connect(); cur = conn.cursor()
    total = remaining_scope(cur)
    log(f'START — total MVP+ scope remaining: {total} beaches')
    if args.dry_run:
        log('[DRY RUN] not executing'); return 0

    batch_n = 0
    while True:
        fids = pick_next_batch(cur, args.batch_size)
        if not fids:
            log('SCOPE EXHAUSTED — done'); break
        batch_n += 1
        t0 = time.time()
        log(f'BATCH {batch_n} — running {len(fids)} fids '
            f'(workers={args.workers}) ... fids: {fids[:5]}...')
        run_batch_parallel(fids, args.workers)
        dt = time.time() - t0

        # Re-open connection in case pgbouncer dropped it during the batch
        try:
            cur.execute('SELECT 1'); cur.fetchone()
        except Exception:
            conn.close(); conn = connect(); cur = conn.cursor()

        ev = evaluate_batch(cur, fids)
        n = len(fids)
        flag_pct = len(ev['flagged']) / n
        empty_pct = len(ev['empty']) / n
        log(f'BATCH {batch_n} done in {dt:.1f}s. '
            f'distribution: {sorted(ev["buckets"].items())}, '
            f'flagged={len(ev["flagged"])} ({flag_pct:.0%}), '
            f'empty={len(ev["empty"])} ({empty_pct:.0%})')
        if ev['flagged']:
            log(f'  flagged fids: {ev["flagged"]}')
        if ev['empty']:
            log(f'  empty fids: {ev["empty"]}')

        if flag_pct >= args.flagged_threshold or empty_pct >= args.empty_threshold:
            log(f'AUTO-PAUSE — thresholds exceeded '
                f'(flagged {flag_pct:.0%} >= {args.flagged_threshold:.0%} '
                f'or empty {empty_pct:.0%} >= {args.empty_threshold:.0%})')
            break

    rem = remaining_scope(cur)
    log(f'END — {rem} beaches still unprocessed in scope')
    return 0


if __name__ == '__main__':
    sys.exit(main())
