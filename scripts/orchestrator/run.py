"""scripts/orchestrator/run.py — pipeline runner with audit logging.

Usage (from repo root):
  python -m scripts.orchestrator.run zone_rules_repass_anchors
  python -m scripts.orchestrator.run zone_rules_repass_full --trigger schedule
  python -m scripts.orchestrator.run --list

Writes a row to public.pipeline_runs at start, then updates with status,
exit code, and stdout/stderr tails when the subprocess finishes. The
admin/pipelines.html page reads from this table to surface freshness +
last-status per pipeline.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
from pathlib import Path

from .registry import PIPELINES, get, list_names

REPO_ROOT = Path(__file__).resolve().parents[2]
TAIL_BYTES = 4000


def _supabase_query(sql: str, returning: bool = False) -> str | None:
    """Run SQL via the supabase CLI. Returns stdout (or None for fire-and-forget).
    File-based execution to dodge Windows cmdline limit."""
    import tempfile, json
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
    if returning:
        idx = out.find('{')
        end = out.rfind('}')
        if idx >= 0 and end > idx:
            return json.loads(out[idx:end+1])
    return None


def audit_start(pipeline_name: str, trigger_kind: str) -> int:
    """Insert pipeline_runs row with status='running'. Returns the row id."""
    res = _supabase_query(f"""
        insert into public.pipeline_runs (pipeline_name, trigger_kind, status)
        values ('{pipeline_name}', '{trigger_kind}', 'running')
        returning id;
    """, returning=True)
    return res['rows'][0]['id'] if res else -1


def audit_finish(run_id: int, exit_code: int, stdout: str, stderr: str) -> None:
    """Mark the pipeline_runs row finished with status + tails."""
    if run_id < 0:
        return
    status = 'success' if exit_code == 0 else 'failed'
    # Escape single quotes for SQL
    def esc(s: str) -> str:
        return (s or '')[-TAIL_BYTES:].replace("'", "''")
    _supabase_query(f"""
        update public.pipeline_runs
           set status = '{status}',
               exit_code = {exit_code},
               stdout_tail = '{esc(stdout)}',
               stderr_tail = '{esc(stderr)}',
               finished_at = now()
         where id = {run_id};
    """)


def run_pipeline(name: str, trigger_kind: str = 'manual') -> int:
    pipe = get(name)
    cmd = pipe['command']
    cwd = pipe.get('cwd', str(REPO_ROOT))

    print(f'== orchestrator: running {name!r}')
    print(f'   {" ".join(cmd)}')
    print(f'   trigger={trigger_kind}  cwd={cwd}')
    print()

    run_id = audit_start(name, trigger_kind)
    try:
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                              encoding='utf-8', errors='replace')
    except Exception as e:
        audit_finish(run_id, 1, '', f'Launcher exception: {e}')
        print(f'ERROR launching: {e}', file=sys.stderr)
        return 1

    audit_finish(run_id, proc.returncode, proc.stdout, proc.stderr)

    print('---- stdout (tail) ----')
    sys.stdout.write(proc.stdout[-TAIL_BYTES:])
    if proc.stderr.strip():
        print('---- stderr (tail) ----', file=sys.stderr)
        sys.stderr.write(proc.stderr[-TAIL_BYTES:])
    print()
    print(f'== orchestrator: {"OK" if proc.returncode == 0 else "FAILED"} '
          f'(exit {proc.returncode}, run_id={run_id})')
    return proc.returncode


def main():
    ap = argparse.ArgumentParser(prog='orchestrator.run')
    ap.add_argument('pipeline', nargs='?', help='Pipeline name from registry.PIPELINES')
    ap.add_argument('--list', action='store_true', help='List registered pipelines')
    ap.add_argument('--trigger', default='manual',
                    choices=['manual', 'schedule', 'sensor', 'script_call'])
    args = ap.parse_args()

    if args.list:
        names = list_names()
        max_n = max(len(n) for n in names)
        print(f'Registered pipelines ({len(names)}):')
        for n in names:
            p = PIPELINES[n]
            sched = p.get('schedule') or '(manual)'
            cost = p.get('cost_estimate') or '?'
            print(f'  {n:{max_n}}  {sched:12}  cost={cost:8}  {p["description"]}')
        return 0

    if not args.pipeline:
        ap.print_help()
        return 2

    return run_pipeline(args.pipeline, trigger_kind=args.trigger)


if __name__ == '__main__':
    sys.exit(main())
