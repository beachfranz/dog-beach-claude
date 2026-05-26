"""_inproc.py — call CLI scripts' main() in-process via sys.argv monkey-patch.

Eliminates subprocess overhead (no Python re-init, no module re-import,
shared connection pool optional). Each op still runs in its own Dagster
worker subprocess so module-state pollution is bounded.
"""
from __future__ import annotations
import sys
from contextlib import contextmanager
from io import StringIO


@contextmanager
def argv_and_capture(script_name: str, args: list[str]):
    """Monkey-patch sys.argv to look like CLI invocation; capture stdout.
    Yields a StringIO that accumulates everything printed during the call.
    Restores sys.argv + sys.stdout on exit (even if main() raises)."""
    saved_argv = sys.argv
    saved_stdout = sys.stdout
    buf = StringIO()
    sys.argv = [script_name] + args
    sys.stdout = buf
    try:
        yield buf
    finally:
        sys.argv = saved_argv
        sys.stdout = saved_stdout


def call_main(main_callable, script_name: str, args: list[str]) -> tuple[int, str]:
    """Run a script's main() in-process. Returns (exit_code, captured_stdout).
    Catches SystemExit so a script's sys.exit() doesn't kill the parent."""
    with argv_and_capture(script_name, args) as buf:
        try:
            rc = main_callable()
            if rc is None:
                rc = 0
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
        except Exception as e:
            buf.write(f"\n[!] in-process exception: {type(e).__name__}: {e}\n")
            rc = 1
    return rc, buf.getvalue()
