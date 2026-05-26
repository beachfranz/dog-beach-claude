"""run.py — standalone per-state runner. Drop-in CLI for the 8-op pipeline.

Use this for ad-hoc proof-point runs while the Dagster wiring (in
scripts/dagster/dog_beach/dog_beach/assets/dog_park_coverage.py) is
gated by an unrelated weather_grid Config issue.

Usage:
  python -m scripts.dog_park_pipeline.run --state OR
  python -m scripts.dog_park_pipeline.run --state WA --skip walk_catalogs
  python -m scripts.dog_park_pipeline.run --state CA --only run_extractor,retry_no_match

Outputs:
  - One metrics row per op into public.dog_park_coverage_runs
  - Summary table at end
"""
from __future__ import annotations
import argparse, json, sys, time
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from . import (
    preflight_check,
    pip_address_city_backfill,
    reclassify_obvious_junk,
    generic_name_display_override,
    walk_catalogs,
    ingest_discovery_queue,
    run_extractor,
    retry_no_match,
    write_run_metric,
)


# Optimal order — CA proof point 2026-05-25 LATE: 0% → 79.5%.
OPS = [
    ("preflight",            lambda s: preflight_check(s)),
    ("pip_address_city",     lambda s: pip_address_city_backfill(s)),
    ("reclassify_junk",      lambda s: reclassify_obvious_junk(s)),
    ("generic_display_names",lambda s: generic_name_display_override(s)),
    ("walk_catalogs",        lambda s: walk_catalogs(s, workers=4, min_parks=3, apply=True)),
    ("ingest_queue",         lambda s: ingest_discovery_queue(s, apply=True)),
    ("run_extractor",        lambda s: run_extractor(s, workers=6, include_no_website=True, apply=True)),
    ("retry_no_match",       lambda s: retry_no_match(s, workers=6, apply=True)),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="State code (CA/OR/WA/...)")
    ap.add_argument("--only", type=str, default=None,
                    help="Comma-separated op names to run (others skipped)")
    ap.add_argument("--skip", type=str, default=None,
                    help="Comma-separated op names to skip")
    ap.add_argument("--start-at", type=str, default=None,
                    help="Skip everything before this op name")
    args = ap.parse_args()

    only = set(args.only.split(",")) if args.only else None
    skip = set(args.skip.split(",")) if args.skip else set()

    started_op = False
    summary = []
    print(f"\n{'='*70}\ndog_park_pipeline.run — state={args.state}\n{'='*70}")
    for name, op in OPS:
        if args.start_at and not started_op:
            if name == args.start_at:
                started_op = True
            else:
                print(f"\n[{name:24}] SKIPPED (before --start-at={args.start_at})")
                continue
        if only and name not in only:
            print(f"\n[{name:24}] SKIPPED (not in --only)")
            continue
        if name in skip:
            print(f"\n[{name:24}] SKIPPED (in --skip)")
            continue
        print(f"\n{'─'*70}\n[{name:24}] starting...")
        started = datetime.now(timezone.utc)
        try:
            metrics = op(args.state)
            ended = datetime.now(timezone.utc)
            write_run_metric(args.state, metrics.get("op", name), started, ended, metrics)
            # Pretty-print summary fields (drop tails)
            display = {k: v for k, v in metrics.items()
                       if not k.endswith("_tail") and k not in ("started_at", "ended_at")}
            print(f"[{name:24}] DONE in {(ended-started).total_seconds():.0f}s  {display}")
            summary.append((name, "ok", (ended - started).total_seconds(), display))
        except Exception as e:
            ended = datetime.now(timezone.utc)
            print(f"[{name:24}] FAIL after {(ended-started).total_seconds():.0f}s: {e}")
            summary.append((name, "fail", (ended - started).total_seconds(), {"error": str(e)}))

    print(f"\n{'='*70}\nSUMMARY for {args.state}:")
    for name, status, secs, _ in summary:
        print(f"  [{status:4}] {name:24} {secs:6.0f}s")
    return 0 if all(s[1] == "ok" for s in summary) else 1


if __name__ == "__main__":
    sys.exit(main())
