"""rescue_reactivated_beaches.py — per-fid enrichment chase.

Per Franz 2026-05-19 — the rollback_arena_auto_promote cohort got
beach_dog_policy + provenance at promote-time but NOTHING ELSE
(no scoring_tier, no photos, no descriptions, no beach_policy_source).
Reactivating them just makes them invisible zombies.

This script runs the enrichment chain on a list of fids:

  1. Reactivate (is_active=true) with audit reason
  2. Refresh catchment cascade (state-scoped — assigns scoring_tier)
  3. Refire BEP cascade for each fid (promotes policies to zone_rules)
  4. Load Flickr photos (per-fid)
  5. Load Wikimedia photos (per-fid)
  6. Vision-tag the new photos (--state-scoped catches just-loaded rows)
  7. Auto-curate (per-fid; picks top N for each beach)

Descriptions are deferred to v2 (no per-fid CLI exists today; run
`run_state_pipeline.py --state X --phase-from descriptions` after).

Run:
  python scripts/rescue_reactivated_beaches.py --fids 5927,4179 --dry-run
  python scripts/rescue_reactivated_beaches.py --fids 5927,4179 --apply
  python scripts/rescue_reactivated_beaches.py --inactive-reason rollback_arena_auto_promote --state CA --county "San Mateo" --apply
  python scripts/rescue_reactivated_beaches.py --skip-photos --fids 4230 --apply
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import argparse
import os
import subprocess
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect():
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def resolve_fids(conn, args) -> tuple[list[int], dict[int, dict]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if args.fids:
        fids = [int(s) for s in args.fids.split(",") if s.strip()]
        cur.execute("SELECT fid, name, state, county_name FROM beaches_gold WHERE fid = ANY(%s)", (fids,))
    else:
        q = "SELECT fid, name, state, county_name FROM beaches_gold WHERE 1=1"
        params: list = []
        if args.inactive_reason:
            q += " AND inactive_reason = %s AND NOT is_active"
            params.append(args.inactive_reason)
        if args.state:
            q += " AND state = %s"
            params.append(args.state.upper())
        if args.county:
            q += " AND county_name = %s"
            params.append(args.county)
        q += " ORDER BY county_name, name"
        cur.execute(q, params)
    rows = cur.fetchall()
    info = {r["fid"]: dict(r) for r in rows}
    return list(info.keys()), info


def step_reactivate(conn, fids, reason):
    with conn.cursor() as cur:
        cur.execute("""UPDATE beaches_gold SET is_active = true, inactive_reason = %s
                        WHERE fid = ANY(%s) AND NOT is_active""", (reason, fids))
        n = cur.rowcount
        conn.commit()
    return n


def step_refresh_catchment(states):
    for state in sorted(states):
        cmd = [sys.executable, "scripts/refresh_catchment.py", "--state", state]
        print(f"    $ {' '.join(cmd)}", flush=True)
        rc = subprocess.run(cmd, cwd=str(ROOT))
        if rc.returncode != 0:
            print(f"    ⚠ refresh_catchment exit {rc.returncode}", flush=True)


def step_bep_refire(conn, fids):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM public.refire_bep_cascade(%s)", (fids,))
        r = cur.fetchone()
        print(f"    refire_bep_cascade → {r}", flush=True)
        conn.commit()


def step_load_photos(fids, script):
    fid_csv = ",".join(str(f) for f in fids)
    cmd = [sys.executable, f"scripts/{script}", "--fids", fid_csv]
    print(f"    $ {script} --fids <{len(fids)} fids>", flush=True)
    rc = subprocess.run(cmd, cwd=str(ROOT))
    if rc.returncode != 0:
        print(f"    ⚠ {script} exit {rc.returncode}", flush=True)


def step_vision_tag(states):
    state_csv = ",".join(sorted(states))
    cmd = [sys.executable, "scripts/load_photo_vision_tags.py",
           "--state", state_csv, "--workers", "5", "--budget-usd", "5"]
    print(f"    $ {' '.join(cmd)}", flush=True)
    rc = subprocess.run(cmd, cwd=str(ROOT))
    if rc.returncode != 0:
        print(f"    ⚠ vision tagger exit {rc.returncode}", flush=True)


def step_auto_curate(fids, n):
    fid_csv = ",".join(str(f) for f in fids)
    cmd = [sys.executable, "scripts/auto_curate.py", "--fids", fid_csv, "--n", str(n)]
    print(f"    $ auto_curate --fids <{len(fids)}> --n {n}", flush=True)
    rc = subprocess.run(cmd, cwd=str(ROOT))
    if rc.returncode != 0:
        print(f"    ⚠ auto_curate exit {rc.returncode}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fids", help="Comma-separated fids")
    grp.add_argument("--inactive-reason", help="Pull all inactive beaches with this reason")
    ap.add_argument("--state", help="Narrow to one state (CA)")
    ap.add_argument("--county", help="Narrow to one county")
    ap.add_argument("--reactivate-reason",
                    default="reactivated_2026_05_19_rollback_audit",
                    help="What to write into inactive_reason on reactivation (audit trail)")
    ap.add_argument("--n", type=int, default=6, help="Auto-curate photos per beach")
    ap.add_argument("--skip-photos", action="store_true")
    ap.add_argument("--skip-vision", action="store_true")
    ap.add_argument("--skip-curate", action="store_true")
    ap.add_argument("--skip-catchment", action="store_true",
                    help="Skip refresh_catchment.py (saves ~3 min when running on already-tiered beaches)")
    ap.add_argument("--apply", action="store_true", help="Actually write")
    args = ap.parse_args()

    conn = _connect()
    try:
        fids, info = resolve_fids(conn, args)
        if not fids:
            print("No fids matched. Nothing to do.")
            return 0
        states = {info[f]["state"] for f in fids}

        print(f"Rescue scope: {len(fids)} beaches across states {sorted(states)}")
        for f in fids[:20]:
            print(f"  fid={f:<10} {info[f]['state']} {(info[f]['county_name'] or '?'):<15} {info[f]['name']}")
        if len(fids) > 20:
            print(f"  ... + {len(fids)-20} more")
        if not args.apply:
            print("\n[dry-run] no writes. Re-run with --apply.")
            return 0

        print("\n=== STEP 1: Reactivate ===")
        n = step_reactivate(conn, fids, args.reactivate_reason)
        print(f"    reactivated: {n}")

        if not args.skip_catchment:
            print("\n=== STEP 2: Refresh catchment (per-state) ===")
            step_refresh_catchment(states)

        print("\n=== STEP 3: BEP refire ===")
        step_bep_refire(conn, fids)

        if not args.skip_photos:
            print("\n=== STEP 4: Load Flickr photos ===")
            step_load_photos(fids, "load_flickr_photos.py")
            print("\n=== STEP 5: Load Wikimedia photos ===")
            step_load_photos(fids, "load_wikimedia_commons_photos.py")

        if not args.skip_vision:
            print("\n=== STEP 6: Vision tag new photos (state-scoped) ===")
            step_vision_tag(states)

        if not args.skip_curate:
            print("\n=== STEP 7: Auto-curate ===")
            step_auto_curate(fids, args.n)

        print("\nDone. Note: descriptions deferred (no per-fid CLI today — re-run "
              "run_state_pipeline.py --state X --phase-from descriptions for prose).")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
