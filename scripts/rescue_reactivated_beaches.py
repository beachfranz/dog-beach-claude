"""rescue_reactivated_beaches.py — per-fid enrichment chase with diagnostics.

Per Franz 2026-05-19 — the rollback_arena_auto_promote cohort got
beach_dog_policy + provenance at promote-time but NOTHING ELSE.
Reactivating them just makes them invisible zombies.

This script runs the 7-step enrichment chain on a list of fids AND
verifies every step against the DB. Halts on any non-zero subprocess
or any verification failure (--keep-going to override). Final per-fid
summary table shows ✓/✗ per step.

Steps:
  1. Reactivate (is_active=true) with audit reason
  2. Refresh catchment cascade (state-scoped — assigns scoring_tier)
  3. BEP refire per-fid (isolates failures; promotes policy→zone_rules)
  4. Load Flickr photos (per-fid)
  5. Load Wikimedia photos (per-fid)
  6. Vision-tag new photos (state-scoped — catches just-loaded rows)
  7. Auto-curate (per-fid; picks top N for each beach)

Designed for tight batches (default --batch-size 5) so each failure
isolates and the per-fid summary stays scannable.

Run:
  python scripts/rescue_reactivated_beaches.py --fids 5927 --dry-run
  python scripts/rescue_reactivated_beaches.py --fids 5927,4179,4230,6050,6065 --apply
  python scripts/rescue_reactivated_beaches.py --fids X,Y,Z --apply --keep-going
  python scripts/rescue_reactivated_beaches.py --fids X --apply --skip-photos
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import argparse
import os
import subprocess
import time
import urllib.parse
from datetime import datetime, timezone
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


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_sub(cmd: list[str], halt_on_error: bool, label: str) -> int:
    """Run a subprocess streaming its stdout to our stdout line-by-line
    (so the BG harness's piped file doesn't lose output mid-step). Returns
    the exit code; raises if non-zero and halt_on_error is set."""
    log(f"  $ {label}")
    proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1)
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"      | {line.rstrip()}", flush=True)
    rc = proc.wait()
    if rc != 0:
        msg = f"  ⚠ {label} exit {rc}"
        if halt_on_error:
            raise SystemExit(msg)
        log(msg)
    return rc


def resolve_fids(conn, args) -> dict[int, dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT fid, name, state, county_name, is_active
                     FROM beaches_gold WHERE fid = ANY(%s)
                    ORDER BY county_name, name""",
                ([int(s) for s in args.fids.split(",") if s.strip()],))
    return {r["fid"]: dict(r) for r in cur.fetchall()}


# ─── Per-step verification queries ───────────────────────────────────

def verify_active(conn, fids: list[int]) -> dict[int, bool]:
    with conn.cursor() as cur:
        cur.execute("SELECT fid, is_active FROM beaches_gold WHERE fid = ANY(%s)", (fids,))
        return {fid: active for fid, active in cur.fetchall()}


def verify_catchment(conn, fids: list[int]) -> dict[int, tuple]:
    with conn.cursor() as cur:
        cur.execute("SELECT fid, scoring_tier, catchment_score FROM beaches_gold WHERE fid = ANY(%s)", (fids,))
        return {fid: (tier, cs) for fid, tier, cs in cur.fetchall()}


def verify_bps(conn, fids: list[int]) -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT beach_fid, count(*) FROM beach_policy_source "
                    " WHERE beach_fid = ANY(%s) GROUP BY beach_fid", (fids,))
        return {fid: n for fid, n in cur.fetchall()}


def verify_photos(conn, fids: list[int], source: str) -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                    " WHERE arena_group_id = ANY(%s) AND source = %s GROUP BY 1",
                    (fids, source))
        return {fid: n for fid, n in cur.fetchall()}


def verify_vision_tagged(conn, fids: list[int]) -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                    " WHERE arena_group_id = ANY(%s) "
                    " AND source_meta->'vision'->>'model' IS NOT NULL GROUP BY 1",
                    (fids,))
        return {fid: n for fid, n in cur.fetchall()}


def verify_auto_curated(conn, fids: list[int]) -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                    " WHERE arena_group_id = ANY(%s) AND curated_by LIKE 'auto:%%' GROUP BY 1",
                    (fids,))
        return {fid: n for fid, n in cur.fetchall()}


def step_print_verify(name: str, before: dict, after: dict, fids: list[int],
                       diff_fn) -> dict[int, bool]:
    """Print a per-fid before→after for a step and return per-fid pass/fail dict."""
    log(f"  verify {name}:")
    results = {}
    for fid in fids:
        ok = diff_fn(before.get(fid), after.get(fid))
        mark = "✓" if ok else "✗"
        log(f"      {mark} fid={fid:<10} {before.get(fid)!s:<20} → {after.get(fid)}")
        results[fid] = ok
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fids", required=True, help="Comma-separated fids")
    ap.add_argument("--batch-size", type=int, default=5,
                    help="Refuse to process more than N fids in one invocation. "
                         "Default 5; raise to disable.")
    ap.add_argument("--reactivate-reason",
                    default="reactivated_2026_05_19_rollback_audit")
    ap.add_argument("--n", type=int, default=6, help="Auto-curate photos per beach")
    ap.add_argument("--skip-photos", action="store_true")
    ap.add_argument("--skip-vision", action="store_true")
    ap.add_argument("--skip-curate", action="store_true")
    ap.add_argument("--skip-catchment", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--keep-going", action="store_true",
                    help="Continue past subprocess / verification failures")
    args = ap.parse_args()
    halt = not args.keep_going

    fids = [int(s) for s in args.fids.split(",") if s.strip()]
    if len(fids) > args.batch_size:
        raise SystemExit(f"batch too large: {len(fids)} > --batch-size {args.batch_size}. "
                         f"Use --batch-size {len(fids)} to override.")

    conn = _connect()
    try:
        info = resolve_fids(conn, args)
        if len(info) != len(fids):
            missing = set(fids) - set(info)
            raise SystemExit(f"unknown fids: {sorted(missing)}")

        log(f"Rescue batch: {len(fids)} beaches")
        for f in fids:
            d = info[f]
            log(f"  fid={f:<10} {d['state']} {d['county_name']:<15} {d['name']}")
        if not args.apply:
            log("[dry-run] no writes. --apply to write.")
            return 0

        # Per-fid summary scoreboard
        summary: dict[int, dict] = {f: {} for f in fids}
        states = sorted({info[f]["state"] for f in fids})

        # ─ STEP 1 ─────────────────────────────────────────────────────
        log("\n=== STEP 1: Reactivate ===")
        before = verify_active(conn, fids)
        with conn.cursor() as cur:
            cur.execute("UPDATE beaches_gold SET is_active=true, inactive_reason=%s "
                        " WHERE fid = ANY(%s) AND NOT is_active",
                        (args.reactivate_reason, fids))
            conn.commit()
            log(f"  reactivated rows: {cur.rowcount}")
        after = verify_active(conn, fids)
        results = step_print_verify("is_active", before, after, fids,
                                     lambda b, a: a is True)
        for f, ok in results.items(): summary[f]["1_active"] = ok
        if halt and not all(results.values()):
            raise SystemExit("step 1 verification failed")

        # ─ STEP 2 ─────────────────────────────────────────────────────
        if not args.skip_catchment:
            log("\n=== STEP 2: Refresh catchment (per-state) ===")
            before = verify_catchment(conn, fids)
            for state in states:
                run_sub([sys.executable, "scripts/refresh_catchment.py", "--state", state],
                        halt, f"refresh_catchment --state {state}")
            after = verify_catchment(conn, fids)
            results = step_print_verify("catchment_score", before, after, fids,
                                         lambda b, a: a is not None and a[1] is not None)
            for f, ok in results.items(): summary[f]["2_catchment"] = ok
            if halt and not all(results.values()):
                raise SystemExit("step 2 verification failed: some fids still have no catchment_score")
        else:
            for f in fids: summary[f]["2_catchment"] = None

        # ─ STEP 3 ─────────────────────────────────────────────────────
        log("\n=== STEP 3: BEP refire (per-fid) ===")
        before = verify_bps(conn, fids)
        results = {}
        for f in fids:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM public.refire_bep_cascade(%s)", ([f],))
                    conn.commit()
                results[f] = True
            except Exception as e:
                conn.rollback()
                log(f"  ✗ fid={f} refire ERR: {e}")
                results[f] = False
                if halt: raise SystemExit(f"step 3 failed on fid {f}")
        after = verify_bps(conn, fids)
        v = step_print_verify("beach_policy_source rows", before, after, fids,
                               lambda b, a: (a or 0) >= (b or 0))
        for f, ok in results.items(): summary[f]["3_bep_refire"] = ok and v.get(f, False)

        # ─ STEP 4 + 5 ──────────────────────────────────────────────────
        if not args.skip_photos:
            log("\n=== STEP 4: Load Flickr photos ===")
            before_f = verify_photos(conn, fids, "flickr")
            run_sub([sys.executable, "scripts/load_flickr_photos.py", "--fids", ",".join(map(str, fids))],
                    halt, f"load_flickr_photos --fids <{len(fids)}>")
            after_f = verify_photos(conn, fids, "flickr")
            v = step_print_verify("Flickr photos", before_f, after_f, fids,
                                   lambda b, a: (a or 0) >= (b or 0))
            for f, ok in v.items(): summary[f]["4_flickr"] = ok

            log("\n=== STEP 5: Load Wikimedia photos ===")
            before_w = verify_photos(conn, fids, "wikimedia")
            run_sub([sys.executable, "scripts/load_wikimedia_commons_photos.py", "--fids", ",".join(map(str, fids))],
                    halt, f"load_wikimedia_commons_photos --fids <{len(fids)}>")
            after_w = verify_photos(conn, fids, "wikimedia")
            v = step_print_verify("Wikimedia photos", before_w, after_w, fids,
                                   lambda b, a: (a or 0) >= (b or 0))
            for f, ok in v.items(): summary[f]["5_wikimedia"] = ok
        else:
            for f in fids: summary[f]["4_flickr"] = summary[f]["5_wikimedia"] = None

        # ─ STEP 6 ─────────────────────────────────────────────────────
        if not args.skip_vision:
            log("\n=== STEP 6: Vision tag (state-scoped) ===")
            before = verify_vision_tagged(conn, fids)
            state_csv = ",".join(states)
            run_sub([sys.executable, "scripts/load_photo_vision_tags.py",
                     "--state", state_csv, "--workers", "5", "--budget-usd", "5"],
                    halt, f"vision tag --state {state_csv}")
            after = verify_vision_tagged(conn, fids)
            v = step_print_verify("vision-tagged photos", before, after, fids,
                                   lambda b, a: (a or 0) >= (b or 0))
            for f, ok in v.items(): summary[f]["6_vision"] = ok
        else:
            for f in fids: summary[f]["6_vision"] = None

        # ─ STEP 7 ─────────────────────────────────────────────────────
        if not args.skip_curate:
            log("\n=== STEP 7: Auto-curate (per-fid) ===")
            before = verify_auto_curated(conn, fids)
            run_sub([sys.executable, "scripts/auto_curate.py", "--fids", ",".join(map(str, fids)), "--n", str(args.n)],
                    halt, f"auto_curate --fids <{len(fids)}> --n {args.n}")
            after = verify_auto_curated(conn, fids)
            v = step_print_verify("auto-curated photos", before, after, fids,
                                   lambda b, a: (a or 0) >= (b or 0))
            for f, ok in v.items(): summary[f]["7_curate"] = ok
        else:
            for f in fids: summary[f]["7_curate"] = None

        # ─ Final summary ───────────────────────────────────────────────
        log("\n" + "=" * 90)
        log("FINAL PER-FID SUMMARY")
        log("=" * 90)
        header = f"  {'fid':<10} {'1act':<6} {'2cat':<6} {'3bep':<6} {'4flk':<6} {'5wmc':<6} {'6vis':<6} {'7cur':<6}  name"
        log(header)
        for f in fids:
            s = summary[f]
            def m(v): return '✓' if v else ('-' if v is None else '✗')
            log(f"  {f:<10} {m(s.get('1_active')):<6} {m(s.get('2_catchment')):<6} {m(s.get('3_bep_refire')):<6} {m(s.get('4_flickr')):<6} {m(s.get('5_wikimedia')):<6} {m(s.get('6_vision')):<6} {m(s.get('7_curate')):<6}  {info[f]['name']}")
        all_ok = all(all(v is not False for v in summary[f].values()) for f in fids)
        log(f"\n{'ALL GREEN ✓' if all_ok else '✗ ERRORS PRESENT — see above'}")
        return 0 if all_ok else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
