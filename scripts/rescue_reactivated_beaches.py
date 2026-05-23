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
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2.extras

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _ping_or_reconnect(conn):
    """Return a live connection. Long subprocesses (e.g. train_photo_model
    ~60s) sit behind pgbouncer's idle timeout; the held conn dies silently.
    Ping with SELECT 1; if broken, close + reopen. Cheap and idempotent."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    except Exception:
        try: conn.close()
        except Exception: pass
        return connect()


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


def verify_zone_rules(conn, fids: list[int]) -> dict[int, int]:
    """Return per-fid count of regions in zone_rules. Refire BEP cascade
    is supposed to populate this. >=1 region = success."""
    with conn.cursor() as cur:
        cur.execute("""SELECT arena_group_id,
                              coalesce(jsonb_array_length(zone_rules->'regions'), 0)
                        FROM beach_dog_policy WHERE arena_group_id = ANY(%s)""", (fids,))
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


def verify_keep_prob_scored(conn, fids: list[int]) -> dict[int, int]:
    """Per-fid count of photos with predicted_keep_prob set. Written by
    train_photo_model.py — must run AFTER vision tag, BEFORE auto-curate."""
    with conn.cursor() as cur:
        cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                    " WHERE arena_group_id = ANY(%s) "
                    " AND (source_meta->>'predicted_keep_prob') IS NOT NULL GROUP BY 1",
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

    conn = connect()
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
            conn = _ping_or_reconnect(conn)
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
        before = verify_zone_rules(conn, fids)
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
        after = verify_zone_rules(conn, fids)
        # Success: cascade ran without error AND zone_rules has ≥1 region after.
        v = step_print_verify("zone_rules regions", before, after, fids,
                               lambda b, a: (a or 0) >= 1)
        for f, ok in results.items(): summary[f]["3_bep_refire"] = ok and v.get(f, False)

        # ─ STEP 4 + 5 ──────────────────────────────────────────────────
        # Verify is "loader ran without error; report per-fid count
        # change". Loaders are sometimes destructive (eviction by new
        # top-N) — count going down is a soft-warn (⚠), not a halt.
        # The only hard fail is the subprocess itself erroring.
        def _photo_verify(src: str, before: dict, after: dict) -> dict[int, str | bool]:
            r = {}
            for f in fids:
                b, a = (before.get(f) or 0), (after.get(f) or 0)
                if a == 0 and b == 0:
                    r[f] = None
                    log(f"      - fid={f:<10} {src}: 0 → 0 (no photos found at this beach)")
                elif a < b:
                    r[f] = "warn"
                    log(f"      ⚠ fid={f:<10} {src}: {b} → {a} (loader evicted {b-a})")
                else:
                    r[f] = True
                    log(f"      ✓ fid={f:<10} {src}: {b} → {a}")
            return r

        if not args.skip_photos:
            log("\n=== STEP 4: Load Flickr photos ===")
            before_f = verify_photos(conn, fids, "flickr")
            run_sub([sys.executable, "scripts/load_flickr_photos.py", "--fids", ",".join(map(str, fids))],
                    halt, f"load_flickr_photos --fids <{len(fids)}>")
            conn = _ping_or_reconnect(conn)
            after_f = verify_photos(conn, fids, "flickr")
            for f, r in _photo_verify("Flickr", before_f, after_f).items():
                summary[f]["4_flickr"] = r

            log("\n=== STEP 5: Load Wikimedia photos ===")
            before_w = verify_photos(conn, fids, "wikimedia")
            run_sub([sys.executable, "scripts/load_wikimedia_commons_photos.py", "--fids", ",".join(map(str, fids))],
                    halt, f"load_wikimedia_commons_photos --fids <{len(fids)}>")
            conn = _ping_or_reconnect(conn)
            after_w = verify_photos(conn, fids, "wikimedia")
            for f, r in _photo_verify("Wikimedia", before_w, after_w).items():
                summary[f]["5_wikimedia"] = r
        else:
            for f in fids: summary[f]["4_flickr"] = summary[f]["5_wikimedia"] = None

        # ─ STEP 6 ─────────────────────────────────────────────────────
        if not args.skip_vision:
            log("\n=== STEP 6: Vision tag (fid-scoped) ===")
            # How many photos exist for these fids — needed to judge success.
            photo_counts = {}
            with conn.cursor() as cur:
                cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                            " WHERE arena_group_id = ANY(%s) GROUP BY 1", (fids,))
                photo_counts = {fid: n for fid, n in cur.fetchall()}
            before = verify_vision_tagged(conn, fids)
            # Fid-scoped so the scoreboard reflects THIS batch only — not a CA-wide tag pass.
            run_sub([sys.executable, "scripts/load_photo_vision_tags.py",
                     "--fids", ",".join(map(str, fids)),
                     "--workers", "5", "--budget-usd", "5",
                     "--chunk-size", str(max(50, sum(photo_counts.values()) + 10))],
                    halt, f"vision tag --fids <{len(fids)}>")
            conn = _ping_or_reconnect(conn)
            after = verify_vision_tagged(conn, fids)
            # Success: if a fid had ≥1 photo, it must now have ≥1 tagged.
            # If a fid had 0 photos (steps 4+5 found none), tagging can't run → mark None.
            v = {}
            for f in fids:
                if (photo_counts.get(f, 0) or 0) == 0:
                    v[f] = None
                    log(f"      - fid={f:<10} no photos to tag (skip)")
                else:
                    ok = (after.get(f) or 0) >= 1
                    mark = '✓' if ok else '✗'
                    log(f"      {mark} fid={f:<10} {(before.get(f) or 0)} → {(after.get(f) or 0)} (of {photo_counts.get(f)} photos)")
                    v[f] = ok
            for f, ok in v.items(): summary[f]["6_vision"] = ok
            if halt and any(ok is False for ok in v.values()):
                raise SystemExit("step 6 verification failed (some fids have photos but no vision tags)")
        else:
            for f in fids: summary[f]["6_vision"] = None

        # ─ STEP 6.5 ───────────────────────────────────────────────────
        # Vision tagger writes source_meta.vision.*. auto_curate reads
        # source_meta.predicted_keep_prob, written by train_photo_model.
        # Without this step, the new vision-tagged photos look "no photos
        # available" to auto-curate. Surfaced by batch-1 re-run halt.
        if not args.skip_curate and not args.skip_vision:
            log("\n=== STEP 6.5: Re-score predicted_keep_prob (model inference) ===")
            before = verify_keep_prob_scored(conn, fids)
            run_sub([sys.executable, "scripts/train_photo_model.py"],
                    halt, "train_photo_model (re-score full population)")
            conn = _ping_or_reconnect(conn)
            after = verify_keep_prob_scored(conn, fids)
            v = {}
            with conn.cursor() as cur:
                cur.execute("SELECT arena_group_id, count(*) FROM beach_photos "
                            " WHERE arena_group_id = ANY(%s) GROUP BY 1", (fids,))
                photo_counts = {fid: n for fid, n in cur.fetchall()}
            for f in fids:
                if (photo_counts.get(f, 0) or 0) == 0:
                    v[f] = None
                    log(f"      - fid={f:<10} no photos (skip)")
                else:
                    ok = (after.get(f) or 0) >= 1
                    mark = '✓' if ok else '✗'
                    log(f"      {mark} fid={f:<10} {(before.get(f) or 0)} → {(after.get(f) or 0)} (of {photo_counts.get(f)} photos)")
                    v[f] = ok
            for f, ok in v.items(): summary[f]["6.5_score"] = ok
            if halt and any(ok is False for ok in v.values()):
                raise SystemExit("step 6.5 verification failed (some photos still have no predicted_keep_prob)")

        # ─ STEP 7 ─────────────────────────────────────────────────────
        if not args.skip_curate:
            log("\n=== STEP 7: Auto-curate (per-fid) ===")
            tagged_counts = verify_vision_tagged(conn, fids)
            before = verify_auto_curated(conn, fids)
            run_sub([sys.executable, "scripts/auto_curate.py", "--fids", ",".join(map(str, fids)), "--n", str(args.n)],
                    halt, f"auto_curate --fids <{len(fids)}> --n {args.n}")
            conn = _ping_or_reconnect(conn)
            after = verify_auto_curated(conn, fids)
            # Per-fid: tagged>0 + curated>0 = ✓; tagged>0 + curated=0 = ⚠
            # (model scored all photos too low — surfaceable to curator);
            # no tagged = - (skip). The script DOESN'T halt on ⚠ — that's
            # legitimate model behavior, not a pipeline error.
            v = {}
            for f in fids:
                t, a = (tagged_counts.get(f) or 0), (after.get(f) or 0)
                if t == 0:
                    v[f] = None
                    log(f"      - fid={f:<10} no vision-tagged photos (skip)")
                elif a >= 1:
                    v[f] = True
                    log(f"      ✓ fid={f:<10} auto-curated {a} of {t} tagged")
                else:
                    v[f] = "warn"
                    log(f"      ⚠ fid={f:<10} 0 of {t} tagged scored high enough — model didn't pick any (curator review)")
            for f, ok in v.items(): summary[f]["7_curate"] = ok
        else:
            for f in fids: summary[f]["7_curate"] = None

        # ─ Final summary ───────────────────────────────────────────────
        log("\n" + "=" * 90)
        log("FINAL PER-FID SUMMARY")
        log("=" * 90)
        header = f"  {'fid':<10} {'1act':<6} {'2cat':<6} {'3bep':<6} {'4flk':<6} {'5wmc':<6} {'6vis':<6} {'6.5sc':<7} {'7cur':<6}  name"
        log(header)
        for f in fids:
            s = summary[f]
            def m(v):
                if v is True: return '✓'
                if v is None: return '-'
                if v == 'warn': return '⚠'
                return '✗'
            log(f"  {f:<10} {m(s.get('1_active')):<6} {m(s.get('2_catchment')):<6} {m(s.get('3_bep_refire')):<6} {m(s.get('4_flickr')):<6} {m(s.get('5_wikimedia')):<6} {m(s.get('6_vision')):<6} {m(s.get('6.5_score')):<7} {m(s.get('7_curate')):<6}  {info[f]['name']}")
        # GREEN = no hard ✗. ⚠ is acceptable (soft warn, expected for some real-world data).
        any_fail = any(v is False for f in fids for v in summary[f].values())
        any_warn = any(v == 'warn' for f in fids for v in summary[f].values())
        if any_fail:
            log("\n✗ HARD ERRORS PRESENT — see above")
            return 1
        elif any_warn:
            log("\n⚠ all hard checks pass; soft warns surfaced (see above)")
            return 0
        else:
            log("\nALL GREEN ✓")
            return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
