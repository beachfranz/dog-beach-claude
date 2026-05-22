"""codify_cascade_phase.py — pipeline phase orchestrator for per-state codify.

Per docs/codify_cascade_phase_spec.md (Franz directive 2026-05-19).

What it does
============
For a given state, builds an inventory of jurisdictions that need codify work
(federal units + counties + cities with scoreable beaches), then pre-checks
which ones are already covered vs need agent dispatch. Emits a JSON manifest
+ console report. v1 stops at the manifest; v2 will wire actual agent dispatch.

Why it exists
=============
On 2026-05-19 we dispatched 7 WA federal codify agents in parallel; 6 of 7
came back with "already shipped by prior session" (the late 2026-05-18 federal
push). ~35 min of LLM cycles wasted re-deriving known-good state. This phase
adds an up-front pre-check so the next state expansion (MA, FL, MI per the
MVP+ pin) only spawns agents for genuinely-uncovered units.

Modes
=====
  python scripts/codify_cascade_phase.py --state WA              # full report
  python scripts/codify_cascade_phase.py --state WA --json-only  # machine-readable
  python scripts/codify_cascade_phase.py --state WA --dispatch   # v2: actually fire agents (not yet)
  python scripts/codify_cascade_phase.py --state WA --verify-only # just the gate metric

Phase position in run_state_pipeline.py
=======================================
AFTER bulk loaders (PAD-US, TIGER) — needs federal coverage map populated.
BEFORE consensus/scoring phases — codify feeds the consensus engine.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


def _connect_pg():
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


# ─── Data shapes ───────────────────────────────────────────────────────

@dataclass
class FederalUnit:
    mng_name: str          # NPS / BLM / USFS / FWS / NOAA / ...
    unit_name: str
    des_tp: str | None
    beaches_in_unit: int
    beaches_linked: int
    ps_ids: list[int]
    coverage_status: str   # 'covered' | 'partial' | 'uncovered'

@dataclass
class JurisdictionUnit:
    kind: str              # 'county' | 'city'
    name: str
    fips_or_id: str | None
    scoreable_beaches: int
    precheck_outcome: str | None = None    # set by precheck
    state_ps_ids: list[int] | None = None

@dataclass
class PhaseReport:
    state: str
    timestamp: str
    federal: dict[str, Any] = field(default_factory=dict)
    counties: dict[str, Any] = field(default_factory=dict)
    cities:   dict[str, Any] = field(default_factory=dict)
    verification: dict[str, Any] = field(default_factory=dict)
    queue:    list[dict[str, Any]] = field(default_factory=list)


# ─── Step 1: enumerate ────────────────────────────────────────────────

def enumerate_federal(conn, state: str) -> list[FederalUnit]:
    """Per-federal-unit coverage via the codify_federal_coverage RPC."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT mng_name, unit_name, des_tp, beaches_in_unit, beaches_linked, "
            "       ps_ids, coverage_status "
            "FROM public.codify_federal_coverage(%s) "
            "ORDER BY mng_name, unit_name",
            (state,),
        )
        return [FederalUnit(**dict(r)) for r in cur.fetchall()]


def enumerate_counties(conn, state: str) -> list[JurisdictionUnit]:
    """Counties that contain ≥1 scoreable beach in the state.

    counties.state_fp is the 2-char FIPS state code; we derive it via
    jurisdictions (which has both fips_state + state) since there's no
    standalone tiger_state_fips lookup table.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            WITH sfp AS (
              SELECT DISTINCT fips_state FROM public.jurisdictions WHERE state = %s LIMIT 1
            )
            SELECT c.name AS name, c.geoid AS fips,
                   count(*)::int AS scoreable_beaches
              FROM public.counties c
              CROSS JOIN sfp
              JOIN public.beaches_gold g
                ON st_intersects(g.geom, c.geom)
             WHERE g.state = %s
               AND g.is_active
               AND g.scoring_tier IN ('daily','hourly')
               AND c.state_fp = sfp.fips_state
             GROUP BY c.name, c.geoid
             ORDER BY scoreable_beaches DESC, c.name
        """, (state, state))
        return [JurisdictionUnit(kind='county', name=r['name'], fips_or_id=r['fips'],
                                  scoreable_beaches=r['scoreable_beaches'])
                for r in cur.fetchall()]


def enumerate_cities(conn, state: str) -> list[JurisdictionUnit]:
    """C1 incorporated cities that contain ≥1 scoreable beach."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT j.name AS name, j.id::text AS jid,
                   count(*)::int AS scoreable_beaches
              FROM public.jurisdictions j
              JOIN public.beaches_gold g
                ON st_intersects(g.geom, j.geom)
             WHERE j.state = %s
               AND g.state = %s
               AND g.is_active
               AND g.scoring_tier IN ('daily','hourly')
               AND j.place_type = 'C1'
             GROUP BY j.name, j.id
             ORDER BY scoreable_beaches DESC, j.name
        """, (state, state))
        return [JurisdictionUnit(kind='city', name=r['name'], fips_or_id=r['jid'],
                                  scoreable_beaches=r['scoreable_beaches'])
                for r in cur.fetchall()]


# ─── Step 2: pre-check ────────────────────────────────────────────────

def precheck_jurisdiction(unit: JurisdictionUnit, state: str) -> JurisdictionUnit:
    """Run the derive_policy_source script as a dry-precheck for one jurisdiction.

    We parse the script's stdout for the `→ outcome: ...` line. The script is
    already idempotent + non-mutating until Step 9 (apply), so a default run
    in non-apply mode IS the precheck.

    Returns the unit with `precheck_outcome` + (optionally) `state_ps_ids` set.
    """
    cmd = [
        sys.executable, "scripts/derive_policy_source_for_jurisdiction.py",
        "--jurisdiction", unit.name,
        "--state-of", state,
    ]
    try:
        # Windows defaults to cp1252; force UTF-8 since the derive script
        # uses non-ASCII glyphs (→ ≥ ≤) in its outcome lines. Without this
        # the parser sees a UnicodeDecodeError + every outcome bucket lands
        # in "other" instead of "defer_state_baseline_covers".
        rc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                            encoding="utf-8", errors="replace", timeout=90)
    except subprocess.TimeoutExpired:
        unit.precheck_outcome = "timeout"
        return unit

    # Parse last "→ outcome:" line
    outcome = None
    state_ps_ids: list[int] | None = None
    for line in (rc.stdout or "").splitlines():
        line = line.strip()
        if line.startswith("→ outcome:"):
            # e.g.  "→ outcome: defer_state_baseline_covers  (state_ps_ids=[167,170])"
            outcome = line.split(":", 1)[1].strip().split()[0]
            if "state_ps_ids=[" in line:
                ids_str = line.split("state_ps_ids=[", 1)[1].split("]", 1)[0]
                state_ps_ids = [int(s) for s in ids_str.split(",") if s.strip().isdigit()]
    if rc.returncode != 0 and not outcome:
        # arg error etc — flag for review
        unit.precheck_outcome = f"script_error_exit_{rc.returncode}"
    else:
        unit.precheck_outcome = outcome or "unknown"
    unit.state_ps_ids = state_ps_ids
    return unit


# ─── Step 5: verify ───────────────────────────────────────────────────

def verify_state(conn, state: str, gate_pct: float = 95.0) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT public.count_uncovered_scoreable_beaches(%s)", (state,))
        uncovered = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*)::int FROM public.beaches_gold "
            " WHERE state=%s AND is_active AND scoring_tier IN ('daily','hourly')",
            (state,),
        )
        scoreable_total = cur.fetchone()[0]
    coverage_pct = 100.0 if scoreable_total == 0 else round(
        100.0 * (scoreable_total - uncovered) / scoreable_total, 1)
    return {
        "scoreable_total": scoreable_total,
        "scoreable_uncovered": uncovered,
        "coverage_pct": coverage_pct,
        "gate_pct": gate_pct,
        # Phase gate: pass if coverage ≥ gate_pct. Default 95% (steady-state /
        # MVP+ readiness). For state-launch mode use 80% — long-tail
        # jurisdictions take multiple passes to discover, so a hard 95% gate
        # halts every new state launch on day 1.
        "passes_gate": coverage_pct >= gate_pct,
    }


# ─── Orchestrator ─────────────────────────────────────────────────────

# Outcomes that mean "no dispatch needed"
NO_DISPATCH_OUTCOMES = {
    "defer_state_baseline_covers",
    "defer_tribal",
    "defer_federal_branch",
    "defer_cdp_covered_by_parent",
    "defer_unknown_class",
    "success_auto_commit",
    "success_human_review",
}

# Outcomes that mean "needs agent dispatch"
DISPATCH_OUTCOMES = {
    "defer_stubborn",   # tried everything, couldn't find — needs web-search / human
}


def run_phase(state: str, *, do_precheck_jurisdictions: bool = True,
              verbose: bool = True, workers: int = 10,
              gate_pct: float = 95.0) -> PhaseReport:
    report = PhaseReport(state=state, timestamp=datetime.now(timezone.utc).isoformat())

    with _connect_pg() as conn:
        if verbose:
            print(f"\n=== Codify-cascade phase — {state} ===", flush=True)

        # ── Federal ────────────────────────────────────────────────────
        fed_units = enumerate_federal(conn, state)
        report.federal = {
            "total_units": len(fed_units),
            "covered":    [asdict(u) for u in fed_units if u.coverage_status == "covered"],
            "partial":    [asdict(u) for u in fed_units if u.coverage_status == "partial"],
            "uncovered":  [asdict(u) for u in fed_units if u.coverage_status == "uncovered"],
        }
        if verbose:
            print(f"\n[federal] {len(fed_units)} units  "
                  f"covered={len(report.federal['covered'])}  "
                  f"partial={len(report.federal['partial'])}  "
                  f"uncovered={len(report.federal['uncovered'])}", flush=True)
            for u in fed_units:
                marker = {"covered": "✓", "partial": "~", "uncovered": "✗"}[u.coverage_status]
                print(f"  {marker} {u.mng_name:<6} {u.unit_name[:45]:<45} "
                      f"{u.beaches_linked}/{u.beaches_in_unit}  ps={u.ps_ids}", flush=True)
            for u in report.federal["uncovered"]:
                report.queue.append({
                    "kind": "federal_unit",
                    "mng_name": u["mng_name"],
                    "unit_name": u["unit_name"],
                    "beaches": u["beaches_in_unit"],
                    "suggested_cmd": _suggest_federal_cmd(state, u),
                })

        # ── Counties + Cities (parallel precheck) ──────────────────────
        # Each precheck spawns a derive_policy_source subprocess (~30-90s,
        # IO-bound). ThreadPoolExecutor is the right shape: per-thread
        # subprocess wait + the inner script holds its own DB conn so
        # we don't share `conn`. Worker cap is the pgbouncer ceiling
        # (15 in session mode; we leave headroom + cap at 10).
        if do_precheck_jurisdictions:
            for kind, enumerator, key in [
                ("county", enumerate_counties, "counties"),
                ("city",   enumerate_cities,   "cities"),
            ]:
                units = enumerator(conn, state)
                n_total = len(units)
                if verbose:
                    print(f"\n[{key}] precheck-ing {n_total} jurisdictions "
                          f"(parallel, {min(workers, n_total)} workers)...",
                          flush=True)
                if n_total > 0:
                    done = [0]
                    t0 = time.time()
                    with ThreadPoolExecutor(max_workers=min(workers, n_total)) as ex:
                        futures = [ex.submit(precheck_jurisdiction, u, state) for u in units]
                        for f in as_completed(futures):
                            done[0] += 1
                            if verbose and (done[0] % 5 == 0 or done[0] == n_total):
                                print(f"  ... {done[0]}/{n_total} done "
                                      f"({time.time()-t0:.0f}s)", flush=True)
                bucket = {
                    "total": len(units),
                    "deferred_state_baseline": [],
                    "needs_dispatch":          [],
                    "other_outcomes":          [],
                }
                for u in units:
                    d = asdict(u)
                    if u.precheck_outcome == "defer_state_baseline_covers":
                        bucket["deferred_state_baseline"].append(d)
                    elif u.precheck_outcome in DISPATCH_OUTCOMES:
                        bucket["needs_dispatch"].append(d)
                        report.queue.append({
                            "kind": f"jurisdiction_{kind}",
                            "name": u.name,
                            "scoreable_beaches": u.scoreable_beaches,
                            "outcome": u.precheck_outcome,
                            "suggested_cmd": _suggest_jurisdiction_cmd(state, u),
                        })
                    else:
                        bucket["other_outcomes"].append(d)
                setattr(report, key, bucket)
                if verbose:
                    print(f"  defer_state_baseline_covers: {len(bucket['deferred_state_baseline'])}", flush=True)
                    print(f"  needs_dispatch:              {len(bucket['needs_dispatch'])}", flush=True)
                    print(f"  other:                       {len(bucket['other_outcomes'])}", flush=True)

        # ── Verify ─────────────────────────────────────────────────────
        report.verification = verify_state(conn, state, gate_pct=gate_pct)
        if verbose:
            v = report.verification
            print(f"\n[verify] scoreable_total={v['scoreable_total']}  "
                  f"uncovered={v['scoreable_uncovered']}  "
                  f"coverage={v['coverage_pct']}%  "
                  f"gate={v['gate_pct']}%  "
                  f"passes_gate={v['passes_gate']}", flush=True)

        # ── Summary ────────────────────────────────────────────────────
        if verbose:
            print(f"\n[queue] {len(report.queue)} units would be dispatched for codify work", flush=True)
            if not report.queue:
                print("  (nothing to do — state codify is current)", flush=True)
            else:
                for q in report.queue[:10]:
                    if q['kind'] == 'federal_unit':
                        print(f"  - federal: {q['mng_name']:<6} {q['unit_name'][:50]}  ({q['beaches']} beaches)", flush=True)
                    else:
                        print(f"  - {q['kind']}: {q['name']}  ({q['scoreable_beaches']} beaches, {q.get('outcome','')})", flush=True)
                if len(report.queue) > 10:
                    print(f"  ... and {len(report.queue) - 10} more", flush=True)

    return report


# ─── Step 3 stub: dispatch suggestion (v1 = report only) ──────────────

def _suggest_federal_cmd(state: str, u: dict) -> str:
    return (
        f"python scripts/derive_policy_source_for_jurisdiction.py "
        f"--state-agency-rule "
        f"--agency-name '<look up by mng_name {u['mng_name']}>' --agency-type federal_department "
        f"--states {state} "
        f"--pad-mng-type FED --pad-mng-name {u['mng_name']} "
        f"--pad-unit-name-ilike \"%{u['unit_name']}%\" "
        f"--subtype <superintendents_compendium|federal_regulation> "
        f"--citation '<unit + section>' --rule-url '<deep-link>' "
        f"--rule <on_leash|not_allowed|off_leash|off_leash_voice_control> --section sand "
        f"--evidence-verbatim '<quote>' --full-text-file /tmp/<slug>.txt "
        f"--label federal_codify_{state.lower()}_{u['mng_name'].lower()}_<slug>"
    )

def _suggest_jurisdiction_cmd(state: str, u: JurisdictionUnit) -> str:
    return (
        f"python scripts/derive_policy_source_for_jurisdiction.py "
        f"--jurisdiction '{u.name}' --state-of {state}  "
        f"# precheck outcome was {u.precheck_outcome}; investigate per playbook"
    )


# ─── CLI ──────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--state", required=True, help="State code (e.g. WA, OR, CA)")
    ap.add_argument("--json-only", action="store_true",
                    help="Print only the JSON report (no human-readable text)")
    ap.add_argument("--no-jurisdictions", action="store_true",
                    help="Skip county/city precheck (federal-only, faster)")
    ap.add_argument("--verify-only", action="store_true",
                    help="Just print the count_uncovered_scoreable_beaches gate metric")
    ap.add_argument("--out-dir", default="tmp", help="Where to write the manifest JSON")
    ap.add_argument("--workers", type=int, default=10,
                    help="Parallel jurisdiction precheck workers (default 10; "
                         "pgbouncer session-mode pool ceiling is 15)")
    ap.add_argument("--gate-pct", type=float, default=95.0,
                    help="Coverage percentage required to pass the gate. "
                         "Default 95.0 (steady-state / MVP+ readiness). "
                         "Use 80.0 for state-launch mode (long-tail jurisdictions "
                         "take multiple passes; hard 95% halts every new state).")
    args = ap.parse_args()

    state = args.state.upper()

    if args.verify_only:
        with _connect_pg() as conn:
            v = verify_state(conn, state, gate_pct=args.gate_pct)
        print(json.dumps(v, indent=2))
        return 0 if v["passes_gate"] else 1

    report = run_phase(
        state,
        do_precheck_jurisdictions=not args.no_jurisdictions,
        verbose=not args.json_only,
        workers=args.workers,
        gate_pct=args.gate_pct,
    )

    # Emit manifest
    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    manifest_path = out_dir / f"codify_cascade_{state.lower()}_{ts}.json"
    manifest_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if args.json_only:
        print(json.dumps(asdict(report), indent=2))
    else:
        print(f"\nManifest: {manifest_path}")

    return 0 if report.verification.get("passes_gate") else 1


if __name__ == "__main__":
    sys.exit(main())
