#!/usr/bin/env python3
"""R1 fix, step 2 (regression audit 2026-06-23): LLM-classify the remaining
off-leash `beach_policy_source` sections as CARVE-OUT (does NOT make THIS beach
off-leash) vs GENUINE.

The deterministic pass (mig 20260623b) already tagged the high-precision clear
leaks (dog-park / activity / private-property / definition). What remains are
the genuine + ambiguous rows where regex can't tell "this beach is off-leash"
from "off-leash is at some OTHER place" (e.g. "dogs off-leash only in designated
dog areas" reads genuine but means the beach is leashed). That distinction is
semantic, so we ask the model per (beach, clause).

Sets `beach_policy_source.is_carveout`. This has NO consumer effect until the
roll-up fix (step 3) reads it, so it's safe to run + validate before flipping
the surface. Idempotent/resumable via a tmp checkpoint.

Usage:
  python scripts/classify_offleash_carveout.py --limit 25            # dry-run sample
  python scripts/classify_offleash_carveout.py --apply --model haiku # full run
  python scripts/classify_offleash_carveout.py --apply --model sonnet
"""
from __future__ import annotations

import sys
import os
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# sys.path bootstrap before scripts.common imports (HARD rule).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2.extras  # noqa: E402
from scripts.common.db import connect  # noqa: E402
from scripts.common.llm import SONNET, HAIKU, call_json  # noqa: E402

# Anthropic pricing ($ per token). Sonnet 4.6 / Haiku 4.5.
RATES = {
    SONNET: {"in": 3.0e-6, "cache_read": 0.30e-6, "out": 15.0e-6},
    HAIKU:  {"in": 0.80e-6, "cache_read": 0.08e-6, "out": 4.0e-6},
}

SYSTEM = """You decide whether a dog OFF-LEASH policy clause makes a SPECIFIC BEACH off-leash, or whether it is a carve-out that does NOT.

You are given a beach (name, county, state) and ONE verbatim policy clause that an extractor labeled "off-leash". Decide which:

is_carveout = false  (GENUINE — this beach IS off-leash from this clause):
  the clause plausibly puts dogs off-leash on THIS beach or its general shoreline/area —
  e.g. it names this beach, says dogs may be off-leash ON the beach, allows voice-control off-leash here,
  or designates this beach / a zone of it as off-leash.

is_carveout = true  (CARVE-OUT — the beach is NOT off-leash from this clause):
  the off-leash applies to something ELSE, not this beach —
   * a DIFFERENT named place: a dog park, a different beach/park, "designated dog areas/parks" elsewhere;
   * an ACTIVITY: obedience class, service/guide/police/SAR dog, hunting, herding, field trial;
   * PRIVATE PROPERTY / "at home" / the owner's premises;
   * a mere DEFINITION of "at large";
   * or the clause is really a LEASH requirement and off-leash is only allowed elsewhere.

Be conservative: pick is_carveout=true UNLESS the clause genuinely puts dogs off-leash on THIS beach.
"Off-leash only in designated dog areas / dog parks" -> true (the beach itself is leashed).
A clause naming a DIFFERENT place than this beach -> true.

Return ONLY JSON: {"is_carveout": true|false, "confidence": "high"|"medium"|"low", "reason": "<one short sentence>"}"""

CKPT = "tmp/carveout_progress.jsonl"


def classify(row, model):
    """Worker (runs in a thread). Returns (row, is_carveout, parsed, usage)."""
    user = f"BEACH: {row['name']} ({row['county_name'] or '?'}, {row['state']})\nCLAUSE: {row['ev']}"
    r = call_json(SYSTEM, user, model=model, max_tokens=200, cache_system=True)
    p = r["parsed"]
    usage = (r.get("input_tokens", 0), r.get("cache_read_input_tokens", 0), r.get("output_tokens", 0))
    return row, bool(p.get("is_carveout")), p, usage


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write is_carveout (else dry-run)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--model", choices=["haiku", "sonnet"], default="haiku")
    ap.add_argument("--workers", type=int, default=8, help="concurrent LLM calls")
    args = ap.parse_args()
    model = {"haiku": HAIKU, "sonnet": SONNET}[args.model]

    done = set()
    if os.path.exists(CKPT):
        for line in open(CKPT, encoding="utf-8"):
            try:
                done.add(json.loads(line)["id"])
            except Exception:
                pass

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT bps.id, bps.beach_fid, bps.evidence_verbatim AS ev,
               coalesce(g.display_name_override, g.name) AS name,
               g.county_name, g.state
          FROM beach_policy_source bps
          JOIN beaches_gold g ON g.fid = bps.beach_fid
         WHERE g.is_active AND g.scoring_tier IN ('daily','hourly')
           AND bps.operative_status = 'operative'
           AND bps.rule IN ('off_leash','off_leash_voice_control')
           AND bps.is_carveout = false
         ORDER BY bps.id
        """
    )
    rows = [r for r in cur.fetchall() if r["id"] not in done]
    if args.limit:
        rows = rows[: args.limit]
    print(f"to classify: {len(rows)} rows (model={args.model}, apply={args.apply}, already_done={len(done)})", flush=True)

    n_cv = n_gen = 0
    tin = tcr = tout = 0  # token tallies: input / cache_read / output
    rate = RATES.get(model, RATES[SONNET])
    fout = open(CKPT, "a", encoding="utf-8") if args.apply else None
    done_n = 0

    def cost_now():
        return tin * rate["in"] + tcr * rate["cache_read"] + tout * rate["out"]

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(classify, r, model): r for r in rows}
        for fut in as_completed(futs):
            r0 = futs[fut]
            try:
                r, cv, p, usage = fut.result()
            except Exception as e:
                print(f"  id={r0['id']} FAIL {str(e)[:120]}", flush=True)
                continue
            ui, ucr, uo = usage
            tin += ui; tcr += ucr; tout += uo
            n_cv += int(cv); n_gen += int(not cv); done_n += 1
            if args.apply:
                cur.execute("UPDATE public.beach_policy_source SET is_carveout=%s WHERE id=%s", (cv, r["id"]))
                conn.commit()
                fout.write(json.dumps({"id": r["id"], "cv": cv}) + "\n")
                fout.flush()
            if done_n <= 25 or done_n % 100 == 0:
                tag = "CARVEOUT" if cv else "genuine "
                print(f"  [{done_n:4}/{len(rows)}] {tag} {r['name'][:24]:24} {r['state']}  ${cost_now():.3f}  {str(p.get('reason',''))[:52]}", flush=True)

    print(f"done: carveout={n_cv}  genuine={n_gen}  (total {n_cv + n_gen})", flush=True)
    print(f"COST: ${cost_now():.3f}  (tokens in={tin} cache_read={tcr} out={tout}; model={args.model}; this run only — first 22 rows from the prior killed run not included)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
