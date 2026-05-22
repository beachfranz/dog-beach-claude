"""Extract enforcement-clause summary from existing policy_source.full_text.

Backfill for the new public.policy_source.penalty_summary column (added
2026-05-18). Reads each ps row's full_text + citation, asks Sonnet for a
1-2 sentence summary of the enforcement clause (classification + fine
amount), writes back via UPDATE.

Mirrors extract_temporal_from_policy_source.py shape. The penalty applies
to whatever rule the section establishes — presence violation ("no dogs
at all") OR leash violation ("off-leash where leash required").

Usage:
  python scripts/extract_penalty_from_policy_source.py --ps-ids 22,52
  python scripts/extract_penalty_from_policy_source.py --state CA
  python scripts/extract_penalty_from_policy_source.py --all
  python scripts/extract_penalty_from_policy_source.py --state CA --refresh
  python scripts/extract_penalty_from_policy_source.py --state CA --dry-run

Default: skips ps rows that already have a non-null penalty_summary.
Pass --refresh to wipe + re-extract.

Cost: ~$0.005-0.01 per ps row via Sonnet 4.5.
"""

from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import argparse
import json
import time
import urllib.error
import urllib.parse

# scripts.common loads .env + injects truststore at package init.
from scripts.common.llm import SONNET, call_json
from scripts.common.supa import supa

MODEL = SONNET

PENALTY_PROMPT = """You are a legal-citation parser. Given a policy_source row's verbatim text + citation, extract the ENFORCEMENT CLAUSE for violating the rule.

The penalty applies to WHATEVER the rule is — presence violation ("no dogs at all") OR leash violation ("off-leash where leash required") OR sub-area/time violation. The penalty summary is rule-agnostic.

Input: JSON with `citation` + `full_text` of one policy_source row.

Output: JSON:
{
  "penalty_summary": null | "<1-2 sentence summary>",
  "confidence":     0.0..1.0,
  "notes":          null | "<flag if uncertain>"
}

Format guidance for penalty_summary:
- Lead with the CLASSIFICATION (infraction / misdemeanor / civil penalty / "no specified penalty")
- Then the FINE AMOUNT(S) if explicit ($N, $N-$M graduated, "up to $N")
- ≤ 2 sentences, ≤ 200 chars

EXAMPLES (good outputs):

EXAMPLE 1 (Solana Beach §11.12.035 — no dogs on beach):
"Misdemeanor punishable by up to $1,000 fine and/or 6 months imprisonment per municipal code §1.16.010."

EXAMPLE 2 (Carmel — citywide leash rule):
"Infraction; fine up to $100 first offense, $200 second, $500 third."

EXAMPLE 3 (Long Beach §6.16.090 — beach dog prohibition):
"Misdemeanor."

EXAMPLE 4 (text mentions enforcement but no dollar):
"Civil penalty; amount not specified in cited section."

EXAMPLE 5 (no penalty language at all in the source text):
{"penalty_summary": null, "confidence": 0.95, "notes": "no enforcement clause in cited text"}

RULES:
1. If the text contains a dollar amount, INCLUDE IT (most useful signal).
2. If the text says "infraction" / "misdemeanor" but no amount, capture the classification.
3. Cross-referenced penalties ("punishable per §1.16") — note the cross-ref but don't fabricate the cited section's content.
4. NULL is the correct answer when source text genuinely doesn't mention enforcement. Don't fabricate.
5. confidence < 0.5 = uncertain (e.g., ambiguous wording, possible cross-ref). Flag in notes.
6. Output ONLY the JSON. No prose. No markdown fences.
"""


def anthropic_call(prompt: str, blob: dict) -> dict:
    """Thin wrapper around common.llm.call_json. Preserves this script's
    callers' contract: returns dict with `parsed` + abbreviated usage keys
    (`in_t`, `out_t`, `cache_r`, `cost`)."""
    r = call_json(prompt, blob, model=MODEL, max_tokens=600)
    in_t, out_t, cache_r = r["input_tokens"], r["output_tokens"], r["cache_read_input_tokens"]
    cost = (in_t * 3.0 + out_t * 15.0 + cache_r * 0.30) / 1_000_000
    return {"parsed": r["parsed"], "in_t": in_t, "out_t": out_t,
            "cache_r": cache_r, "cost": cost}


def fetch_ps_rows(ps_ids: list[int] | None, state: str | None,
                  all_rows: bool, refresh: bool) -> list[dict]:
    params: dict = {
        "select": "id,subtype,citation,full_text,penalty_summary",
        "full_text": "not.is.null",
        "order": "id.asc",
    }
    if not refresh:
        params["penalty_summary"] = "is.null"
    if ps_ids:
        params["id"] = "in.(" + ",".join(str(i) for i in ps_ids) + ")"

    rows = supa("/rest/v1/policy_source", params=params)
    if state:
        ids = [r["id"] for r in rows]
        if not ids:
            return []
        beach_link = supa("/rest/v1/beach_policy_source", params={
            "select": "policy_source_id,beach_fid,beaches_gold(state)",
            "policy_source_id": "in.(" + ",".join(str(i) for i in ids) + ")",
        })
        keep = {r["policy_source_id"] for r in beach_link
                if (r.get("beaches_gold") or {}).get("state") == state}
        rows = [r for r in rows if r["id"] in keep]
    return rows


def extract_one(row: dict, dry_run: bool) -> dict:
    pid = row["id"]
    blob = {"citation": row.get("citation"),
            "full_text": row.get("full_text")}
    if dry_run:
        print(f"      DRY-RUN: would call Sonnet for ps={pid}")
        return {"pid": pid, "status": "dry-run", "cost": 0.0}
    try:
        out = anthropic_call(PENALTY_PROMPT, blob)
    except Exception as e:
        print(f"      ERROR ps={pid}: {e}", file=sys.stderr)
        return {"pid": pid, "status": "error", "cost": 0.0}

    parsed = out["parsed"]
    penalty = parsed.get("penalty_summary")
    conf = float(parsed.get("confidence", 0.0))
    notes = parsed.get("notes")

    if penalty:
        marker = "✅" if conf >= 0.7 else ("⚠️" if conf >= 0.4 else "❌")
        excerpt = penalty[:120] + ("…" if len(penalty) > 120 else "")
        print(f"      {marker} conf={conf:.2f}  {excerpt}")
    else:
        print(f"      ∅ no penalty clause (conf={conf:.2f})"
              + (f"  notes: {notes}" if notes else ""))

    # UPDATE policy_source SET penalty_summary = ...
    supa(f"/rest/v1/policy_source", method="PATCH",
         params={"id": f"eq.{pid}"},
         body={"penalty_summary": penalty})
    return {"pid": pid, "status": "ok",
            "has_penalty": bool(penalty),
            "in": out["in_t"], "out": out["out_t"], "cost": out["cost"]}


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill penalty_summary for policy_source rows")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--ps-ids", help="comma-separated policy_source ids")
    g.add_argument("--state", help="all ps linked to ≥1 beach in this state")
    g.add_argument("--all", action="store_true", help="every ps row with full_text")
    ap.add_argument("--refresh", action="store_true",
                    help="re-extract even when penalty_summary is already populated")
    ap.add_argument("--dry-run", action="store_true",
                    help="do not call Sonnet, do not UPDATE")
    ap.add_argument("--limit", type=int, help="cap to first N rows")
    args = ap.parse_args()

    ps_ids = None
    if args.ps_ids:
        ps_ids = [int(x.strip()) for x in args.ps_ids.split(",") if x.strip()]
    rows = fetch_ps_rows(ps_ids=ps_ids, state=args.state,
                         all_rows=args.all, refresh=args.refresh)
    if args.limit:
        rows = rows[:args.limit]
    if not rows:
        print("No policy_source rows to process.")
        return 0

    print(f"Targets: {len(rows)} ps row(s)")
    totals = {"ok": 0, "with_penalty": 0, "null": 0, "error": 0,
              "in": 0, "out": 0, "cost": 0.0}
    for i, r in enumerate(rows, 1):
        cite = (r.get("citation") or "")[:60]
        print(f"  [{i}/{len(rows)}] ps={r['id']}  {cite}")
        st = extract_one(r, dry_run=args.dry_run)
        if st["status"] == "ok":
            totals["ok"] += 1
            if st.get("has_penalty"):
                totals["with_penalty"] += 1
            else:
                totals["null"] += 1
            totals["in"] += st["in"]; totals["out"] += st["out"]; totals["cost"] += st["cost"]
        elif st["status"] == "error":
            totals["error"] += 1
        time.sleep(0.15)

    print("\n=== TOTALS ===")
    print(f"  processed:        {totals['ok']}")
    print(f"  with penalty:     {totals['with_penalty']}")
    print(f"  no penalty (∅):   {totals['null']}")
    print(f"  errors:           {totals['error']}")
    print(f"  tokens in/out:    {totals['in']} / {totals['out']}")
    print(f"  cost:             ${totals['cost']:.4f}")
    return 0 if totals["error"] == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
