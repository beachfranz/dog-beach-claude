"""categorize_policy_sources_extraction_mode.py
=================================================

One-shot Sonnet classifier that labels every `public.policy_source` row
with an `extraction_mode` so the downstream `extract_and_load_deep.py`
pipeline knows whether to run bulk-mode (one cached LLM call per source,
applied deterministically to every beach) or pair-mode (per-beach LLM).

Categories:
  - universal:        statute is uniform across all in-scope beaches.
                      No named exceptions, no fuzzy carve-outs.
  - named_exception:  statute lists specific named beaches/places as
                      exceptions (e.g. "Fort Funston excepted from §6.16
                      city dog ban"). Cached extraction + deterministic
                      name_match check per beach.
  - fuzzy_exception:  statute uses phrases like "designated areas" /
                      "where posted" / "with permission of operator" —
                      the exception list isn't enumerable from the
                      statute alone. Cached base rule + per-beach LLM
                      for carve-out decisions.
  - per_beach:        rule is structurally per-property (e.g. an MOU
                      between one operator and one site). Pair-mode
                      required.

Persistence:
  - policy_source.extraction_mode → final category
  - policy_source_extraction_cache (cache_key='category:{sha}:{model}')
    → full classifier output incl. excepted_places + fuzzy_markers +
    reasoning, so re-runs are idempotent free no-ops.

Usage:
  python scripts/categorize_policy_sources_extraction_mode.py            # dry-run, no writes
  python scripts/categorize_policy_sources_extraction_mode.py --apply
  python scripts/categorize_policy_sources_extraction_mode.py --apply --ps-ids 173,48
  python scripts/categorize_policy_sources_extraction_mode.py --apply --limit 10
  python scripts/categorize_policy_sources_extraction_mode.py --apply --refresh   # re-categorize already-tagged sources

Cost: ~$0.001 per source via Sonnet 4.6 (short prompt + ~8k input tokens
truncated full_text + tiny JSON output). 394 sources ≈ $0.30 one-shot.
"""
from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import argparse
import json
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2.extras

from scripts.common.db import connect
from scripts.common.llm import SONNET, call_json
from scripts.common.policy_source_cache import (
    build_cache_key, cache_lookup, cache_write,
)


MODEL = SONNET
NAMESPACE = "category"

SYSTEM_PROMPT = """You are classifying a single legal/policy document by its EXCEPTION
STRUCTURE for use in a dog-beach policy extraction pipeline.

The pipeline reads the document and decides, for each beach the document
applies to, whether dogs are allowed and under what conditions. We want
to know whether the document's rules are uniform across all in-scope
beaches, or whether they list exceptions, or whether they reference
fuzzy "designated areas" that require per-beach checking.

Pick exactly ONE of these seven categories:

1. universal
   The rule is identical across every beach the document covers. No
   named exceptions, no "designated areas" wording, no per-property
   conditional logic, no carveout subset. (Examples: "no dogs in any
   state park beach unless on leash" with no exceptions list; "leash
   required throughout unincorporated county".)

2. named_exception
   The rule has a default position AND lists specific named places
   (beaches, parks, units, sub-areas) as exceptions. The exception list
   is finite and enumerable FROM THIS DOCUMENT'S TEXT. (Example: "Dogs
   prohibited on all city beaches EXCEPT Fort Funston and Crissy Field
   where off-leash is permitted." — exceptions: ["Fort Funston", "Crissy
   Field"].)

   IMPORTANT: if the "exception" is a pointer to ANOTHER section/statute
   (e.g. "except as provided in Section 6.16.310"), use cross_reference
   instead, NOT named_exception. The excepted_places list must contain
   actual place names, not code section references.

3. fuzzy_exception
   The rule's exceptions are fuzzy AND the default rule is itself
   case-specific. Use this when there is no clean "universal default +
   carveout subset" structure. Rare — prefer universal_with_fuzzy_carveout
   below when the default rule IS clearly universal.

4. per_beach
   The document is structurally per-property — e.g. an MOU between a
   single operator and a single site, a concession lease, or an
   agency-administrative-policy that names one specific beach only.
   Applying it to other beaches without LLM context would be unsafe.

5. universal_with_fuzzy_carveout
   The DEFAULT rule is universal across all in-scope beaches AND there
   is a fuzzy carveout subset that requires per-beach disambiguation.
   (Example: WAC §352-32-060 — "all pets must be on a leash everywhere
   in state parks EXCEPT on designated swimming beaches." Default
   (leash) is universal; carveout (which beaches are "designated
   swimming beaches") needs per-beach checking.)

   Distinguishing from fuzzy_exception: in universal_with_fuzzy_carveout
   the DEFAULT is universal — for any beach not in the carveout, the
   answer is deterministic. In fuzzy_exception the rule structure itself
   is murky.

6. cross_reference
   The exception list is a pointer to ANOTHER policy_source (a different
   ordinance section, a different agency policy, etc.) rather than
   actual named places enumerated in this document. (Example: "Dogs
   prohibited on all city beaches except as provided in Section
   6.16.310.") The actual exception list lives in the cross-referenced
   source, not here. The cross_reference category tells downstream code
   to resolve transitively.

7. deferred
   The statute defers to external state that CANNOT be resolved from
   text alone — for example, on-site signage ("where signs posted"),
   discretionary operator permission ("with permission of the parks
   director"), or seasonal/condition flags posted at the entrance.
   Unlike fuzzy_exception, there is NO authoritative list anywhere that
   we could in principle look up — the answer is genuinely "ask the
   site / check signage." Downstream we will emit a "cannot
   disambiguate" sentinel rather than hallucinating a verdict.

Also identify the TEMPORAL AXIS of the rule — whether and how the rule
varies by time. This is ORTHOGONAL to the spatial category above; both
fields are independent.

temporal_axis values:
- "none"     — rule is uniform across time (year-round, 24/7)
- "daily"    — rule varies by time of day (e.g. "before 9am and after 6pm")
- "seasonal" — rule varies by season/date range (e.g. "June 1 – Sept 15")
- "both"     — has both daily and seasonal windows

Return ONLY this JSON, no prose, no markdown:
{
  "extraction_mode": "universal" | "named_exception" | "fuzzy_exception" | "per_beach" | "universal_with_fuzzy_carveout" | "cross_reference" | "deferred",
  "excepted_places":     ["<exact name>", ...],   // populated for named_exception or universal_with_fuzzy_carveout (carveout subset if it has any named anchors); else []
  "fuzzy_markers":       ["<exact phrase>", ...], // populated for fuzzy_exception or universal_with_fuzzy_carveout; else []
  "cross_reference_target": "<section/citation reference>" | null, // populated for cross_reference; else null
  "deferral_reason":     "signage" | "operator_permission" | "condition_dependent" | null, // populated for deferred; else null
  "temporal_axis":       "none" | "daily" | "seasonal" | "both",
  "temporal_markers":    ["<exact phrase>", ...], // populated when temporal_axis != "none"; else []
  "confidence":          "high" | "medium" | "low",
  "reasoning":           "<one or two sentences explaining the spatial choice + temporal axis>"
}"""


USER_TEMPLATE = """SUBTYPE: {subtype}
CITATION: {citation}

FULL TEXT:
{full_text}"""


# Hard cap on full_text we ship to Sonnet. Most policy_source.full_text
# rows are well under this; longer ones get truncated. The classifier
# only needs the exception structure, not the entire ordinance.
_TEXT_CAP = 12000


def build_user_message(subtype: str, citation: str, full_text: str | None) -> str:
    text = (full_text or "").strip()
    if len(text) > _TEXT_CAP:
        text = text[:_TEXT_CAP] + "\n\n[...TRUNCATED FOR CLASSIFIER...]"
    return USER_TEMPLATE.format(
        subtype=subtype or "(unknown)",
        citation=citation or "(uncited)",
        full_text=text or "(empty)",
    )


# Cache key. SYSTEM_PROMPT is the only piece that affects classifier
# output; bumping its text invalidates all cached classifications.
CACHE_KEY = build_cache_key(NAMESPACE, MODEL, SYSTEM_PROMPT)


def classify_one(subtype: str, citation: str, full_text: str | None) -> dict:
    """Call Sonnet; return parsed JSON dict."""
    user_msg = build_user_message(subtype, citation, full_text)
    r = call_json(
        SYSTEM_PROMPT, user_msg,
        model=MODEL, max_tokens=600, cache_system=True,
    )
    return {
        "parsed": r["parsed"],
        "input_tokens": r["input_tokens"],
        "output_tokens": r["output_tokens"],
        "cache_read_input_tokens": r.get("cache_read_input_tokens", 0),
    }


def select_targets(cur, ps_ids: list[int] | None, limit: int | None,
                   refresh: bool) -> list[dict]:
    """Pick which policy_sources to (re-)categorize."""
    where = []
    params: list = []
    if ps_ids:
        where.append("ps.id = ANY(%s)")
        params.append(ps_ids)
    if not refresh and not ps_ids:
        # Default mode: skip sources already labeled
        where.append("ps.extraction_mode IS NULL")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(
        f"""
        SELECT ps.id, ps.subtype, ps.citation, ps.full_text, ps.extraction_mode
          FROM public.policy_source ps
          {where_sql}
         ORDER BY ps.id
         {limit_sql}
        """,
        params,
    )
    return cur.fetchall()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Write to DB. Without this, dry-run prints classifications.")
    ap.add_argument("--ps-ids", type=str, default=None,
                    help="Comma-separated subset of policy_source ids.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap target count (after --ps-ids filter).")
    ap.add_argument("--refresh", action="store_true",
                    help="Re-categorize sources that already have extraction_mode set.")
    ap.add_argument("--no-cache", action="store_true",
                    help="Skip cache lookup; always call LLM. Cache writes still happen.")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERR: ANTHROPIC_API_KEY not set in env"); return 1

    ps_ids = None
    if args.ps_ids:
        ps_ids = [int(x) for x in args.ps_ids.split(",") if x.strip()]

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    targets = select_targets(cur, ps_ids, args.limit, args.refresh)
    print(f"Targets: {len(targets)} policy_source rows  "
          f"(cache_key={CACHE_KEY})", flush=True)
    if not targets:
        return 0

    totals = {"universal": 0, "named_exception": 0, "fuzzy_exception": 0,
              "per_beach": 0, "universal_with_fuzzy_carveout": 0,
              "cross_reference": 0, "deferred": 0,
              "low_conf": 0, "cache_hit": 0, "parse_error": 0}
    tok = {"in": 0, "out": 0, "cached": 0}
    t0 = time.time()

    for i, row in enumerate(targets, 1):
        ps_id   = row["id"]
        subtype = row["subtype"]
        citation = row["citation"]

        # Cache short-circuit
        cached = None if args.no_cache else cache_lookup(cur, CACHE_KEY, ps_id)
        if cached is not None:
            parsed = cached.get("parsed") or cached
            totals["cache_hit"] += 1
            print(f"  [{i}/{len(targets)}] ps={ps_id} CACHE_HIT", flush=True)
        else:
            try:
                resp = classify_one(subtype, citation, row.get("full_text"))
            except Exception as e:
                print(f"  [{i}/{len(targets)}] ps={ps_id} LLM_FAIL: {e}", flush=True)
                continue
            parsed = resp["parsed"]
            tok["in"]     += resp["input_tokens"]
            tok["out"]    += resp["output_tokens"]
            tok["cached"] += resp["cache_read_input_tokens"]

        mode = parsed.get("extraction_mode")
        conf = parsed.get("confidence", "low")
        excepted = parsed.get("excepted_places") or []
        fuzzy = parsed.get("fuzzy_markers") or []
        reasoning = (parsed.get("reasoning") or "")[:200]

        if mode not in ("universal", "named_exception",
                        "fuzzy_exception", "per_beach",
                        "universal_with_fuzzy_carveout",
                        "cross_reference", "deferred"):
            totals["parse_error"] += 1
            print(f"  [{i}/{len(targets)}] ps={ps_id} BAD_MODE={mode!r}", flush=True)
            continue

        if conf == "low":
            totals["low_conf"] += 1

        print(f"  [{i}/{len(targets)}] ps={ps_id} {subtype[:18]:<18} "
              f"→ {mode} ({conf}) "
              f"{'excepted=' + str(len(excepted)) + ' ' if excepted else ''}"
              f"{'fuzzy=' + str(len(fuzzy)) + ' ' if fuzzy else ''}"
              f"| {reasoning[:80]}", flush=True)

        totals[mode] += 1

        if args.apply:
            # Persist the column AND cache the classifier output. Both
            # in one txn per ps so a crash doesn't drift them apart.
            cur.execute(
                "UPDATE public.policy_source SET extraction_mode = %s, updated_at = now() "
                " WHERE id = %s",
                (mode, ps_id),
            )
            if cached is None:
                # Only write cache when we just paid for the call.
                cache_write(cur, CACHE_KEY, ps_id, {
                    "parsed": parsed,
                    "model": MODEL,
                    "cached_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                })
            conn.commit()

    elapsed = time.time() - t0
    print()
    print("=== SUMMARY ===")
    for k in ("universal", "named_exception", "fuzzy_exception", "per_beach",
              "universal_with_fuzzy_carveout", "cross_reference", "deferred",
              "low_conf", "cache_hit", "parse_error"):
        print(f"  {k:30} = {totals[k]:>5}")
    # Sonnet 4.6 pricing per spec sheet (rough): ~$3/$15 per Mtok in/out.
    # Cached input bills at ~10% of regular. Adjust if pricing changes.
    cost = (tok["in"] - tok["cached"]) * 3.0e-6 + tok["cached"] * 0.3e-6 + tok["out"] * 15e-6
    print(f"  tokens in={tok['in']:>7} (cached={tok['cached']})  out={tok['out']:>6}  "
          f"est_cost≈${cost:.3f}  elapsed={elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
