"""Extract canonical operating hours (when the beach itself is OPEN, not
when dogs are allowed) from policy_source.full_text via Sonnet.

Per task #118. Mirrors scripts/extract_temporal_from_policy_source.py
(the §9.5 dog-access-temporal extractor) but:
  - Reads operating-hours prose from ordinance/compendium text
  - Writes to beaches_gold.operating_hours JSONB (source='agency')
  - NOT to beach_policy_source_temporal (that's dog-access carve-outs)

Output shape per beach (JSONB):
  {
    "default":  "06:00-22:00" | "sunrise-sunset" | "24/7" | "closed",
    "seasonal": [
      {"from": "MM-DD", "to": "MM-DD", "hours": "<spec>"}
    ],
    "notes":    "<optional context — sub-area scope etc.>"
  }

Skip rule (priority chain):
  - manual_curator → never overwrite
  - operator       → don't overwrite (operator-posted policy is more
                    authoritative for operating hours specifically)
  - everything else (osm, inferred, agency, NULL) → overwrite

Cache: by (ps_id, prompt_sha + model). Re-running with same ps + same
prompt is a no-op.

Cost: ~$0.005-0.01 per ps via Sonnet 4.5. MD 162 ps ≈ $0.80-1.60.

Usage:
  python scripts/extract_operating_hours_from_policy_source.py --state MD
  python scripts/extract_operating_hours_from_policy_source.py --ps-ids 5,17
  python scripts/extract_operating_hours_from_policy_source.py --state MD --pilot 5
  python scripts/extract_operating_hours_from_policy_source.py --refresh --ps-ids 5
"""

from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.parse

# Importing from scripts.common triggers load_dotenv + truststore injection
# at package init (see scripts/common/__init__.py).
from scripts.common.llm import SONNET, call_json
from scripts.common.supa import supa

MODEL = SONNET

# ─── Prompt ──────────────────────────────────────────────────────────

PROMPT = """You are a legal-citation parser specialized in beach/park operating hours. Your job is to extract STRUCTURED OPERATING HOURS (when the BEACH itself is OPEN to the public) from the text of a policy_source row.

You are NOT extracting dog-access rules (when dogs are allowed). You are extracting when the beach is physically open vs closed to all visitors.

Input: a JSON blob with the policy_source's text + linked beach context.

Output: a JSON object with this shape:
{
  "operating_hours": null  OR  {
    "default":  "<spec>",           // year-round default; see SPEC syntax below
    "seasonal": [                    // optional seasonal overrides
      {"from": "MM-DD", "to": "MM-DD", "hours": "<spec>"}
    ],
    "notes":    null or string,      // sub-area scope, ambiguity, etc.
    "confidence": 0.0..1.0,
    "evidence_quote": "verbatim snippet from source"
  }
}

SPEC syntax (one of):
  "HH:MM-HH:MM"      — explicit clock hours (24h, e.g., "06:00-22:00")
  "sunrise-sunset"   — dawn-to-dusk
  "24/7"             — always open
  "closed"           — closed (seasonal closure only — never the default)

If the policy_source text does NOT mention operating hours of the beach, return:
{"operating_hours": null}

EXAMPLES:

EXAMPLE 1 (NPS compendium with explicit hours):
Source text: "GWMP Compendium 2025: The parkway is open daily from 6:00 a.m. to 10:00 p.m. Pets must be on a 6-foot leash."
Output:
{
  "operating_hours": {
    "default": "06:00-22:00",
    "seasonal": [],
    "notes": null,
    "confidence": 0.95,
    "evidence_quote": "The parkway is open daily from 6:00 a.m. to 10:00 p.m."
  }
}

EXAMPLE 2 (state-park regulation with seasonal lifeguard window):
Source text: "Sandy Point State Park is open year-round from 6:00 a.m. to sunset. Lifeguards on duty Memorial Day through Labor Day from 11:00 a.m. to 6:00 p.m."
Output:
{
  "operating_hours": {
    "default": "06:00-sunset",
    "seasonal": [],
    "notes": "Lifeguard staffing window (Memorial Day–Labor Day, 11 AM–6 PM) noted in source but is a staffing schedule, not an operating-hours change.",
    "confidence": 0.90,
    "evidence_quote": "Sandy Point State Park is open year-round from 6:00 a.m. to sunset"
  }
}

EXAMPLE 3 (municipal code — no operating hours mentioned, just animal rules):
Source text: "It shall be unlawful for any person to permit any dog to be at large within the corporate limits of the Town. Dogs shall be kept on leash at all times."
Output:
{"operating_hours": null}

EXAMPLE 4 (seasonal park closure):
Source text: "Wickes Beach is open April 1 through October 31. The refuge is closed to the public during the winter waterfowl management season."
Output:
{
  "operating_hours": {
    "default": "sunrise-sunset",
    "seasonal": [
      {"from": "11-01", "to": "03-31", "hours": "closed"}
    ],
    "notes": "Closed Nov 1 – Mar 31 for winter waterfowl management. Open Apr 1 – Oct 31 default dawn-to-dusk (inferred from refuge convention; source doesn't specify exact daily hours).",
    "confidence": 0.75,
    "evidence_quote": "Wickes Beach is open April 1 through October 31"
  }
}

EXAMPLE 5 (Ocean City — seasonal prohibition, NOT operating hours):
Source text: "Allow a dog to be on the public beaches or boardwalk of the municipality during the period from May 1 through September 30 of each year."
Output:
{"operating_hours": null}
(The beach itself is open year-round; the May 1 – Sep 30 rule is a DOG-ACCESS rule, not an operating-hours rule. Operating hours unchanged.)

EXAMPLE 6 (Worcester Co Rec & Parks posted swim-beach):
Source text: "Public Landing Beach: Open Memorial Day weekend through Labor Day, 8:00 a.m. to dusk. Lifeguards on duty during posted summer season."
Output:
{
  "operating_hours": {
    "default": "closed",
    "seasonal": [
      {"from": "05-25", "to": "09-05", "hours": "08:00-sunset"}
    ],
    "notes": "Lifeguarded swim beach only operates Memorial Day–Labor Day; outside that window the swim beach proper is closed (though informal beach access may persist).",
    "confidence": 0.80,
    "evidence_quote": "Open Memorial Day weekend through Labor Day, 8:00 a.m. to dusk"
  }
}

RULES:
1. OPERATING HOURS = when the beach is OPEN TO THE PUBLIC. Not when dogs are allowed (that's a separate extractor).
2. evidence_quote MUST be a verbatim substring of source text.
3. If the source only describes dog rules / leash requirements / behavioral restrictions with no operating-hours mention, return {"operating_hours": null}.
4. "Sunrise-sunset" / "dawn-to-dusk" are valid default specs.
5. Seasonal carve-outs use "MM-DD" with explicit fixed dates (Memorial Day = 05-25 approx; Labor Day = 09-05 approx — round to nearest plausible weekend; mention the holiday in notes).
6. confidence < 0.7 means human review needed; explain in notes.
7. Sub-area scope (e.g., "swim beach only", "north of 29th street") → flag in notes; the populator will treat the operating hours as applying to the beach overall but the rendered copy should hedge.
8. Output ONLY the JSON object. No prose, no markdown fences.
"""

_PROMPT_SHA = hashlib.sha256(PROMPT.encode()).hexdigest()[:8]
CACHE_KEY = f"oh:{MODEL}:{_PROMPT_SHA}"

# ─── LLM wrapper ─────────────────────────────────────────────────────

def anthropic_call(input_blob: dict) -> dict:
    """Thin wrapper around common.llm.call_json that flattens to this
    script's expected dict shape."""
    r = call_json(PROMPT, input_blob, model=MODEL, max_tokens=1024)
    parsed = r["parsed"]
    return {
        "operating_hours": parsed.get("operating_hours"),
        "input_tokens":  r["input_tokens"],
        "output_tokens": r["output_tokens"],
        "cache_read_input_tokens": r["cache_read_input_tokens"],
    }


# ─── DB read / write ─────────────────────────────────────────────────

def fetch_ps_rows(ps_ids: list[int] | None, state: str | None,
                  pilot: int | None) -> list[dict]:
    """For a state, return ps rows that have ≥1 BPS attaching to active
    scoreable beaches in that state. For --ps-ids, return exactly those."""
    if ps_ids:
        params = {"id": f"in.({','.join(str(i) for i in ps_ids)})",
                  "select": "id,citation,full_text"}
        return supa("/rest/v1/policy_source", params=params)
    assert state, "must supply --state OR --ps-ids"
    # PostgREST can't do a JOIN-with-distinct cleanly; use the RPC OR
    # raw SQL via the management API. Simpler: fetch all BPS for state's
    # beaches → distinct ps_ids → batch-fetch ps rows.
    bps_params = {
        "select": "policy_source_id,beaches_gold!inner(state,is_active,scoring_tier)",
        "beaches_gold.state": f"eq.{state}",
        "beaches_gold.is_active": "is.true",
        "beaches_gold.scoring_tier": f"in.(daily,hourly)",
    }
    bps_rows = supa("/rest/v1/beach_policy_source", params=bps_params)
    ps_id_set = sorted({r["policy_source_id"] for r in bps_rows if r.get("policy_source_id")})
    if not ps_id_set:
        return []
    if pilot:
        ps_id_set = ps_id_set[:pilot]
    out: list[dict] = []
    # batch fetch (PostgREST in.(...) URL length ceiling — chunk by 100)
    for i in range(0, len(ps_id_set), 100):
        chunk = ps_id_set[i:i + 100]
        params = {
            "id": f"in.({','.join(str(x) for x in chunk)})",
            "select": "id,citation,full_text",
        }
        out.extend(supa("/rest/v1/policy_source", params=params))
    return out


def fetch_beach_fids_for_ps(ps_id: int) -> list[int]:
    """Beaches that this ps attaches to (via BPS)."""
    params = {
        "policy_source_id": f"eq.{ps_id}",
        "select": "beach_fid",
    }
    rows = supa("/rest/v1/beach_policy_source", params=params)
    return sorted({r["beach_fid"] for r in rows if r.get("beach_fid")})


def cache_lookup(ps_id: int) -> dict | None:
    """Check if we've already extracted this ps under this prompt+model."""
    params = {
        "policy_source_id": f"eq.{ps_id}",
        "cache_key": f"eq.{CACHE_KEY}",
        "select": "result_json",
        "limit": "1",
    }
    try:
        rows = supa("/rest/v1/policy_source_extraction_cache", params=params)
        return rows[0]["result_json"] if rows else None
    except urllib.error.HTTPError as e:
        # Table doesn't exist → no cache available; first-run path
        if e.code == 404:
            return None
        raise


def cache_write(ps_id: int, result_json: dict) -> None:
    try:
        supa(
            "/rest/v1/policy_source_extraction_cache",
            method="POST",
            body={
                "policy_source_id": ps_id,
                "cache_key": CACHE_KEY,
                "result_json": result_json,
            },
        )
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  warn: cache table not present; extraction will re-run next time", flush=True)
        elif e.code == 409:
            pass  # conflict = already cached, fine
        else:
            raise


def update_beach_operating_hours(beach_fids: list[int], hours: dict,
                                  confidence: float, ps_id: int) -> int:
    """Update beaches_gold.operating_hours for the given fids.
    Skip beaches where source IN ('manual_curator', 'operator') — those
    are higher priority than agency-extracted.
    """
    if not beach_fids:
        return 0
    n_updated = 0
    payload = {
        "operating_hours": json.dumps(hours),
        "operating_hours_source": "agency",
        "operating_hours_confidence": round(confidence, 2),
        "operating_hours_last_verified": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    for fid in beach_fids:
        params = {
            "fid": f"eq.{fid}",
            # don't overwrite manual_curator or operator
            "or": "(operating_hours_source.is.null,operating_hours_source.in.(osm,inferred,agency))",
            "select": "fid",
        }
        try:
            rows = supa(
                f"/rest/v1/beaches_gold?{urllib.parse.urlencode(params)}",
                method="PATCH",
                body=payload,
            )
            n_updated += len(rows)
        except urllib.error.HTTPError as e:
            print(f"  warn: fid={fid} update failed: {e.code}", flush=True)
    return n_updated


# ─── Per-ps extraction ───────────────────────────────────────────────

def extract_one_ps(ps: dict, *, refresh: bool, dry_run: bool) -> dict:
    """Returns counts dict."""
    pid = ps["id"]
    citation = (ps.get("citation") or "")[:80]
    full_text = ps.get("full_text") or ""

    if not full_text.strip():
        return {"ps_id": pid, "status": "no_full_text", "beaches_updated": 0}

    # Cache check
    if not refresh:
        cached = cache_lookup(pid)
        if cached is not None:
            oh = cached.get("operating_hours")
            if oh is None:
                return {"ps_id": pid, "status": "cached_no_hours", "beaches_updated": 0}
            if not dry_run:
                fids = fetch_beach_fids_for_ps(pid)
                n = update_beach_operating_hours(
                    fids, oh, oh.get("confidence", 0.7), pid
                )
                return {"ps_id": pid, "status": "cached_applied",
                        "beaches_updated": n, "fids": len(fids)}
            return {"ps_id": pid, "status": "cached_dry_run", "beaches_updated": 0}

    # Live extraction
    input_blob = {
        "policy_source_id": pid,
        "citation":         citation,
        "full_text":        full_text[:8000],  # cap to keep cost bounded
    }
    try:
        result = anthropic_call(input_blob)
    except Exception as e:
        return {"ps_id": pid, "status": f"llm_error:{type(e).__name__}",
                "beaches_updated": 0, "error": str(e)[:200]}

    oh = result["operating_hours"]
    in_toks  = result["input_tokens"]
    out_toks = result["output_tokens"]

    if dry_run:
        print(f"  ps={pid} {citation} → {('hours' if oh else 'no_hours')} "
              f"(in={in_toks} out={out_toks})", flush=True)
        return {"ps_id": pid, "status": "dry_run", "beaches_updated": 0,
                "in_tokens": in_toks, "out_tokens": out_toks}

    # Cache the result
    cache_write(pid, {"operating_hours": oh, "model": MODEL})

    if oh is None:
        return {"ps_id": pid, "status": "no_hours", "beaches_updated": 0,
                "in_tokens": in_toks, "out_tokens": out_toks}

    # Apply to beaches
    fids = fetch_beach_fids_for_ps(pid)
    n = update_beach_operating_hours(fids, oh, oh.get("confidence", 0.7), pid)
    return {"ps_id": pid, "status": "applied",
            "beaches_updated": n, "fids": len(fids),
            "in_tokens": in_toks, "out_tokens": out_toks,
            "default": oh.get("default"), "n_seasonal": len(oh.get("seasonal") or [])}


# ─── CLI ─────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--state", help="2-letter state code (extracts all ps with BPS in state)")
    grp.add_argument("--ps-ids", help="comma-sep list of ps ids")
    ap.add_argument("--pilot", type=int, default=None,
                    help="cap to first N ps rows for testing")
    ap.add_argument("--refresh", action="store_true",
                    help="ignore cache, re-extract")
    ap.add_argument("--dry-run", action="store_true",
                    help="print decisions without writing to DB")
    args = ap.parse_args()

    ps_ids = None
    if args.ps_ids:
        ps_ids = [int(x.strip()) for x in args.ps_ids.split(",") if x.strip()]

    rows = fetch_ps_rows(ps_ids, args.state, args.pilot)
    print(f"Targets: {len(rows)} policy_source rows  "
          f"(prompt_sha={_PROMPT_SHA}, model={MODEL})", flush=True)

    summary = {
        "applied": 0, "no_hours": 0, "cached_applied": 0, "cached_no_hours": 0,
        "no_full_text": 0, "dry_run": 0, "errors": 0,
        "beaches_updated_total": 0, "in_tokens": 0, "out_tokens": 0,
    }

    t0 = time.time()
    for i, ps in enumerate(rows, 1):
        res = extract_one_ps(ps, refresh=args.refresh, dry_run=args.dry_run)
        if res["status"].startswith("llm_error"):
            summary["errors"] += 1
        else:
            summary[res["status"]] = summary.get(res["status"], 0) + 1
        summary["beaches_updated_total"] += res.get("beaches_updated", 0)
        summary["in_tokens"]  += res.get("in_tokens", 0)
        summary["out_tokens"] += res.get("out_tokens", 0)
        if i % 10 == 0 or i == len(rows):
            elapsed = time.time() - t0
            print(f"  [{i}/{len(rows)}] elapsed={elapsed:.0f}s  "
                  f"applied={summary['applied']} no_hours={summary['no_hours']} "
                  f"cached={summary['cached_applied']+summary['cached_no_hours']} "
                  f"err={summary['errors']}", flush=True)

    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    # rough cost: $3/MTok in + $15/MTok out for Sonnet 4.5 (excluding cache)
    est_cost = (summary["in_tokens"] * 3.0 + summary["out_tokens"] * 15.0) / 1_000_000
    print(f"  est_cost: ${est_cost:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
