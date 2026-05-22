"""Extract structured temporal carve-outs from policy_source text via Sonnet.

Reads each `policy_source` row's full_text + linked beach_policy_source
rows' evidence_verbatim + status_note + rule_modifier. Asks Sonnet to
identify seasonal + daily-window carve-outs. Writes structured rows to
beach_policy_source_temporal per [[consensus-source-authority]] phase H2
(see docs/consensus_phase_h_temporal_spec.md).

The temporal triggers fire automatically on INSERT — flat columns +
zone_rules cascade through the existing chain (H3 + H4 will tighten
how the structured fields surface in the rendered shape).

Usage:
  python scripts/extract_temporal_from_policy_source.py --ps-ids 5,17,52
  python scripts/extract_temporal_from_policy_source.py --state CA --pilot 10
  python scripts/extract_temporal_from_policy_source.py --state CA --all
  python scripts/extract_temporal_from_policy_source.py --refresh --ps-ids 5  # wipe + re-extract
  python scripts/extract_temporal_from_policy_source.py --dry-run --ps-ids 5

Default: skips ps rows that already have temporal rows (presence = extracted).
Pass --refresh to wipe existing temporal rows for the given ps and re-extract.

Cost: ~$0.005-0.01 per ps row via Sonnet 4.5. ~$2.50 to backfill ~250 CA ps rows.
"""

from __future__ import annotations

import sys
# Windows default stdout is cp1252; force UTF-8 so § / ↔ / em-dashes
# in policy citations don't crash the run.
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

# ─── Prompt ────────────────────────────────────────────────────────────

PROMPT = """You are a legal-citation parser specialized in dog/beach rules. Your job is to extract STRUCTURED temporal carve-outs from the text of a policy_source row (a codified ordinance, regulation, or operator-posted rule).

Input: a JSON blob with the policy_source's text + all linked beach quotes/notes/modifier.

Output: a JSON object with ONE field `windows` — an array of structured window objects. Each describes ONE seasonal or daily carve-out present in the source text.

Window object schema:
{
  "exception_rule":       "not_allowed" | "on_leash" | "off_leash" | "off_leash_voice_control",
  "window_kind":          "seasonal" | "daily" | "seasonal_and_daily",
  "effective_from_md":    "MM-DD" or null,    // seasonal start, year-agnostic
  "effective_to_md":      "MM-DD" or null,    // seasonal end, year-agnostic
  "anchor_start":         <see anchors below> or null,
  "anchor_end":           <see anchors below> or null,
  "daily_start":          "HH:MM" or null,    // 24h time
  "daily_end":            "HH:MM" or null,    // 24h time (end<start = wrap-around)
  "season_label":         "Summer" or "Off-leash season" or null,  // human-friendly
  "confidence":           0.0..1.0,
  "evidence_quote":       "verbatim snippet from the source that this came from",
  "notes":                null or string for ambiguity / human-review flags
}

Allowed anchor values (case-sensitive):
  Date anchors (seasonal windows only):
    "labor_day", "labor_day_after", "memorial_day", "memorial_day_after",
    "mlk_day", "presidents_day", "plover_open" (Sep 16), "plover_close" (Mar 15),
    "dst_start", "dst_end"
  Time anchors (daily windows only):
    "dawn", "dusk"

Window-kind rules:
  - "seasonal": date range only (no daily times). Either effective_from_md/_to_md OR date anchors.
  - "daily": hour range only, year-round. Either daily_start/_end OR dawn/dusk anchors.
  - "seasonal_and_daily": BOTH a date range AND a time-of-day window apply.

Examples:

EXAMPLE 1 (Del Mar §4.08.020(B)(1) — compound carve-out):
Source text: "Dogs may be off leash on the beaches or waters located northerly of a line being an extension of 29th Street from the day after Labor Day through June 15th, and from dawn to 8:00 a.m. year-round."
Output:
{
  "windows": [
    {
      "exception_rule": "off_leash_voice_control",
      "window_kind": "seasonal",
      "anchor_start": "labor_day_after",
      "effective_to_md": "06-15",
      "season_label": "Off-leash season (Labor Day +1 → Jun 15)",
      "confidence": 0.95,
      "evidence_quote": "from the day after Labor Day through June 15th",
      "notes": null
    },
    {
      "exception_rule": "off_leash_voice_control",
      "window_kind": "daily",
      "anchor_start": "dawn",
      "daily_end": "08:00",
      "season_label": "Dawn to 8 AM (year-round)",
      "confidence": 0.95,
      "evidence_quote": "from dawn to 8:00 a.m. year-round",
      "notes": null
    }
  ]
}

EXAMPLE 2 (snowy plover seasonal closure — common pattern):
Source text: "Dogs are prohibited on the beach from March 15 through September 15 to protect western snowy plover nesting habitat."
Output:
{
  "windows": [
    {
      "exception_rule": "not_allowed",
      "window_kind": "seasonal",
      "anchor_start": "plover_close",
      "anchor_end": "plover_open",
      "season_label": "Snowy plover nesting closure (Mar 15 – Sep 15)",
      "confidence": 0.95,
      "evidence_quote": "Dogs are prohibited on the beach from March 15 through September 15",
      "notes": "Federal/state-coordinated closure; check OR + CA south-coast beaches"
    }
  ]
}

EXAMPLE 3 (Rosie's Dog Beach posted hours):
Source text: "Rosie's Dog Beach is open 6:00 a.m. to 8:00 p.m. daily. Dogs must be under visual and voice control at all times."
Output:
{
  "windows": [
    {
      "exception_rule": "off_leash_voice_control",
      "window_kind": "daily",
      "daily_start": "06:00",
      "daily_end": "20:00",
      "season_label": "Posted hours (6 AM – 8 PM)",
      "confidence": 0.9,
      "evidence_quote": "Rosie's Dog Beach is open 6:00 a.m. to 8:00 p.m. daily",
      "notes": null
    }
  ]
}

EXAMPLE 4 (Long Beach §6.16.310 dog exercise area):
Source text: "The dog exercise area shall be open from 6:00 a.m. to 8:00 p.m."
Output:
{
  "windows": [
    {
      "exception_rule": "off_leash",
      "window_kind": "daily",
      "daily_start": "06:00",
      "daily_end": "20:00",
      "season_label": "Open hours",
      "confidence": 0.9,
      "evidence_quote": "open from 6:00 a.m. to 8:00 p.m.",
      "notes": null
    }
  ]
}

EXAMPLE 5 (Del Mar §4.08.020(C) — summer prohibition):
Source text: "Dogs are prohibited on the beach between the Powerhouse Park north end and the 29th Street line during the period of June 15th through Labor Day."
Output:
{
  "windows": [
    {
      "exception_rule": "not_allowed",
      "window_kind": "seasonal",
      "effective_from_md": "06-15",
      "anchor_end": "labor_day",
      "season_label": "Summer prohibition",
      "confidence": 0.95,
      "evidence_quote": "during the period of June 15th through Labor Day",
      "notes": "Sub-area scope (Powerhouse Park north end to 29th St) not captured here — encode via separate section if needed"
    }
  ]
}

NO-WINDOW CASE — if the source text describes ONLY a year-round rule with no seasonal or daily carve-outs (e.g., "Dogs prohibited at all times" / "6-foot leash required" with no time/season qualifiers), return:
{"windows": []}

EXAMPLE 6 (no temporal carve-out):
Source text: "It shall be unlawful for any person to permit any dog to be on the public beach, leashed or unleashed."
Output:
{"windows": []}

RULES:
1. ONLY emit windows where the source text gives a CONCRETE temporal qualifier. Don't infer "probably summer". If the text doesn't say it, defer.
2. evidence_quote MUST be a verbatim substring of the source text (with light trimming OK). Don't paraphrase.
3. Use date anchors (labor_day etc.) rather than literal MM-DD when the source uses the named date.
4. dawn/dusk are valid for daily windows only.
5. confidence < 0.7 means human review needed; explain in notes.
6. If the source mentions a sub-area scope ("between X and Y streets", "Lot F, Tract 10624"), note it in `notes` — sub-area scope is handled separately via beach_policy_source.section values.
7. Common false-friends to ignore:
   - "seasonal lifeguards" — about lifeguard staffing, not dog rules
   - "the dog park is currently undergoing renovation" — operational, not regulatory
   - Ordinance adoption dates ("Ord. 2024-15, adopted Sep 3, 2024") — metadata, not rule
8. Output ONLY the JSON. No prose. No markdown fences. No commentary.
"""

# ─── HTTP helpers ──────────────────────────────────────────────────────

def anthropic_call(prompt: str, input_blob: dict) -> dict:
    """Thin wrapper around common.llm.call_json that preserves the
    `windows` + usage-counts return shape this script's callers expect."""
    r = call_json(prompt, input_blob, model=MODEL, max_tokens=2048)
    parsed = r["parsed"]
    return {
        "windows":                 parsed.get("windows", []),
        "input_tokens":            r["input_tokens"],
        "output_tokens":           r["output_tokens"],
        "cache_read_input_tokens": r["cache_read_input_tokens"],
    }


# ─── DB read / write ───────────────────────────────────────────────────

def fetch_ps_rows(ps_ids: list[int] | None, state: str | None,
                  pilot: int | None) -> list[dict]:
    """Pull policy_source rows + their linked beach_policy_source rows."""
    params: dict = {
        "select": "id,subtype,citation,full_text,issuing_agency_id",
        "order": "id.asc",
    }
    if ps_ids:
        params["id"] = "in.(" + ",".join(str(i) for i in ps_ids) + ")"
    if pilot:
        params["limit"] = str(pilot)
    ps_rows = supa("/rest/v1/policy_source", params=params)
    if not ps_rows:
        return []
    if state:
        # filter to ps rows linked to ≥1 beach in this state
        ids = [r["id"] for r in ps_rows]
        beach_states = supa(
            "/rest/v1/beach_policy_source",
            params={
                "select": "policy_source_id,beach_fid,beaches_gold(state)",
                "policy_source_id": "in.(" + ",".join(str(i) for i in ids) + ")",
            },
        )
        ok = {r["policy_source_id"] for r in beach_states
              if (r.get("beaches_gold") or {}).get("state") == state}
        ps_rows = [r for r in ps_rows if r["id"] in ok]
    return ps_rows


def fetch_bps_for_ps(ps_id: int) -> list[dict]:
    """All beach_policy_source rows linked to this ps + their bps content."""
    return supa("/rest/v1/beach_policy_source", params={
        "select": "beach_fid,policy_source_id,section,rule,evidence_verbatim,status_note,rule_modifier",
        "policy_source_id": f"eq.{ps_id}",
    })


def already_has_temporal(ps_id: int) -> bool:
    """True if ≥1 temporal row exists for any bps linked to this ps."""
    rows = supa("/rest/v1/beach_policy_source_temporal", params={
        "select": "id",
        "policy_source_id": f"eq.{ps_id}",
        "limit": "1",
    })
    return len(rows) > 0


def wipe_temporal_for_ps(ps_id: int) -> int:
    """Delete existing temporal rows for this ps (--refresh path)."""
    rows = supa("/rest/v1/beach_policy_source_temporal", params={
        "select": "id",
        "policy_source_id": f"eq.{ps_id}",
    })
    if not rows:
        return 0
    for r in rows:
        supa(f"/rest/v1/beach_policy_source_temporal", method="DELETE",
             params={"id": f"eq.{r['id']}"})
    return len(rows)


def write_temporal_rows(bps_targets: list[dict], windows: list[dict],
                        extracted_via: str) -> int:
    """For each (bps target × window), INSERT one temporal row."""
    if not windows:
        return 0
    inserts = []
    for bps in bps_targets:
        for w in windows:
            row = {
                "beach_fid":        bps["beach_fid"],
                "policy_source_id": bps["policy_source_id"],
                "section":          bps["section"],
                "exception_rule":   w["exception_rule"],
                "window_kind":      w["window_kind"],
                "effective_from_md": w.get("effective_from_md"),
                "effective_to_md":   w.get("effective_to_md"),
                "anchor_start":      w.get("anchor_start"),
                "anchor_end":        w.get("anchor_end"),
                "daily_start":       w.get("daily_start"),
                "daily_end":         w.get("daily_end"),
                "season_label":      w.get("season_label"),
                "extracted_via":     extracted_via,
                "extractor_confidence": w.get("confidence"),
                "notes":             w.get("notes"),
            }
            inserts.append({k: v for k, v in row.items() if v is not None or k in
                            ("effective_from_md", "effective_to_md", "anchor_start",
                             "anchor_end", "daily_start", "daily_end",
                             "season_label", "notes")})
    if not inserts:
        return 0
    # Insert one at a time so a single conflict (unique-index dup or
    # CHECK violation from a model-output edge case) doesn't kill the
    # whole batch. Errors get logged + counted; the run continues.
    n_ok = 0
    for row in inserts:
        try:
            supa("/rest/v1/beach_policy_source_temporal", method="POST", body=row)
            n_ok += 1
        except urllib.error.HTTPError as e:
            body_excerpt = ""
            try:
                body_excerpt = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                pass
            print(f"      WARN: insert failed for fid={row.get('beach_fid')} "
                  f"ps={row.get('policy_source_id')} window_kind={row.get('window_kind')} "
                  f"→ HTTP {e.code}: {body_excerpt}")
    return n_ok


# ─── Per-ps extraction ─────────────────────────────────────────────────

def build_input_blob(ps: dict, bps_rows: list[dict]) -> dict:
    """Pack ps + per-bps content into a tight blob for Sonnet."""
    return {
        "policy_source": {
            "subtype":   ps.get("subtype"),
            "citation":  ps.get("citation"),
            "full_text": ps.get("full_text"),
        },
        "beach_policy_source_rows": [
            {
                "beach_fid":         r.get("beach_fid"),
                "section":           r.get("section"),
                "rule":              r.get("rule"),
                "evidence_verbatim": r.get("evidence_verbatim"),
                "status_note":       r.get("status_note"),
                "rule_modifier":     r.get("rule_modifier"),
            }
            for r in bps_rows
        ],
    }


def extract_for_ps(ps: dict, refresh: bool, dry_run: bool) -> dict:
    """Run extraction on one ps row. Returns stats."""
    pid = ps["id"]
    bps_rows = fetch_bps_for_ps(pid)
    if not bps_rows:
        return {"pid": pid, "status": "no-bps", "windows": 0, "in": 0, "out": 0, "cost": 0.0}

    if refresh:
        wiped = wipe_temporal_for_ps(pid)
        print(f"      wiped {wiped} existing temporal rows for ps={pid}")
    elif already_has_temporal(pid):
        return {"pid": pid, "status": "skip-cached", "windows": 0, "in": 0, "out": 0, "cost": 0.0}

    blob = build_input_blob(ps, bps_rows)
    if dry_run:
        print(json.dumps(blob, indent=2)[:1500])
        return {"pid": pid, "status": "dry-run", "windows": 0, "in": 0, "out": 0, "cost": 0.0}

    try:
        out = anthropic_call(PROMPT, blob)
    except Exception as e:
        print(f"      ERROR: {e}", file=sys.stderr)
        return {"pid": pid, "status": "error", "windows": 0, "in": 0, "out": 0, "cost": 0.0}

    windows = out.get("windows", [])
    n_inserted = write_temporal_rows(bps_rows, windows, extracted_via=MODEL) if windows else 0

    # Sonnet 4.5 pricing (approximate): $3/Mt in, $15/Mt out, cache read $0.30/Mt
    in_t  = out.get("input_tokens", 0)
    out_t = out.get("output_tokens", 0)
    cache_r = out.get("cache_read_input_tokens", 0)
    cost = (in_t * 3.0 + out_t * 15.0 + cache_r * 0.30) / 1_000_000

    return {
        "pid": pid, "status": "ok" if windows else "ok-empty",
        "windows": len(windows), "rows": n_inserted,
        "in": in_t, "out": out_t, "cache_r": cache_r, "cost": cost,
    }


# ─── Main ──────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Extract temporal carve-outs from policy_source via Sonnet")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--ps-ids", help="comma-separated policy_source ids")
    g.add_argument("--state",  help="all ps rows linked to ≥1 beach in this state")
    g.add_argument("--all",    action="store_true", help="every ps row in the table")
    ap.add_argument("--pilot", type=int, help="cap to first N ps rows (use with --state/--all)")
    ap.add_argument("--refresh", action="store_true",
                    help="wipe existing temporal rows for the targeted ps before re-extracting")
    ap.add_argument("--dry-run", action="store_true",
                    help="build input blob + print; do NOT call Sonnet or write")
    args = ap.parse_args()

    if args.ps_ids:
        ps_ids = [int(x.strip()) for x in args.ps_ids.split(",") if x.strip()]
        ps_rows = fetch_ps_rows(ps_ids=ps_ids, state=None, pilot=None)
    elif args.state:
        ps_rows = fetch_ps_rows(ps_ids=None, state=args.state, pilot=args.pilot)
    else:
        ps_rows = fetch_ps_rows(ps_ids=None, state=None, pilot=args.pilot)

    if not ps_rows:
        print("No policy_source rows matched.", file=sys.stderr)
        return 0

    print(f"Targets: {len(ps_rows)} policy_source rows")
    totals = {"windows": 0, "rows": 0, "in": 0, "out": 0, "cache_r": 0, "cost": 0.0,
              "ok": 0, "ok_empty": 0, "skipped": 0, "errors": 0, "no_bps": 0}

    for i, ps in enumerate(ps_rows, 1):
        cite = (ps.get("citation") or "")[:60]
        print(f"  [{i}/{len(ps_rows)}] ps={ps['id']}  {cite}")
        st = extract_for_ps(ps, refresh=args.refresh, dry_run=args.dry_run)
        if st["status"] == "ok":
            totals["ok"] += 1
            print(f"      {st['windows']} window(s); {st.get('rows', 0)} temporal row(s) inserted; "
                  f"in={st['in']} out={st['out']} cache_r={st.get('cache_r', 0)} ${st['cost']:.4f}")
        elif st["status"] == "ok-empty":
            totals["ok_empty"] += 1
            print(f"      no temporal windows; in={st['in']} out={st['out']} ${st['cost']:.4f}")
        elif st["status"] == "skip-cached":
            totals["skipped"] += 1
            print(f"      skip (already has temporal rows; --refresh to redo)")
        elif st["status"] == "no-bps":
            totals["no_bps"] += 1
            print(f"      skip (no bps rows linked)")
        elif st["status"] == "error":
            totals["errors"] += 1
        elif st["status"] == "dry-run":
            pass

        totals["windows"] += st["windows"]
        totals["rows"]    += st.get("rows", 0)
        totals["in"]      += st["in"]
        totals["out"]     += st["out"]
        totals["cache_r"] += st.get("cache_r", 0)
        totals["cost"]    += st["cost"]

        time.sleep(0.2)  # gentle rate-limit

    print("\n=== TOTALS ===")
    print(f"  ok with windows:     {totals['ok']}")
    print(f"  ok empty (no carve): {totals['ok_empty']}")
    print(f"  skipped (cached):    {totals['skipped']}")
    print(f"  no bps:              {totals['no_bps']}")
    print(f"  errors:              {totals['errors']}")
    print(f"  windows total:       {totals['windows']}")
    print(f"  temporal rows:       {totals['rows']}")
    print(f"  tokens in / out:     {totals['in']} / {totals['out']}  (cache_r {totals['cache_r']})")
    print(f"  cost:                ${totals['cost']:.4f}")
    return 0 if totals["errors"] == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
