"""park_url_leash_prompt_ab.py — A/B test for the park_url leash prompt.

C3 step 1: compare current vs proposed prompt on a curated candidate set
of beaches. Reads cached `raw_text` from `park_url_extractions` so we
don't refetch HTML. Calls Anthropic with both prompts, prints side-by-side
field-by-field diff.

No DB writes. Pure read + LLM + stdout.

Run:
  python scripts/one_off/park_url_leash_prompt_ab.py
"""
from __future__ import annotations
import io
import json
import os
import sys
import urllib.parse
from pathlib import Path

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

CLIENT = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
MODEL = "claude-sonnet-4-20250514"

# Candidate beach set
# - 5 with off_leash_flag=true and known sub-zone variation (suspect 'mixed' truth)
# - 5 with all-on-leash (regression check — should stay 'required')
# - 2 with no dogs at all (sanity: should stay null/required-irrelevant)
CANDIDATES = [
    # Beaches that SHOULD likely produce 'mixed' under v2:
    (8560, "Del Mar Dog Beach",       "expect_mixed"),
    (8635, "East Beach (Crissy)",      "expect_mixed"),
    (8894, "Harbor Beach",             "expect_mixed"),
    (9113, "Kings Beach",              "expect_mixed"),
    (9168, "Golden Gate Beach",        "expect_mixed"),
    # Beaches that should stay 'required' (regression-check):
    (8740, "Wildcat Beach",            "expect_unchanged"),
    (3407, "Garrapata State Beach",    "expect_unchanged"),
    (8236, "Stinson Beach",            "expect_unchanged"),
    (107,  "Dan Blocker Beach",        "expect_unchanged"),
    (5940, "Dockweiler State Beach",   "expect_unchanged"),
]


# ============================================================================
# Old prompt (verbatim from extract_from_park_url.py)
# ============================================================================
PROMPT_V1 = """\
You are extracting structured beach metadata from a park or beach's official webpage.

Return ONLY a valid JSON object with exactly these keys (use null when not stated on the page):

{
  "dogs_allowed":           "yes" | "no" | "seasonal" | "restricted" | "unknown" | null,
  "dogs_leash_required":    "required" | "off_leash_ok" | "mixed" | null,
  "dogs_restricted_hours":  [{"start":"HH:MM","end":"HH:MM"}] | null,
  "dogs_seasonal_rules":    [{"from":"MM-DD","to":"MM-DD","notes":string}] | null,
  "dogs_zone_description":  string | null,
  "dogs_policy_notes":      string | null,
  "extraction_confidence":  number 0.00-1.00,
  "extraction_notes":       string | null
}

Rules:
- Extract ONLY what is explicitly stated on the page. Do not infer or guess.
- "seasonal" for dogs_allowed = allowed at some times of year and not others.
- "restricted" = allowed with notable rules (specific zones, time windows, leash mandates beyond standard).
- For dogs_restricted_hours: hours when dogs are NOT allowed (the off-window).
- For dogs_seasonal_rules: explicit date ranges with different rules.
- extraction_confidence: 0.95 for clear structured "DOG RULES:" sections; 0.75 for inferred from prose; 0.50 for partial; lower if ambiguous.
- Reply with raw JSON only — no markdown fences, no preamble.
"""


# ============================================================================
# New prompt — adds explicit guidance for dogs_leash_required = "mixed"
# ============================================================================
PROMPT_V2 = """\
You are extracting structured beach metadata from a park or beach's official webpage.

Return ONLY a valid JSON object with exactly these keys (use null when not stated on the page):

{
  "dogs_allowed":           "yes" | "no" | "seasonal" | "restricted" | "unknown" | null,
  "dogs_leash_required":    "required" | "off_leash_ok" | "mixed" | null,
  "dogs_restricted_hours":  [{"start":"HH:MM","end":"HH:MM"}] | null,
  "dogs_seasonal_rules":    [{"from":"MM-DD","to":"MM-DD","notes":string}] | null,
  "dogs_zone_description":  string | null,
  "dogs_policy_notes":      string | null,
  "extraction_confidence":  number 0.00-1.00,
  "extraction_notes":       string | null
}

Rules:
- Extract ONLY what is explicitly stated on the page. Do not infer or guess.
- "seasonal" for dogs_allowed = allowed at some times of year and not others.
- "restricted" = allowed with notable rules (specific zones, time windows, leash mandates beyond standard).
- For dogs_restricted_hours: hours when dogs are NOT allowed (the off-window).
- For dogs_seasonal_rules: explicit date ranges with different rules.
- extraction_confidence: 0.95 for clear structured "DOG RULES:" sections; 0.75 for inferred from prose; 0.50 for partial; lower if ambiguous.

dogs_leash_required guidance (NEW):
- "required" — leash policy is uniform on-leash across the entire described area.
- "off_leash_ok" — entire described area is off-leash (a designated off-leash dog beach).
- "mixed" — the page describes section-aware leash variation. Use when ANY of:
    * a designated off-leash zone or strip exists within an otherwise on-leash beach
    * different sections (sand vs. trails vs. parking) have explicitly different leash rules
    * time-of-day variation flips leash status (e.g., off-leash before 9am, on-leash otherwise)
    * the page distinguishes "swimming area" leash rule vs. "beach" leash rule
- null — page does not address leash policy at all.

DO NOT use "mixed" for general "leash required at all times" + "service animals exempt" — that's still "required".

CRITICAL — dogs_allowed and dogs_leash_required are INDEPENDENT dimensions:
- dogs_allowed is about WHERE dogs are allowed (zones / times / blanket prohibition).
- dogs_leash_required is about THE LEASH RULE within the dog-allowed zones.
- A beach can be dogs_allowed="restricted" AND dogs_leash_required="required" —
  restriction is on WHERE, leash rule is uniform on-leash within the allowed zones.
- Use dogs_allowed="restricted" when SOME areas allow dogs and OTHER areas do NOT
  (e.g., "Dogs allowed on North Beach but not on the playground/tot lot",
  "Dogs prohibited in nesting zone but allowed on rest of beach").
- DO NOT downgrade dogs_allowed from "restricted" to "yes" just because the leash
  field captures section-aware leash rules. Section-aware allow/deny still belongs
  in dogs_allowed.

dogs_restricted_hours guidance (NEW): this field is the ONLY place time-of-day
rules live. Surface ANY time-based flip, not just full prohibition windows:
- "leashes optional before 9am" → emit window 09:00-end of day as "leash window"
- "off-leash before 8am or after 6pm" → emit two windows when leash IS required
- "dogs prohibited 10am-6pm in summer" → emit a prohibition window
- Page silent on time-rules → keep null.
The dogs_restricted_hours array captures windows when dogs are NOT allowed OR
when leash status differs from the default. Document in dogs_policy_notes.

has_lifeguards detection (NEW): set true when page mentions ANY of:
- "lifeguard tower" / "lifeguards on duty" / "lifeguard patrol" / "lifeguard service"
- staffed seasonal hours (e.g., "lifeguards Memorial Day to Labor Day")
- "supervised swimming" / "patrol hours" / "ocean rescue"
- a season pass mentioning lifeguard fees
Set false ONLY if explicitly stated absent (e.g., "no lifeguard on duty",
"swim at your own risk — unguarded beach"). Set null when the page doesn't
mention lifeguards at all.

Reply with raw JSON only — no markdown fences, no preamble.
"""


def fetch_pages():
    """Pull cached raw_text per candidate fid."""
    fids = [c[0] for c in CANDIDATES]
    pages = {}
    with psycopg2.connect(**PG) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            select arena_group_id as fid, raw_text, length(raw_text) as len, source_url
              from public.park_url_extractions
             where arena_group_id = any(%s) and raw_text is not null
             order by arena_group_id, length(raw_text) desc
        """, (fids,))
        for row in cur.fetchall():
            if row["fid"] not in pages:
                pages[row["fid"]] = row
    return pages


def call_prompt(prompt: str, page_text: str) -> dict:
    """Single LLM call. Returns parsed JSON or {'error': ...} dict."""
    try:
        msg = CLIENT.messages.create(
            model=MODEL, max_tokens=1500, temperature=0,
            system=prompt,
            messages=[{"role": "user", "content": page_text}],
        )
        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.startswith("json"): text = text[4:]
        return json.loads(text)
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}


def main():
    pages = fetch_pages()
    print(f"Pulled cached raw_text for {len(pages)} of {len(CANDIDATES)} candidates")
    print(f"Model: {MODEL}\n")

    def _b(v):
        if v is True:  return "T"
        if v is False: return "F"
        return "·"
    def _h(v):  # restricted_hours summary
        if not v: return "·"
        return f"[{len(v)}]"

    print(f"{'fid':>5} {'name':<26} {'expect':<14} "
          f"{'v1 leash':<11} {'v2 leash':<11} "
          f"{'v1 allow':<10} {'v2 allow':<10} "
          f"{'lg':<4} {'hrs':<4} flip?")
    print("-" * 140)

    flips = 0
    runs = 0
    results = []
    for fid, name, expect in CANDIDATES:
        page = pages.get(fid)
        if not page:
            print(f"{fid:>5} {name[:26]:<26} {expect:<14} (no cached page)")
            continue
        v1 = call_prompt(PROMPT_V1, page["raw_text"])
        v2 = call_prompt(PROMPT_V2, page["raw_text"])
        runs += 1
        results.append((fid, name, v1, v2))
        v1_leash = v1.get("dogs_leash_required") or "—"
        v2_leash = v2.get("dogs_leash_required") or "—"
        v1_allow = v1.get("dogs_allowed") or "—"
        v2_allow = v2.get("dogs_allowed") or "—"
        leash_flip = v1_leash != v2_leash
        allow_flip = v1_allow != v2_allow
        lg_diff = _b(v1.get("has_lifeguards")) + ">" + _b(v2.get("has_lifeguards"))
        hrs_diff = _h(v1.get("dogs_restricted_hours")) + ">" + _h(v2.get("dogs_restricted_hours"))
        flag = ("LEASH " if leash_flip else "") + ("ALLOW" if allow_flip else "")
        if leash_flip or allow_flip: flips += 1
        print(f"{fid:>5} {name[:26]:<26} {expect:<14} "
              f"{v1_leash:<11} {v2_leash:<11} "
              f"{v1_allow:<10} {v2_allow:<10} "
              f"{lg_diff:<4} {hrs_diff:<4} {flag}")

    # Aggregate: lifeguard recovery rate
    lg_v1_set = sum(1 for _,_,v1,_ in results if v1.get("has_lifeguards") is not None)
    lg_v2_set = sum(1 for _,_,_,v2 in results if v2.get("has_lifeguards") is not None)
    hr_v1_set = sum(1 for _,_,v1,_ in results if v1.get("dogs_restricted_hours"))
    hr_v2_set = sum(1 for _,_,_,v2 in results if v2.get("dogs_restricted_hours"))
    print()
    print(f"Total runs: {runs} (= {runs*2} LLM calls). Flips: {flips}/{runs}.")
    print(f"has_lifeguards conclusive (non-null):       v1={lg_v1_set}/{runs}  v2={lg_v2_set}/{runs}")
    print(f"dogs_restricted_hours non-empty arrays:     v1={hr_v1_set}/{runs}  v2={hr_v2_set}/{runs}")
    print()

    # Per-row detail dump for spot-check on flips
    print("=" * 80)
    print("DETAIL DUMPS for fids where v1 != v2 (spot-check):")
    print("=" * 80)
    for fid, name, expect in CANDIDATES:
        page = pages.get(fid)
        if not page: continue
        v1 = call_prompt(PROMPT_V1, page["raw_text"])
        v2 = call_prompt(PROMPT_V2, page["raw_text"])
        if (v1.get("dogs_leash_required") != v2.get("dogs_leash_required")
            or v1.get("dogs_allowed") != v2.get("dogs_allowed")):
            print(f"\n--- fid {fid} {name} ({page['source_url']}) ---")
            print(f"V1: leash={v1.get('dogs_leash_required')!r} allowed={v1.get('dogs_allowed')!r}")
            print(f"    notes: {(v1.get('dogs_policy_notes') or '')[:200]}")
            print(f"V2: leash={v2.get('dogs_leash_required')!r} allowed={v2.get('dogs_allowed')!r}")
            print(f"    notes: {(v2.get('dogs_policy_notes') or '')[:200]}")
            print(f"    zone:  {(v2.get('dogs_zone_description') or '')[:200]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
