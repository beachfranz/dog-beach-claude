"""research_state_dogs_policy.py — LLM-produced baseline row for state_dogs_policy.

Closes the "preflight halts on missing state seed" gap for virgin states.
Pipeline's action_state_policy_seed calls this when the row is absent.

Strategy:
  - LLM (claude-sonnet-4-6) produces a structured row from training-data
    knowledge of the state's statewide leash/parks regulation.
  - confidence=0.4 + notes flag the row as auto-generated, needs operator
    review.
  - Honors [[state-dogs-policy-honest-framing]] — if the state has NO
    statewide leash law (only a state-park rule), prompt forces that
    truthfully rather than misattributing.

Usage:
  python scripts/research_state_dogs_policy.py --state DE                # print, no DB write
  python scripts/research_state_dogs_policy.py --state DE --apply        # insert row
  python scripts/research_state_dogs_policy.py --state DE --apply --overwrite

Exit codes:
  0 — produced a row (printed or inserted)
  1 — LLM call failed or returned malformed JSON
  2 — row already exists (use --overwrite to replace)
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path

# sys.path bootstrap — per HARD pin sys-path-bootstrap-for-common-imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from anthropic import Anthropic
from scripts.common.db import connect
from scripts.common.llm import SONNET

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# 50 states + DC
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

SYSTEM = (
    "You research U.S. state-level regulations governing dogs on PUBLIC BEACHES "
    "and IN PUBLIC PARKS. You output a single JSON object matching the schema "
    "given.\n\n"
    "RULES OF HONESTY (these are load-bearing):\n"
    "1. If the state has NO statewide leash law (e.g., MD's leash rules are "
    "   county-level only, not state-level), set has_statewide_law=false and "
    "   write source_quote / ordinance_ref / scope_notes to reflect that "
    "   truthfully. Do NOT misattribute a state-park-system regulation as a "
    "   statewide default.\n"
    "2. If the state DOES have a statewide law (e.g., DE Title 7 Ch. 17, "
    "   CA Coastal Act, OR Beach Bill, WA SMA), cite it precisely with the "
    "   real statute number.\n"
    "3. Coastal states often have a separate coastal/beach access regime "
    "   (OR Beach Bill, CA Coastal Act, WA SMA, MA Public Trust). If so, "
    "   note it in scope_notes — but the 'default' row is the broad "
    "   on-public-lands rule.\n"
    "4. dogs_allowed='yes' means dogs are generally permitted on the state's "
    "   public beaches (perhaps leashed); 'no' means generally prohibited; "
    "   'mixed' means it varies significantly by jurisdiction.\n"
    "5. has_on_leash=true means leashed dogs are explicitly allowed somewhere "
    "   on public beach/park land; has_off_leash=true means off-leash is "
    "   explicitly allowed in some designated areas.\n"
    "6. confidence stays 0.4 — this row is auto-generated and needs operator "
    "   review. Do NOT bump it higher.\n"
)


def build_user_prompt(state_code: str, state_name: str) -> str:
    return (
        f"Research {state_name} ({state_code}) statewide dogs policy for "
        f"public beaches + parks. Produce one JSON object:\n\n"
        f"{{\n"
        f'  "state_code": "{state_code}",\n'
        f'  "state_name": "{state_name}",\n'
        f'  "dogs_allowed": "yes" | "no" | "mixed",\n'
        f'  "default_rule": "yes" | "no" | "mixed",\n'
        f'  "has_on_leash": true | false,\n'
        f'  "has_off_leash": true | false,\n'
        f'  "has_statewide_law": true | false,\n'
        f'  "source_quote": "<verbatim quote from the statute/regulation '
        f'if has_statewide_law=true; explanation of absence if false>",\n'
        f'  "source_url": "<URL to the statute/regulation page if known, else null>",\n'
        f'  "ordinance_ref": "<statute citation, e.g. \'DE Code Title 7 Ch. 17\', '
        f'or \'(no statewide leash law; county/state-park rules apply)\'>",\n'
        f'  "scope_notes": "<2-4 sentences explaining scope: '
        f'statewide vs state-park-only, coastal-only carve-outs, '
        f'major county/city exceptions, seasonal closures>"\n'
        f"}}\n\n"
        f"Return ONLY the JSON. No prose before or after."
    )


def extract_json(raw: str) -> dict:
    """Strip code fences and parse."""
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[1] if "\n" in s else s
        if s.endswith("```"):
            s = s.rsplit("```", 1)[0]
        if s.startswith("json"):
            s = s.split("\n", 1)[1]
    return json.loads(s.strip())


def call_llm(state_code: str, state_name: str) -> dict:
    resp = ANTHROPIC.messages.create(
        model=SONNET,
        max_tokens=1500,
        system=SYSTEM,
        messages=[{"role": "user", "content": build_user_prompt(state_code, state_name)}],
    )
    raw = resp.content[0].text if resp.content else ""
    return extract_json(raw)


def validate(row: dict, state_code: str) -> None:
    required = {
        "state_code", "state_name", "dogs_allowed", "default_rule",
        "has_on_leash", "has_off_leash", "has_statewide_law",
        "source_quote", "ordinance_ref", "scope_notes",
    }
    missing = required - set(row.keys())
    if missing:
        raise ValueError(f"missing fields: {sorted(missing)}")
    if row["state_code"] != state_code:
        raise ValueError(f"state_code mismatch: got {row['state_code']!r}")
    for tri in ("dogs_allowed", "default_rule"):
        if row[tri] not in ("yes", "no", "mixed"):
            raise ValueError(f"{tri!r} must be yes|no|mixed; got {row[tri]!r}")
    for boo in ("has_on_leash", "has_off_leash", "has_statewide_law"):
        if not isinstance(row[boo], bool):
            raise ValueError(f"{boo!r} must be bool; got {row[boo]!r}")


def insert(row: dict, overwrite: bool) -> str:
    """INSERT (or UPDATE if --overwrite) one state_dogs_policy row.
    Returns 'inserted' | 'updated' | 'exists'.
    """
    notes = "auto-generated by research_state_dogs_policy.py; needs operator review."
    if not row["has_statewide_law"]:
        notes += " has_statewide_law=false: framed honestly per honest-framing pin."

    insert_sql = """
        insert into public.state_dogs_policy
          (state_code, state_name, county_fips_filter, scope_label,
           dogs_allowed, default_rule, has_on_leash, has_off_leash,
           source_quote, source_url, ordinance_ref, scope_notes,
           confidence, notes, scraped_at)
        values (%s, %s, NULL, 'default',
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                0.4, %s, now())
        on conflict (state_code, scope_label) do nothing
        returning state_code
    """
    upsert_sql = insert_sql.replace(
        "on conflict (state_code, scope_label) do nothing",
        "on conflict (state_code, scope_label) do update set "
        "  state_name=excluded.state_name, dogs_allowed=excluded.dogs_allowed, "
        "  default_rule=excluded.default_rule, has_on_leash=excluded.has_on_leash, "
        "  has_off_leash=excluded.has_off_leash, source_quote=excluded.source_quote, "
        "  source_url=excluded.source_url, ordinance_ref=excluded.ordinance_ref, "
        "  scope_notes=excluded.scope_notes, confidence=excluded.confidence, "
        "  notes=excluded.notes, scraped_at=excluded.scraped_at"
    )
    sql = upsert_sql if overwrite else insert_sql

    params = (
        row["state_code"], row["state_name"],
        row["dogs_allowed"], row["default_rule"],
        row["has_on_leash"], row["has_off_leash"],
        row["source_quote"], row.get("source_url"),
        row["ordinance_ref"], row["scope_notes"], notes,
    )
    c = connect()
    c.set_session(autocommit=True)
    try:
        with c.cursor() as cur:
            cur.execute(sql, params)
            returned = cur.fetchone()
            if returned is None:
                return "exists"
            return "updated" if overwrite else "inserted"
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="2-letter state code")
    ap.add_argument("--apply", action="store_true", help="insert into DB (default: print only)")
    ap.add_argument("--overwrite", action="store_true", help="UPDATE if row exists")
    args = ap.parse_args()

    state = args.state.upper()
    if state not in STATE_NAMES:
        print(f"ERR: unknown state code {state!r}", file=sys.stderr)
        return 1
    state_name = STATE_NAMES[state]

    print(f"researching state_dogs_policy for {state} ({state_name})...", file=sys.stderr)
    try:
        row = call_llm(state, state_name)
        validate(row, state)
    except json.JSONDecodeError as e:
        print(f"ERR: LLM returned malformed JSON: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERR: {e}", file=sys.stderr)
        return 1

    print(json.dumps(row, indent=2))

    if not args.apply:
        print(f"(dry-run; pass --apply to INSERT)", file=sys.stderr)
        return 0

    outcome = insert(row, overwrite=args.overwrite)
    print(f"row {outcome}: {state}", file=sys.stderr)
    if outcome == "exists":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
