"""
extract_operator_dogs_policy_v2.py — Phase DP-C codify-pattern rescue.

For city operators where the existing extract_operator_dogs_policy.py picker
rejected every candidate (Yelp / news / aggregator scope-mismatch), use the
codify substrate (smart_fetch + Playwright + web_search escalation) to find
the city's authoritative dog/leash policy page directly.

Default targets: 6 picker-rejected cities — SF (335), SJ (317), Sac (271),
Davis (193), Irvine (142), Santa Rosa (403).

Writes to operator_dogs_policy: policy_found, default_rule, leash_required,
summary, source_url, area_* fields, notes. Re-fires populate_from_city_dog_policy_gold
for affected beaches.

Usage:
  python scripts/extract_operator_dogs_policy_v2.py --dry-run   # default
  python scripts/extract_operator_dogs_policy_v2.py --apply --ids 335,271
  python scripts/extract_operator_dogs_policy_v2.py --apply     # all 6
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
import urllib.parse, urllib.request

import psycopg2.extras
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET
from scripts.extract_research_v2 import strip_html, name_match
from scripts.extract_per_beach_offleash_v2 import (
    smart_fetch, is_thin_or_blocked, is_url_deep_enough, authority_score,
)

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]


# ── Per-city candidate URL builders ─────────────────────────────────────

def candidates_for_operator(operator_id: int, name: str) -> list[dict]:
    """Per-city authoritative dog/leash policy URL candidates.
    Most cities have a dedicated dog/parks-rules page at a predictable slug."""
    cands: list[dict] = []
    if operator_id == 335:  # San Francisco
        cands += [
            {"url": "https://sfrecpark.org/954/Dogs-In-Parks", "platform": "sf_rec_dogs", "auth": 4},
            {"url": "https://sfrecpark.org/destinations/dog-play-areas", "platform": "sf_rec_dog_play_areas", "auth": 4},
            {"url": "https://www.sf.gov/topics/dogs-parks", "platform": "sfgov_dogs", "auth": 4},
        ]
    elif operator_id == 317:  # San Jose
        cands += [
            {"url": "https://www.sanjoseca.gov/your-government/departments-offices/parks-recreation-neighborhood-services/parks/dog-parks", "platform": "sj_dog_parks", "auth": 4},
            {"url": "https://www.sanjoseca.gov/your-government/appointed-bodies/animal-care-services/laws-and-regulations", "platform": "sj_animal", "auth": 4},
        ]
    elif operator_id == 271:  # Sacramento
        cands += [
            {"url": "https://www.cityofsacramento.gov/parks/parks-virtual-tour/parks/dog-parks", "platform": "sac_dog_parks", "auth": 4},
            {"url": "https://www.cityofsacramento.gov/community-response/animal-care-services/leash-laws", "platform": "sac_leash", "auth": 4},
        ]
    elif operator_id == 193:  # Davis
        cands += [
            {"url": "https://www.cityofdavis.org/city-hall/parks-and-community-services/parks-and-greenbelts/dog-parks", "platform": "davis_dog_parks", "auth": 4},
            {"url": "https://www.cityofdavis.org/residents/animal-services", "platform": "davis_animal", "auth": 4},
        ]
    elif operator_id == 142:  # Irvine
        cands += [
            {"url": "https://www.cityofirvine.org/parks-trails/parks-locator", "platform": "irvine_parks", "auth": 4},
            {"url": "https://library.municode.com/ca/irvine/codes/code_of_ordinances?nodeId=TIT4PUSA_DIV5ANRE", "platform": "irvine_municode", "auth": 4},
        ]
    elif operator_id == 403:  # Santa Rosa
        cands += [
            {"url": "https://srcity.org/2316/Dog-Parks", "platform": "santa_rosa_dog_parks", "auth": 4},
            {"url": "https://srcity.org/2192/Parks-Rules-Regulations", "platform": "santa_rosa_rules", "auth": 4},
        ]
    return cands


# ── LLM prompt — city-wide dog policy ───────────────────────────────────

PROMPT_SYSTEM = (
    "You are extracting the city-wide dog policy from a single authoritative "
    "source page on the city's official .gov domain. Use ONLY the provided "
    "source content. CRITICAL: extract the policy that applies to the WHOLE "
    "city (or all city parks). Do NOT invent details. If the source covers "
    "ONLY one park (not the whole city), set city_wide=false and explain "
    "what scope the source actually covers."
)

PROMPT_USER_TEMPLATE = """SOURCE PAGE CONTENT:
{content}

CITY OPERATOR: {name}

Extract the city-wide dog policy. Return JSON:
{{
  "policy_found": <true|false: does the source actually state a city-wide policy?>,
  "city_wide": <true|false: does the policy apply to the WHOLE city / all city parks?>,
  "scope_notes": <text explaining what scope is covered (e.g. "all city parks" / "dog parks only" / "Mission Bay only">,
  "default_rule": <"allowed"|"prohibited"|"restricted"|"seasonal"|null>,
  "leash_required": <true|false|null>,
  "summary": <2-3 sentences capturing the city-wide rule>,
  "designated_dog_zones": <text listing off-leash dog parks if mentioned>,
  "time_windows": <text if hours/season restrictions, else null>,
  "cite_quote": <verbatim quote 50-300 chars from source supporting the rule>,
  "confidence": "high" | "medium" | "low"
}}"""


def call_llm_operator(content: str, op_name: str, enable_web_search: bool = False) -> dict:
    user = PROMPT_USER_TEMPLATE.format(content=content[:30000], name=op_name)
    body = {
        "model": SONNET,
        "max_tokens": 1500,
        "system": PROMPT_SYSTEM,
        "messages": [{"role": "user", "content": user}],
    }
    if enable_web_search:
        body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}]
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"), method="POST",
    )
    req.add_header("x-api-key", ANTHROPIC_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=180) as r:
        resp = json.loads(r.read().decode("utf-8"))
    text_blocks = [b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text"]
    raw = (text_blocks[-1] if text_blocks else "").strip()
    if raw.startswith("```"):
        parts = raw.split("```", 2)
        if len(parts) >= 2:
            inner = parts[1]
            if inner.lower().startswith("json"):
                inner = inner[4:].lstrip()
            raw = inner.rsplit("```", 1)[0].strip()
    if not raw.startswith("{"):
        i, j = raw.find("{"), raw.rfind("}")
        if i >= 0 and j > i:
            raw = raw[i:j+1]
    try:
        return json.loads(raw)
    except Exception:
        return {"policy_found": False, "_parse_error": raw[:300]}


DEFAULT_IDS = [335, 317, 271, 193, 142, 403]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--ids", type=str, default=None,
                    help="Comma-separated operators.id (plural table). Default: 6 rescue cities.")
    ap.add_argument("--no-web-search", action="store_true")
    args = ap.parse_args()

    target_ids = [int(x) for x in args.ids.split(",")] if args.ids else DEFAULT_IDS

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, canonical_name FROM public.operators WHERE id = ANY(%s)
    """, (target_ids,))
    operators = {o['id']: o['canonical_name'] for o in cur.fetchall()}

    print(f"6-city operator rescue — targeting {len(operators)}; apply={args.apply}")

    summary = {"success_direct": 0, "success_web_search": 0,
               "no_policy": 0, "all_failed": 0}

    for op_id in target_ids:
        if op_id not in operators:
            continue
        op_name = operators[op_id]
        print(f"\n[{op_id}] {op_name}")
        cands = candidates_for_operator(op_id, op_name)
        print(f"  candidates: {len(cands)}")

        winning = None
        ws_tried = False

        for c in cands:
            text_raw, why = smart_fetch(c['url'])
            if text_raw is None:
                print(f"    [-] {c['url'][:80]}  (fetch fail: {why})")
                continue
            if is_thin_or_blocked(text_raw) and not args.no_web_search and not ws_tried:
                ws_tried = True
                print(f"    [~] {c['url'][:80]}  (thin/blocked → web_search)")
                try:
                    extracted = call_llm_operator(
                        f"[Original URL was thin/blocked: {c['url']}]\nUse web_search to find {op_name}'s authoritative city-wide dog/leash policy.",
                        op_name, enable_web_search=True,
                    )
                    if extracted.get('policy_found') and extracted.get('city_wide'):
                        winning = {"url": c['url'], "extracted": extracted, "via": "web_search_thin"}
                        summary['success_web_search'] += 1
                        break
                except Exception as e:
                    print(f"    [!] web_search error: {e}")
                continue

            text = strip_html(text_raw) if "<" in text_raw else text_raw[:60000]
            try:
                extracted = call_llm_operator(text, op_name)
            except Exception as e:
                print(f"    [!] LLM error: {e}")
                continue
            pf = extracted.get('policy_found')
            cw = extracted.get('city_wide')
            scope = extracted.get('scope_notes', '?')[:60]
            print(f"    [{pf}/{cw}] auth={c['auth']} via {c['platform']} scope={scope}")
            if pf and cw:
                winning = {"url": c['url'], "extracted": extracted, "via": c['platform']}
                summary['success_direct'] += 1
                break

        # Beach-level web_search escalation when all candidates failed
        if not winning and not args.no_web_search and not ws_tried:
            print(f"    [~] all candidates failed → web_search escalation")
            try:
                extracted = call_llm_operator(
                    f"All deterministic URLs for {op_name} failed. Use web_search "
                    f"to find the city's authoritative dog/leash policy from "
                    f"AUTHORITATIVE source (.gov, .ca.us) — not Yelp, BringFido, "
                    f"or news sites.",
                    op_name, enable_web_search=True,
                )
                if extracted.get('policy_found') and extracted.get('city_wide'):
                    winning = {"url": "[web_search]", "extracted": extracted,
                               "via": "web_search_escalation"}
                    summary['success_web_search'] += 1
                else:
                    print(f"    [-] web_search: policy_found={extracted.get('policy_found')} city_wide={extracted.get('city_wide')}")
                    summary['all_failed'] += 1
            except Exception as e:
                print(f"    [!] web_search escalation error: {e}")
                summary['all_failed'] += 1
        elif not winning:
            summary['all_failed'] += 1

        # Apply: write to operator_dogs_policy
        if winning and args.apply:
            e = winning['extracted']
            rule = e.get('default_rule')
            db_rule = (
                'yes' if rule == 'allowed' else
                'no'  if rule == 'prohibited' else
                'restricted' if rule == 'restricted' else
                'seasonal'   if rule == 'seasonal' else
                None
            )
            cur.execute("""
                INSERT INTO public.operator_dogs_policy
                    (operator_id, policy_found, applies_to_all,
                     default_rule, leash_required, summary,
                     designated_dog_zones, time_windows,
                     source_url, pass_a_confidence, pass_a_quotes, pass_a_at,
                     notes, verified_by, updated_at)
                VALUES (%s, true, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s::jsonb, now(),
                        'extract_operator_dogs_policy_v2.py — codify-pattern rescue',
                        'llm', now())
                ON CONFLICT (operator_id) DO UPDATE SET
                    policy_found    = true,
                    applies_to_all  = EXCLUDED.applies_to_all,
                    default_rule    = EXCLUDED.default_rule,
                    leash_required  = EXCLUDED.leash_required,
                    summary         = EXCLUDED.summary,
                    designated_dog_zones = EXCLUDED.designated_dog_zones,
                    time_windows    = EXCLUDED.time_windows,
                    source_url      = EXCLUDED.source_url,
                    pass_a_confidence = EXCLUDED.pass_a_confidence,
                    pass_a_quotes   = EXCLUDED.pass_a_quotes,
                    pass_a_at       = EXCLUDED.pass_a_at,
                    notes           = EXCLUDED.notes,
                    verified_by     = EXCLUDED.verified_by,
                    updated_at      = now()
            """, (
                op_id, e.get('city_wide', True), db_rule,
                e.get('leash_required'), e.get('summary'),
                e.get('designated_dog_zones'),
                json.dumps([{'text': e.get('time_windows')}]) if e.get('time_windows') else None,
                winning['url'] if winning['url'] != '[web_search]' else None,
                {'high': 0.92, 'medium': 0.78, 'low': 0.60}.get(e.get('confidence', 'medium'), 0.78),
                json.dumps([e.get('cite_quote')] if e.get('cite_quote') else []),
            ))
            conn.commit()
            print(f"    [APPLY] wrote operator_dogs_policy: rule={db_rule} via={winning['via']}")

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:22} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
