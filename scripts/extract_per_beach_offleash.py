"""
extract_per_beach_offleash.py — explicit per-beach off-leash extraction.

Per Franz 2026-05-25 LATE: many famous beaches (Will Rogers, Corona del Mar,
Coronado, Huntington State, Pt Dume) have nuanced policies ("off-leash before
9am only" / "allowed on the bluffs but not the sand" / "off-season only")
that get FLATTENED to has_off_leash=false in beach_dog_policy. Franz's rule:
"any time off-leash on the sand is Tier 1 experience/hour, not 3ish."

The extracted `this_beach_has_off_leash_anytime` field is a single boolean
that asks the LLM, with cite-required verbatim quote, whether THIS beach
specifically has off-leash hours on the sand at ANY time.

Strategy (mirrors extract_research_v2.py disambiguation framework):
  1. Pull candidate URLs from existing BEP + operator websites for this fid
  2. For each URL: fetch HTML, apply name-match + geo-gate
  3. Call Claude with the explicit per-beach question + cite-required
  4. If ≥1 URL returns yes-with-verbatim: write BEP row with claimed_values
     {'allowed':'yes', 'leash_required':'off_leash', 'area_sand':'off_leash',
      'this_beach_has_off_leash_anytime':'yes', 'source_quote': quote,
      'time_window': extracted text}
  5. Cascade auto-fires: consensus → promote → refresh_scoring_tier

Usage:
  python scripts/extract_per_beach_offleash.py --dry-run  # default
  python scripts/extract_per_beach_offleash.py --apply --limit 10
  python scripts/extract_per_beach_offleash.py --apply --fids 8270,8472
"""
from __future__ import annotations
import argparse, json, os, re, sys
from pathlib import Path

import psycopg2.extras
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET

# Reuse the helpers from extract_research_v2
from scripts.extract_research_v2 import (
    gather_urls, fetch_html, strip_html, name_match, geo_gate, authority_score
)

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


# Default candidate set: 25 famous CA beaches stuck on tier=none
DEFAULT_CANDIDATES = [
    8472,   # Will Rogers State Beach
    8333,   # Corona del Mar State Beach
    8715,   # Coronado Beach
    705,    # Huntington State Beach
    8340,   # Blacks Beach (already reactivated; rule still wrong)
    8867,   # Leadbetter Beach
    9069,   # Newport Municipal Beach
    8298,   # Santa Cruz Main Beach
    8430,   # Seabright Beach
    8301,   # Its Beach
    8564,   # Capitola Beach
    8351,   # Windansea Beach
    9720,   # Point Dume State Beach
    8247,   # Venice Beach
    8254,   # Zuma Beach
    9334,   # Topanga County Beach
    8386,   # Paradise Cove
    623,    # Paradise Cove Beach
    8554,   # Crown Point Beach
    8546,   # El Carmel Point
    6247,   # Gazos Creek State Beach
    4916,   # Ocean Beach Tide Pools
    8457,   # Pescadero Beach
    1474,   # China Cove Beach
    6136,   # China Cove (Pt Lobos)
]


def call_llm_per_beach_offleash(content: str, beach_name: str, city: str | None) -> dict:
    """Cite-required per-beach off-leash question. Returns dict with yes/no/unknown."""
    geo = f", {city}" if city else ""
    sys_prompt = (
        f"You are extracting dog-policy information about '{beach_name}{geo}'. "
        f"Use ONLY the provided source page. CRITICAL: only answer about THIS beach "
        f"specifically. If the source mentions OTHER nearby beaches (e.g., Rosie's "
        f"Dog Beach in a Long Beach context), do NOT confuse those rules with "
        f"this beach. If the source mentions this beach but does not address "
        f"off-leash, return 'unknown'. If the source does not mention this beach "
        f"by name (or its commonly-known abbreviation), return 'no_match'."
    )
    user_prompt = f"""SOURCE PAGE CONTENT:
{content}

QUESTION: For '{beach_name}{geo}' SPECIFICALLY, is there ANY time window (any hours, any season, any specific designated area on the sand) when dogs are allowed OFF-LEASH on the sand? Time-windowed off-leash (e.g., "before 9am only") counts as YES.

Return JSON:
{{
  "answer": "yes" | "no" | "unknown" | "no_match",
  "quote": "<verbatim quote from source, 50-300 chars, MUST contain the beach name>",
  "time_window": "<descriptive time/area window if yes, e.g. 'before 9am and after 6pm Sept-June' or 'designated dog beach area at pier'; null if no/unknown>",
  "confidence": "high" | "medium" | "low"
}}"""
    resp = ANTHROPIC.messages.create(
        model=SONNET,
        max_tokens=800,
        system=sys_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = resp.content[0].text if resp.content else ""
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except Exception:
        m = re.search(r'"answer"\s*:\s*"([^"]+)"', raw)
        return {"answer": m.group(1) if m else "parse_error", "raw": raw[:300]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fids", type=str, default=None,
                    help="comma-separated fids; overrides default candidate set")
    ap.add_argument("--max-urls-per-beach", type=int, default=3)
    args = ap.parse_args()

    if args.fids:
        target_fids = [int(x.strip()) for x in args.fids.split(",")]
    else:
        target_fids = DEFAULT_CANDIDATES
    if args.limit:
        target_fids = target_fids[:args.limit]

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT g.fid, g.name, g.county_name, g.state, g.lat, g.lon, j.name AS city_name
          FROM public.beaches_gold g
          LEFT JOIN public.poi_landing pl ON pl.fid = g.fid
          LEFT JOIN public.jurisdictions j ON j.fips_place = pl.place_fips
         WHERE g.fid = ANY(%s)
    """, (target_fids,))
    beaches = {b['fid']: b for b in cur.fetchall()}
    missing = [f for f in target_fids if f not in beaches]
    if missing:
        print(f"missing in beaches_gold: {missing}")

    print(f"Targeting {len(beaches)} beaches; apply={args.apply}")

    total_yes = 0
    total_no = 0
    total_unknown = 0
    total_no_match = 0
    total_skipped = 0

    for fid in target_fids:
        if fid not in beaches:
            continue
        b = beaches[fid]
        name = b['name']
        city = b['city_name']
        print(f"\n[{fid}] {name}  city={city}  county={b['county_name']}")

        urls = sorted(gather_urls(cur, fid, name), key=lambda u: -u['auth'])
        urls = urls[:args.max_urls_per_beach]
        print(f"  candidate URLs: {len(urls)}")
        if not urls:
            total_skipped += 1
            continue

        any_yes = False
        winning = None
        for u in urls:
            html = fetch_html(u['url'])
            if not html:
                print(f"    [-] {u['url'][:60]}  (fetch fail)")
                continue
            text = strip_html(html)
            if not name_match(text, name):
                print(f"    [-] {u['url'][:60]}  (name not on page)")
                continue
            if not geo_gate(text, city, b['county_name']):
                print(f"    [-] {u['url'][:60]}  (geo gate)")
                continue
            try:
                resp = call_llm_per_beach_offleash(text, name, city)
            except Exception as e:
                print(f"    [!] LLM error: {e}")
                continue
            ans = resp.get('answer')
            qt = (resp.get('quote') or '')[:80]
            print(f"    [{ans}] auth={u['auth']} url={u['url'][:60]}")
            print(f"        quote: {qt}")
            if ans == 'yes' and name.lower() in (resp.get('quote') or '').lower():
                any_yes = True
                winning = {'url': u['url'], 'resp': resp}
                break  # first yes-with-quoting wins

        if any_yes:
            total_yes += 1
            if args.apply:
                claimed = {
                    'allowed': 'yes',
                    'leash_required': 'off_leash',
                    'area_sand': 'off_leash',
                    'this_beach_has_off_leash_anytime': 'yes',
                    'source_quote': winning['resp'].get('quote'),
                    'time_window': winning['resp'].get('time_window'),
                    'confidence_text': winning['resp'].get('confidence'),
                }
                conf = {'high': 0.90, 'medium': 0.75, 'low': 0.60}.get(
                    winning['resp'].get('confidence', 'medium'), 0.75)
                cur.execute("""
                    INSERT INTO public.beach_enrichment_provenance
                        (gold_fid, field_group, source, confidence, claimed_values,
                         source_url, is_canonical, relevance_verified, notes, updated_at)
                    VALUES (%s, 'dogs', 'per_beach_offleash_v1', %s, %s::jsonb,
                            %s, true, true,
                            'extract_per_beach_offleash.py — explicit per-beach off-leash with cite-required',
                            now())
                    ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
                    DO UPDATE SET
                        confidence = EXCLUDED.confidence,
                        claimed_values = EXCLUDED.claimed_values,
                        source_url = EXCLUDED.source_url,
                        is_canonical = true,
                        relevance_verified = true,
                        updated_at = now()
                """, (fid, conf, json.dumps(claimed), winning['url']))
                conn.commit()
                # Trigger cascade for this fid
                cur.execute("SELECT public.compute_beach_field_consensus(%s)", (fid,))
                cur.execute("SELECT public.promote_canonical_dogs_to_beach_dog_policy(%s)", (fid,))
                conn.commit()
        else:
            # Tally what we got
            last_ans = resp.get('answer') if 'resp' in dir() else 'no_url_passed_gates'
            if last_ans == 'no':       total_no += 1
            elif last_ans == 'unknown': total_unknown += 1
            else:                       total_no_match += 1

    print(f"\nSUMMARY: yes={total_yes}, no={total_no}, unknown={total_unknown}, "
          f"no_match={total_no_match}, skipped={total_skipped} (no candidate URLs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
