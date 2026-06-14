"""
extract_dogs_disambiguate_v1.py
================================

Disambiguates BEP rows that have a contradictory shape:
  allowed = 'no'
  leash_required IN ('on_leash','off_leash','mixed')
  AND no per-section evidence (no area_*, sections, rules, prohibited_areas,
  designated_dog_zones)

These rows typically come from an earlier extractor that conflated city-wide
leash policy (which applies AT THE BEACH in places like Trinidad) with a
beach-level prohibition (Alameda, where dogs ARE banned from beaches).

Per scope notes:
  - When the city rule applies on beaches, leash_required=on_leash is the
    beach answer; allowed should be 'yes'.
  - When the city prohibits dogs from beaches specifically (but allows them
    in parks), allowed=no is correct; leash_required is orphan.

This script uses Sonnet (the higher-precision model) to read the row's
existing `notes` field + beach identity and emit a disambiguated answer.
Writes a new BEP row with source='per_beach_disambiguate_v1' that wins
canonical (confidence 0.92 high / 0.80 medium) when the answer is clear,
or a sentinel `per_beach_disambiguate_v1_no_result` row when ambiguous.

Usage:
  python scripts/extract_dogs_disambiguate_v1.py --apply
  python scripts/extract_dogs_disambiguate_v1.py --apply --limit 10
  python scripts/extract_dogs_disambiguate_v1.py --apply --source city_policy
"""
from __future__ import annotations

import argparse, json, os, sys, time
import urllib.request

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.llm import SONNET
from scripts.common.db import connect


ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_KEY")
if not ANTHROPIC_KEY:
    sys.exit("ANTHROPIC_API_KEY not set in env")


PROMPT_SYSTEM = (
    "You are reading dog-policy text extracted from a city / county / "
    "operator source and disambiguating it FOR ONE SPECIFIC BEACH. "
    "Earlier extraction produced a contradictory output: it said dogs are "
    "NOT allowed at this beach AND that a leash rule applies. That "
    "combination is logically impossible unless the leash rule belongs to a "
    "broader scope (city-wide, not beach-specific) where dogs ARE allowed "
    "in some places but NOT this beach.\n\n"
    "Your job: read the policy notes and decide which interpretation is "
    "correct FOR THIS BEACH:\n"
    "  A) Beach prohibition. Dogs are flatly banned at this beach. The "
    "leash rule applies elsewhere (parks, etc.) but not here. → "
    "allowed='no', leash_required=null.\n"
    "  B) Beach with leash. Dogs ARE allowed at this beach, just on leash "
    "(or off-leash, mixed). The original allowed='no' was a misread of the "
    "leash phrasing. → allowed='yes', leash_required preserves the rule.\n"
    "  C) Ambiguous. Notes don't clearly distinguish. → answer='ambiguous'."
)

PROMPT_USER_TEMPLATE = """BEACH: {name}{geo}

ORIGINAL EXTRACTION (contradictory):
  allowed: no
  leash_required: {leash}

POLICY NOTES:
  {notes}

Decide which scenario fits this beach. Return JSON ONLY:
{{
  "scenario": "A" | "B" | "ambiguous",
  "corrected_allowed": "no" | "yes" | null,
  "corrected_leash_required": "on_leash" | "off_leash" | "mixed" | null,
  "confidence": "high" | "medium" | "low",
  "reasoning": "<one sentence explaining the choice based on the notes>"
}}"""


def call_sonnet(beach_name: str, geo: str, leash: str, notes: str) -> dict:
    body = {
        "model": SONNET,
        "max_tokens": 600,
        "system": PROMPT_SYSTEM,
        "messages": [{
            "role": "user",
            "content": PROMPT_USER_TEMPLATE.format(
                name=beach_name, geo=geo, leash=leash, notes=notes,
            ),
        }],
    }
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
    text = "\n".join(text_blocks).strip()
    # Extract JSON
    try:
        start = text.index("{")
        end = text.rindex("}")
        return json.loads(text[start:end + 1])
    except (ValueError, json.JSONDecodeError) as e:
        return {"scenario": "parse_error", "reasoning": f"parse: {e}; raw: {text[:200]}"}


def write_disambiguated(cur, fid: int, scenario: str, allowed: str | None,
                        leash: str | None, conf: float, reasoning: str,
                        original_source: str) -> None:
    claimed = {
        "allowed": allowed,
        "leash_required": leash,
        "source_quote": reasoning,
        "extraction_method": "disambiguate_v1",
        "disambiguated_from": original_source,
        "scenario": scenario,
    }
    cur.execute("""
        INSERT INTO public.beach_enrichment_provenance
            (gold_fid, field_group, source, confidence, claimed_values,
             is_canonical, relevance_verified, updated_at)
        VALUES (%s, 'dogs', 'per_beach_disambiguate_v1', %s, %s::jsonb,
                false, false, now())
        ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
        DO UPDATE SET
            confidence = EXCLUDED.confidence,
            claimed_values = EXCLUDED.claimed_values,
            updated_at = now()
    """, (fid, conf, json.dumps(claimed)))


def write_sentinel(cur, fid: int, reason: str) -> None:
    claimed = {
        "status": "ambiguous",
        "reasoning": reason,
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    cur.execute("""
        INSERT INTO public.beach_enrichment_provenance
            (gold_fid, field_group, source, confidence, claimed_values,
             is_canonical, relevance_verified, updated_at)
        VALUES (%s, 'dogs', 'per_beach_disambiguate_v1_no_result', 0.10,
                %s::jsonb, false, false, now())
        ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
        DO UPDATE SET
            claimed_values = EXCLUDED.claimed_values,
            updated_at = now()
    """, (fid, json.dumps(claimed)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--source", type=str, default=None,
                    help="Filter to one BEP source (e.g. city_policy)")
    ap.add_argument("--skip-if-fresh-within", type=int, default=30,
                    help="Skip beaches already disambiguated within N days")
    args = ap.parse_args()

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    where_source = "AND bep.source = %s" if args.source else ""
    params = [args.source] if args.source else []

    cur.execute(f"""
        SELECT bep.gold_fid AS fid, bep.source,
               coalesce(b.display_name_override, b.name) AS name,
               b.county_name, b.state,
               bep.claimed_values->>'leash_required' AS leash,
               bep.claimed_values->>'notes' AS notes
        FROM public.beach_enrichment_provenance bep
        JOIN public.beaches_gold b ON b.fid = bep.gold_fid
        WHERE bep.field_group = 'dogs'
          AND bep.claimed_values->>'allowed' = 'no'
          AND bep.claimed_values->>'leash_required' IN ('on_leash','off_leash','mixed')
          AND bep.claimed_values->>'notes' IS NOT NULL
          AND length(bep.claimed_values->>'notes') > 20
          AND NOT (
            bep.claimed_values ? 'area_sand'        OR bep.claimed_values ? 'area_water'
            OR bep.claimed_values ? 'area_trails'      OR bep.claimed_values ? 'area_picnic_area'
            OR bep.claimed_values ? 'area_parking_lot' OR bep.claimed_values ? 'area_campground'
            OR bep.claimed_values ? 'sections'         OR bep.claimed_values ? 'rules'
            OR bep.claimed_values ? 'prohibited_areas' OR bep.claimed_values ? 'designated_dog_zones'
          )
          AND bep.source NOT IN ('operator_county','operator_pad_us','operator_city',
                                 'state_dogs_policy_v1','pad_us_dogs_policy_v1')
          {where_source}
        ORDER BY bep.gold_fid
    """, params)
    candidates = cur.fetchall()
    print(f"Found {len(candidates)} bare contradictory rows")

    # Apply freshness guard
    if args.skip_if_fresh_within > 0 and candidates:
        fids = [c["fid"] for c in candidates]
        cur.execute("""
            SELECT DISTINCT gold_fid
            FROM public.beach_enrichment_provenance
            WHERE source IN ('per_beach_disambiguate_v1', 'per_beach_disambiguate_v1_no_result')
              AND gold_fid = ANY(%s)
              AND coalesce(updated_at, now()) > now() - (%s || ' days')::interval
        """, (fids, args.skip_if_fresh_within))
        fresh = {r["gold_fid"] for r in cur.fetchall()}
        if fresh:
            candidates = [c for c in candidates if c["fid"] not in fresh]
            print(f"Skipping {len(fresh)} already-fresh; {len(candidates)} remain")

    if args.limit:
        candidates = candidates[:args.limit]

    summary = {"A_prohibition": 0, "B_with_leash": 0, "ambiguous": 0,
               "parse_error": 0, "low_conf_skipped": 0}

    for row in candidates:
        fid = row["fid"]
        name = row["name"]
        city = row.get("county_name") or "-"
        geo = f", {city}, {row['state']}" if row.get("state") else ""
        leash = row["leash"]
        notes = (row["notes"] or "")[:1500]
        print(f"\n[{fid}] {name}  leash={leash}  source={row['source']}")
        print(f"  notes: {notes[:120]}")

        try:
            resp = call_sonnet(name, geo, leash, notes)
        except Exception as e:
            print(f"  [!] LLM error: {e}")
            continue

        scenario = resp.get("scenario", "")
        conf_text = resp.get("confidence", "low")
        reasoning = resp.get("reasoning", "")
        print(f"  [{scenario}/{conf_text}] {reasoning[:120]}")

        if not args.apply:
            continue

        if scenario == "A":
            summary["A_prohibition"] += 1
            conf = 0.92 if conf_text == "high" else 0.80 if conf_text == "medium" else None
            if conf is None:
                summary["low_conf_skipped"] += 1
                write_sentinel(cur, fid, f"low conf scenario A: {reasoning}")
            else:
                write_disambiguated(cur, fid, "A", "no", None, conf, reasoning, row["source"])
        elif scenario == "B":
            summary["B_with_leash"] += 1
            corrected_leash = resp.get("corrected_leash_required") or leash
            conf = 0.92 if conf_text == "high" else 0.80 if conf_text == "medium" else None
            if conf is None:
                summary["low_conf_skipped"] += 1
                write_sentinel(cur, fid, f"low conf scenario B: {reasoning}")
            else:
                write_disambiguated(cur, fid, "B", "yes", corrected_leash, conf, reasoning, row["source"])
        elif scenario == "ambiguous":
            summary["ambiguous"] += 1
            write_sentinel(cur, fid, reasoning)
        else:
            summary["parse_error"] += 1

        conn.commit()

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:25} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
