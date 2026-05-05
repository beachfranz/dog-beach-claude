"""eval_closed_zones_v3.py — closed_zones_v3 prompt eval against the 8-anchor fixture.

For each anchor:
  1. Pull all dogs-field-group BEP rows for the fid
  2. Look up the canonical operator name (cpad mng_agncy → operators(city) → operators(county))
  3. Render the prompt with the inputs
  4. Call Anthropic Haiku
  5. Parse JSON, score against the canonical variant for that anchor
  6. Report per-anchor pass/fail + summary

Outputs:
  - stdout: per-anchor scores + summary table
  - tests/zone_rules_eval_output.json: full LLM output + scores for review

Cost estimate: 8 anchors × ~1.5K input tokens × Haiku 4.5 ≈ $0.01.
Re-run safe; idempotent.
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

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

ANTHROPIC = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
MODEL = "claude-haiku-4-5-20251001"

# Canonical variant pick per fid (from project_zone_rules_design_locked.md)
CANONICAL_VARIANT = {
    6202: "A_conservative",  # Coronado Dog Beach
    9716: "A_conservative",  # Fiesta Island
    8339: "A_conservative",  # Little Corona Del Mar
    9717: "A_conservative",  # Huntington Beach Dog Beach
    3407: "B_rich",          # Garrapata State Beach (3 named regions)
    8673: "B_rich",          # Carpinteria State Beach (agency-default inference)
    8356: "B_rich",          # Mission Beach (full amenity stack)
    8740: "A_conservative",  # Wildcat Beach (per-beach scope discipline)
}

# ============================================================================
# Prompt body (mirrors docs/closed_zones_v3.md). Kept inline so the as-shipped
# prompt is the as-evaluated prompt — single source of truth.
# ============================================================================
PROMPT_BODY = """You extract structured per-section dog-policy rules for ONE specific beach.

INPUT
- beach_name: the specific beach to extract rules for
- operator_name: the agency or organization managing the beach
- county: county name
- source_text: prose from one or more sources describing dog policy at this beach
  or at its operator's beaches generally; each labeled with [source, conf]

Your output describes dog rules at THIS specific beach only.

SECTION TAXONOMY (closed list of 12 - use only these section names)
- sand              open beach / sand surface
- water_swim        ocean or lake water
- trails            footpaths, hiking trails, multi-use paths
- boardwalk         boardwalk surface (distinct from trails)
- bluff             elevated bluff or cliff terrain
- dunes             dune areas (often plover-protected)
- picnic_area       picnic tables, day-use lawns, designated picnic spots
- parking_lot       parking lots and the path between car and beach
- campground        overnight camping zones
- playground        kids' playground equipment
- restrooms_showers restroom or shower buildings
- tide_pools        tide pool zones (often marine-protected)

RULE VALUES (closed list of 4 - use only these)
- not_allowed   dogs prohibited
- on_leash      dogs allowed only on leash
- off_leash     dogs allowed without leash
- unknown       source genuinely does not say AND no agency-class default applies

OUTPUT SHAPE (valid JSON only, no prose, no markdown fences)
{{
  "regions": [
    {{
      "name": null,
      "time_windows": [{{"start": "HH:MM", "end": "HH:MM", "rule": "<rule_value>"}}],
      "seasonal":     [{{"months": "<MMM-MMM>", "reason": "<short text>"}}],
      "evidence":     {{"quote": "<verbatim>", "source": "<src>", "source_url": "<url>"}},
      "sections": {{
        "<section_name>": {{
          "rule": "<rule_value>",
          "time_windows": [],
          "seasonal":     [],
          "sub_zones":    [{{"name": "<short>", "rule": "<rule_value>"}}],
          "evidence":     {{}}
        }}
      }}
    }}
  ],
  "global_notes": "<plain-prose summary for moderators>",
  "extraction_method": "closed_zones_v3"
}}

All fields under time_windows, seasonal, sub_zones, evidence are optional.
Omit a key entirely rather than emitting null/empty - except `name` on regions
(use null for the default unnamed region).

EDITORIAL HEURISTICS (apply in this order)

1. SECTION EXISTENCE FIRST. Only include a section if it physically exists at
   THIS beach. If the beach has no campground, omit `campground` entirely. Do
   NOT use "unknown" rule for a non-existent feature. Use the source text +
   beach-type knowledge (state parks usually have campgrounds; urban beaches
   often have boardwalks; remote backcountry beaches don't have parking lots
   or playgrounds).

   1a. BEACH-NAME HINTS. The beach name itself hints at sections. If the name
       contains "Boardwalk", "Pier", "Dunes", "Bluff", or "Tide Pools", include
       the matching section even if the source text doesn't mention it. Example:
       "Mission Beach Boardwalk" implies a `boardwalk` section regardless of
       what the source says.

   1b. IMPLICIT SECTION PAIRING. If `sand` is `not_allowed`, also emit
       `water_swim` as `not_allowed` — water access at most beaches requires
       traversing the sand, so the rules track together. Same direction holds
       for `dunes` if dunes are present.

2. PER-BEACH SCOPE DISCIPLINE. The source text may describe many beaches with
   different rules. Output rules for the beach named in the input ONLY. If the
   text says "Kehoe Beach Trail allowed; all other Point Reyes beaches
   prohibited" and the input beach is Wildcat (not Kehoe), output the
   prohibition rules. First identify which bucket THIS beach falls into,
   THEN extract rules for that bucket.

3. AGENCY-DEFAULT INFERENCE. When the source text doesn't address every
   existing section but the operator's agency-class policy is well known,
   INFER the implied sections. Apply these defaults:
   - California State Parks (CDPR): developed-areas-only. campground,
     picnic_area, parking_lot, restrooms_showers, connecting trails -> on_leash.
     sand beach -> not_allowed unless explicitly listed otherwise.
   - National Park Service (NPS): generally not_allowed everywhere except
     listed carve-outs.
   - Most cities: on_leash unless explicitly prohibited; playgrounds typically
     not_allowed regardless.
   - State wildlife / refuges (USFWS, CDFW): not_allowed in protected zones.
   When inferring, you do NOT need an evidence quote - the inference is from
   agency policy, not from this source.

4. MULTI-REGION ONLY WHEN GEOGRAPHY IS FUNDAMENTALLY DISTINCT. Use a single
   unnamed region (name: null) when one rule (or one set of modifiers)
   applies across the whole beach. Use multiple named regions ONLY when parts
   of the beach have fundamentally different rules (e.g. allowed in area A,
   prohibited in area B). Time-of-day or seasonal differences alone do NOT
   warrant a region split - put those at the region or section level as
   modifiers.

   4a. THE "ALL OTHER AREAS" SIGNAL. If you find yourself writing a sub_zone
       with name like "all other areas", "rest of beach", "everywhere else",
       etc., AND that sub_zone has a different rule than the section's default
       — that is the trigger to split into named regions instead. The
       "everywhere else" sub_zone is a smell that the section's default rule
       is wrong. Garrapata example: Gate 19 allows leashed dogs but rest of
       park prohibits — model this as TWO regions ("Gate 19 area" allows;
       "rest of park" prohibits), NOT as `sand.rule = on_leash` with a
       sub_zone "all other areas: not_allowed".

5. MODIFIER PLACEMENT: REGION-LEVEL CASCADE BY DEFAULT. Citywide ordinances
   or operator-wide time/seasonal patterns that apply to ALL sections of a
   region go at the REGION level - they cascade to every section. Put a
   modifier at the SECTION level only when ONE section bucks the cascade.
   Section-level rules and modifiers TERMINALLY OVERRIDE the region cascade
   (e.g. a playground prohibited 24/7 in a city beach with a 9am-6pm dogs-
   prohibited window: section gets `rule = not_allowed` with NO time_windows -
   the cascade does not extend it).

6. EVIDENCE. When a section rule comes from a specific source quote, attach
   an `evidence` field with the verbatim quote (no paraphrasing). When the
   same quote backs multiple sections, attach it at the region level. Do not
   fabricate quotes. Inferred rules from agency-class defaults need no
   evidence.

NOW EXTRACT FOR THE INPUT BELOW

INPUT
beach_name: {beach_name}
operator_name: {operator_name}
county: {county}
source_text:
{source_text}

OUTPUT (valid JSON only, no prose, no markdown fences):"""


# ============================================================================
# DB helpers
# ============================================================================

def pull_beach_inputs(conn, fid):
    """Pull beach metadata + canonical operator name + all dogs evidence rows.

    Operator lookup priority:
      1. canonical governance evidence in BEP (governing_body_name) — most authoritative
      2. cpad_units.mng_agncy via beaches_gold.cpad_unit_id
      3. operators(level='city') via c1_jurisdiction_id
      4. operators(level='county') via county_geoid
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            select g.fid, g.name, g.county_name,
                   coalesce(
                     (select bep.claimed_values->>'governing_body_name'
                        from public.beach_enrichment_provenance bep
                       where bep.gold_fid = g.fid
                         and bep.field_group = 'governance'
                         and bep.is_canonical
                         and bep.claimed_values->>'governing_body_name' is not null
                       limit 1),
                     (select cu.mng_agncy from public.cpad_units cu
                       where cu.unit_id = g.cpad_unit_id),
                     (select op.canonical_name from public.operators op
                       where op.jurisdiction_id = g.c1_jurisdiction_id
                         and op.level = 'city' limit 1),
                     (select op.canonical_name from public.operators op
                       where op.county_geoid = g.county_geoid
                         and op.level = 'county' limit 1)
                   ) as operator_name
              from public.beaches_gold g
             where g.fid = %s
            """,
            (fid,),
        )
        beach = dict(cur.fetchone())

        cur.execute(
            """
            select source, confidence::float as confidence,
                   claimed_values->>'allowed' as allowed,
                   claimed_values->>'leash_required' as leash,
                   claimed_values->>'notes' as notes,
                   claimed_values->>'zone_description' as zone_desc,
                   claimed_values->>'designated_dog_zones' as designated,
                   claimed_values->>'prohibited_areas' as prohibited,
                   source_url
              from public.beach_enrichment_provenance
             where gold_fid = %s and field_group = 'dogs'
             order by confidence desc, source
            """,
            (fid,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    return beach, rows


def format_source_text(rows):
    out = []
    for r in rows:
        parts = []
        if r["notes"]:
            parts.append(r["notes"])
        if r["zone_desc"]:
            parts.append(f"[zones] {r['zone_desc']}")
        if r["designated"]:
            parts.append(f"[designated] {r['designated']}")
        if r["prohibited"]:
            parts.append(f"[prohibited] {r['prohibited']}")
        if not parts:
            parts.append(
                f"(structured-only: allowed={r['allowed']}, leash={r['leash']})"
            )
        body = " ".join(parts)
        url = f" url={r['source_url']}" if r["source_url"] else ""
        out.append(f"[{r['source']}, conf {r['confidence']:.2f}]{url} {body}")
    return "\n".join(out)


# ============================================================================
# LLM call + parsing
# ============================================================================

def call_llm(prompt):
    resp = ANTHROPIC.messages.create(
        model=MODEL,
        max_tokens=2500,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text, resp.usage


def parse_output(text):
    t = text.strip()
    if t.startswith("```"):
        # Strip markdown fence (```json ... ```)
        t = t.split("\n", 1)[1] if "\n" in t else t[3:]
        t = t.rsplit("```", 1)[0]
    return json.loads(t.strip())


# ============================================================================
# Scoring
# ============================================================================

def all_sections(zr):
    out = {}
    for r in zr.get("regions", []):
        for sec, body in (r.get("sections") or {}).items():
            out[sec] = body
    return out


def score(actual, expected):
    notes = []
    points = 0

    # 1. Region count
    a_n = len(actual.get("regions", []))
    e_n = len(expected.get("regions", []))
    if a_n == e_n:
        notes.append(f"region_count={a_n} ✓")
        points += 1
    else:
        notes.append(f"region_count: actual={a_n}, expected={e_n} ✗")

    # 2. Section coverage (canonical's sections appear in actual)
    a_sec = all_sections(actual)
    e_sec = all_sections(expected)
    missing = set(e_sec) - set(a_sec)
    extra = set(a_sec) - set(e_sec)
    if not missing:
        notes.append(f"sections: all {len(e_sec)} canonical present ✓")
        points += 1
    else:
        notes.append(f"sections: missing {missing} ✗")
    if extra:
        notes.append(f"sections: extra (plausible-or-not) {extra} i")

    # 3. Rule-value accuracy on shared sections
    shared = set(a_sec) & set(e_sec)
    matches = sum(1 for s in shared if a_sec[s].get("rule") == e_sec[s].get("rule"))
    total = len(shared)
    if total > 0:
        pct = matches * 100 // total
        marker = "✓" if pct >= 80 else ("≈" if pct >= 60 else "✗")
        notes.append(f"rule_accuracy: {matches}/{total} ({pct}%) {marker}")
        if pct >= 80:
            points += 1
    else:
        notes.append("rule_accuracy: no shared sections to score")

    return points, notes


# ============================================================================
# Main
# ============================================================================

def main():
    fixture = json.loads(
        (ROOT / "tests" / "zone_rules_anchors.json").read_text(encoding="utf-8")
    )
    conn = psycopg2.connect(**PG)

    results = []
    total_in = 0
    total_out = 0
    for anchor in fixture["anchors"]:
        fid = anchor["fid"]
        target_variant = CANONICAL_VARIANT[fid]
        canonical = anchor["variants"][target_variant]

        beach, rows = pull_beach_inputs(conn, fid)
        source_text = format_source_text(rows)
        prompt = PROMPT_BODY.format(
            beach_name=beach["name"],
            operator_name=beach.get("operator_name") or "(unknown)",
            county=beach.get("county_name") or "",
            source_text=source_text,
        )

        print(f"\n=== fid={fid} {anchor['name']} (target: {target_variant}) ===")
        print(f"  operator: {beach.get('operator_name')}")
        print(f"  source rows: {len(rows)}, prompt chars: {len(prompt)}")

        try:
            raw, usage = call_llm(prompt)
            total_in += usage.input_tokens
            total_out += usage.output_tokens
        except Exception as e:
            print(f"  ✗ LLM error: {e}")
            results.append({"fid": fid, "name": anchor["name"], "score": 0,
                            "notes": [f"llm_error: {e}"]})
            continue

        try:
            actual = parse_output(raw)
        except json.JSONDecodeError as e:
            print(f"  ✗ JSON parse error: {e}")
            print(f"  raw[:400]: {raw[:400]}")
            results.append({"fid": fid, "name": anchor["name"], "score": 0,
                            "notes": [f"json_parse_error: {e}"], "raw": raw})
            continue

        s, notes = score(actual, canonical)
        for n in notes:
            print(f"    {n}")
        results.append({
            "fid": fid, "name": anchor["name"],
            "target_variant": target_variant,
            "score": s, "notes": notes,
            "actual": actual, "expected": canonical,
            "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
        })

    # Summary
    print("\n=== SUMMARY ===")
    total_pts = sum(r["score"] for r in results)
    max_pts = len(results) * 3
    print(f"Score: {total_pts}/{max_pts} ({100*total_pts//max_pts if max_pts else 0}%)")
    for r in results:
        print(f"  fid={r['fid']:>5} {r['name'][:32]:<32} {r['score']}/3   "
              f"({r.get('target_variant','')})")
    print(f"\nTokens: in={total_in:,}  out={total_out:,}")
    # Haiku 4.5 pricing (approx 2026): $1/MTok in, $5/MTok out
    cost = total_in * 1.0 / 1_000_000 + total_out * 5.0 / 1_000_000
    print(f"Estimated cost (Haiku 4.5): ${cost:.4f}")

    out_path = ROOT / "tests" / "zone_rules_eval_output.json"
    out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\nFull output: {out_path}")
    return 0


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.exit(main())
