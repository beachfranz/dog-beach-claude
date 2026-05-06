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

SECTION TAXONOMY (closed list of 10 - use only these section names)
- sand              open beach / sand surface
- trails            footpaths, hiking trails, multi-use paths
- boardwalk         boardwalk surface (distinct from trails)
- bluff             elevated bluff or cliff terrain
- dunes             dune areas (broader than nesting; can be plover-adjacent)
- picnic_area       picnic tables, day-use lawns, designated picnic spots
- campground        overnight camping zones
- playground        kids' playground equipment
- tide_pools        tide pool zones (often marine-protected)
- nesting_zones     snowy-plover, least-tern, or other species nesting
                    closures. Typically a marked strip on sand or in dunes,
                    seasonally closed (Mar-Sep most common). Use this when
                    the source mentions a specific named/marked nesting
                    closure even if the source doesn't use the words
                    "nesting" or "plover" -- e.g. "fenced bird protection
                    area" or "wildlife closure zone" qualifies.

(Excluded: water_swim (duplicative of sand; reserved for water-quality
overlay), restrooms_showers (dogs don't enter restrooms), parking_lot
(rule is universally on_leash). Parking and restroom presence are flat
amenity checks in scoring, not dog-policy sections.)

RULE VALUES (closed list of 4 - use only these)
- not_allowed   dogs prohibited
- on_leash      dogs allowed only on leash
- off_leash     dogs allowed without leash
- unknown       source genuinely does not say AND no agency-class default applies

OUTPUT SHAPE (valid JSON only, no prose, no markdown fences)
{{
  "seasons": [
    {{
      "name": "All year",
      "dates": null,
      "regions": [
        {{
          "name": null,
          "evidence":  {{"quote": "<verbatim>", "source": "<src>", "source_url": "<url>"}},
          "sections": {{
            "<section_name>": {{
              "rule": "<rule_value>",
              "time_windows": [{{"start": "HH:MM", "end": "HH:MM", "rule": "<rule_value>"}}],
              "sub_zones":    [{{"name": "<short>", "rule": "<rule_value>"}}],
              "evidence":     {{}}
            }}
          }}
        }}
      ]
    }}
  ],
  "global_notes": "<plain-prose summary for moderators>",
  "extraction_method": "closed_zones_v3"
}}

HIERARCHY (top to bottom):
  seasons[] -> regions[] -> sections{{}} -> sub_zones[]

All fields under time_windows, sub_zones, evidence are optional. Omit a key
entirely rather than emitting null/empty -- except `name` on regions (use null
for the default unnamed region).

SEASON COVERAGE INVARIANT:
- If the source text describes NO seasonal differences, emit ONE season with
  `name: "All year"` and `dates: null`. The default for most beaches.
- If multiple seasons are declared (peak vs off-season etc.), all entries
  together MUST cover the full calendar year. No silent gaps.
- If only ONE season is named in source text (e.g. "snowy plover Mar-Sep:
  prohibited"), emit it AND an explicit "Rest of year" complement carrying
  the off-protection rules. NEVER leave part of the year uncovered.
- Time_windows live INSIDE sections within seasons. Don't put `seasonal`
  fields on time_windows -- the containing season provides that scope.

SEASON DATES FORMAT:
- `dates: null` for "All year" (the default season). Required for the unnamed
  "All year" season; permitted on any season with no formal date bounds.
- `dates: {{"start": "MM-DD", "end": "MM-DD"}}` for date-bounded seasons.
  MM-DD is month-day, year-agnostic since seasons recur. Examples:
    Peak season (Jun 16 to Labor Day):  {{"start": "06-16", "end": "09-04"}}
    Snowy plover season (Mar 1-Sep 30): {{"start": "03-01", "end": "09-30"}}
    Off-season (Sep 5 to Jun 15):       {{"start": "09-05", "end": "06-15"}}
  When `end` is calendar-earlier than `start` (e.g. "09-05" to "06-15"), the
  season WRAPS the new year — covers Sep 5 through Dec 31 AND Jan 1 through
  Jun 15. This is normal for "off-season" entries.
- Resolve named holidays (Labor Day, Memorial Day, etc.) to approximate dates
  using the current year as reference. Labor Day 2026 = first Monday of Sep
  ~= 09-07, but for season boundaries treat it as 09-04 (end of week before).
- Do NOT use month-integer arrays. Calendar months can't represent mid-month
  boundaries (Jun 16 is in the middle of June; both peak and off-season
  would claim June). Use explicit MM-DD dates.

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

   2a. CATALOG-VS-ZONES DISCIPLINE. CRITICAL — read carefully.

       The input may include a `sibling_beaches` list. These are SEPARATE
       beach records in our catalog that exist in their own right. When a
       sibling exists for an area, that area is NOT a region of THIS beach
       — it's its own beach with its own zone_rules elsewhere.

       Hard rule: if the source text describes a sub-area that corresponds
       to ANY sibling (by any descriptive name — "Main Beach," "rest of
       [parent]," "south end," etc.), DO NOT emit a region for it. ONLY
       output regions for areas that are part of THIS beach's footprint.

       The match between sibling names and descriptive sub-area names
       requires spatial/geographic reasoning, not literal string match.
       "Main Beach" in source text + "Del Mar Beach" in siblings = SAME
       AREA = OMIT.

       Strong heuristic: if the input beach name contains "Dog Beach,"
       "Off-Leash," "Pet Beach," or similar carve-out terminology, your
       output should describe ONLY the carve-out footprint. Any region you
       would label "rest of," "main," "south of," or "outside the dog
       beach" is a sibling and must be omitted.

       Example for THIS exact case: input "Del Mar Dog Beach"; sibling
       includes "Del Mar Beach". Source describes "Del Mar's two-zone
       policy: Dog Beach (north end) allowed, Main Beach prohibited."
       CORRECT output: ONE region for the Dog Beach footprint with
       seasonal/time rules. INCORRECT: two regions including "Main Beach
       and South."

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

5. MODIFIER PLACEMENT: SEASON-LEVEL THEN REGION-LEVEL CASCADE. Time-of-year
   logic lives in the seasons[] array at the top. Citywide time-of-day
   ordinances or operator-wide region patterns nest inside the season at the
   region level. Put time_windows at the SECTION level only when ONE section
   bucks its season+region context. Section-level rules and modifiers
   TERMINALLY OVERRIDE inherited cascades (e.g. a playground prohibited 24/7
   in a city beach with a 9am-6pm dogs-prohibited window: section gets
   `rule = not_allowed` with NO time_windows -- the cascade does not extend).

6. EVIDENCE. When a section rule comes from a specific source quote, attach
   an `evidence` field with the verbatim quote (no paraphrasing). When the
   same quote backs multiple sections, attach it at the region level. Do not
   fabricate quotes. Inferred rules from agency-class defaults need no
   evidence.

SPATIAL CONTEXT (authoritative — overrides ambiguous source text):

When `pet_allowed_carveout` is non-empty, this beach IS on an authoritative
list of explicit dog-allowed zones within a larger prohibition area. Emit
allowed rules with confidence (on_leash for sand/trails). Even if the source
text is ambiguous about THIS specific beach, the spatial overlay is ground
truth — the beach falls inside a known carve-out polygon.

When `pet_prohibited_zone` is non-empty, this beach IS in an explicit
prohibition zone. Emit not_allowed for sand/trails/dunes regardless of
ambient prose.

When `wildlife_critical_habitat` is non-empty, this beach overlaps designated
species critical habitat (snowy plover, least tern, etc.). ALWAYS emit a
nesting_zones section with seasonal closure (default Mar 1 - Sep 30 if no
species-specific dates given). Even if the source text doesn't mention
nesting, the spatial overlay is authoritative.

When all three are empty, you have no spatial ground truth — fall back to
the source text and per-beach scope discipline (heuristic 2).

NOW EXTRACT FOR THE INPUT BELOW

INPUT
beach_name: {beach_name}
operator_name: {operator_name}
county: {county}
sibling_beaches: {sibling_beaches}
pet_allowed_carveout: {pet_allowed_carveout}
pet_prohibited_zone: {pet_prohibited_zone}
wildlife_critical_habitat: {wildlife_critical_habitat}
source_text:
{source_text}

OUTPUT (valid JSON only, no prose, no markdown fences):"""


# ============================================================================
# DB helpers
# ============================================================================

def pull_beach_inputs(conn, fid):
    """Pull beach metadata + canonical operator name + all dogs evidence rows
    + sibling beaches (other catalog records sharing a name root + nearby).

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

        # Sibling beaches: same county + within ~2km + sharing a name token of
        # length>=4 (e.g. "Del Mar Dog Beach" + "Del Mar Beach" share "Del Mar").
        # Used by the prompt's catalog-vs-zones discipline (heuristic 2a).
        cur.execute(
            """
            with this_beach as (
              select fid, name, county_name, lat, lon,
                     (select array_agg(t)
                        from unnest(regexp_split_to_array(lower(name), '[^a-z]+')) t
                       where length(t) >= 4) as tokens
                from public.beaches_gold where fid = %s
            )
            select g.fid, g.name
              from public.beaches_gold g, this_beach t
             where g.is_active and g.fid <> t.fid
               and g.county_name = t.county_name
               and (
                 -- share at least one ≥4-char token
                 (select bool_or(tk = any(t.tokens))
                    from unnest(regexp_split_to_array(lower(g.name), '[^a-z]+')) tk
                   where length(tk) >= 4)
               )
               and abs(g.lat - t.lat) < 0.05    -- ~5km lat
               and abs(g.lon - t.lon) < 0.05
             order by g.name
             limit 8
            """,
            (fid,),
        )
        siblings = [f"{r['name']} (fid {r['fid']})" for r in cur.fetchall()]
        beach["sibling_beaches"] = ", ".join(siblings) if siblings else "(none)"

        # Spatial overlay context — authoritative ground truth from
        # public.dog_policy_zones via the beach_dog_policy_zones view
        cur.execute(
            """
            select category, zone_name, source_agency,
                   effective_dates, notes
              from public.beach_dog_policy_zones
             where fid = %s
             order by category, zone_name
            """,
            (fid,),
        )
        carveouts, prohibits, habitats = [], [], []
        for r in cur.fetchall():
            label = f"{r['zone_name']} ({r['source_agency']})"
            if r["effective_dates"]:
                label += f" dates={r['effective_dates']}"
            if r["category"] == "pet_allowed_carveout":
                carveouts.append(label)
            elif r["category"] == "pet_prohibited_zone":
                prohibits.append(label)
            elif r["category"] == "wildlife_critical_habitat":
                habitats.append(label)
        beach["pet_allowed_carveout"] = "; ".join(carveouts) if carveouts else "(none)"
        beach["pet_prohibited_zone"] = "; ".join(prohibits) if prohibits else "(none)"
        beach["wildlife_critical_habitat"] = "; ".join(habitats) if habitats else "(none)"

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
            sibling_beaches=beach.get("sibling_beaches") or "(none)",
            pet_allowed_carveout=beach.get("pet_allowed_carveout") or "(none)",
            pet_prohibited_zone=beach.get("pet_prohibited_zone") or "(none)",
            wildlife_critical_habitat=beach.get("wildlife_critical_habitat") or "(none)",
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
