# `closed_zones_v3` — section-aware dog-policy extraction prompt

**Status:** Draft 2026-05-04. Pre-LLM-call. Awaiting redline before eval.

Prompt-engineering target for step 4 of the zone_rules build order. Produces structured `zone_rules` jsonb (regions → sections → sub_zones hierarchy with time_windows / seasonal / evidence modifiers) for one specific beach from existing prose evidence.

This is what step 5 (text-repass) calls. ~1,014 BEP rows × Haiku ≈ $0.60.

Spec: `~/.claude/projects/.../memory/project_zone_rules_design_locked.md`
Fixture (gold set): `tests/zone_rules_anchors.json`

---

## Prompt (template variables in `{{ ... }}`)

```text
You extract structured per-section dog-policy rules for ONE specific beach.

INPUT
- beach_name: the specific beach to extract rules for
- operator_name: the agency or organization managing the beach
- county: county name
- source_text: prose from one or more sources describing dog policy at this beach
  or at its operator's beaches generally; each labeled with [source, conf]

Your output describes dog rules at THIS specific beach only.

SECTION TAXONOMY (closed list of 10 — use only these section names)
- sand              open beach / sand surface
- trails            footpaths, hiking trails, multi-use paths
- boardwalk         boardwalk surface (distinct from trails)
- bluff             elevated bluff or cliff terrain
- dunes             dune areas (broader than nesting; can be plover-adjacent)
- picnic_area       picnic tables, day-use lawns, designated picnic spots
- campground        overnight camping zones
- playground        kids' playground equipment
- tide_pools        tide pool zones (often marine-protected)
- nesting_zones     snowy-plover, least-tern, or other species nesting closures
                    (typically a marked strip on sand or in dunes, seasonally
                    closed Mar–Sep most commonly; year-round buffer at some)

(Excluded from dog-policy extraction: `water_swim` (duplicative of sand;
reserved for water-quality overlay), `restrooms_showers` (dogs don't enter
restrooms), `parking_lot` (rule is functionally universal `on_leash`).
Parking and restroom *presence* are captured as flat amenity checks in
scoring, not as sections.)

RULE VALUES (closed list of 4 — use only these)
- not_allowed   dogs prohibited
- on_leash      dogs allowed only on leash
- off_leash     dogs allowed without leash
- unknown       source genuinely does not say AND no agency-class default applies

OUTPUT SHAPE (valid JSON only, no prose, no markdown fences)
{
  "regions": [
    {
      "name": null | "<region name>",
      "time_windows": [ { "start": "HH:MM", "end": "HH:MM", "rule": "<rule_value>" } ],
      "seasonal":     [ { "months": "<MMM-MMM>", "reason": "<short text>" } ],
      "evidence":     { "quote": "<verbatim>", "source": "<src>", "source_url": "<url>" },
      "sections": {
        "<section_name>": {
          "rule": "<rule_value>",
          "time_windows": [...],   // optional; section-level OVERRIDES region-level
          "seasonal":     [...],   // optional
          "sub_zones":    [ { "name": "<short>", "rule": "<rule_value>" } ],  // optional
          "evidence":     { ... }  // optional
        }
      }
    }
  ],
  "global_notes": "<plain-prose summary for moderators>",
  "extraction_method": "closed_zones_v3"
}

All fields under `time_windows`, `seasonal`, `sub_zones`, `evidence` are optional.
Omit a key entirely rather than emitting null/empty — except `name` on regions
(use null for the default unnamed region).

EDITORIAL HEURISTICS (apply in this order)

1. SECTION EXISTENCE FIRST. Only include a section if it physically exists at
   THIS beach. If the beach has no campground, omit `campground` entirely. Do
   NOT use "unknown" rule for a non-existent feature. Use the source text +
   beach-type knowledge (state parks usually have campgrounds; urban beaches
   often have boardwalks; remote backcountry beaches don't have parking lots
   or playgrounds).

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
     picnic_area, parking_lot, restrooms_showers, connecting trails →
     on_leash. sand beach → not_allowed unless explicitly listed otherwise.
   - National Park Service (NPS): generally not_allowed everywhere except
     listed carve-outs.
   - Most cities: on_leash unless explicitly prohibited; playgrounds typically
     not_allowed regardless.
   - State wildlife / refuges (USFWS, CDFW): not_allowed in protected zones.
   When inferring, you do NOT need an evidence quote — the inference is from
   agency policy, not from this source.

4. MULTI-REGION ONLY WHEN GEOGRAPHY IS FUNDAMENTALLY DISTINCT. Use a single
   unnamed region (`name: null`) when one rule (or one set of modifiers)
   applies across the whole beach. Use multiple named regions ONLY when parts
   of the beach have fundamentally different rules (e.g. allowed in area A,
   prohibited in area B). Time-of-day or seasonal differences alone do NOT
   warrant a region split — put those at the region or section level as
   modifiers.

5. MODIFIER PLACEMENT: REGION-LEVEL CASCADE BY DEFAULT. Citywide ordinances
   or operator-wide time/seasonal patterns that apply to ALL sections of a
   region go at the REGION level — they cascade to every section. Put a
   modifier at the SECTION level only when ONE section bucks the cascade.
   Section-level rules and modifiers TERMINALLY OVERRIDE the region cascade
   (e.g. a playground prohibited 24/7 in a city beach with a 9am-6pm dogs-
   prohibited window: section gets `"rule": "not_allowed"` with NO
   time_windows — the cascade does not extend it).

6. EVIDENCE. When a section rule comes from a specific source quote, attach
   an `evidence` field with the verbatim quote (no paraphrasing). When the
   same quote backs multiple sections, attach it at the region level. Do not
   fabricate quotes. Inferred rules from agency-class defaults need no
   evidence.

NOW EXTRACT FOR THE INPUT BELOW

INPUT
beach_name: {{ beach_name }}
operator_name: {{ operator_name }}
county: {{ county }}
source_text:
{{ source_text }}

OUTPUT (valid JSON only, no prose, no markdown fences):
```

---

## Few-shot examples (under consideration — adds ~3K tokens but stabilizes structure)

If a no-shot pass against the 8 anchors produces structurally-valid output that disagrees with the gold variants on >1 anchor, fold these in:

| Example | Anchor | What it teaches |
|---|---|---|
| 1. Simple base case | Coronado Dog Beach (canonical: A) | Single region, no modifiers, partial section coverage (omit absent sections) |
| 2. Region-level cascade | Little Corona Del Mar (canonical: A) | Citywide time-window at region level, not repeated per section |
| 3. Multi-region geography | Garrapata (canonical: B) | When to split into named regions vs use sub_zones |
| 4. Per-beach scope | Wildcat Beach (canonical: A) | Operator text describes other beaches; output applies only to THIS beach |

Few-shots not in v3 draft yet — start with heuristic-only prompt and see if the eval reveals them as needed.

---

## Eval plan

`scripts/eval_closed_zones_v3.py` (to be written when prompt is approved):

1. Load `tests/zone_rules_anchors.json`
2. For each anchor:
   - Pull all dogs-field-group BEP rows for that fid (same query the inliner uses)
   - Format `source_text` as `[source, conf X.XX] <quote>` per row
   - Render the prompt with the template variables
   - Call Anthropic API (Haiku first, Sonnet for sweep)
   - Parse JSON output; structural validity check
   - Score against canonical variant: section coverage, rule accuracy, modifier placement, region count
3. Report per-anchor pass/fail + a summary table

Acceptance: 6 of 8 anchors must structurally match (Garrapata's multi-region structure is the hardest — getting that one right is the litmus). Rule values must match on the canonical winners; missing sections OK; extra sections OK if they're physically plausible.

---

## Open questions before running eval

1. **Source-text rendering** — currently planning `[source_name, conf 0.92] <quote>` per row. Worth including `is_canonical` flag too so the LLM can lean on the resolver's pick? Or does that bias?
2. **Operator name input** — pulling from `operators` table or from a hardcoded table by `c1_jurisdiction_id` / `cpad_unit_id`? For the Wildcat test it's "National Park Service" or "Point Reyes National Seashore" — does the LLM know what to do with either?
3. **Few-shots in v1 of the prompt or wait?** — adds ~3K tokens. Start without; add only if eval shows they're needed.
4. **Calibration follow-up** — once eval passes, real production calibration is against the 1,014-row repass output. Plan: spot-check 10% manually, compute Wilson agreement vs research+park_url proxy truth (same as city/county_policy methodology).

---

## Cross-refs
- `project_zone_rules_design_locked.md` — the spec the prompt encodes
- `tests/zone_rules_anchors.json` — gold set the prompt's output is graded against
- `extraction_prompt_variants` — table where this prompt lands when shipped (variant_key=`closed_zones_v3`, field_name=`zone_rules`, expected_shape=`structured_json`)
- `populate_from_research_gold.sql` / `populate_from_park_url_gold.sql` — eventual callers that lift the parsed `zone_rules` jsonb into BEP claimed_values
