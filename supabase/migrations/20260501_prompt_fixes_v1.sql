-- Prompt-quality fixes from 2026-04-30 gold-set review.
--
-- Findings:
--   * dogs_leash_required: LLM was 50% wrong, defaulting to 'required' when
--     beaches actually have zoned policies (off-leash on sand, leashed on
--     trails/parking). Prompt didn't ask about zones explicitly.
--   * raw_address: LLM was too conservative — returned 'unknown' for
--     "Anita Street Beach" instead of inferring "Anita Street" from the
--     beach name itself.
--   * 2 typo rows in gold set survived prior cleanup ("Exlude.", "Unlclear")
--     because they didn't match the regex. Fuzzy clean.

begin;

-- 1) Typo cleanup — fuzzy match near-misses of the sentinel values
delete from public.beach_policy_gold_set
 where lower(verified_value) ~ '^(excl|unclr|unkno|unlcl|exlu|exlc)';

-- 2) dogs_leash_required — deactivate old + insert v2 prompts
update public.extraction_prompt_variants
   set active = false,
       notes  = coalesce(notes, '') || ' [superseded 2026-05-01 zoning fix]'
 where field_name = 'dogs_leash_required' and active = true;

insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('dogs_leash_required', 'direct_enum_v2',
   'What is the leash policy for dogs at this beach? Reply exactly one of: required, optional, mixed_by_zone, varies_by_time, unclear. ' ||
   'IMPORTANT: Look for ZONED differences — many beaches require leashes in parking lots and on trails but allow off-leash on the sand. ' ||
   'If different rules apply to different physical areas (sand vs trails vs parking lot vs water), return mixed_by_zone. ' ||
   'If the same leash rule applies everywhere on the beach property, return required or optional. ' ||
   'If rules vary by time of day or season but are uniform across zones, return varies_by_time. ' ||
   'No other text.',
   'enum', 'claude-haiku-4-5-20251001', true, true, '2026-05-01 zoning fix'),
  ('dogs_leash_required', 'json_evidence_v2',
   'Return valid JSON: {"leash": one of "required"|"optional"|"mixed_by_zone"|"varies_by_time"|"unclear", ' ||
   '"sand_rule": string|null, "trail_rule": string|null, "parking_rule": string|null, "evidence": string|null}. ' ||
   'Distinguish per-zone rules; if any zone differs, the top-level "leash" must be "mixed_by_zone". No markdown.',
   'structured_json', 'claude-sonnet-4-6', false, true, '2026-05-01 zoning fix'),
  ('dogs_leash_required', 'describe_prose_v2',
   'In one or two sentences, describe the leash policy at this beach. Be explicit about whether the rule differs ' ||
   'between zones — for example, "leashed in parking lot and on trails, off-leash on sand and in the water" ' ||
   'is a typical zoned answer. If rules are uniform everywhere, say so plainly.',
   'text', 'claude-sonnet-4-6', false, true, '2026-05-01 zoning fix');

-- 3) raw_address — deactivate old + insert v2 prompts that allow inference
update public.extraction_prompt_variants
   set active = false,
       notes  = coalesce(notes, '') || ' [superseded 2026-05-01 address inference fix]'
 where field_name = 'raw_address' and active = true;

insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('raw_address', 'direct_q_v2',
   'What is the street address or location of this beach? Return the most specific address available, in one line. ' ||
   'Use this priority: ' ||
   '(1) An explicit address on the page ("123 Main St, Anytown CA 92648"), ' ||
   '(2) cross-streets named on the page ("between Goldenwest and Beach Blvd"), ' ||
   '(3) a street name embedded in the beach name itself ("Anita Street Beach" → "Anita Street"; "Ocean Boulevard Beach" → "Ocean Boulevard"), ' ||
   '(4) the city/area named on the page ("Half Moon Bay coast"). ' ||
   'Return "unknown" only if NONE of (1)-(4) is available. Do not include marketing text or neighborhood descriptions.',
   'text', 'claude-sonnet-4-6', true, true, '2026-05-01 inference fix'),
  ('raw_address', 'json_components_v2',
   'Return valid JSON: {"street": string|null, "cross_streets": string|null, "city": string|null, "state": string|null, "zip": string|null, "best_address": string}. ' ||
   '"best_address" is the most specific single-line address you can produce from page content OR from the beach name itself ' ||
   '(e.g., "Anita Street Beach" → "Anita Street, Laguna Beach, CA"). Use "unknown" only if no inference is possible. No markdown.',
   'structured_json', 'claude-sonnet-4-6', false, true, '2026-05-01 inference fix');

commit;

-- Verification
select 'active variants' as label, field_name, count(*) as n
  from public.extraction_prompt_variants
 where active = true and field_name in ('dogs_leash_required','raw_address')
 group by field_name order by field_name;

select 'remaining gold-set rows' as label, count(*) as n from public.beach_policy_gold_set;
