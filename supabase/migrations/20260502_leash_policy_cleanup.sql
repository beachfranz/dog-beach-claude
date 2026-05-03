-- Remove `not_allowed` from leash_policy enum.
--
-- Why: redundant with dogs_allowed=no (added to the curator field set
-- 2026-05-02). The leash_policy field's scope is now purely "given dogs
-- are allowed, what's the leash rule" — the binary "are dogs here at all"
-- lives in dogs_allowed. Cleaner orthogonal fields.
--
-- Existing extraction rows with parsed_value='not_allowed' stay in
-- beach_policy_extractions for audit. The curator's truth-dropdown won't
-- offer that option going forward; curator picks 'unknown' or null when
-- dogs aren't allowed.

begin;

update public.extraction_prompt_variants
   set prompt_template =
'What is the dominant leash rule for dogs at this beach (the rule that applies most of the time, in most areas)? Assume dogs ARE allowed — if they are not, leave leash_policy as unknown and use the separate dogs_allowed field instead.

Choose exactly one:
- off_leash: dogs may be off-leash freely
- on_leash: dogs must be on a leash (typically 6 feet)
- leash_optional: dogs may be on or off leash at the visitor''s choice
- varies_by_time: rule changes during the day or by season (e.g. on-leash daytime, off-leash early morning)
- unknown: source does not specify the leash rule

Return only the chosen value.',
       notes = coalesce(notes,'') || ' [not_allowed removed 2026-05-02 — redundant with dogs_allowed]'
 where field_name = 'leash_policy'
   and variant_key in ('enum_v3', 'enum_v3__sonnet', 'enum_v3__opus');

commit;

-- Verify (informational):
-- SELECT field_name, variant_key, target_model, active, is_canon
--   FROM public.extraction_prompt_variants
--  WHERE field_name = 'leash_policy' AND active
--  ORDER BY target_model;
