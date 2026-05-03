-- Split parking_type into parking_lot + parking_street.
--
-- Why: 57% of recent parking_type extractions came back 'unclear' across
-- Haiku/Sonnet/Opus. The single combined enum (lot/street/both/none/unclear)
-- doesn't capture cost, which is the load-bearing distinction for visitors.
-- Splitting into two narrower enums (each free/paid/none/unclear) gives
-- the LLM a simpler question per call and captures every combination
-- without enum-explosion.
--
-- Cross-model: 3 models × 2 fields = 6 new variants. All is_canon=true.
-- Old parking_type variants deactivated; historical extractions remain in
-- beach_policy_extractions for audit but the curator filters them out via
-- the active-variant allowlist in admin-list-gold-candidates.

begin;

insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('parking_lot', 'enum_v1',
   'Does this beach have a designated parking lot for visitors? If yes, is it free or charges a fee?

Choose exactly one:
- free: parking lot exists and is free of charge
- paid: parking lot exists and charges a fee (day-use, hourly, metered, $$)
- none: no parking lot — visitors must use street parking only
- unclear: the source does not specify whether a lot exists or its cost

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Parking-lot extraction (cost-aware). Replaces parking_type with a narrower question.'),

  ('parking_street', 'enum_v1',
   'Is street parking available near this beach access (curbside parking on adjacent streets)? If yes, is it free or paid (metered, time-limited with fee, or permit-required)?

Choose exactly one:
- free: street parking is available and free of charge
- paid: street parking exists but is metered, time-limited with a fee, or permit-required
- none: no street parking near the access — must use a lot
- unclear: the source does not specify

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Street-parking extraction (cost-aware). Replaces parking_type with a narrower question.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape,
      notes = excluded.notes;

-- Cross-model partners (Sonnet + Opus) — same prompts, different models.
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
select field_name, variant_key || '__' || tag, prompt_template, expected_shape, model, true, true,
       'Cross-model partner — parking split tri-model bake-off'
  from public.extraction_prompt_variants v
  cross join (values
    ('sonnet', 'claude-sonnet-4-6'),
    ('opus',   'claude-opus-4-7')
  ) as m(tag, model)
 where v.field_name in ('parking_lot','parking_street')
   and v.variant_key = 'enum_v1'
on conflict (field_name, variant_key) do nothing;

-- Deactivate the old parking_type variants (single-axis enum, no cost info).
update public.extraction_prompt_variants
   set active = false, is_canon = false,
       notes = coalesce(notes,'') || ' [superseded 2026-05-02 by parking_lot + parking_street split — 57% unclear rate on combined enum]'
 where field_name = 'parking_type';

commit;

-- Sanity check (informational):
-- SELECT field_name, variant_key, target_model, is_canon, active
--   FROM public.extraction_prompt_variants
--  WHERE field_name IN ('parking_type','parking_lot','parking_street')
--  ORDER BY field_name, active DESC, target_model;
