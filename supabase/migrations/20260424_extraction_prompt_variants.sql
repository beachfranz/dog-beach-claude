-- extraction_prompt_variants — LLM question library + calibration support
-- (2026-04-24). See project_extraction_calibration.md.
--
-- For each target field, we keep 3 variants with distinct phrasings:
--   *_direct      — plain English, enumerated answer set
--   *_json        — ask for structured JSON, easier to parse downstream
--   *_describe    — open-ended prose, good for nuance
--
-- Active variants all run per page; extraction_calibration scores each.
-- Winners get promoted to is_canon over time (empirical, per state).

create table if not exists public.extraction_prompt_variants (
  id              bigserial primary key,
  field_name      text not null,
  variant_key     text not null,
  prompt_template text not null,
  expected_shape  text not null check (expected_shape in ('enum','text','structured_json','bool')),
  active          boolean not null default true,
  is_canon        boolean not null default false,
  notes           text,
  created_at      timestamptz not null default now(),
  unique (field_name, variant_key)
);

create index if not exists extraction_prompt_variants_field_idx
  on public.extraction_prompt_variants(field_name) where active = true;

comment on table public.extraction_prompt_variants is
  'Library of LLM question variants per target field. Runner executes all active variants per page; extraction_calibration scores them; winners get is_canon=true. Per state-specific calibration preserved. See project_extraction_calibration.md.';

-- ── Tier A: dog policy fields ────────────────────────────────────────────
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, notes) values

('dogs_allowed', 'direct_enum',
 'Are dogs allowed at this beach? Reply with exactly one of: yes, no, seasonal, unclear. No other text.',
 'enum', 'Enumerated single-word answer'),
('dogs_allowed', 'json_evidence',
 'Return valid JSON with exactly these keys: {"dogs_allowed": one of "yes"|"no"|"seasonal"|"unclear", "evidence": exact quote from the source text supporting your answer (or null)}. No markdown fences.',
 'structured_json', 'JSON with evidence quote'),
('dogs_allowed', 'describe_prose',
 'In one sentence, describe whether dogs are allowed at this beach. Start with Yes, No, Seasonal, or Unclear.',
 'text', 'Prose answer with leading verdict'),

('dogs_leash_required', 'direct_enum',
 'What are the leash rules for dogs at this beach? Reply exactly one of: required, off_leash_ok, mixed_by_zone, varies_by_time, unclear. No other text.',
 'enum', 'Enumerated leash policy'),
('dogs_leash_required', 'json_structured',
 'Return valid JSON with exactly these keys: {"leash_required": one of "required"|"off_leash_ok"|"mixed_by_zone"|"varies_by_time"|"unclear", "leash_length_ft": integer or null, "evidence": exact quote or null}. No markdown fences.',
 'structured_json', 'JSON including leash-length detail'),
('dogs_leash_required', 'when_off_leash',
 'Under what circumstances, if any, may dogs be off-leash at this beach? If leash is always required, reply "leash always required". If dogs are never allowed, reply "dogs not allowed". Otherwise describe conditions.',
 'text', 'Inverse framing: asks about off-leash conditions'),

('dogs_off_leash_area', 'direct_q',
 'Is there a designated off-leash area for dogs at this beach? If yes, name it and describe its hours. If no, reply exactly: none.',
 'text', 'Plain English with structured fallback'),
('dogs_off_leash_area', 'json_structured',
 'Return valid JSON: {"off_leash_area_exists": true or false, "area_name": string or null, "hours": string or null, "notes": string or null}. No markdown fences.',
 'structured_json', 'Structured for reliable parsing'),
('dogs_off_leash_area', 'zone_describe',
 'Describe any zones or sections of this beach where dogs may be off-leash, including names, boundaries, and hours. If none exist, reply "no designated off-leash zones".',
 'text', 'Asks for enumerated zones'),

('dogs_time_restrictions', 'direct_q',
 'Are there time-of-day restrictions on when dogs may be at this beach? Reply "none" or describe the restriction.',
 'text', 'Yes/no with description'),
('dogs_time_restrictions', 'json_structured',
 'Return valid JSON: {"has_time_restriction": true or false, "allowed_hours": string or null, "prohibited_hours": string or null, "evidence": string or null}. No markdown fences.',
 'structured_json', 'Structured with both allowed and prohibited'),
('dogs_time_restrictions', 'hours_summary',
 'In one sentence, during what hours of the day are dogs allowed at this beach? Reply "all hours" if unrestricted, or "not allowed" if dogs are prohibited entirely.',
 'text', 'Inverse: asks allowed-hours not restricted-hours'),

('dogs_seasonal_restrictions', 'direct_q',
 'Are there seasonal restrictions (summer-only leash rules, nesting-season closures, etc.) on dogs at this beach? Reply "none" or describe.',
 'text', 'Yes/no with description'),
('dogs_seasonal_restrictions', 'json_structured',
 'Return valid JSON: {"has_seasonal_restriction": true or false, "description": string or null, "affected_period": string or null}. No markdown fences.',
 'structured_json', 'Structured format'),
('dogs_seasonal_restrictions', 'year_round_q',
 'Does the dog policy at this beach stay the same year-round, or change seasonally? Reply "same year-round" or describe the seasonal variation.',
 'text', 'Inverse framing: asks about constancy vs change'),

-- ── Tier B minimal — access + drinking water (dog-relevant amenity) ──────

('access_rule', 'direct_enum',
 'How is access to this beach regulated? Reply exactly one of: free, paid_parking, paid_access, permit, members_only, mixed, unclear. No other text.',
 'enum', 'Access-type enumeration'),
('access_rule', 'json_structured',
 'Return valid JSON: {"access_type": one of "free"|"paid_parking"|"paid_access"|"permit"|"members_only"|"mixed"|"unclear", "fee_text": string or null, "parking_type": string or null}. No markdown fences.',
 'structured_json', 'Structured with fee detail'),
('access_rule', 'cost_q',
 'Is there a fee or permit required to access this beach or its parking? Reply "free" or describe the cost.',
 'text', 'Cost framing'),

('has_drinking_water', 'direct_bool',
 'Is drinking water (potable fountains or fill stations) available at this beach? Reply exactly: yes, no, or unclear.',
 'bool', 'Binary yes/no/unclear'),
('has_drinking_water', 'json_structured',
 'Return valid JSON: {"drinking_water_available": true, false, or null, "evidence": exact quote or null}. No markdown fences.',
 'structured_json', 'Structured with evidence'),
('has_drinking_water', 'amenity_list',
 'List the amenities at this beach related to water fountains, hydration, restrooms, or similar facilities.',
 'text', 'Indirect: ask for amenity list and parse');

-- Count by field
-- (Run separately if you want to verify: select field_name, count(*) from extraction_prompt_variants group by field_name)
