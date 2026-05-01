-- Redesign access/parking field semantics.
-- The legacy `access_rule` / `access_text` / `has_parking` / `parking_type`
-- variants all collapsed parking logistics under "access" wording, leaving
-- no field that answered "is this beach accessible to a person/dog at all?"
--
-- New schema:
--   public_access    enum: yes / no / restricted / unclear
--   access_text      text: prose nuance (boat-only, hike-in, gated, permit Apr-Sep, ...)
--   parking_type     enum: lot / street / both / none / unclear
--   parking_payment  enum: free / paid / mixed / unclear
--
-- Dropped:
--   access_rule  (was a confused mix of payment + permit + access)
--   has_parking  (subsumed by parking_type with `none` as the no-parking value)
--
-- Existing extracted rows for access_rule + has_parking stay in
-- beach_policy_extractions as audit history but are no longer surfaced.
-- access_text + parking_type rows for the 11 gold-set beaches are deleted
-- so re-extraction populates fresh under the new prompts.

begin;

-- 1. Deactivate all current variants for the 4 affected field_names.
update public.extraction_prompt_variants
   set active = false,
       notes  = coalesce(notes, '') || ' [deprecated 2026-05-02 access/parking redesign]'
 where field_name in ('access_rule', 'access_text', 'has_parking', 'parking_type');

-- 2. Insert new variants.
--    Naming: variant_key uses an _v2 suffix to make it obvious which generation a row came from.

-- public_access (new field, enum)
insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('public_access', 'direct_enum_v2',
   'Is this beach open to the general public? Reply exactly one of: yes, no, restricted, unclear. "yes" = anyone can show up and walk on. "restricted" = open but with a real barrier (boat-only, permit required, hike-only, gated, seasonal). "no" = not publicly accessible (private property, military). No other text.',
   'enum', 'claude-haiku-4-5-20251001', true, true, '2026-05-02 redesign'),
  ('public_access', 'json_evidence_v2',
   'Return valid JSON: {"access": one of "yes"|"no"|"restricted"|"unclear", "barrier": string|null, "evidence": string|null}. The "barrier" field describes any access barrier in 1 short phrase ("boat-only", "permit required", "private gated", "1-mile hike-in", "seasonal Apr-Sep") or null if access is unrestricted. No markdown.',
   'structured_json', 'claude-sonnet-4-6', false, true, '2026-05-02 redesign'),
  ('public_access', 'describe_q_v2',
   'In one sentence, can a member of the public reach this beach today, and if there are any constraints, what are they? Examples of constraints: boat-only, permit required, hike-in, gated/private, seasonal closure.',
   'text', 'claude-sonnet-4-6', false, true, '2026-05-02 redesign');

-- access_text (rewritten — beach access nuance, NOT parking)
insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('access_text', 'direct_q_v2',
   'In one to two sentences, describe how a person reaches this beach. Cover any access barriers: boat-only, hike-in distance, permit/reservation required, private/gated, seasonal closures, ADA limitations. Do NOT discuss parking — that is a separate field. Say "open public access" if there are no barriers.',
   'text', 'claude-sonnet-4-6', true, true, '2026-05-02 redesign'),
  ('access_text', 'describe_prose_v2',
   'Describe the beach access situation in plain prose. Focus on: how visitors physically get to the sand, any barriers (private land, hike, boat, permit, fee), and any time-windowed restrictions. Skip parking — that is captured separately.',
   'text', 'claude-sonnet-4-6', false, true, '2026-05-02 redesign');

-- parking_type (rewritten — location only, payment is separate)
insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('parking_type', 'direct_enum_v2',
   'What type of parking is available at this beach? Reply exactly one of: lot, street, both, none, unclear. "lot" = dedicated parking lot. "street" = on-street only. "both" = lot AND street. "none" = no parking. Ignore whether it costs money — payment is a separate question. No other text.',
   'enum', 'claude-haiku-4-5-20251001', true, true, '2026-05-02 redesign'),
  ('parking_type', 'json_evidence_v2',
   'Return valid JSON: {"type": one of "lot"|"street"|"both"|"none"|"unclear", "lot_name": string|null, "evidence": string|null}. Ignore cost; that is a separate field. No markdown.',
   'structured_json', 'claude-sonnet-4-6', false, true, '2026-05-02 redesign');

-- parking_payment (new field, enum)
insert into public.extraction_prompt_variants (field_name, variant_key, prompt_template, expected_shape, target_model, is_canon, active, notes) values
  ('parking_payment', 'direct_enum_v2',
   'Is parking at this beach free or paid? Reply exactly one of: free, paid, mixed, unclear. "free" = no charge. "paid" = pay-to-park (meter, kiosk, attended). "mixed" = some free + some paid (e.g., free street + paid lot). "unclear" = source does not say. No other text.',
   'enum', 'claude-haiku-4-5-20251001', true, true, '2026-05-02 redesign'),
  ('parking_payment', 'json_evidence_v2',
   'Return valid JSON: {"payment": one of "free"|"paid"|"mixed"|"unclear", "hourly_usd": number|null, "daily_usd": number|null, "evidence": string|null}. No markdown.',
   'structured_json', 'claude-sonnet-4-6', false, true, '2026-05-02 redesign');

-- 3. Clear old extractions for the 4 affected field_names on the 11 gold-set arena_groups
--    so the re-extraction run produces a clean slate.
delete from public.beach_policy_extractions
 where field_name in ('access_rule', 'access_text', 'has_parking', 'parking_type')
   and arena_group_id in (453, 8606, 6202, 8560, 8358, 6212, 8901, 8453, 3671, 2078, 6411);

-- Also clear stale gold-set truth rows for `access_rule` and `has_parking` (these
-- field names are gone). Keep `access_text` and `parking_type` truth rows null —
-- they will be re-curated against new prompts.
delete from public.beach_policy_gold_set
 where field_name in ('access_rule', 'has_parking');

-- 4. Reload extracted_at to NULL trigger? No — extractions table is event log, not state.

commit;

-- Verification
select 'active variants for redesigned fields' as check_label,
       field_name, count(*) as n
  from public.extraction_prompt_variants
 where active = true and field_name in ('public_access','access_text','parking_type','parking_payment')
 group by field_name order by field_name;

select 'remaining old extractions (should be 0)' as check_label,
       field_name, count(*) as n
  from public.beach_policy_extractions
 where field_name in ('access_rule','access_text','has_parking','parking_type')
   and arena_group_id in (453, 8606, 6202, 8560, 8358, 6212, 8901, 8453, 3671, 2078, 6411)
 group by field_name;
