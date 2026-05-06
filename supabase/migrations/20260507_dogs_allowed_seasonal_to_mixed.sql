-- 20260507_dogs_allowed_seasonal_to_mixed.sql
--
-- Per Franz 2026-05-07: retire `seasonal` as a `dogs_allowed` output
-- value everywhere. Map to `mixed` for now. Real seasonality semantics
-- live in the structured `zone_rules.sections.<section>.time_windows`
-- and `zone_rules.seasonal_closures` jsonb keys — those keep their
-- granular meaning. The flat-field summary loses the `seasonal` enum.
--
-- Same shape as 20260505_dogs_allowed_restricted_to_mixed.sql.

begin;

-- ── 1. Update _consensus_normalize so future BEP `seasonal` votes map to `mixed`.
create or replace function public._consensus_normalize(p_field text, p_value text)
returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes'
        when 'no'  then 'no'
        when 'restricted' then 'mixed'   -- retired 2026-05-05
        when 'seasonal'   then 'mixed'   -- retired 2026-05-07
        when 'mixed' then 'mixed'
        else null end
      when 'leash_policy' then case lower(trim(p_value))
        when 'on_leash' then 'on_leash' when 'off_leash' then 'off_leash'
        when 'mixed' then 'mixed' when 'mixed_by_zone' then 'mixed'
        when 'required' then 'on_leash' when 'optional' then 'off_leash'
        when 'varies_by_time' then 'varies_by_time'
        else null end
      when 'has_lifeguards' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_restrooms' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_parking' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_showers' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_disabled_access' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      else lower(trim(p_value))
    end
  end;
$$;

-- ── 2. Rewrite existing BEP rows: claimed_values.allowed = 'seasonal' → 'mixed'.
update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(claimed_values, '{allowed}', '"mixed"'::jsonb),
       updated_at = now()
 where field_group = 'dogs'
   and claimed_values->>'allowed' = 'seasonal';

-- ── 3. Update existing source-of-truth tables.
update public.cpad_unit_dogs_policy
   set dogs_allowed = 'mixed' where dogs_allowed = 'seasonal';
update public.cpad_unit_dogs_policy
   set default_rule = 'mixed' where default_rule = 'seasonal';
update public.pad_us_unit_dogs_policy
   set dogs_allowed = 'mixed' where dogs_allowed = 'seasonal';
update public.pad_us_unit_dogs_policy
   set default_rule = 'mixed' where default_rule = 'seasonal';

-- ── 4. Drop `seasonal` from CHECK constraints across the policy stack.
alter table public.cpad_unit_dogs_policy
  drop constraint if exists cpad_unit_dogs_policy_dogs_allowed_check,
  drop constraint if exists cpad_unit_dogs_policy_default_rule_check;
alter table public.cpad_unit_dogs_policy
  add constraint cpad_unit_dogs_policy_dogs_allowed_check
    check (dogs_allowed = any (array['yes','no','mixed','unknown'])),
  add constraint cpad_unit_dogs_policy_default_rule_check
    check (default_rule = any (array['yes','no','mixed','unknown']));

alter table public.pad_us_unit_dogs_policy
  drop constraint if exists pad_us_unit_dogs_policy_dogs_allowed_check,
  drop constraint if exists pad_us_unit_dogs_policy_default_rule_check;
alter table public.pad_us_unit_dogs_policy
  add constraint pad_us_unit_dogs_policy_dogs_allowed_check
    check (dogs_allowed = any (array['yes','no','mixed','unknown'])),
  add constraint pad_us_unit_dogs_policy_default_rule_check
    check (default_rule is null or default_rule = any (array['yes','no','mixed','unknown']));

-- ── 5. Recompute consensus + auto-promote for the affected fids.
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where field_group = 'dogs' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_dogs_to_beach_dog_policy(fid_iter, 0.5);
  end loop;
end $do$;

commit;
