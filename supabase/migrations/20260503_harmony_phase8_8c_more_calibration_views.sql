-- 20260503_harmony_phase8_8c_more_calibration_views.sql
--
-- Phase 8 slice 8c: extend the calibration framework to two more fields
-- (leash_policy + lifeguard). Same shape as the dogs_allowed binary view.
--
-- leash_policy mapping:
--   curator vocab:  on_leash / off_leash / varies_by_time
--   evidence path:  field_group='dogs', claimed_values->>'leash_required'
--                   (legacy text: 'required' / 'optional' / null
--                    or boolean serialized as 'true'/'false')
--   binary:         leash | no_leash | null
--
-- lifeguard mapping:
--   curator vocab:  full_time / seasonal / none / unknown
--   evidence path:  field_group='practical', (claimed_values->>'has_lifeguards')::boolean
--   binary:         has | no | null

begin;

-- Normalizer helpers
create or replace function public._normalize_leash_policy(v text)
returns text language sql immutable as $$
  select case lower(trim(v))
    when 'on_leash'        then 'leash'
    when 'on-leash'        then 'leash'
    when 'required'        then 'leash'
    when 'true'            then 'leash'
    when 'off_leash'       then 'no_leash'
    when 'off-leash'       then 'no_leash'
    when 'optional'        then 'no_leash'
    when 'false'           then 'no_leash'
    else                        null
  end
$$;

create or replace function public._normalize_lifeguard(v text)
returns text language sql immutable as $$
  select case lower(trim(v))
    when 'full_time' then 'has'
    when 'seasonal'  then 'has'
    when 'true'      then 'has'
    when 'none'      then 'no'
    when 'false'     then 'no'
    else                  null
  end
$$;

-- View: leash_policy calibration
create or replace view public.gold_leash_policy_calibration_binary as
with truth as (
  select arena_group_id as gold_fid,
         lower(trim(verified_value))                  as truth_raw,
         public._normalize_leash_policy(verified_value) as truth_binary,
         curator_confidence
    from public.beach_policy_gold_set
   where field_name = 'leash_policy'
     and verified_value is not null
),
evidence as (
  select e.gold_fid,
         e.source,
         lower(trim(e.claimed_values->>'leash_required'))                as extracted_raw,
         public._normalize_leash_policy(e.claimed_values->>'leash_required') as extracted_binary,
         e.confidence,
         e.is_canonical
    from public.beach_enrichment_provenance e
   where e.field_group = 'dogs'
     and e.claimed_values ? 'leash_required'
)
select e.gold_fid, g.name as beach_name, e.source,
       t.truth_raw, t.truth_binary, t.curator_confidence,
       e.extracted_raw, e.extracted_binary,
       e.confidence as extraction_confidence, e.is_canonical,
       (t.truth_binary = e.extracted_binary)         as agreement,
       (t.truth_binary is null or e.extracted_binary is null) as inconclusive
  from truth t
  join evidence e on e.gold_fid = t.gold_fid
  join public.beaches_gold g on g.fid = e.gold_fid;

-- View: lifeguard calibration
create or replace view public.gold_lifeguard_calibration_binary as
with truth as (
  select arena_group_id as gold_fid,
         lower(trim(verified_value))                  as truth_raw,
         public._normalize_lifeguard(verified_value)  as truth_binary,
         curator_confidence
    from public.beach_policy_gold_set
   where field_name = 'lifeguard'
     and verified_value is not null
),
evidence as (
  select e.gold_fid,
         e.source,
         lower(trim(e.claimed_values->>'has_lifeguards'))                as extracted_raw,
         public._normalize_lifeguard(e.claimed_values->>'has_lifeguards') as extracted_binary,
         e.confidence,
         e.is_canonical
    from public.beach_enrichment_provenance e
   where e.field_group = 'practical'
     and e.claimed_values ? 'has_lifeguards'
)
select e.gold_fid, g.name as beach_name, e.source,
       t.truth_raw, t.truth_binary, t.curator_confidence,
       e.extracted_raw, e.extracted_binary,
       e.confidence as extraction_confidence, e.is_canonical,
       (t.truth_binary = e.extracted_binary)         as agreement,
       (t.truth_binary is null or e.extracted_binary is null) as inconclusive
  from truth t
  join evidence e on e.gold_fid = t.gold_fid
  join public.beaches_gold g on g.fid = e.gold_fid;

comment on view public.gold_leash_policy_calibration_binary is
  'Phase 8 slice 8c. Per-source comparison of leash_policy extraction vs curator gold-set truth. Binary normalized: on_leash/required/true → leash; off_leash/optional/false → no_leash; varies_by_time/null → null.';

comment on view public.gold_lifeguard_calibration_binary is
  'Phase 8 slice 8c. Per-source comparison of has_lifeguards extraction vs curator lifeguard truth. Binary normalized: full_time/seasonal/true → has; none/false → no; unknown/null → null.';

commit;
