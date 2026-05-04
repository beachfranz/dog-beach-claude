-- 20260503_harmony_phase8_8b_calibration_normalized.sql
--
-- Phase 8 slice 8b: vocabulary-normalized version of the
-- gold_dogs_allowed_calibration view.
--
-- The strict view (slice 8) showed low agreement % because the curator
-- vocabulary (yes / no / unclear / seasonal) and extractor vocabulary
-- (yes / no / restricted / seasonal) didn't line up — extractor's
-- "restricted" was the dominant value, curator's was "yes."
--
-- Per Franz's locked rule (project_dog_policy_exceptions_canonical.md
-- and EOD 2026-05-02 conversation): "dogs_allowed is always binary —
-- any allowed = yes, nuance goes in notes." Apply that mapping to both
-- sides before comparison:
--   yes / restricted / seasonal / allowed   → 'yes'
--   no / prohibited                         → 'no'
--   unclear / unknown / null                → null (not binarizable; skip)
--
-- The strict view stays for vocabulary-mismatch debugging; this
-- normalized view is the operational one for "is the extractor
-- actually trustworthy."

begin;

create or replace function public._normalize_dogs_allowed(v text)
returns text
language sql immutable
as $$
  select case lower(trim(v))
    when 'yes'        then 'yes'
    when 'restricted' then 'yes'
    when 'seasonal'   then 'yes'
    when 'allowed'    then 'yes'
    when 'no'         then 'no'
    when 'prohibited' then 'no'
    else                   null
  end
$$;

create or replace view public.gold_dogs_allowed_calibration_binary as
with truth as (
  select arena_group_id as gold_fid,
         lower(trim(verified_value))                  as truth_raw,
         public._normalize_dogs_allowed(verified_value) as truth_binary,
         curator_confidence
    from public.beach_policy_gold_set
   where field_name = 'dogs_allowed'
     and verified_value is not null
),
evidence as (
  select e.gold_fid,
         e.source,
         lower(trim(e.claimed_values->>'allowed'))                as extracted_raw,
         public._normalize_dogs_allowed(e.claimed_values->>'allowed') as extracted_binary,
         e.confidence,
         e.is_canonical
    from public.beach_enrichment_provenance e
   where e.field_group = 'dogs'
     and e.gold_fid is not null
     and e.claimed_values ? 'allowed'
)
select e.gold_fid,
       g.name as beach_name,
       e.source,
       t.truth_raw,
       t.truth_binary,
       t.curator_confidence,
       e.extracted_raw,
       e.extracted_binary,
       e.confidence as extraction_confidence,
       e.is_canonical,
       (t.truth_binary = e.extracted_binary)         as agreement,
       (t.truth_binary is null or e.extracted_binary is null) as inconclusive
  from truth t
  join evidence e on e.gold_fid = t.gold_fid
  join public.beaches_gold g on g.fid = e.gold_fid;

comment on view public.gold_dogs_allowed_calibration_binary is
  'Phase 8 slice 8b. Vocabulary-normalized version of gold_dogs_allowed_calibration. Both sides mapped to binary yes/no via _normalize_dogs_allowed (yes/restricted/seasonal/allowed → yes; no/prohibited → no; unclear/unknown/null → null). The "agreement" column is the real signal; "inconclusive" rows are where one side could not be binarized.';

commit;
