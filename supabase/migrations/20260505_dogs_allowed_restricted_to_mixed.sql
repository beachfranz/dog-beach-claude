-- 20260505_dogs_allowed_restricted_to_mixed.sql
--
-- Vocabulary cleanup at the BEP and consensus layers: drop `restricted`
-- as a `dogs_allowed` value, fold it into `mixed` (which already has the
-- "rules vary by area/section" semantic). Two values that meant
-- approximately the same thing — pick one canonical name.
--
-- WHY THIS MATTERS:
-- _consensus_normalize was silently mapping `restricted` → `yes` for the
-- dogs_allowed field. That collapse is the structural cause of the
-- conf=1.00 audit mismatches:
--   - McGrath State Beach: sources say `restricted` (sand prohibited;
--     campground allowed). Normalizer mapped → `yes`. Consensus winner
--     = `yes`. Consumer surface says `yes`. User reads "yes," walks to
--     sand, gets a citation.
--   - Emma Wood / Torrance / Blacks / South Mission / Natural Bridges
--     all the same pattern.
--
-- After this migration `restricted` no longer exists in the dogs vocab;
-- `mixed` carries the "rules vary" signal. The promoter compression
-- of `mixed` to consumer-surface 3-value vocab (yes/no/seasonal) is a
-- separate decision tracked downstream — for now `mixed` flows through
-- consensus untouched; the promoter handles it next.

begin;

-- ── 1. Update _consensus_normalize so future BEP inserts of `restricted`
--       normalize to `mixed` (not `yes`). Defense-in-depth in case any
--       extraction prompt still emits the old word.
create or replace function public._consensus_normalize(p_field text, p_value text)
returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes'
        when 'no'  then 'no'
        when 'restricted' then 'mixed'   -- WAS: 'yes' (the bug)
        when 'seasonal' then 'seasonal'
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

-- ── 2. Rewrite existing BEP rows: claimed_values.allowed = 'restricted' → 'mixed'.
--       BEP is the curated-projection layer; underlying source extractions
--       in policy_research_extractions / park_url_extractions etc. retain
--       their original wording.
update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(claimed_values, '{allowed}', '"mixed"'::jsonb),
       updated_at = now()
 where field_group = 'dogs'
   and claimed_values->>'allowed' = 'restricted';

-- ── 3. Snapshot affected fids for re-consensus.
create temp table _affected_fids on commit drop as
  select distinct gold_fid from public.beach_enrichment_provenance
   where gold_fid is not null and field_group = 'dogs';

-- ── 4. Recompute consensus + auto-promote for every affected beach.
--       The normalizer change means winning_value can shift on any beach
--       that had restricted in evidence; safest path is recompute all.
do $do$
declare fid_iter bigint;
begin
  for fid_iter in select gold_fid from _affected_fids loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;
end $do$;

commit;
