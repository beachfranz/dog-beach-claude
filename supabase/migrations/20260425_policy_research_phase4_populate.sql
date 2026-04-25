-- Phase 4 of POLICY_RESEARCH_MIGRATION (2026-04-25)
--
-- Replace populate_from_research with a new version that reads from
-- policy_research_extractions instead of beaches_staging_new. Origin
-- column on the new table maps to source value emitted in evidence rows:
--   'v2_dog_policy_old'  → source='old_school_llm'
--   'v2_dog_policy_v2'   → source='research'
--   'manual'             → source='manual'
--
-- Companion cleanup: delete existing source='research' rows in
-- beach_enrichment_provenance because they'll be re-emitted with
-- source='old_school_llm' by the new function. Without this, we'd have
-- duplicate evidence (same data, two different source labels) which
-- the resolver would treat as multi-source agreement boost — misleading.
--
-- Risk: medium. Tested by re-running and comparing to golden baseline.

create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.policy_research_extractions
    where extraction_status in ('success', 'imported_legacy')
      and (p_fid is null or fid = p_fid)
  ),
  -- Map origin → source value emitted in evidence rows
  tagged as (
    select *,
      case origin
        when 'v2_dog_policy_old' then 'old_school_llm'
        when 'v2_dog_policy_v2'  then 'research'
        when 'manual'            then 'manual'
      end as evidence_source
    from successful
  ),
  -- Build dogs jsonb (only emit when at least one dog field is set)
  dogs_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from tagged
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- Build practical jsonb (hours + parking + amenities)
  practical_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from tagged
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'practical', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_research(int) is
  'Layer-2 populator: emits dogs + practical evidence rows from policy_research_extractions. Origin column on the source row maps to the evidence source: v2_dog_policy_old → old_school_llm; v2_dog_policy_v2 → research; manual → manual. Phase 4 of POLICY_RESEARCH_MIGRATION (2026-04-25). Replaces the prior version which read from beaches_staging_new.';

-- Clean up: delete redundant source='research' evidence rows. They'll be
-- re-emitted with source='old_school_llm' when populate_from_research runs.
-- This is reversible by restoring the prior populate_from_research function
-- (which read beaches_staging_new and wrote source='research') and re-running.
delete from public.beach_enrichment_provenance
 where source = 'research';
