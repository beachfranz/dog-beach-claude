-- 20260503_harmony_phase6_3c2a_park_url_gold.sql
--
-- Phase 6.3c part 2a of the harmony migration. Un-defers the SIMPLER
-- half of populate_from_park_url_gold — the dogs + practical evidence
-- emission. The buffer-rescued governance attribution path
-- (joins us_beach_points + beach_cpad_candidates, both legacy-fid)
-- stays deferred as part 2b.
--
-- Reads park_url_extractions filtered by extraction_status='success'.
-- Source name = 'park_url'. Includes the audit columns
-- (cpad_unit_name, extraction_type, cpad_role) on the evidence row
-- like the legacy emitter does.

begin;

create or replace function public.populate_from_park_url_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and arena_group_id is not null
      and (p_fid is null or arena_group_id = p_fid)
  ),
  -- Pick best row per beach (gold unique constraint can't include source_url)
  ranked as (
    select *, row_number() over (
      partition by arena_group_id
      order by extraction_confidence desc nulls last, scraped_at desc nulls last
    ) as rnk
    from successful
  ),
  best as (select * from ranked where rnk = 1),
  dogs_built as (
    select arena_group_id, source_url, scraped_at, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from best
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, cpad_role, updated_at)
    select arena_group_id, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type,
      public._cpad_role(cpad_unit_name), now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          source_url      = excluded.source_url,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role       = excluded.cpad_role,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
  practical_built as (
    select arena_group_id, source_url, extraction_confidence,
           cpad_unit_name, extraction_type,
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
    from best
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, cpad_role, updated_at)
    select arena_group_id, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type,
      public._cpad_role(cpad_unit_name), now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          source_url      = excluded.source_url,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role       = excluded.cpad_role,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url_gold(bigint) is
  'Phase 6.3c part 2a of harmony migration. Reads park_url_extractions (arena_group_id is gold_fid), emits dogs + practical evidence keyed on gold_fid with audit columns (cpad_unit_name, extraction_type, cpad_role). Includes the audit columns from the legacy emitter. The buffer-rescued governance attribution path remains deferred as part 2b — needs gold-spine equivalents of us_beach_points + beach_cpad_candidates.';

commit;
