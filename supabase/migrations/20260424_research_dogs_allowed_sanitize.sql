-- Sanitize dogs_allowed in populate_from_research (2026-04-24)
--
-- Old beaches_staging_new used dogs_allowed='mixed' (480 rows) to mean
-- "allowed in some times/areas, not others." New enum dropped 'mixed'
-- in favor of 'restricted' since the structured dogs_seasonal_rules and
-- dogs_zone_description fields capture the nuance.
--
-- Mapping: yes→yes, no→no, seasonal→seasonal, unknown→unknown,
--          mixed→restricted, anything else→null

create or replace function public.bsn_dogs_allowed_to_enum(p_v text)
returns text
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'yes'        then 'yes'
    when 'no'         then 'no'
    when 'seasonal'   then 'seasonal'
    when 'restricted' then 'restricted'
    when 'unknown'    then 'unknown'
    when 'mixed'      then 'restricted'   -- old vocabulary → new
    else                   null
  end;
$$;

create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with bsn as (
    select
      bsn.src_fid as fid,
      public.bsn_confidence_to_numeric(bsn.enrichment_confidence) as confidence,
      public.bsn_dogs_allowed_to_enum(bsn.dogs_allowed) as dogs_allowed,
      public.bsn_leash_to_enum(bsn.dogs_leash_required) as leash_required,
      bsn.dogs_daily_windows,
      bsn.dogs_seasonal_closures,
      coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area) as zone_description,
      bsn.dogs_policy_notes,
      bsn.hours_text,
      bsn.has_parking,
      public.bsn_parking_type_to_enum(bsn.parking_type) as parking_type,
      bsn.parking_notes,
      bsn.has_restrooms, bsn.has_showers, bsn.has_lifeguards, bsn.has_picnic_area,
      bsn.has_food, bsn.has_drinking_water, bsn.has_fire_pits, bsn.has_disabled_access
    from public.beaches_staging_new bsn
    join public.locations_stage s on s.fid = bsn.src_fid
    where bsn.src_fid is not null
      and (p_fid is null or bsn.src_fid = p_fid)
  ),
  dogs_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   leash_required,
        'restricted_hours', dogs_daily_windows,
        'seasonal_rules',
          case
            when dogs_seasonal_closures is null
              or jsonb_typeof(dogs_seasonal_closures) <> 'array'
              or jsonb_array_length(dogs_seasonal_closures) = 0
            then null
            else (
              select jsonb_agg(jsonb_build_object(
                'from',  e->>'start',
                'to',    e->>'end',
                'notes', e->>'reason'
              ))
              from jsonb_array_elements(dogs_seasonal_closures) e
            )
          end,
        'zone_description', zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from bsn
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'dogs', 'research', confidence, v, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  practical_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_lifeguards',     has_lifeguards,
        'has_picnic_area',    has_picnic_area,
        'has_food',           has_food,
        'has_drinking_water', has_drinking_water,
        'has_fire_pits',      has_fire_pits,
        'has_disabled_access',has_disabled_access
      )) as v
    from bsn
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'practical', 'research', confidence, v, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
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
