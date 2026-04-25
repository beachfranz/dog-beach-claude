-- Sanitize parking_type in populate_from_research + cleanup leftover
-- check constraint name from the rename migration (2026-04-24).
--
-- Old beaches_staging_new used parking_type as a free-text field with
-- cost-info values like 'paid' and 'free'. Our new enum is strict:
-- (lot/street/metered/mixed/none). Map old → new where sensible:
--   'lot' / 'street' / 'mixed' / 'metered' / 'none' → pass through
--   'paid'                                          → 'metered' (best guess)
--   'free' or anything else                         → null

-- Rename the lingering parking_type check constraint (cosmetic — was missed
-- in the table rename migration because Postgres auto-named it from the
-- inline CHECK).
alter table public.locations_stage
  rename constraint us_beach_points_staging_parking_type_check
  to locations_stage_parking_type_check;

-- Helper: sanitize old parking_type text → new enum
create or replace function public.bsn_parking_type_to_enum(p_v text)
returns text
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'lot'     then 'lot'
    when 'street'  then 'street'
    when 'mixed'   then 'mixed'
    when 'metered' then 'metered'
    when 'none'    then 'none'
    when 'paid'    then 'metered'   -- best guess
    else                null         -- 'free', other freeform → null
  end;
$$;

-- Recreate populate_from_research with the sanitizer applied to parking_type
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
      bsn.dogs_allowed,
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
