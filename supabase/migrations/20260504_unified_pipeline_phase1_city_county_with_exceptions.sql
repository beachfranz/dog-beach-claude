-- 20260504_unified_pipeline_phase1_city_county_with_exceptions.sql
--
-- Two fixes to the city_policy / county_policy populators caught
-- 2026-05-04 by Franz on regression spot-check:
--
-- 1. CPAD operator override gate. A beach inside LA County that's
--    operated by CDPR (State Park) shouldn't inherit LA County's
--    default dog rule. State / Federal / Tribal / Special District
--    operators override the city/county jurisdiction.
--
-- 2. Exception lookup. operator_policy_exceptions has explicit per-beach
--    overrides — e.g. City of San Diego default is "no" but Fiesta
--    Island exception is "off_leash". Populator must emit the exception
--    when the beach name fuzzy-matches an exception row.
--
-- Order of resolution:
--   gate (skip if state/federal/tribal/SD operator)
--   exception match (emit exception rule)
--   else default rule

begin;

-- Helper: does any exception row for this operator match this beach by name?
-- Uses pg_trgm similarity (already enabled). Returns the matched rule, or
-- NULL if no exception applies. Picks the most-specific match.
create or replace function public._operator_exception_for_beach(
  p_operator_id bigint, p_beach_name text
) returns text language sql stable as $$
  with hits as (
    select rule, similarity(lower(beach_name), lower(p_beach_name)) as sim,
           length(beach_name) as len
      from public.operator_policy_exceptions
     where operator_id = p_operator_id
       and (
         -- Exact substring match in either direction
         lower(p_beach_name) like '%' || lower(beach_name) || '%'
         or lower(beach_name) like '%' || lower(p_beach_name) || '%'
         -- Or trigram similarity high enough
         or similarity(lower(beach_name), lower(p_beach_name)) >= 0.45
       )
  )
  select rule from hits
  order by sim desc, len desc  -- prefer longest exception name when tied
  limit 1;
$$;

-- City populator with exception lookup + override gate
create or replace function public.populate_from_city_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url,
           public._operator_exception_for_beach(op.id, g.name) as exception_rule,
           -- Override gate: skip if the beach has a CPAD operator at
           -- state/federal/tribal/special-district level (those override
           -- city jurisdiction rules)
           (select cu.mng_ag_lev from public.cpad_units cu
             where cu.unit_id = g.cpad_unit_id) as cpad_lev
      from public.beaches_gold g
      join public.operators op
        on op.jurisdiction_id = g.c1_jurisdiction_id
       and op.level = 'city'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.c1_jurisdiction_id is not null
  ),
  filtered as (
    -- Skip beaches whose primary CPAD operator is state/federal/tribal/SD —
    -- those operators' rules override the city jurisdiction's defaults.
    select * from matches
     where cpad_lev is null
        or cpad_lev not in ('State', 'Federal', 'Tribal', 'Special District')
  ),
  resolved as (
    -- For each beach, derive the actual rule to emit:
    --   exception_rule (if matched) OR default_rule
    select fid, beach_name, operator_name, dog_policy_url, op_conf, source_url,
           coalesce(exception_rule, default_rule) as effective_rule,
           -- exception_rule is finer-grained; bump confidence when present
           case when exception_rule is not null then 0.95 else op_conf end as conf,
           leash_required, summary, area_sand, area_water, area_picnic_area,
           area_parking_lot, area_trails, area_campground,
           designated_dog_zones, prohibited_areas, time_windows, seasonal_closures,
           exception_rule
      from filtered
  ),
  built as (
    select fid, operator_name, dog_policy_url, conf, source_url, exception_rule,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when effective_rule in ('yes','allowed','restricted','seasonal','off_leash','on_leash') then 'yes'
               when effective_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when effective_rule = 'off_leash' then 'off_leash'
               when effective_rule = 'on_leash'  then 'on_leash'
               when leash_required is true and effective_rule not in ('off_leash') then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand', area_sand, 'area_water', area_water,
             'area_picnic_area', area_picnic_area, 'area_parking_lot', area_parking_lot,
             'area_trails', area_trails, 'area_campground', area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows', time_windows, 'seasonal_closures', seasonal_closures,
             'operator_name', operator_name,
             'exception_applied', exception_rule
           )) as v
      from resolved
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'city_policy',
           coalesce(conf, 0.85), v,
           coalesce(source_url, dog_policy_url), now()
      from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

comment on function public.populate_from_city_dog_policy_gold(bigint) is
  '2026-05-04 v2. City-ordinance dog policy with exception lookup + '
  'CPAD operator override gate. Skips beaches whose CPAD operator is '
  'state/federal/tribal/special-district. For matching exceptions, '
  'emits the exception rule (off_leash/prohibited/etc) at confidence 0.95 '
  'instead of the default rule. Joins beaches_gold.c1_jurisdiction_id → '
  'operators(level=city).jurisdiction_id.';

-- County populator with same fixes
create or replace function public.populate_from_county_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url,
           public._operator_exception_for_beach(op.id, g.name) as exception_rule,
           (select cu.mng_ag_lev from public.cpad_units cu
             where cu.unit_id = g.cpad_unit_id) as cpad_lev,
           -- For county, also skip when beach is inside an incorporated city
           -- (the city's rules apply, not the county's).
           g.c1_jurisdiction_id as in_incorporated_city
      from public.beaches_gold g
      join public.operators op
        on op.county_geoid = g.county_geoid
       and op.level = 'county'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.county_geoid is not null
  ),
  filtered as (
    select * from matches
     where (cpad_lev is null or cpad_lev not in ('State','Federal','Tribal','Special District','City'))
       -- County rule applies to unincorporated land. If the beach is inside
       -- an incorporated city polygon, the city's rule supersedes.
       and in_incorporated_city is null
  ),
  resolved as (
    select fid, beach_name, operator_name, dog_policy_url, op_conf, source_url,
           coalesce(exception_rule, default_rule) as effective_rule,
           case when exception_rule is not null then 0.95 else op_conf end as conf,
           leash_required, summary, area_sand, area_water, area_picnic_area,
           area_parking_lot, area_trails, area_campground,
           designated_dog_zones, prohibited_areas, time_windows, seasonal_closures,
           exception_rule
      from filtered
  ),
  built as (
    select fid, operator_name, dog_policy_url, conf, source_url, exception_rule,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when effective_rule in ('yes','allowed','restricted','seasonal','off_leash','on_leash') then 'yes'
               when effective_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when effective_rule = 'off_leash' then 'off_leash'
               when effective_rule = 'on_leash'  then 'on_leash'
               when leash_required is true and effective_rule not in ('off_leash') then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand', area_sand, 'area_water', area_water,
             'area_picnic_area', area_picnic_area, 'area_parking_lot', area_parking_lot,
             'area_trails', area_trails, 'area_campground', area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows', time_windows, 'seasonal_closures', seasonal_closures,
             'operator_name', operator_name,
             'exception_applied', exception_rule
           )) as v
      from resolved
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'county_policy',
           coalesce(conf, 0.85), v,
           coalesce(source_url, dog_policy_url), now()
      from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

comment on function public.populate_from_county_dog_policy_gold(bigint) is
  '2026-05-04 v2. County-ordinance dog policy with exception lookup + '
  'override gate. Skips beaches whose CPAD operator is state/federal/'
  'tribal/SD/city, AND skips beaches inside an incorporated-city polygon '
  '(city rules supersede). Joins via beaches_gold.county_geoid → '
  'operators(level=county).county_geoid.';

commit;
