-- 20260507_has_parking_landing.sql
--
-- Land the has_parking consensus signal into beach_amenities. The
-- OSM amenity emitter (osm_amenities_v1) votes has_parking=true for
-- 607 beaches via spatial intersect with osm_amenities (parking
-- nodes within 150m). The vote was sitting in beach_field_consensus
-- with no column to land in — the existing practical promoter
-- whitelisted only the older sources and stored parking only as the
-- categorical parking_type column.
--
-- Adds has_parking boolean to beach_amenities + a consensus-driven
-- promote shim that lands has_parking + the other binary amenity
-- flags from consensus, complementing the legacy is_canonical-row
-- promoter (which still handles parking_type / hours_text).
--
-- Idempotent.

begin;

-- ============================================================
-- 1. Schema
-- ============================================================
alter table public.beach_amenities
  add column if not exists has_parking boolean;

-- ============================================================
-- 2. Consensus → beach_amenities shim
-- Reads winning_value for each binary practical field from
-- beach_field_consensus and upserts into beach_amenities. Doesn't
-- touch parking_type / hours_text / source / notes.
-- ============================================================
create or replace function public.promote_practical_consensus_binaries(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint)
language plpgsql as $$
declare ins int := 0; upd int := 0;
begin
  with consensus as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'has_parking')        as v_parking,
      max(winning_value) filter (where field_name = 'has_restrooms')      as v_restrooms,
      max(winning_value) filter (where field_name = 'has_showers')        as v_showers,
      max(winning_value) filter (where field_name = 'has_picnic_area')    as v_picnic,
      max(winning_value) filter (where field_name = 'has_drinking_water') as v_water,
      max(winning_value) filter (where field_name = 'has_food')           as v_food,
      max(winning_value) filter (where field_name = 'has_lifeguards')     as v_lifeguards,
      max(winning_value) filter (where field_name = 'has_fire_pits')      as v_fire_pits,
      max(winning_value) filter (where field_name = 'has_disabled_access') as v_disabled
      from public.beach_field_consensus
     where field_group = 'practical' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  derived as (
    select gold_fid as fid,
      public._norm_bool(to_jsonb(v_parking))    as new_has_parking,
      public._norm_bool(to_jsonb(v_restrooms))  as new_has_restrooms,
      public._norm_bool(to_jsonb(v_showers))    as new_has_showers,
      public._norm_bool(to_jsonb(v_picnic))     as new_has_picnic_area,
      public._norm_bool(to_jsonb(v_water))      as new_has_drinking_water,
      public._norm_bool(to_jsonb(v_food))       as new_has_food,
      public._norm_bool(to_jsonb(v_lifeguards)) as new_has_lifeguards,
      public._norm_bool(to_jsonb(v_fire_pits))  as new_has_fire_pits,
      public._norm_bool(to_jsonb(v_disabled))   as new_has_disabled_access
    from consensus
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upserted as (
    insert into public.beach_amenities
      (arena_group_id, has_parking, has_restrooms, has_showers, has_picnic_area,
       has_drinking_water, has_food, has_lifeguards, has_fire_pits, has_disabled_access,
       source, curated_at, notes)
    select d.fid,
      d.new_has_parking, d.new_has_restrooms, d.new_has_showers, d.new_has_picnic_area,
      d.new_has_drinking_water, d.new_has_food, d.new_has_lifeguards, d.new_has_fire_pits,
      d.new_has_disabled_access,
      'auto_promoted_from_consensus', now(),
      'Phase 1C consensus promote (binaries from beach_field_consensus)'
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_has_parking is not null or d.new_has_restrooms is not null
         or d.new_has_showers is not null or d.new_has_picnic_area is not null
         or d.new_has_drinking_water is not null or d.new_has_food is not null
         or d.new_has_lifeguards is not null or d.new_has_fire_pits is not null
         or d.new_has_disabled_access is not null)
    on conflict (arena_group_id) do update
      set has_parking         = coalesce(excluded.has_parking,         beach_amenities.has_parking),
          has_restrooms       = coalesce(excluded.has_restrooms,       beach_amenities.has_restrooms),
          has_showers         = coalesce(excluded.has_showers,         beach_amenities.has_showers),
          has_picnic_area     = coalesce(excluded.has_picnic_area,     beach_amenities.has_picnic_area),
          has_drinking_water  = coalesce(excluded.has_drinking_water,  beach_amenities.has_drinking_water),
          has_food            = coalesce(excluded.has_food,            beach_amenities.has_food),
          has_lifeguards      = coalesce(excluded.has_lifeguards,      beach_amenities.has_lifeguards),
          has_fire_pits       = coalesce(excluded.has_fire_pits,       beach_amenities.has_fire_pits),
          has_disabled_access = coalesce(excluded.has_disabled_access, beach_amenities.has_disabled_access),
          curated_at          = now()
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted)
    into ins, upd from upserted;
  return query select ins::bigint, upd::bigint;
end $$;

grant execute on function public.promote_practical_consensus_binaries(bigint)
  to authenticated, service_role;

-- ============================================================
-- 3. One-shot run
-- ============================================================
select public.promote_practical_consensus_binaries(null);

commit;
