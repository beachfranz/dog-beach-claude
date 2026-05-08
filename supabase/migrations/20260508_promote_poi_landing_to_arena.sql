-- 20260508_promote_poi_landing_to_arena.sql
--
-- The "in-between" pipeline gap: landing → arena promotion was Python-only,
-- with no audit trail or backfill mechanism. This codifies it as a SQL
-- function for POI source.
--
-- Filter: is_active = true AND is_dog_beach_signal = true. The 880 active
-- POI rows without dog_beach_signal are intentionally NOT promoted (they
-- aren't dog-relevant beaches per the original signal).
--
-- Idempotent: only inserts rows whose fid isn't already in arena.
-- Operates with arena clustering safe-mode flag so the auto-promote
-- trigger doesn't fire during bulk inserts.

begin;

create or replace function public.promote_poi_landing_to_arena()
returns table(promoted bigint, already_in_arena bigint, candidates bigint)
language plpgsql
security definer
as $function$
declare
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_candidates bigint := 0;
begin
  -- Set safe-mode so auto-promote trigger doesn't fire as we insert
  perform set_config('app.arena_clustering_active', 'true', true);

  select count(*) into v_candidates
    from public.poi_landing pl
   where pl.is_active is true
     and pl.is_dog_beach_signal is true;

  with applied as (
    insert into public.arena (
      fid, name, address, lat, lon, county_fips, county_name,
      source_code, source_id, cpad_unit_id, geom,
      is_active, group_id, nav_lat, nav_lon, nav_source
    )
    select pl.fid,
           pl.name,
           pl.address_full,
           st_y(pl.geom) as lat,
           st_x(pl.geom) as lon,
           pl.county_geoid,
           pl.county_name,
           'poi',
           'poi/' || pl.fid::text,
           pl.cpad_unit_id,
           pl.geom,
           true,
           pl.fid,                       -- start as singleton
           st_y(pl.geom),                -- nav from POI geom
           st_x(pl.geom),
           'poi_geom'
      from public.poi_landing pl
     where pl.is_active is true
       and pl.is_dog_beach_signal is true
       and not exists (
         select 1 from public.arena a
          where a.source_code = 'poi' and a.fid = pl.fid
       )
       and pl.geom is not null
    returning 1
  )
  select count(*) into v_promoted from applied;

  v_existing := v_candidates - v_promoted;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_promoted, v_existing, v_candidates;
end;
$function$;

grant execute on function public.promote_poi_landing_to_arena() to anon, authenticated, service_role;

-- Promotion-candidacy view: shows what's eligible but not yet in arena.
create or replace view public.landing_promotion_candidates_v as
  select 'poi' as source,
         pl.fid::text as source_id,
         pl.name,
         pl.county_name,
         pl.is_active,
         pl.is_dog_beach_signal,
         pl.fetched_at as last_fetched
    from public.poi_landing pl
   where pl.is_active is true
     and pl.is_dog_beach_signal is true
     and not exists (
       select 1 from public.arena a
        where a.source_code = 'poi' and a.fid = pl.fid
     )
   union all
  select 'osm' as source,
         'osm/' || ol.type || '/' || ol.id::text as source_id,
         ol.tags->>'name' as name,
         ol.county_name,
         ol.is_active,
         null as is_dog_beach_signal,
         ol.fetched_at as last_fetched
    from public.osm_landing ol
   where ol.tags->>'natural' = 'beach'
     and ol.is_active is true
     and not exists (
       select 1 from public.arena a
        where a.source_code = 'osm'
          and a.source_id = 'osm/' || ol.type || '/' || ol.id::text
     );

grant select on public.landing_promotion_candidates_v to anon, authenticated, service_role;

commit;
