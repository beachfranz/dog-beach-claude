-- 20260509_promote_poi_arena_conflict_nothing.sql
--
-- Issue #25: promote_poi_landing_to_arena() failed with
--   "duplicate key value violates unique constraint arena_pkey"
-- when the activation+signal flips for non-CA states unlocked 6,169
-- candidates and 29+ of those poi_landing.fid values collide with
-- existing arena.fid values from OSM-source rows.
--
-- The function's WHERE NOT EXISTS check is too narrow:
--     and a.source_code = 'poi' and a.fid = pl.fid
-- arena.fid is the PK regardless of source_code, so an OSM row with
-- the same fid number already occupies that slot. The function should
-- silently skip (ON CONFLICT DO NOTHING) — the OSM row already
-- represents that beach; we don't need a duplicate POI row for it.
--
-- This was masked previously because per-state launches had small
-- POI sets and lucked out on no collisions. The activation pass
-- earlier today made it national, surfacing the problem.

begin;

create or replace function public.promote_poi_landing_to_arena()
returns table(promoted bigint, already_in_arena bigint, candidates bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_candidates bigint := 0;
begin
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
           st_y(pl.geom),
           st_x(pl.geom),
           pl.county_geoid,
           pl.county_name,
           'poi',
           'poi/' || pl.fid::text,
           pl.cpad_unit_id,
           pl.geom,
           true,
           pl.fid,
           st_y(pl.geom),
           st_x(pl.geom),
           'poi_geom'
      from public.poi_landing pl
     where pl.is_active is true
       and pl.is_dog_beach_signal is true
       and pl.geom is not null
    on conflict (fid) do nothing                  -- ★ issue #25 fix
    returning 1
  )
  select count(*) into v_promoted from applied;

  v_existing := v_candidates - v_promoted;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_promoted, v_existing, v_candidates;
end $function$;

commit;
