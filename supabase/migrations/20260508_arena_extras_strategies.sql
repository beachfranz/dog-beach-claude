-- 20260508_arena_extras_strategies.sql
--
-- Step 3 of the bulletproofing chain (Pin #28: clustering v2).
--
-- The existing populate_arena_group_id() has 5 strategies:
--   1. Reset to singleton
--   2. OSM relation graph
--   3. OSM same-EXACT-name + county + 5km
--   4. Refresh polygon view
--   5. POI inside OSM polygon (within 100m)
--
-- Audit 2026-05-08 surfaced 12 unclustered dupe pairs in beaches_gold that
-- slipped through:
--   - Lake Arrowhead Village Beach / Lake Arrowhead Village Lakefront
--     (intra-OSM, near-but-not-exact name)
--   - Santa Monica Beach (POI) / Santa Monica State Beach (OSM)
--     (cross-source, no polygon containment trigger because POI was outside
--      the 100m buffer or there's no polygon at all)
--   - Fish Rock Beach at Anchor Bay / Fish Rock Beach (intra-POI)
--   - San Simeon Creek Beach / San Simeon Beach (intra-POI)
--
-- This migration adds three new strategies as a separate extras function
-- so the original 5-strategy clustering stays unchanged. Run extras AFTER
-- populate_arena_group_id() in the orchestrator.
--
-- All three strategies use _dedup_pair_safe(name_a, name_b, 0.7) as the
-- safety gate — catches the "X Beach" / "X Dog Beach" mismatch trap.

begin;

create or replace function public.populate_arena_extras()
returns table(intra_osm_trigram bigint, intra_poi bigint, cross_source_name bigint)
language plpgsql
security definer
as $function$
declare
  v_intra_osm bigint := 0;
  v_intra_poi bigint := 0;
  v_cross bigint := 0;
begin
  -- Set clustering safe-mode so the auto-promote trigger doesn't fire
  -- on these group_id changes.
  perform set_config('app.arena_clustering_active', 'true', true);

  -- STRATEGY 6: Trigram intra-OSM. Same as Strategy 3 but with similarity
  -- gate >= 0.7 and the dog-mismatch guard. Catches "Lake Arrowhead Village
  -- Beach" / "Lake Arrowhead Village Lakefront" type pairs.
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm'
       and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips
       and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 5000)
       and public._dedup_pair_safe(a.name, b.name, 0.7)
       and a.name_lc <> b.name_lc  -- Strategy 3 already handled exact matches
  ),
  pick as (
    select distinct on (loser) loser, winner
      from pairs
     order by loser, winner asc
  ),
  applied as (
    update public.arena a
       set group_id = p.winner
      from pick p
     where a.fid = p.loser
       and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_osm from applied;

  -- STRATEGY 7: Intra-POI. POI dupes from CSV import. Tighter distance
  -- (200m) since POI rows are points without polygon shape.
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi'
       and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips
       and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 500)
       and public._dedup_pair_safe(a.name, b.name, 0.7)
  ),
  pick as (
    select distinct on (loser) loser, winner
      from pairs
     order by loser, winner asc
  ),
  applied as (
    update public.arena a
       set group_id = p.winner
      from pick p
     where a.fid = p.loser
       and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_poi from applied;

  -- STRATEGY 8: Cross-source name-based POI -> OSM. Catches POIs that
  -- failed Strategy 5's polygon containment (e.g. POI is 200m outside
  -- the OSM polygon, or no polygon exists).
  with poi_singletons as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi'
       and is_active = true
       and group_id = fid
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
  ),
  osm_targets as (
    select fid, group_id, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm'
       and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
  ),
  candidates as (
    select p.fid as poi_fid, o.group_id as target_group,
           similarity(p.name_lc, o.name_lc) as sim,
           st_distance(p.geom::geography, o.geom::geography) as meters
      from poi_singletons p
      join osm_targets o
        on o.county_fips = p.county_fips
       and st_dwithin(p.geom::geography, o.geom::geography, 1000)
       and public._dedup_pair_safe(p.name, o.name, 0.7)
  ),
  pick as (
    select distinct on (poi_fid) poi_fid, target_group
      from candidates
     order by poi_fid, sim desc, meters asc
  ),
  applied as (
    update public.arena a
       set group_id = p.target_group
      from pick p
     where a.fid = p.poi_fid
       and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_cross from applied;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_intra_osm, v_intra_poi, v_cross;
end;
$function$;

grant execute on function public.populate_arena_extras() to anon, authenticated, service_role;

commit;
