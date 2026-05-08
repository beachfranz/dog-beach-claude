-- 20260508_arena_extras_strategy_10.sql
--
-- Strategy 10: token-subset matching for arena clustering (Pin #31).
-- Earlier attempt reverted due to cycle + transitive-group bugs; both
-- fixes baked in here.
--
-- Bug 1 fix (cycle for symmetric singleton pairs): add ordering constraint
-- so that when BOTH rows are singletons, only the higher-fid loses to
-- the lower-fid. Reverse direction is implicitly skipped.
--
-- Bug 2 fix (transitive group resolution): target_group = coalesce(
-- a.group_id, a.fid) so a winner that's already in a group routes b
-- into a's group (not just a.fid). Plus a chain-flatten post-step so
-- any 2-step chains collapse to 1-step.
--
-- Catches the 2 remaining sub-trigram-threshold dupes:
--   Lake Arrowhead Village Beach / Lake Arrowhead Village Lakefront
--   Fish Rock Beach at Anchor Bay / Fish Rock Beach

begin;

drop function if exists public.populate_arena_extras();

create function public.populate_arena_extras()
returns table(
  intra_osm_trigram bigint,
  intra_poi bigint,
  cross_source_name bigint,
  token_subset bigint,
  chains_flattened bigint
)
language plpgsql
security definer
as $function$
declare
  v_intra_osm bigint := 0;
  v_intra_poi bigint := 0;
  v_cross bigint := 0;
  v_tokens bigint := 0;
  v_flattened bigint := 0;
begin
  perform set_config('app.arena_clustering_active', 'true', true);

  -- STRATEGY 6: Trigram intra-OSM (5km, sim ≥ 0.7)
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 5000)
       and public._dedup_pair_safe(a.name, b.name, 0.7)
       and a.name_lc <> b.name_lc
  ),
  pick as (select distinct on (loser) loser, winner from pairs order by loser, winner),
  applied as (
    update public.arena a set group_id = p.winner from pick p
     where a.fid = p.loser and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_osm from applied;

  -- STRATEGY 7: Intra-POI (1km, sim ≥ 0.7)
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 1000)
       and public._dedup_pair_safe(a.name, b.name, 0.7)
  ),
  pick as (select distinct on (loser) loser, winner from pairs order by loser, winner),
  applied as (
    update public.arena a set group_id = p.winner from pick p
     where a.fid = p.loser and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_poi from applied;

  -- STRATEGY 8: Cross-source POI -> OSM by name (1km, sim ≥ 0.7)
  with poi_singletons as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi' and is_active = true and group_id = fid
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
  ),
  osm_targets as (
    select fid, group_id, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
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
      from candidates order by poi_fid, sim desc, meters asc
  ),
  applied as (
    update public.arena a set group_id = p.target_group from pick p
     where a.fid = p.poi_fid and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_cross from applied;

  -- ★ STRATEGY 10: Token-subset matching (catches sub-trigram dupes)
  -- BUG-FIX 1: For symmetric singleton pairs, require a.fid < b.fid (or a
  -- already grouped) so updates fire in only one direction.
  -- BUG-FIX 2: target_group = coalesce(a.group_id, a.fid) so when a is
  -- already in a group, b joins that group rather than pointing at a.fid.
  with eligible as (
    select fid, group_id, name, county_fips, geom
      from public.arena
     where is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
  ),
  candidates as (
    select b.fid as loser_fid,
           coalesce(a.group_id, a.fid) as target_group,
           st_distance(a.geom::geography, b.geom::geography) as meters
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips
       and a.fid <> b.fid
       and b.group_id = b.fid              -- b is singleton
       -- Cycle fix: a is already grouped OR a is the lower-fid singleton
       and (a.group_id <> a.fid OR a.fid < b.fid)
       and st_dwithin(a.geom::geography, b.geom::geography, 1000)
       and public._dedup_token_subset(a.name, b.name)
  ),
  pick as (
    select distinct on (loser_fid) loser_fid, target_group
      from candidates order by loser_fid, meters asc
  ),
  applied as (
    update public.arena a set group_id = p.target_group from pick p
     where a.fid = p.loser_fid
       and a.group_id = a.fid
       and p.target_group <> a.fid
    returning 1
  )
  select count(*) into v_tokens from applied;

  -- ★ Chain flatten: any 2-step group_id chain collapses to 1-step.
  -- (a → b → c becomes a → c). Iterate until fixed-point (max 5 passes).
  for i in 1..5 loop
    with applied as (
      update public.arena a set group_id = b.group_id
        from public.arena b
       where a.group_id = b.fid
         and b.group_id <> b.fid
         and a.group_id <> a.fid
      returning 1
    )
    select v_flattened + count(*) into v_flattened from applied;
    -- If nothing changed in this pass, break
    if not exists(
      select 1 from public.arena a join public.arena b on a.group_id = b.fid
       where a.group_id <> a.fid and b.group_id <> b.fid
    ) then exit; end if;
  end loop;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_intra_osm, v_intra_poi, v_cross, v_tokens, v_flattened;
end;
$function$;

grant execute on function public.populate_arena_extras() to anon, authenticated, service_role;

commit;
