-- 20260508_arena_clustering_safe_mode.sql
--
-- Safe-mode flag for clustering re-runs. Tonight's populate_arena_group_id()
-- re-run inserted 379 unwanted gold rows because the auto-promote trigger
-- (tg_arena_auto_promote_to_gold) fires on every UPDATE — including the
-- step-1 group_id reset that runs at the start of clustering.
--
-- Fix: have populate_arena_group_id() set a transaction-local flag at start.
-- The auto-promote trigger checks the flag and skips if set. Flag clears
-- automatically when the transaction ends. Other arena triggers (geom-setter,
-- dog-beach guard) are unaffected — they still need to run for data integrity.
--
-- Naming: GUC variable `app.arena_clustering_active` ('true'/'false', default missing).

begin;

-- 1. Patch the auto-promote trigger function with a safe-mode check at the top.
create or replace function public.tg_arena_auto_promote_to_gold()
returns trigger
language plpgsql
as $function$
declare
  v_state text;
begin
  -- SAFE-MODE: skip during clustering re-runs. Set by populate_arena_group_id().
  if current_setting('app.arena_clustering_active', true) = 'true' then
    return NEW;
  end if;

  if NEW.source_code not in ('poi', 'manual') then return NEW; end if;
  if NEW.nav_lat is null or NEW.nav_lon is null then return NEW; end if;
  if NEW.is_active is not true then return NEW; end if;
  if exists (select 1 from public.beaches_gold where fid = NEW.fid) then
    return NEW;
  end if;

  -- GROUP GUARD (added earlier 2026-05-08): refuse if arena group already
  -- represented in active beaches_gold by a different fid.
  if exists (
    select 1
      from public.beaches_gold g
      join public.arena a2 on a2.fid = g.fid
     where a2.group_id = coalesce(NEW.group_id, NEW.fid)
       and g.is_active
       and g.fid <> NEW.fid
  ) then
    return NEW;
  end if;

  v_state := case
    when NEW.county_name in (
      'Los Angeles','Orange','San Diego','Santa Barbara','Ventura',
      'San Mateo','Marin','San Francisco','Sonoma','Mendocino','Humboldt',
      'Del Norte','Monterey','Santa Cruz','San Luis Obispo','Alameda',
      'Contra Costa','Solano','Napa','Lake'
    ) then 'CA'
    else null
  end;
  if v_state is null then return NEW; end if;

  insert into public.beaches_gold
    (fid, name, lat, lon, county_name, state, geom, source_code,
     group_id, location_id, promoted_from, promoted_at, is_active)
  values
    (NEW.fid, NEW.name, NEW.nav_lat, NEW.nav_lon, NEW.county_name,
     v_state, NEW.geom, NEW.source_code,
     coalesce(NEW.group_id, NEW.fid),
     public._make_location_slug(NEW.name, NEW.county_name, NEW.fid),
     'arena_auto_promote', now(), true)
  on conflict (fid) do nothing;

  return NEW;
end
$function$;

-- 2. Wrap populate_arena_group_id() to set/unset the safe-mode flag.
-- We do this by replacing the function with an outer wrapper that calls
-- the existing body inside a flag-bracketed block.
create or replace function public.populate_arena_group_id()
returns table(singletons bigint, relation_grouped bigint, name_clustered bigint, poi_matched bigint)
language plpgsql
security definer
as $function$
declare
  v_relation_grouped bigint := 0;
  v_name_clustered   bigint := 0;
  v_poi_matched      bigint := 0;
  v_singletons       bigint := 0;
begin
  -- SAFE-MODE: tell the auto-promote trigger to stand down for this transaction.
  perform set_config('app.arena_clustering_active', 'true', true);

  -- 1. Reset to singleton (group_id = fid)
  update public.arena set group_id = fid;

  -- 2. OSM relation grouping
  with rel as (
    select distinct on (l.id) l.id as rel_osm_id, l.members
      from public.osm_landing l
     where l.type = 'relation'
       and l.tags->>'natural' = 'beach'
       and l.members is not null
     order by l.id, l.fetched_at desc
  ),
  rel_arena as (
    select r.rel_osm_id, a.fid as rel_arena_fid, r.members
      from rel r
      join public.arena a
        on a.source_code = 'osm'
       and a.source_id = 'osm/relation/' || r.rel_osm_id::text
  ),
  member_ways as (
    select ra.rel_arena_fid, ma.fid as member_arena_fid
      from rel_arena ra,
           jsonb_array_elements(ra.members) m
      join public.arena ma
        on ma.source_code = 'osm'
       and ma.source_id = 'osm/way/' || (m->>'ref')
       and m->>'type' = 'way'
       and m->>'role' = 'outer'
     where m->>'type' = 'way' and m->>'role' = 'outer'
  ),
  to_group as (
    select rel_arena_fid as group_fid, rel_arena_fid as member_fid from rel_arena
    union
    select rel_arena_fid, member_arena_fid from member_ways
  ),
  applied as (
    update public.arena a
       set group_id = tg.group_fid
      from to_group tg
     where a.fid = tg.member_fid
       and a.group_id = a.fid
       and a.fid <> tg.group_fid
    returning 1
  )
  select count(*) into v_relation_grouped from applied;

  -- 3. Name + county + proximity clustering
  with eligible as (
    select fid, lower(trim(name)) as name_lc, county_fips, geom
      from public.arena
     where source_code = 'osm'
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.name_lc = b.name_lc
       and a.county_fips = b.county_fips
       and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 5000)
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
  select count(*) into v_name_clustered from applied;

  -- 4. Refresh polygon view
  refresh materialized view public.arena_group_polys;

  -- 5. POI-into-polygon containment with name-similarity tiebreaker
  with cands as (
    select poi.fid as poi_fid,
           g.group_id,
           coalesce(similarity(lower(coalesce(poi.name,'')),
                               lower(coalesce(gleader.name,''))), 0) as sim,
           ST_Area(g.poly::geography) as area_m2
      from public.arena poi
      join public.arena_group_polys g
        on poi.source_code = 'poi'
       and poi.is_active = true
       and ST_DWithin(g.poly::geography, poi.geom::geography, 100)
      join public.arena gleader on gleader.fid = g.group_id
  ),
  pick as (
    select distinct on (poi_fid) poi_fid, group_id
      from cands
     order by poi_fid, sim desc, area_m2 asc
  ),
  applied as (
    update public.arena a
       set group_id = p.group_id
      from pick p
     where a.fid = p.poi_fid
       and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_poi_matched from applied;

  -- 6. Count remaining singletons
  select count(*) into v_singletons
    from public.arena where group_id = fid;

  -- Flag auto-clears at transaction end, but be explicit.
  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_singletons, v_relation_grouped, v_name_clustered, v_poi_matched;
end;
$function$;

commit;
