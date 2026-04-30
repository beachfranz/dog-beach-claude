-- arena.group_id — clusters of arena rows that represent the same beach.
--
-- Default: group_id = fid (singleton). Reset+populate is idempotent.
--
-- Population order (most-authoritative first; later passes don't override
-- earlier ones because we only assign when group_id = fid, i.e. still
-- singleton):
--
-- 1. OSM relation grouping: for each beach relation in osm_landing whose
--    member ways are also in arena, all members + the relation itself
--    share the relation's arena.fid as group_id.
--
-- 2. Name + county + proximity clustering: OSM-source arena rows that
--    share lower(trim(name)), share county_fips, and sit within 5km of
--    each other (geography distance) collapse into one group, keyed by
--    the lowest fid in the cluster. 5km picked to catch long thin state
--    beaches mapped as 2-3 polys (Monterey State, Bolsa Chica State,
--    Will Rogers State, Ventura City — all real multi-part beaches).
--    Same-county filter prevents name-collision merges (Hidden Beach,
--    Shell Beach, North Beach, South Beach all share names across
--    multiple unrelated counties).
--
-- 3. POI-into-polygon containment: for each active POI within 100m of
--    a polygon group's union, assign the POI to that group. Tiebreaker
--    when a POI sits in multiple groups: highest similarity to group
--    leader's name, then smallest polygon area. This locks the
--    "polygon name signal beats spatial nesting" rule found via dry-run.
--    Uses materialized view arena_group_polys (refreshed inside this
--    function after step 2).

alter table public.arena
  add column if not exists group_id bigint;

create index if not exists arena_group_id_idx on public.arena (group_id);


-- ── Reset+populate function (idempotent) ────────────────────────────

-- Materialized view: per-group polygon (union of OSM beach polygons whose
-- arena row shares the group_id). Used by the POI-matching step.
-- Refresh is triggered inside populate_arena_group_id().
drop materialized view if exists public.arena_group_polys cascade;
create materialized view public.arena_group_polys as
with osm_polys as (
  select distinct on (l.type, l.id)
         l.type, l.id, ST_MakeValid(l.geom_full) as geom_full
    from public.osm_landing l
   where l.geom_full is not null
     and l.tags->>'natural' = 'beach'
     and ST_GeometryType(l.geom_full) in ('ST_Polygon','ST_MultiPolygon')
   order by l.type, l.id, l.fetched_at desc
)
select a.group_id, ST_Union(p.geom_full) as poly
  from public.arena a
  join osm_polys p
    on a.source_code = 'osm'
   and a.source_id = 'osm/' || p.type || '/' || p.id::text
 group by a.group_id;

create index if not exists arena_group_polys_gix
  on public.arena_group_polys using gist (poly);


drop function if exists public.populate_arena_group_id();
create or replace function public.populate_arena_group_id()
returns table (singletons bigint, relation_grouped bigint, name_clustered bigint, poi_matched bigint)
language plpgsql
security definer
as $function$
declare
  v_relation_grouped bigint := 0;
  v_name_clustered   bigint := 0;
  v_poi_matched      bigint := 0;
  v_singletons       bigint := 0;
begin
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
    -- The arena fid of each relation
    select r.rel_osm_id, a.fid as rel_arena_fid, r.members
      from rel r
      join public.arena a
        on a.source_code = 'osm'
       and a.source_id = 'osm/relation/' || r.rel_osm_id::text
  ),
  member_ways as (
    -- For each (relation, member way), find the arena fid for that way
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
    -- Collect all rows that should share each relation's group:
    -- the relation's own arena row + each member way's arena row
    select rel_arena_fid as group_fid, rel_arena_fid as member_fid from rel_arena
    union
    select rel_arena_fid, member_arena_fid from member_ways
  ),
  applied as (
    update public.arena a
       set group_id = tg.group_fid
      from to_group tg
     where a.fid = tg.member_fid
       and a.group_id = a.fid       -- only apply if still singleton
       and a.fid <> tg.group_fid    -- skip the canonical row itself
    returning 1
  )
  select count(*) into v_relation_grouped from applied;

  -- 3. Name + county + proximity clustering
  -- For each pair of OSM-source rows with same lower(name), same county_fips,
  -- within 2km of each other, route the higher-fid row's group_id to the
  -- lower-fid row's group_id.
  with eligible as (
    select fid, lower(trim(name)) as name_lc, county_fips, geom
      from public.arena
     where source_code = 'osm'
       and name is not null and trim(name) <> ''
       and county_fips is not null
       and geom is not null
       and group_id = fid              -- still singleton after step 2
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
  -- A row may pair with multiple winners; pick the smallest winner per loser
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

  -- 4. Refresh polygon view (depends on freshly-set OSM group_ids from steps 2-3)
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

  return query select v_singletons, v_relation_grouped, v_name_clustered, v_poi_matched;
end;
$function$;
