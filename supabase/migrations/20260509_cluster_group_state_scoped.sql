-- 20260509_cluster_group_state_scoped.sql
--
-- populate_arena_group_id() ran globally on the entire arena table on
-- every state launch. After the activation pass took arena from ~5k to
-- 12,231 rows, the O(N²) trigram clustering blew past the 600s
-- statement_timeout (DE v5b halt). For a state with 55 rows, we should
-- be processing 55 rows, not 12k.
--
-- New: populate_arena_group_id(p_state text DEFAULT NULL).
--   - p_state=NULL → global behavior preserved (for ad-hoc reclustering)
--   - p_state='XX' → only arena rows in counties of that state
--
-- Each step's joins/updates filter to the per-state cohort. The
-- materialized view arena_group_polys is global so POI cross-state
-- containment still works, but its refresh is unaffected.

begin;

create or replace function public.populate_arena_group_id(p_state text default null)
returns table(singletons bigint, relation_grouped bigint, name_clustered bigint, poi_matched bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_relation_grouped bigint := 0;
  v_name_clustered   bigint := 0;
  v_poi_matched      bigint := 0;
  v_singletons       bigint := 0;
  v_state_fp         text;
begin
  perform set_config('app.arena_clustering_active', 'true', true);

  -- Resolve state FP from 2-letter code (NULL stays NULL for global mode)
  v_state_fp := case p_state
    when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05'
    when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10'
    when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16'
    when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20'
    when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24'
    when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28'
    when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32'
    when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36'
    when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40'
    when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45'
    when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49'
    when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54'
    when 'WI' then '55' when 'WY' then '56'
  end;

  -- Step 1: Reset group_id (only for state's rows when scoped)
  if v_state_fp is null then
    update public.arena set group_id = fid where true;
  else
    update public.arena a set group_id = fid
      from public.counties c
     where c.geoid = a.county_fips and c.state_fp = v_state_fp;
  end if;

  -- Step 2: OSM relation grouping — scope members to state cohort
  with rel as (
    select distinct on (l.id) l.id as rel_osm_id, l.members
      from public.osm_landing l
     where l.type = 'relation' and l.tags->>'natural' = 'beach' and l.members is not null
     order by l.id, l.fetched_at desc
  ),
  rel_arena as (
    select r.rel_osm_id, a.fid as rel_arena_fid, r.members
      from rel r
      join public.arena a
        on a.source_code = 'osm' and a.source_id = 'osm/relation/' || r.rel_osm_id::text
      left join public.counties c on c.geoid = a.county_fips
     where v_state_fp is null or c.state_fp = v_state_fp
  ),
  member_ways as (
    select ra.rel_arena_fid, ma.fid as member_arena_fid
      from rel_arena ra, jsonb_array_elements(ra.members) m
      join public.arena ma
        on ma.source_code = 'osm' and ma.source_id = 'osm/way/' || (m->>'ref')
       and m->>'type' = 'way' and m->>'role' = 'outer'
     where m->>'type' = 'way' and m->>'role' = 'outer'
  ),
  to_group as (
    select rel_arena_fid as group_fid, rel_arena_fid as member_fid from rel_arena
    union
    select rel_arena_fid, member_arena_fid from member_ways
  ),
  applied as (
    update public.arena a set group_id = tg.group_fid
      from to_group tg
     where a.fid = tg.member_fid and a.group_id = a.fid and a.fid <> tg.group_fid
    returning 1
  )
  select count(*) into v_relation_grouped from applied;

  -- Step 3: Name + county + 5km proximity (only state rows when scoped)
  with eligible as (
    select a.fid, lower(trim(a.name)) as name_lc, a.county_fips, a.geom
      from public.arena a
      left join public.counties c on c.geoid = a.county_fips
     where a.source_code = 'osm' and a.name is not null and trim(a.name) <> ''
       and a.county_fips is not null and a.geom is not null and a.group_id = a.fid
       and (v_state_fp is null or c.state_fp = v_state_fp)
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a join eligible b
        on a.name_lc = b.name_lc and a.county_fips = b.county_fips
       and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 5000)
  ),
  pick as (
    select distinct on (loser) loser, winner from pairs order by loser, winner asc
  ),
  applied as (
    update public.arena a set group_id = p.winner
      from pick p where a.fid = p.loser and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_name_clustered from applied;

  refresh materialized view public.arena_group_polys;

  -- Step 4: POI-into-polygon (only state's POIs when scoped)
  with cands as (
    select poi.fid as poi_fid,
           g.group_id,
           coalesce(similarity(lower(coalesce(poi.name,'')),
                               lower(coalesce(gleader.name,''))), 0) as sim,
           ST_Area(g.poly::geography) as area_m2
      from public.arena poi
      left join public.counties c on c.geoid = poi.county_fips
      join public.arena_group_polys g
        on poi.source_code = 'poi' and poi.is_active = true
       and ST_DWithin(g.poly::geography, poi.geom::geography, 2000)
      join public.arena gleader on gleader.fid = g.group_id
     where v_state_fp is null or c.state_fp = v_state_fp
  ),
  pick as (
    select distinct on (poi_fid) poi_fid, group_id from cands
     order by poi_fid, sim desc, area_m2 asc
  ),
  applied as (
    update public.arena a set group_id = p.group_id
      from pick p where a.fid = p.poi_fid and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_poi_matched from applied;

  -- Singleton count, scoped or global
  if v_state_fp is null then
    select count(*) into v_singletons from public.arena where group_id = fid;
  else
    select count(*) into v_singletons
      from public.arena a
      join public.counties c on c.geoid = a.county_fips
     where a.group_id = a.fid and c.state_fp = v_state_fp;
  end if;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_singletons, v_relation_grouped, v_name_clustered, v_poi_matched;
end $function$;

grant execute on function public.populate_arena_group_id(text) to anon, authenticated, service_role;

commit;
