-- 20260508_clustering_dedup_fixes.sql
--
-- Two surgical fixes to clustering / dedup:
--
-- 1. promote_to_gold's "skip if sibling already in gold" check requires
--    the existing sibling to be is_active=true. When the sibling has been
--    Tier-3 inactivated (e.g. FWS no-dogs trigger fired), a later POI
--    arrival in the same group gets promoted as a *new* gold row instead
--    of being skipped. Result: same group, two gold rows, both eventually
--    inactive. Verified on OR: Moolack/Short Sand/Merchants/Sporthaven/
--    Wakeman all show 0m duplicate pairs from this hole.
--    Fix: drop the is_active filter — any gold row in the group blocks
--    a new promotion, regardless of active state.
--
-- 2. populate_arena_group_id step 4 (POI exact-match against OSM) joins
--    on st_dwithin(geom, 1000m) + normalized-name equality. POI
--    us_beach_points is a small curated source (~8K nationwide) so the
--    name match is high-precision; bumping to 2000m catches Koberg Beach
--    (1093m) and Euchre Creek (1735m) without false positives.

begin;

-- ── Fix #1 ──────────────────────────────────────────────────────────────
create or replace function public.promote_to_gold(
  p_fids bigint[], p_score boolean default false, p_publish boolean default true
)
returns table(
  rows_promoted bigint, rows_already_in_gold bigint,
  containment_evidence bigint, source_evidence bigint,
  beach_dog_policy_changed bigint, beach_amenities_changed bigint,
  noaa_assigned bigint, slugs_assigned bigint
)
language plpgsql
as $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1
           from public.beaches_gold g
           join public.arena a2 on a2.fid = g.fid
          where a2.group_id = coalesce(a.group_id, a.fid)
            -- DROPPED: and g.is_active   (was creating the dedup hole)
            and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, is_scoreable, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.source_code, t.source_id,
      coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, null,
      public._infer_state_from_county(t.county_name),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00', coalesce(p_score, false),
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._resolve_polygon_containment(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  if p_publish then
    foreach fid_iter in array p_fids loop
      perform public.compute_beach_field_consensus(fid_iter);
      perform public.promote_canonical_to_consumer_tables(fid_iter);
    end loop;
    select count(*) into bdp_changed from public.beach_dog_policy
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
    select count(*) into ba_changed from public.beach_amenities
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  end if;

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

-- ── Fix #2 ──────────────────────────────────────────────────────────────
-- Bump POI↔OSM clustering threshold 1000m → 2000m. Safe because:
-- (a) The join requires *exact normalized-name match* (alphanumeric-only).
-- (b) us_beach_points is curated (~8K nationwide), so name collisions
--     within 2km of an OSM beach are almost certainly the same beach.
-- This catches Koberg Beach (1093m) and Euchre Creek Beach (1735m) on OR.

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
  perform set_config('app.arena_clustering_active', 'true', true);

  update public.arena set group_id = fid where true;

  -- Step 2: OSM relation grouping
  with rel as (
    select distinct on (l.id) l.id as rel_osm_id, l.members
      from public.osm_landing l
     where l.type = 'relation' and l.tags->>'natural' = 'beach' and l.members is not null
     order by l.id, l.fetched_at desc
  ),
  rel_arena as (
    select r.rel_osm_id, a.fid as rel_arena_fid, r.members
      from rel r join public.arena a
        on a.source_code = 'osm' and a.source_id = 'osm/relation/' || r.rel_osm_id::text
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

  -- Step 3: Name + county + 5km proximity (OSM only, by source filter)
  with eligible as (
    select fid, lower(trim(name)) as name_lc, county_fips, geom
      from public.arena
     where source_code = 'osm' and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null and group_id = fid
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

  -- Step 4 (★ bumped 1000m -> 2000m): POI-into-polygon and POI-vs-OSM exact name
  with cands as (
    select poi.fid as poi_fid,
           g.group_id,
           coalesce(similarity(lower(coalesce(poi.name,'')),
                               lower(coalesce(gleader.name,''))), 0) as sim,
           ST_Area(g.poly::geography) as area_m2
      from public.arena poi
      join public.arena_group_polys g
        on poi.source_code = 'poi' and poi.is_active = true
       and ST_DWithin(g.poly::geography, poi.geom::geography, 2000)
      join public.arena gleader on gleader.fid = g.group_id
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

  select count(*) into v_singletons from public.arena where group_id = fid;
  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_singletons, v_relation_grouped, v_name_clustered, v_poi_matched;
end;
$function$;

commit;
