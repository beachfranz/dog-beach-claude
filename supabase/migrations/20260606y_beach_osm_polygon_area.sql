-- 20260606y_beach_osm_polygon_area.sql
--
-- Wire `osm_features` beach polygons into beach_polygon_membership +
-- denormalize an `area_m2` onto `beaches_gold`, achieving parity with
-- how `dog_parks_gold.area_m2` works.
--
-- BACKGROUND
-- `dog_parks_gold` carries `area_m2` because dog parks are typically
-- polygon entities in OSM (leisure=dog_park) and the ingest path
-- captures them as polygons. For beaches, the ingest writes
-- osm_features rows but the canonical `geom` is a point-on-surface;
-- the actual polygon lives in `geom_full` (1,501 of 1,686 active
-- beach-tagged features have ST_Polygon or ST_MultiPolygon geometry
-- there, 89% coverage). That polygon data was never linked back to
-- beaches_gold — `beach_polygon_membership` only carried containing
-- jurisdictions (county, city, PAD-US park unit, etc.), not the OSM
-- beach footprint.
--
-- THIS MIGRATION
-- 1. Add `area_m2` column to beaches_gold (mirrors dog_parks_gold).
-- 2. Extend refresh_beach_polygon_membership() with an osm_features
--    block, same shape as the other 7 polygon-kind blocks. Match by
--    spatial containment / proximity, write to beach_polygon_membership
--    with polygon_kind='osm_features'.
-- 3. After the membership refresh, denormalize the SMALLEST containing
--    osm beach polygon's area onto beaches_gold.area_m2. Smallest =
--    most specific (e.g., a beach inside a state park gets the beach
--    polygon's area, not the park's area).
-- 4. Run for every active state in one pass.
--
-- STATE-LAUNCH WIRING
-- Automatic — the existing PIP-prelaunch phase
-- (see run_state_pipeline.py refresh_beach_polygon_membership() call)
-- now picks up the osm_features block without any pipeline edits.
--
-- ONGOING REFRESH
-- The `process_geom_change_queue` cron re-fires refire_bep_cascade for
-- beaches whose geom changes — that path doesn't currently re-run
-- refresh_beach_polygon_membership. New-beach inserts go through
-- tg_after_insert_gold_promote_chain which DOES call the per-fid
-- variant of refresh_beach_polygon_membership. So new beaches will
-- pick up osm_features memberships correctly. Existing beaches whose
-- nearest OSM polygon CHANGES (rare — only when osm_features ingests
-- new beach features) need a manual re-run for now. That's a small
-- enough population to accept as a known gap.

-- ─── 1. Add area_m2 column to beaches_gold ───────────────────────────
ALTER TABLE public.beaches_gold
  ADD COLUMN IF NOT EXISTS area_m2 numeric;

COMMENT ON COLUMN public.beaches_gold.area_m2 IS
  'Denormalized beach footprint area in m^2, from the smallest containing OSM '
  'natural=beach polygon (geom_full). Populated by refresh_beach_polygon_membership(). '
  'NULL when no osm_features polygon contains/covers the beach point.';

-- ─── 2. Extend refresh_beach_polygon_membership with osm_features ────
CREATE OR REPLACE FUNCTION public.refresh_beach_polygon_membership(p_state text, p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(kind text, n_rows bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  v_total_pad_us  bigint := 0;
  v_total_cpad    bigint := 0;
  v_total_jur     bigint := 0;
  v_total_tcs     bigint := 0;
  v_total_county  bigint := 0;
  v_total_mil     bigint := 0;
  v_total_tribal  bigint := 0;
  v_total_osm     bigint := 0;
  v_state_fp      text;
  v_failed_check  text;
begin
  if p_state is null then
    raise exception 'refresh_beach_polygon_membership: p_state required';
  end if;

  select string_agg(polygon_table || ': ' || note, '; ')
    into v_failed_check
    from public.assert_pip_indices(p_state)
   where status = 'fail';
  if v_failed_check is not null then
    raise exception 'PIP-prelaunch failed: %', v_failed_check;
  end if;

  select distinct fips_state into v_state_fp
    from public.jurisdictions
   where state = p_state
   limit 1;

  delete from public.beach_polygon_membership m
   using public.beaches_gold g
   where m.gold_fid = g.fid
     and g.state = p_state
     and (p_fid is null or g.fid = p_fid);

  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
     polygon_name, mng_name, mng_type, own_name, own_type, loc_mang, raw_attrs)
  select b.fid, 'pad_us_unit', pu.unit_id::text,
         case
           when st_contains(pu.geom, b.geom)                         then 4
           when st_dwithin(pu.geom_geog, b.geog, 100)                then 3
           when st_dwithin(pu.geom_geog, b.geog, 500)                then 2
           else                                                            1
         end,
         st_distance(pu.geom_geog, b.geog),
         st_area(pu.geom_geog),
         pu.unit_name, pu.mng_name, pu.mng_type, pu.own_name, pu.own_type, pu.loc_mang,
         pu.raw_attrs
    from beaches b
    join public.pad_us_units pu
      on pu.state = p_state
     and st_dwithin(pu.geom_geog, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_pad_us = row_count;

  if p_state = 'CA' then
    with beaches as (
      select fid, geom, geom::geography as geog
        from public.beaches_gold
       where state = 'CA' and is_active = true
         and (p_fid is null or fid = p_fid)
         and geom is not null
    )
    insert into public.beach_polygon_membership
      (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
       polygon_name, mng_name, mng_type, own_name, raw_attrs)
    select b.fid, 'cpad_unit', cu.unit_id::text,
           case
             when st_contains(cu.geom, b.geom)                          then 4
             when st_dwithin(cu.geom::geography, b.geog, 100)            then 3
             when st_dwithin(cu.geom::geography, b.geog, 500)            then 2
             else                                                            1
           end,
           st_distance(cu.geom::geography, b.geog),
           st_area(cu.geom::geography),
           cu.unit_name, cu.mng_agncy, cu.mng_ag_lev, cu.agncy_name,
           jsonb_build_object(
             'mng_ag_id', cu.mng_ag_id, 'mng_ag_typ', cu.mng_ag_typ,
             'site_name', cu.site_name, 'park_url', cu.park_url,
             'access_typ', cu.access_typ, 'agncy_typ', cu.agncy_typ,
             'agncy_lev', cu.agncy_lev, 'land_water', cu.land_water)
      from beaches b
      join public.cpad_units cu on st_dwithin(cu.geom::geography, b.geog, 2000)
    on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
    get diagnostics v_total_cpad = row_count;
  end if;

  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
     polygon_name)
  select b.fid, 'c1_city', j.id::text,
         case
           when st_contains(j.geom, b.geom)                          then 4
           when st_dwithin(j.geom::geography, b.geog, 100)            then 3
           when st_dwithin(j.geom::geography, b.geog, 500)            then 2
           else                                                            1
         end,
         st_distance(j.geom::geography, b.geog),
         st_area(j.geom::geography),
         j.name
    from beaches b
    join public.jurisdictions j
      on j.state = p_state
     and st_dwithin(j.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_jur = row_count;

  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
     polygon_name, raw_attrs)
  select b.fid, 'tcs_town', tcs.id::text,
         case
           when st_contains(tcs.geom, b.geom)                        then 4
           when st_dwithin(tcs.geom_geog, b.geog, 100)               then 3
           when st_dwithin(tcs.geom_geog, b.geog, 500)               then 2
           else                                                            1
         end,
         st_distance(tcs.geom_geog, b.geog),
         st_area(tcs.geom_geog),
         tcs.name,
         jsonb_build_object(
           'classfp', tcs.classfp, 'mtfcc', tcs.mtfcc,
           'funcstat', tcs.funcstat, 'geoid', tcs.geoid,
           'fips_cousub', tcs.fips_cousub)
    from beaches b
    join public.tiger_county_subdivisions tcs
      on tcs.state = p_state
     and st_dwithin(tcs.geom_geog, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_tcs = row_count;

  with beaches as (
    select fid, geom, geom::geography as geog, county_fips
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
     polygon_name)
  select b.fid, 'county', c.geoid,
         case
           when st_contains(c.geom, b.geom)                          then 4
           when st_dwithin(c.geom::geography, b.geog, 100)            then 3
           when st_dwithin(c.geom::geography, b.geog, 500)            then 2
           else                                                            1
         end,
         st_distance(c.geom::geography, b.geog),
         st_area(c.geom::geography),
         c.name
    from beaches b
    join public.counties c
      on c.state_fp = v_state_fp
     and st_dwithin(c.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_county = row_count;

  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2)
  select b.fid, 'military_base', mb.objectid::text,
         case
           when st_contains(mb.geom, b.geom)                          then 4
           when st_dwithin(mb.geom::geography, b.geog, 100)            then 3
           when st_dwithin(mb.geom::geography, b.geog, 500)            then 2
           else                                                            1
         end,
         st_distance(mb.geom::geography, b.geog),
         st_area(mb.geom::geography)
    from beaches b
    join public.military_bases mb
      on mb.state_postal = p_state
     and st_dwithin(mb.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_mil = row_count;

  with state_poly as (
    select geom from public.states where state_code = p_state
  ), beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  ), in_state_tribal as (
    select tl.objectid, tl.geom
      from public.tribal_lands tl
      join state_poly sp on st_intersects(tl.geom, sp.geom)
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2)
  select b.fid, 'tribal_land', tl.objectid::text,
         case
           when st_contains(tl.geom, b.geom)                          then 4
           when st_dwithin(tl.geom::geography, b.geog, 100)            then 3
           when st_dwithin(tl.geom::geography, b.geog, 500)            then 2
           else                                                            1
         end,
         st_distance(tl.geom::geography, b.geog),
         st_area(tl.geom::geography)
    from beaches b
    join in_state_tribal tl on st_dwithin(tl.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_tribal = row_count;

  -- ── NEW: OSM natural=beach polygons (the actual beach footprint) ─────
  -- Source: osm_features rows where tags->>'natural' = 'beach' AND
  -- geom_full IS NOT NULL (a real polygon/multipolygon).
  -- 1,501 of 1,686 active OSM beach-tagged features have polygon geometry
  -- in geom_full (89%). We spatial-join via geom_full against the beach's
  -- point; the smallest containing polygon is the most specific (selected
  -- in the denormalization step below).
  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
  )
  insert into public.beach_polygon_membership
    (gold_fid, polygon_kind, polygon_id, match_strength, dist_m, area_m2,
     polygon_name, raw_attrs)
  select b.fid,
         'osm_features',
         of.osm_type || ':' || of.osm_id,
         case
           when st_contains(of.geom_full, b.geom)                       then 4
           when st_dwithin(of.geom_full::geography, b.geog, 100)        then 3
           when st_dwithin(of.geom_full::geography, b.geog, 500)        then 2
           else                                                                  1
         end,
         st_distance(of.geom_full::geography, b.geog),
         st_area(of.geom_full::geography),
         of.name,
         jsonb_build_object(
           'osm_type',  of.osm_type,
           'osm_id',    of.osm_id,
           'surface',   of.surface,
           'fee',       of.fee,
           'fence',     of.fence,
           'managing_agency', of.managing_agency,
           'tags',      of.tags)
    from beaches b
    join public.osm_features of
      on of.tags ? 'natural'
     and of.tags->>'natural' = 'beach'
     and of.geom_full is not null
     and st_dwithin(of.geom_full::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_osm = row_count;

  -- ── Denormalize area_m2 onto beaches_gold from the smallest containing
  --    osm_features polygon. Picking the smallest gives the most specific
  --    match (a beach inside a larger state-park polygon and a smaller
  --    beach-polygon both contain the point — we want the beach polygon).
  --    match_strength=4 (st_contains) is preferred; we accept lower
  --    strength as fallback so beaches near (but not inside) a polygon
  --    still get an area estimate.
  with chosen as (
    select distinct on (m.gold_fid)
      m.gold_fid,
      m.area_m2
    from public.beach_polygon_membership m
    join public.beaches_gold g on g.fid = m.gold_fid
    where g.state = p_state
      and (p_fid is null or g.fid = p_fid)
      and m.polygon_kind = 'osm_features'
      and m.area_m2 is not null
    order by m.gold_fid, m.match_strength desc, m.area_m2 asc
  )
  update public.beaches_gold g
     set area_m2 = chosen.area_m2
    from chosen
   where g.fid = chosen.gold_fid;

  return query
    select 'pad_us_unit'::text   as kind, v_total_pad_us as n_rows
    union all select 'cpad_unit',      v_total_cpad
    union all select 'c1_city',        v_total_jur
    union all select 'tcs_town',       v_total_tcs
    union all select 'county',         v_total_county
    union all select 'military_base',  v_total_mil
    union all select 'tribal_land',    v_total_tribal
    union all select 'osm_features',   v_total_osm;
end
$function$;
