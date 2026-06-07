-- 20260606zc_pad_us_soft_delete_giant_polygons.sql
--
-- Add `is_active` to pad_us_units and soft-delete two HI catch-all
-- polygons that OOM the Postgres backend during per-fid promote.
--
-- ROOT CAUSE
-- HI run #46 promote phase repeatedly killed the Postgres backend with
-- "server closed the connection unexpectedly" — same failure on direct
-- backend (not just pgbouncer/Supavisor), so the kill is Postgres OOM,
-- not a pool sever.
--
-- Per-fid `refresh_beach_polygon_membership('HI', fid)` runs at 1.9s
-- (healthy). Per-50-fid chunk: ~1m 45s on a fresh connection. But
-- ~130-150 fids into a single connection's lifetime, accumulated query
-- planner memory blows past the compute tier's RAM and Postgres OOMs.
--
-- Per-fid the spatial join evaluates `ST_DWithin(pu.geom_geog, b.geog,
-- 2000)` against every PAD-US unit within 2km. HI has two CATCH-ALL
-- polygons that dominate the working memory:
--
--   unit_id 26687: "State Department of Land State Resource Management
--                   Area"                                 238,129 vertices
--   unit_id 26513: "County Land Local Other or Unknown"   221,946 vertices
--
-- These are PAD-US "bucket" categories where the source dumped all
-- non-specifically-categorized state and county land. They have no
-- consumer value (no specific name, no managing agency for policy
-- purposes) but their massive vertex counts dominate the join cost.
--
-- THIS MIGRATION
-- 1. Add `is_active boolean NOT NULL DEFAULT true` to pad_us_units. The
--    loader (load_pad_us_state.py) does `ON CONFLICT (unit_id) DO
--    UPDATE` without touching is_active, so the flag survives future
--    PAD-US reloads.
-- 2. Set is_active=false for the two HI giants.
-- 3. Filter pad_us_units by `is_active` in refresh_beach_polygon_membership
--    so the join skips inactive polygons entirely.
--
-- Same pattern applies to any future polygon with >100K vertices that
-- causes OOM during launch. Add it to the soft-delete list.

-- ─── 1. Column ───────────────────────────────────────────────────────
ALTER TABLE public.pad_us_units
  ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN public.pad_us_units.is_active IS
  'Soft-delete flag. Set false to exclude a unit from spatial joins '
  '(used by refresh_beach_polygon_membership). Targets catch-all '
  'polygons with >100K vertices that OOM Postgres during per-fid '
  'promote. Loader preserves this on UPSERT.';

CREATE INDEX IF NOT EXISTS pad_us_units_is_active_idx
  ON public.pad_us_units (is_active)
  WHERE NOT is_active;

-- ─── 2. Soft-delete the two HI giants ────────────────────────────────
UPDATE public.pad_us_units
   SET is_active = false
 WHERE unit_id IN (26687, 26513);

-- ─── 3. refresh_beach_polygon_membership skips inactive units ────────
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
     and pu.is_active                                       -- NEW: skip soft-deleted
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

  with beaches as (
    select fid, geom
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
         obp.osm_type || ':' || obp.osm_id,
         4, 0,
         obp.area_m2,
         obp.name,
         jsonb_build_object(
           'osm_type',  obp.osm_type,
           'osm_id',    obp.osm_id,
           'name_short', obp.name_short,
           'tags',      obp.tags)
    from beaches b
    join public.osm_beach_polys obp
      on st_contains(obp.geom, b.geom)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_osm = row_count;

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
    order by m.gold_fid, m.area_m2 asc
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
