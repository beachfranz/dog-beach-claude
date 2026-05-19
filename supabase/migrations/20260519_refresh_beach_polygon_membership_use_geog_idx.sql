-- 20260519_refresh_beach_polygon_membership_use_geog_idx.sql
--
-- Perf fix: refresh_beach_polygon_membership was 14s per fid on WA
-- (measured with EXPLAIN ANALYZE). Root cause: the pad_us_units join
-- uses `pu.geom::geography`, which casts every row at runtime and
-- defeats the existing `pad_us_units_geom_geog_idx` GIST index on the
-- precomputed `geom_geog` column.
--
-- pad_us_units has 308,001 rows. Per-row geography casting is the
-- dominant cost. Using pu.geom_geog directly lets the planner use the
-- geog GIST index and should drop per-fid time from ~14s to sub-second.
--
-- Other polygon tables (counties=3k, jurisdictions=32k, military=89,
-- tribal=138) don't have geog indexes but their row counts are small
-- enough that the geom-cast cost is negligible. Leaving them alone.
--
-- Only the PAD-US block of the function body changes. Everything else
-- is identical to 20260514_beach_polygon_membership_table.sql.

begin;

drop function if exists public.refresh_beach_polygon_membership(text, bigint);

create function public.refresh_beach_polygon_membership(
  p_state text,
  p_fid   bigint default null
)
returns table(kind text, n_rows bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_total_pad_us  bigint := 0;
  v_total_cpad    bigint := 0;
  v_total_jur     bigint := 0;
  v_total_county  bigint := 0;
  v_total_mil     bigint := 0;
  v_total_tribal  bigint := 0;
begin
  if p_state is null then
    raise exception 'refresh_beach_polygon_membership: p_state required';
  end if;

  -- Clear the scope being recomputed
  delete from public.beach_polygon_membership m
   using public.beaches_gold g
   where m.gold_fid = g.fid
     and g.state = p_state
     and (p_fid is null or g.fid = p_fid);

  -- ── PAD-US units ────────────────────────────────────────────────────
  -- Uses pu.geom_geog directly (not pu.geom::geography) so the GIST
  -- index on geom_geog actually fires. Big win on 308k-row table.
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

  -- ── CPAD units (CA only) ────────────────────────────────────────────
  -- Unchanged — CA-only and cpad_units row count is small.
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

  -- ── TIGER jurisdictions (32k rows; cast cost acceptable) ────────────
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

  -- ── Counties (3k rows; trivial) ─────────────────────────────────────
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
      on st_dwithin(c.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_county = row_count;

  -- ── Military bases (89 rows; trivial) ───────────────────────────────
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
    join public.military_bases mb on st_dwithin(mb.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_mil = row_count;

  -- ── Tribal lands (138 rows; trivial) ────────────────────────────────
  with beaches as (
    select fid, geom, geom::geography as geog
      from public.beaches_gold
     where state = p_state and is_active = true
       and (p_fid is null or fid = p_fid)
       and geom is not null
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
    join public.tribal_lands tl on st_dwithin(tl.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_tribal = row_count;

  return query
    select 'pad_us_unit'::text   as kind, v_total_pad_us as n_rows
    union all select 'cpad_unit',      v_total_cpad
    union all select 'c1_city',        v_total_jur
    union all select 'county',         v_total_county
    union all select 'military_base',  v_total_mil
    union all select 'tribal_land',    v_total_tribal;
end $function$;

grant execute on function public.refresh_beach_polygon_membership(text, bigint)
  to anon, authenticated, service_role;

commit;
