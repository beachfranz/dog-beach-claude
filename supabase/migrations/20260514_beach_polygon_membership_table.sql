-- 20260514_beach_polygon_membership_table.sql
--
-- Phase 1 of task #12 (beach_polygon_membership materialized lookup).
-- See project_beach_polygon_membership_spec.md.
--
-- Today: build_beach_evidence(fid) runs 5 separate ST_Intersects scans per
-- fid (populate_polygon_containment_gold + populate_from_cpad_gold +
-- populate_from_pad_us_gold + _emit_evidence_from_pad_us_dogs_policy +
-- governance dispatcher). With OR's 151 scoreable beaches × huge
-- federal PAD-US polygons, that dominates the ~18s/fid cost.
--
-- This phase: create the membership lookup table + populator. Phase 2
-- (next migration) rewrites the polygon-based populators to read from
-- this table instead of running their own ST_Intersects.
--
-- Idempotent: refresh_beach_polygon_membership(state, fid) DELETEs the
-- scope first, then re-populates. Safe to call repeatedly.

begin;

create table if not exists public.beach_polygon_membership (
  gold_fid        bigint not null references public.beaches_gold(fid) on delete cascade,
  polygon_kind    text   not null,         -- 'pad_us_unit' / 'cpad_unit' / 'c1_city' / 'cdp' / 'county' / 'military_base' / 'tribal_land'
  polygon_id      text   not null,         -- source polygon's unique id (text — varies per source)
  match_strength  int    not null check (match_strength between 1 and 4),
                                            -- 4=within, 3=100m, 2=500m, 1=2km
  dist_m          numeric,                  -- 0 when within; else point-to-polygon distance
  area_m2         numeric,                  -- polygon area (drives size_bonus in resolver)

  -- Polygon attributes snapshot — saves re-joining the source table
  polygon_name    text,
  mng_name        text,                     -- PAD-US mng_name code (SPR/USFS/NPS/FWS/CITY/CNTY/etc.) when applicable
  mng_type        text,                     -- STAT / LOC / FED / DIST / TRIB / etc.
  own_name        text,                     -- PAD-US own_name (owner)
  own_type        text,
  loc_mang        text,                     -- PAD-US loc_mang (manager name) — critical for polygon_to_operator dispatcher
  raw_attrs       jsonb,                    -- source-specific extras (Pub_Access, agncy_name, etc.)

  computed_at     timestamptz not null default now(),

  primary key (gold_fid, polygon_kind, polygon_id)
);

create index if not exists beach_polygon_membership_kind_fid_idx
  on public.beach_polygon_membership (polygon_kind, gold_fid);

create index if not exists beach_polygon_membership_polygon_idx
  on public.beach_polygon_membership (polygon_kind, polygon_id);

comment on table public.beach_polygon_membership is
  'Materialized per-fid polygon containment lookup. Populated once per state load via refresh_beach_polygon_membership(). Read by populate_polygon_containment_gold + dependent populators instead of running per-fid ST_Intersects. See project_beach_polygon_membership_spec.md.';


-- ────────────────────────────────────────────────────────────────────────
-- refresh_beach_polygon_membership(p_state, p_fid)
-- ────────────────────────────────────────────────────────────────────────
--
-- DELETEs the scope first, then re-emits rows from each polygon source.
-- match_strength derived per joint spatial×name matrix (project pin
-- match_confidence_joint_spatial_name):
--   4 = ST_Contains (within)
--   3 = ST_DWithin 100m
--   2 = ST_DWithin 500m
--   1 = ST_DWithin 2km (outer bound)

-- Drop first since CREATE OR REPLACE can't change function return type
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
           when st_contains(pu.geom, b.geom)                          then 4
           when st_dwithin(pu.geom::geography, b.geog, 100)            then 3
           when st_dwithin(pu.geom::geography, b.geog, 500)            then 2
           else                                                            1
         end,
         st_distance(pu.geom::geography, b.geog),
         st_area(pu.geom::geography),
         pu.unit_name, pu.mng_name, pu.mng_type, pu.own_name, pu.own_type, pu.loc_mang,
         pu.raw_attrs
    from beaches b
    join public.pad_us_units pu
      on pu.state = p_state
     and st_dwithin(pu.geom::geography, b.geog, 2000)
  on conflict (gold_fid, polygon_kind, polygon_id) do nothing;
  get diagnostics v_total_pad_us = row_count;

  -- ── CPAD units (CA only) ────────────────────────────────────────────
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

  -- ── TIGER jurisdictions (incorporated places + CDPs) ────────────────
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

  -- ── Counties ────────────────────────────────────────────────────────
  -- counties.state_fp matches state via lookup; just join on geometry
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

  -- ── Military bases ──────────────────────────────────────────────────
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

  -- ── Tribal lands ────────────────────────────────────────────────────
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
