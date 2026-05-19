-- 20260519_pip_prelaunch_state_filters_all_polygons.sql
--
-- Per docs/pip_prelaunch_spec.md (Franz directive 2026-05-19):
-- Every polygon kind joined in refresh_beach_polygon_membership MUST
-- be narrowed to in-state rows before the spatial test. Every polygon
-- table MUST have a GIST index on the join geometry. Both invariants
-- are asserted at PIP-prelaunch.
--
-- This migration:
--   1. Creates assert_pip_indices(p_state) — pre-flight assertion
--   2. Adds state filtering to military_bases join (state_postal column)
--   3. Adds state-polygon prefilter to tribal_lands join (no state column)
--   4. Calls assert_pip_indices as step 1 inside refresh_beach_polygon_membership
--
-- Together with the two earlier perf fixes (geom_geog on pad_us,
-- state_fp on counties), per-fid time drops from ~1.24s → expected <0.8s.

begin;

-- ────────────────────────────────────────────────────────────────────
-- assert_pip_indices(p_state)
-- ────────────────────────────────────────────────────────────────────
-- Pre-flight check called as step 1 of refresh_beach_polygon_membership.
-- Returns one row per polygon table with status='ok'|'warn'|'fail'.
-- Fails fast if any expected GIST index is missing — never silently
-- scan-the-world.

create or replace function public.assert_pip_indices(p_state text)
returns table(polygon_table text, status text, note text)
language plpgsql stable
set search_path = 'public', 'pg_catalog'
as $function$
declare
  v_has_idx boolean;
begin
  if p_state is null then
    raise exception 'assert_pip_indices: p_state required';
  end if;

  -- ── pad_us_units: needs geom_geog GIST + state column ──────────────
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'pad_us_units'
       and (indexdef ilike '%(geom_geog%)' or indexdef ilike '%(geom)%')
  ) into v_has_idx;
  polygon_table := 'pad_us_units';
  status := case when v_has_idx then 'ok' else 'fail' end;
  note   := case when v_has_idx then 'GIST(geom_geog) present, state filter via pu.state'
                 else 'MISSING GIST geom_geog index — full scan risk' end;
  return next;

  -- ── jurisdictions: GIST + state column ─────────────────────────────
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'jurisdictions'
       and indexdef ilike '%(geom)%'
  ) into v_has_idx;
  polygon_table := 'jurisdictions';
  status := case when v_has_idx then 'ok' else 'fail' end;
  note   := case when v_has_idx then 'GIST(geom) present, state filter via j.state'
                 else 'MISSING GIST geom index' end;
  return next;

  -- ── counties: GIST + state_fp via lookup ───────────────────────────
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'counties'
       and indexdef ilike '%(geom)%'
  ) into v_has_idx;
  polygon_table := 'counties';
  status := case when v_has_idx then 'ok' else 'fail' end;
  note   := case when v_has_idx then 'GIST(geom) present, state filter via state_fp lookup'
                 else 'MISSING GIST geom index' end;
  return next;

  -- ── military_bases: GIST + state_postal direct ─────────────────────
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'military_bases'
       and indexdef ilike '%(geom)%'
  ) into v_has_idx;
  polygon_table := 'military_bases';
  status := case when v_has_idx then 'ok' else 'fail' end;
  note   := case when v_has_idx then 'GIST(geom) present, state filter via mb.state_postal'
                 else 'MISSING GIST geom index' end;
  return next;

  -- ── tribal_lands: GIST + state-poly intersect prefilter ────────────
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'tribal_lands'
       and indexdef ilike '%(geom)%'
  ) into v_has_idx;
  polygon_table := 'tribal_lands';
  status := case when v_has_idx then 'ok' else 'fail' end;
  note   := case when v_has_idx then 'GIST(geom) present, state filter via ST_Intersects(state poly)'
                 else 'MISSING GIST geom index' end;
  return next;

  -- ── states: needed for tribal_lands prefilter (and others lacking state cols) ──
  select exists(
    select 1 from pg_indexes
     where schemaname = 'public' and tablename = 'states'
       and indexdef ilike '%(geom)%'
  ) into v_has_idx;
  polygon_table := 'states';
  status := case when v_has_idx then 'ok' else 'warn' end;
  note   := case when v_has_idx then 'GIST(geom) present (used as state-poly source)'
                 else 'recommended GIST(geom) — would speed state-poly prefilters' end;
  return next;
end;
$function$;

grant execute on function public.assert_pip_indices(text)
  to anon, authenticated, service_role;

comment on function public.assert_pip_indices(text) is
  'PIP-prelaunch index assertion. Returns per-polygon-table {ok,warn,fail}. '
  'Called as step 1 of refresh_beach_polygon_membership; halts on any fail. '
  'Add new polygon kinds here when joined in PIP. See docs/pip_prelaunch_spec.md.';


-- Ensure recommended index exists (CONCURRENTLY would be nicer but
-- migration runs in a tx — use IF NOT EXISTS instead).
create index if not exists states_geom_gix
  on public.states using gist (geom);


-- ────────────────────────────────────────────────────────────────────
-- refresh_beach_polygon_membership v3
-- ────────────────────────────────────────────────────────────────────
-- Changes vs v2:
--   - Step 0: call assert_pip_indices and raise on any 'fail'
--   - military_bases: add state_postal filter
--   - tribal_lands: add state-polygon intersect prefilter
-- Spec compliance: every polygon kind has a state filter.

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
  v_state_fp      text;
  v_failed_check  text;
begin
  if p_state is null then
    raise exception 'refresh_beach_polygon_membership: p_state required';
  end if;

  -- ── Step 0: PIP-prelaunch assertion ────────────────────────────────
  select string_agg(polygon_table || ': ' || note, '; ')
    into v_failed_check
    from public.assert_pip_indices(p_state)
   where status = 'fail';
  if v_failed_check is not null then
    raise exception 'PIP-prelaunch failed: %', v_failed_check;
  end if;

  -- Resolve state_fp once (used by counties join).
  select distinct fips_state into v_state_fp
    from public.jurisdictions
   where state = p_state
   limit 1;

  -- Clear the scope being recomputed
  delete from public.beach_polygon_membership m
   using public.beaches_gold g
   where m.gold_fid = g.fid
     and g.state = p_state
     and (p_fid is null or g.fid = p_fid);

  -- ── PAD-US units (state-filtered + geom_geog idx) ──────────────────
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

  -- ── CPAD units (CA only; table is CA-scoped) ───────────────────────
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

  -- ── TIGER jurisdictions (state-filtered) ───────────────────────────
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

  -- ── Counties (state_fp pre-filter) ─────────────────────────────────
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

  -- ── Military bases (state_postal direct) ───────────────────────────
  -- NEW: was previously unfiltered (scanned all 89 US bases per beach).
  -- Direct state_postal column is the cheapest pre-filter.
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

  -- ── Tribal lands (state-polygon intersect prefilter) ───────────────
  -- NEW: tribal_lands has NO state column. Use state-poly intersect
  -- prefilter (states.geom GIST index added by this migration).
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

comment on function public.refresh_beach_polygon_membership(text, bigint) is
  'Per-state (and optional per-fid) PIP populator. Calls assert_pip_indices '
  'as step 0; halts on any fail status. Each polygon kind state-filtered per '
  'docs/pip_prelaunch_spec.md. See benchmark trail in that doc.';

commit;
