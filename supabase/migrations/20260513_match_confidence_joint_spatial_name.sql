-- 20260513_match_confidence_joint_spatial_name.sql
--
-- Restores name similarity to the CORE of match scoring (Franz 2026-05-13).
-- Spatial AND name are both core determiners; authority and size are side
-- bonuses in the resolver.
--
-- Architecture:
--   match_confidence = joint(spatial_strength, name_tokens)
--                      ^ from polygon-containment populator, NO double-count
--
--   score (resolver) = match_confidence + authority_bonus + size_bonus
--                      ^ no separate name_sim_bonus — already in confidence
--
-- The joint matrix:
--                     name_tokens 0    name_tokens 1   name_tokens 2+
--   spatial=4 inside  0.85             0.90            0.95
--   spatial=3 100m    0.75             0.80            0.85
--   spatial=2 500m    0.65             0.70            0.75
--   spatial=1 2km     0.50             0.55            0.60
--   spatial=0         (no row)         (no row)        (no row)
--
-- Formula: confidence = 0.50 + 0.10·(spatial_strength − 1) + 0.05·name_tier
--   where name_tier = 0 (no match), 1 (one token), 2 (two+ tokens)

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. Update _polygon_match_confidence — now takes BOTH signals.
--    Signature changes from (strength) to (strength, name_tokens).
-- ────────────────────────────────────────────────────────────────────────

drop function if exists public._polygon_match_confidence(integer);

create or replace function public._polygon_match_confidence(
  p_spatial_strength integer,
  p_name_tokens      integer default 0
) returns numeric
language sql immutable as $$
  select case
    when p_spatial_strength is null or p_spatial_strength < 1 then 0.00
    else
      0.50
      + 0.10 * (p_spatial_strength - 1)::numeric
      + case
          when coalesce(p_name_tokens, 0) >= 2 then 0.10
          when coalesce(p_name_tokens, 0) >= 1 then 0.05
          else                                       0.00
        end
  end::numeric;
$$;

grant execute on function public._polygon_match_confidence(integer, integer)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 2. Drop name_sim_bonus from _governance_score (signal now lives in
--    match_confidence; bonus would double-count).
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._governance_score(
  p_confidence       numeric,
  p_claimed_values   jsonb,
  p_source           text,
  p_operator_level   text default null
)
returns numeric
language sql immutable as $$
  select
    coalesce(p_confidence, 0)
    -- ── authority_bonus ──
    + case
        when p_source = 'manual' then 0.10
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'FED'
                  or p_operator_level = 'federal') then 0.08
        when p_claimed_values->>'polygon_kind' = 'military_base' then 0.07
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'STAT'
                  or p_operator_level = 'state')   then 0.06
        when p_claimed_values->>'polygon_kind' = 'tribal_land'  then 0.06
        when p_claimed_values->>'polygon_kind' = 'cpad_unit'    then 0.06
        when p_claimed_values->>'polygon_kind' = 'c1_city'      then 0.03
        when p_claimed_values->>'polygon_kind' = 'cdp'          then 0.02
        when p_claimed_values->>'polygon_kind' = 'county'       then 0.00
        else                                                       -0.05
      end
    -- ── size_bonus ──
    + case
        when (p_claimed_values->>'area_m2')::numeric is null then 0
        when (p_claimed_values->>'area_m2')::numeric < 1e6   then 0.05
        when (p_claimed_values->>'area_m2')::numeric < 1e7   then 0.03
        when (p_claimed_values->>'area_m2')::numeric < 1e8   then 0.01
        else                                                      0.00
      end
$$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. Update each polygon_containment populator to pass name_tokens
--    into _polygon_match_confidence. Five sources: pad_us_v2, jurisdictions,
--    counties, military, tribal. Same shape, same scoring helpers, just
--    threading the name_tokens argument through.
-- ────────────────────────────────────────────────────────────────────────

-- jurisdictions
create or replace function public.populate_jurisdictions_containment_gold(p_fid bigint default null)
returns integer language plpgsql as $function$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, j.id as jurisdiction_id, j.name, j.place_type, j.county,
           st_area(j.geom::geography) as area_m2,
           st_distance(j.geom::geography, g.geom::geography)::int as dist_m,
           st_contains(j.geom, g.geom::geometry) as is_inside,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), j.name)) as name_token_overlap,
           case when j.place_type = 'C1' then 'c1_city'
                when j.place_type = 'U1' then 'cdp'
                else                          'jurisdiction_other' end as polygon_kind
    from public.beaches_gold g
    join public.jurisdictions j on st_dwithin(j.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid) and g.geom is not null and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength from candidates
  ),
  ranked as (
    select *, row_number() over (partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, jurisdiction_id asc) as rnk
    from scored where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'jurisdictions',
    public._polygon_match_confidence(match_strength, name_token_overlap),
    jsonb_build_object('polygon_kind', polygon_kind, 'polygon_id', jurisdiction_id,
      'polygon_name', name, 'place_type', place_type, 'county', county,
      'name_token_overlap', name_token_overlap, 'area_m2', area_m2,
      'dist_m', dist_m, 'match_strength', match_strength), now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
                updated_at = now(), is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- counties
create or replace function public.populate_counties_containment_gold(p_fid bigint default null)
returns integer language plpgsql as $function$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, c.geoid as county_geoid, c.name as county_name, c.name_full,
           st_area(c.geom::geography) as area_m2,
           st_distance(c.geom::geography, g.geom::geography)::int as dist_m,
           st_contains(c.geom, g.geom::geometry) as is_inside,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), c.name)) as name_token_overlap
    from public.beaches_gold g
    join public.counties c on st_dwithin(c.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid) and g.geom is not null and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength from candidates
  ),
  ranked as (
    select *, row_number() over (partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, county_geoid asc) as rnk
    from scored where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'counties',
    public._polygon_match_confidence(match_strength, name_token_overlap),
    jsonb_build_object('polygon_kind', 'county', 'polygon_id', county_geoid,
      'polygon_name', county_name, 'name_full', name_full,
      'name_token_overlap', name_token_overlap, 'area_m2', area_m2,
      'dist_m', dist_m, 'match_strength', match_strength), now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
                updated_at = now(), is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- military_bases
create or replace function public.populate_military_containment_gold(p_fid bigint default null)
returns integer language plpgsql as $function$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, m.objectid, m.site_name, m.component, m.joint_base,
           st_area(m.geom::geography) as area_m2,
           st_distance(m.geom::geography, g.geom::geography)::int as dist_m,
           st_contains(m.geom, g.geom::geometry) as is_inside,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), m.site_name)) as name_token_overlap
    from public.beaches_gold g
    join public.military_bases m on st_dwithin(m.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid) and g.geom is not null and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength from candidates
  ),
  ranked as (
    select *, row_number() over (partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, objectid asc) as rnk
    from scored where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'military_bases',
    public._polygon_match_confidence(match_strength, name_token_overlap),
    jsonb_build_object('polygon_kind', 'military_base', 'polygon_id', objectid,
      'polygon_name', site_name, 'component', component, 'joint_base', joint_base,
      'name_token_overlap', name_token_overlap, 'area_m2', area_m2,
      'dist_m', dist_m, 'match_strength', match_strength), now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
                updated_at = now(), is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- tribal_lands
create or replace function public.populate_tribal_containment_gold(p_fid bigint default null)
returns integer language plpgsql as $function$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, t.objectid, t.lar_id, t.lar_name, t.gis_acres,
           st_area(t.geom::geography) as area_m2,
           st_distance(t.geom::geography, g.geom::geography)::int as dist_m,
           st_contains(t.geom, g.geom::geometry) as is_inside,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), t.lar_name)) as name_token_overlap
    from public.beaches_gold g
    join public.tribal_lands t on st_dwithin(t.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid) and g.geom is not null and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength from candidates
  ),
  ranked as (
    select *, row_number() over (partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, objectid asc) as rnk
    from scored where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'tribal_lands',
    public._polygon_match_confidence(match_strength, name_token_overlap),
    jsonb_build_object('polygon_kind', 'tribal_land', 'polygon_id', objectid,
      'polygon_name', lar_name, 'lar_id', lar_id, 'gis_acres', gis_acres,
      'name_token_overlap', name_token_overlap, 'area_m2', area_m2,
      'dist_m', dist_m, 'match_strength', match_strength), now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
                updated_at = now(), is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- pad_us_v2
create or replace function public.populate_pad_us_containment_gold_v2(p_fid bigint default null)
returns integer language plpgsql as $function$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, pu.unit_id, pu.unit_name, pu.own_type, pu.own_name,
           pu.mng_type, pu.mng_name, pu.des_tp, pu.category,
           st_area(pu.geom::geography) as area_m2,
           st_distance(pu.geom_geog, g.geom::geography)::int as dist_m,
           st_contains(pu.geom, g.geom::geometry) as is_inside,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), pu.unit_name)) as name_token_overlap,
           pu.unit_name ~* '\mbeach\M' as has_beach_word,
           pu.unit_name ~* '(wilderness|wsa|acec|wsr|ira|nca)' as is_env_overlay,
           pu.raw_attrs
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_dwithin(pu.geom_geog, g.geom::geography, 5000)
       and pu.state = g.state
     where (p_fid is null or g.fid = p_fid) and g.geom is not null and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength from candidates
  ),
  ranked as (
    select *, row_number() over (partition by fid
      order by match_strength desc, is_env_overlay asc, has_beach_word desc,
               name_token_overlap desc, area_m2 asc, unit_id asc) as rnk
    from scored where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'pad_us_v2',
    public._polygon_match_confidence(match_strength, name_token_overlap),
    jsonb_build_object('polygon_kind', 'pad_us_unit', 'polygon_id', unit_id,
      'polygon_name', unit_name, 'own_type', own_type, 'own_name', own_name,
      'mng_type', mng_type, 'mng_name', mng_name, 'des_tp', des_tp,
      'category', category, 'pub_access', raw_attrs->>'Pub_Access',
      'date_est', raw_attrs->>'Date_Est', 'gis_acres', raw_attrs->>'GIS_Acres',
      'loc_ds', raw_attrs->>'Loc_Ds', 'is_env_overlay', is_env_overlay,
      'has_beach_word', has_beach_word, 'name_token_overlap', name_token_overlap,
      'area_m2', area_m2, 'dist_m', dist_m, 'match_strength', match_strength), now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
                updated_at = now(), is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

commit;
