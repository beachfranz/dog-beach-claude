-- 20260513_unified_containment_scoring.sql
--
-- Standardizes the 4 simple polygon-containment populators (jurisdictions,
-- counties, military, tribal) on the PAD-US v2 scoring pattern. Per
-- Franz: they're all the same operation against different source tables —
-- standardize the rules.
--
-- BEFORE: each populator chose its own confidence (fixed 0.90, 0.95, 0.99)
--         with a simple st_intersects + area_m2 tiebreak. Beaches just
--         outside a city boundary (50m away, same name) got no match.
--
-- AFTER: all use the v2 pattern:
--   match_strength 4 = inside          → confidence 0.95
--   match_strength 3 = within_100m     → confidence 0.85
--   match_strength 2 = name_match >= 2 → confidence 0.78
--   match_strength 1 = name_match >= 1 AND dist <= 2000m → confidence 0.68
--   match_strength 0 = (skip)
--
-- Tiebreak (consistent across all sources):
--   match_strength desc → name_token_overlap desc → area_m2 asc → id asc
--
-- Two helpers extracted: _polygon_match_strength and _polygon_match_confidence.
-- All 5 spatial-containment populators (PAD-US v2 + the 4 simple ones)
-- now call them, so changing the scoring tiers = one place.
--
-- PAD-US v2's overlay-demotion (is_env_overlay, has_beach_word) tiebreak
-- is preserved — those are PAD-US-specific concerns (wilderness within
-- NWR within Pacific corridor). Doesn't apply to TIGER places/counties/etc.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. Shared scoring helpers
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._polygon_match_strength(
  p_is_inside    boolean,
  p_dist_m       integer,
  p_name_tokens  integer
) returns integer
language sql immutable as $$
  select case
    when p_is_inside                                       then 4
    when p_dist_m is not null and p_dist_m <= 100          then 3
    when coalesce(p_name_tokens, 0) >= 2                   then 2
    when coalesce(p_name_tokens, 0) >= 1
         and p_dist_m is not null and p_dist_m <= 2000     then 1
    else 0
  end;
$$;

create or replace function public._polygon_match_confidence(
  p_match_strength integer
) returns numeric
language sql immutable as $$
  select case p_match_strength
    when 4 then 0.95
    when 3 then 0.85
    when 2 then 0.78
    when 1 then 0.68
    else        0.50
  end::numeric;
$$;

grant execute on function public._polygon_match_strength(boolean, integer, integer)
  to anon, authenticated, service_role;
grant execute on function public._polygon_match_confidence(integer)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 2. jurisdictions (TIGER places — cities + CDPs)
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_jurisdictions_containment_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
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
                else                          'jurisdiction_other'
           end as polygon_kind
    from public.beaches_gold g
    join public.jurisdictions j
      on st_dwithin(j.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength
    from candidates
  ),
  ranked as (
    select *, row_number() over (
      partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, jurisdiction_id asc
    ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'jurisdictions',
    public._polygon_match_confidence(match_strength),
    jsonb_build_object(
      'polygon_kind',       polygon_kind,
      'polygon_id',         jurisdiction_id,
      'polygon_name',       name,
      'place_type',         place_type,
      'county',             county,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                updated_at = now(),
                is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. counties (TIGER counties)
--
-- Counties almost never have name_match (beaches aren't named after
-- counties). 99% will be match_strength=4 (inside). But the unified
-- scoring still applies — and an unusual edge case (e.g. a beach
-- straddling a county boundary) gets sensible handling.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_counties_containment_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
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
    join public.counties c
      on st_dwithin(c.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength
    from candidates
  ),
  ranked as (
    select *, row_number() over (
      partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, county_geoid asc
    ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'counties',
    public._polygon_match_confidence(match_strength),
    jsonb_build_object(
      'polygon_kind',       'county',
      'polygon_id',         county_geoid,
      'polygon_name',       county_name,
      'name_full',          name_full,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                updated_at = now(),
                is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 4. military_bases
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_military_containment_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
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
    join public.military_bases m
      on st_dwithin(m.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength
    from candidates
  ),
  ranked as (
    select *, row_number() over (
      partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, objectid asc
    ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'military_bases',
    public._polygon_match_confidence(match_strength),
    jsonb_build_object(
      'polygon_kind',       'military_base',
      'polygon_id',         objectid,
      'polygon_name',       site_name,
      'component',          component,
      'joint_base',         joint_base,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                updated_at = now(),
                is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 5. tribal_lands
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_tribal_containment_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
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
    join public.tribal_lands t
      on st_dwithin(t.geom::geography, g.geom::geography, 5000)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  scored as (
    select *, public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength
    from candidates
  ),
  ranked as (
    select *, row_number() over (
      partition by fid
      order by match_strength desc, name_token_overlap desc, area_m2 asc, objectid asc
    ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'tribal_lands',
    public._polygon_match_confidence(match_strength),
    jsonb_build_object(
      'polygon_kind',       'tribal_land',
      'polygon_id',         objectid,
      'polygon_name',       lar_name,
      'lar_id',             lar_id,
      'gis_acres',          gis_acres,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                updated_at = now(),
                is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 6. Refactor PAD-US v2 to use the shared helpers (preserves env_overlay
--    + has_beach_word tiebreak signals that are PAD-US-specific).
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_pad_us_containment_gold_v2(p_fid bigint default null)
returns integer
language plpgsql
as $function$
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
     where (p_fid is null or g.fid = p_fid)
       and g.geom is not null
       and g.is_active
  ),
  scored as (
    select *,
      public._polygon_match_strength(is_inside, dist_m, name_token_overlap) as match_strength
    from candidates
  ),
  ranked as (
    select *, row_number() over (
      partition by fid
      order by
        match_strength      desc,
        is_env_overlay      asc,   -- PAD-US-specific: demote wilderness/ACEC
        has_beach_word      desc,  -- PAD-US-specific: prefer named "beach" units
        name_token_overlap  desc,
        area_m2             asc,
        unit_id             asc
    ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'pad_us_v2',
    public._polygon_match_confidence(match_strength),
    jsonb_build_object(
      'polygon_kind',       'pad_us_unit',
      'polygon_id',         unit_id,
      'polygon_name',       unit_name,
      'own_type',           own_type,
      'own_name',           own_name,
      'mng_type',           mng_type,
      'mng_name',           mng_name,
      'des_tp',             des_tp,
      'category',           category,
      'pub_access',         raw_attrs->>'Pub_Access',
      'date_est',           raw_attrs->>'Date_Est',
      'gis_acres',          raw_attrs->>'GIS_Acres',
      'loc_ds',             raw_attrs->>'Loc_Ds',
      'is_env_overlay',     is_env_overlay,
      'has_beach_word',     has_beach_word,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                updated_at = now(),
                is_canonical = false;
  get diagnostics rows_touched = row_count;
  return rows_touched;
end $function$;

commit;
