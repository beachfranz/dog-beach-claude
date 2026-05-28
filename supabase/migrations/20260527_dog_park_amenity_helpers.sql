-- 20260527_dog_park_amenity_helpers.sql
--
-- Promote the "promote all parks in state" loop and the "amenity coverage
-- summary" ad-hoc queries into proper SQL functions per memory pin
-- [[promote_ad_hoc_tools_to_process]]. Both used by the Dagster amenity
-- re-extraction asset (and reusable by hand).

begin;

-- ── promote_canonical_dog_park_policies_for_state(state)
-- Iterates all active parks in state and runs promote_canonical_dog_park_policy
-- for each. Returns count of parks touched. Reusable: post-extract sweep.

create or replace function public.promote_canonical_dog_park_policies_for_state(
  p_state text
) returns integer language plpgsql as $$
declare
  v_count integer := 0;
  f bigint;
begin
  for f in
    select fid from public.dog_parks_gold
    where state = p_state and is_active = true
    order by fid
  loop
    perform public.promote_canonical_dog_park_policy(f);
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$$;

comment on function public.promote_canonical_dog_park_policies_for_state is
  'Bulk-promote all active dog parks in a state from BEP rows into '
  'dog_park_dog_policy. Returns count of parks touched. Called by '
  'Dagster amenity-refresh asset after extractor runs.';


-- ── dog_park_amenity_coverage(state)
-- Returns coverage stats for all amenity columns. Used by the
-- Dagster asset for metadata reporting + by humans for spot checks.

create or replace function public.dog_park_amenity_coverage(
  p_state text default null
) returns table(
  total_active integer,
  has_fence_pct numeric,
  has_drinking_water_pct numeric,
  double_gate_pct numeric,
  small_dog_area_pct numeric,
  large_dog_area_pct numeric,
  lighting_pct numeric,
  has_shade_pct numeric,
  has_agility_pct numeric,
  has_water_play_pct numeric,
  has_picnic_tables_pct numeric,
  surface_overlay_pct numeric,
  description_overlay_pct numeric
) language sql stable as $$
  with active as (
    select dpg.fid
    from public.dog_parks_gold dpg
    where dpg.is_active = true
      and (p_state is null or dpg.state = p_state)
  ),
  joined as (
    select a.fid, dpp.*
    from active a
    left join public.dog_park_dog_policy dpp on dpp.dog_park_fid = a.fid
  )
  select
    count(*)::int as total_active,
    round(100.0 * count(*) filter (where has_fence is not null) / nullif(count(*), 0), 1) as has_fence_pct,
    round(100.0 * count(*) filter (where has_drinking_water is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where double_gate is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where small_dog_area is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where large_dog_area is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where lighting is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where has_shade is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where has_agility is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where has_water_play is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where has_picnic_tables is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where surface_overlay is not null) / nullif(count(*), 0), 1),
    round(100.0 * count(*) filter (where description_overlay is not null) / nullif(count(*), 0), 1)
  from joined;
$$;

comment on function public.dog_park_amenity_coverage is
  'Returns % of active parks (optionally filtered by state) that have each '
  'amenity column populated (any value, including false). Use for post-extract '
  'coverage reporting.';

commit;
