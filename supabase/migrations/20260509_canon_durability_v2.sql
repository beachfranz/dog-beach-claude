-- 20260509_canon_durability_v2.sql
--
-- Refines 20260509_canon_durability_fixes.sql after testing it against
-- the full DB. The first version's `assert_promote_complete_for_state`
-- demanded noaa_station_id on every ACTIVE beach. That caught a real
-- gap (21 of 404 WA active beaches had NULL station_id), but those
-- beaches turned out to be inland: Lake Chelan, Yakima River, Spokane,
-- Pend Oreille — 100-400km from the nearest NOAA tide gauge because
-- they aren't coastal. They're legitimate catalog entries but should
-- not be `is_scoreable=true` (no tide data → no day-scoring possible).
--
-- Right semantic:
--   - noaa_station_id NULL is OK on inland/lake beaches (catalog only)
--   - noaa_station_id IS NOT NULL is REQUIRED on every is_scoreable beach
--   - align_is_scoreable_to_tier flips is_scoreable=false on any beach
--     with NULL station_id, even if its tier would otherwise score it
--
-- Also: tighten the promote-completeness assertion to scoreable beaches
-- only. Inland beaches get a free pass on station_id but still need
-- county_fips / state / location_id (the structural fields).

begin;

-- ── 1. align_is_scoreable_to_tier — also require noaa_station_id ───────

create or replace function public.align_is_scoreable_to_tier(p_state text)
returns table(promoted int, demoted int)
language plpgsql
as $function$
declare
  v_promoted int := 0;
  v_demoted  int := 0;
begin
  -- Promote: tier-1+2 with NOAA station → is_scoreable=true
  with cands as (
    select g.fid
      from public.beaches_gold g
      join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.state = p_state and g.is_active
       and g.noaa_station_id is not null   -- ★ new requirement
       and not g.is_scoreable
       and public.beach_location_tier(
             bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
             bdp.dogs_prohibited_start::text)
           in ('1_off-leash','2_on-leash')
  )
  update public.beaches_gold g
     set is_scoreable = true
    from cands
   where g.fid = cands.fid;
  get diagnostics v_promoted = row_count;

  -- Demote: scoreable but EITHER tier 3/4/unknown OR no NOAA station
  with cands as (
    select g.fid
      from public.beaches_gold g
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.state = p_state and g.is_active and g.is_scoreable
       and (
         -- No tide data
         g.noaa_station_id is null
         -- Or no policy / out-of-scope tier
         or bdp.arena_group_id is null
         or public.beach_location_tier(
               bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
               bdp.dogs_prohibited_start::text)
             not in ('1_off-leash','2_on-leash')
       )
  )
  update public.beaches_gold g
     set is_scoreable = false
    from cands
   where g.fid = cands.fid;
  get diagnostics v_demoted = row_count;

  return query select v_promoted, v_demoted;
end $function$;

grant execute on function public.align_is_scoreable_to_tier(text) to anon, authenticated, service_role;


-- ── 2. assert_promote_complete_for_state — require station only on scoreable
-- Active inland beaches with NULL station_id are OK (catalog-only).
-- Active scoreable beaches MUST have a station (no tide data = no scoring).

create or replace function public.assert_promote_complete_for_state(p_state text)
returns boolean
language plpgsql
stable
as $$
declare
  v_missing_cfips int;
  v_missing_state int;
  v_total_active  int;
begin
  select count(*),
         count(*) filter (where county_fips is null),
         count(*) filter (where state is null)
    into v_total_active, v_missing_cfips, v_missing_state
    from public.beaches_gold
   where state = p_state and is_active;

  if v_missing_cfips > 0 then
    raise exception
      'state %: % of % active beaches have NULL county_fips after promote. '
      'See issue #2/#18 (county_fips populator drift).',
      p_state, v_missing_cfips, v_total_active;
  end if;

  if v_missing_state > 0 then
    raise exception
      'state %: % of % active beaches have NULL state after promote. '
      'Check _infer_state_from_county_fips and _infer_state_from_county.',
      p_state, v_missing_state, v_total_active;
  end if;

  return true;
end $$;


-- ── 3. New SQL function: assert station coverage on scoreable beaches ──
-- Used as the criterion for a new phase `noaa_station_check`. Halts if
-- any scoreable beach in state lacks a station. Cheap idempotent check.

create or replace function public.assert_scoreable_have_noaa_for_state(p_state text)
returns boolean
language plpgsql
stable
as $$
declare
  v_missing int;
  v_total   int;
  v_sample  text;
begin
  select count(*) filter (where is_scoreable),
         count(*) filter (where is_scoreable and noaa_station_id is null)
    into v_total, v_missing
    from public.beaches_gold
   where state = p_state and is_active;

  if v_missing > 0 then
    select string_agg(name, ', ' order by name) into v_sample
      from (select name from public.beaches_gold
              where state = p_state and is_active and is_scoreable
                and noaa_station_id is null
              limit 5) x;
    raise exception
      'state %: % of % scoreable beaches have NULL noaa_station_id (issue #21). '
      'Sample: %. Either backfill via _nearest_noaa_station(geom, 100), or '
      'they are inland beaches that should be is_scoreable=false (run '
      'align_is_scoreable_to_tier).',
      p_state, v_missing, v_total, coalesce(v_sample, '(none)');
  end if;

  return true;
end $$;


grant execute on function public.assert_promote_complete_for_state(text)        to anon, authenticated, service_role;
grant execute on function public.assert_scoreable_have_noaa_for_state(text)     to anon, authenticated, service_role;

commit;
