-- 20260509_inland_beaches_scoreable.sql
--
-- Franz: inland beaches without NOAA stations should remain scoreable.
-- The Edge Function will skip the tide fetch and the scoring math will
-- treat tideHeight=null the same way it treats null crowd/busyness:
-- a neutral 0.5 fallback. Pump-up effect is implicit because tide can
-- no longer drive the score down (high tide → 0.0).
--
-- This migration:
--   1. Removes the noaa_station_id IS NOT NULL gate from
--      align_is_scoreable_to_tier (issue #21 mitigation v2 — was too
--      strict).
--   2. Drops assert_scoreable_have_noaa_for_state (the canon's
--      noaa_station_check phase). Inland beaches without a station are
--      a normal state, not a halt condition.
--
-- Edge Function counterpart: index.ts no longer bails on null
-- noaa_station_id; processes beach with empty tideMap, scoring uses
-- 0.5 fallback per existing null-tideHeight path.

begin;

-- ── 1. align_is_scoreable_to_tier — drop NOAA requirement ──────────────
create or replace function public.align_is_scoreable_to_tier(p_state text)
returns table(promoted int, demoted int)
language plpgsql
as $function$
declare
  v_promoted int := 0;
  v_demoted  int := 0;
begin
  -- Promote: tier-1+2 → is_scoreable=true (no NOAA gate)
  with cands as (
    select g.fid
      from public.beaches_gold g
      join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.state = p_state and g.is_active
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

  -- Demote: scoreable but EITHER no policy OR not tier 1+2
  -- (Inland beaches with no NOAA station stay scoreable as long as
  -- tier classifies as 1+2 — Edge Function handles missing tide.)
  with cands as (
    select g.fid
      from public.beaches_gold g
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.state = p_state and g.is_active and g.is_scoreable
       and (
         bdp.arena_group_id is null
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


-- ── 2. assert_scoreable_have_noaa_for_state — relax to advisory ────────
-- Was a hard halt. Now returns true with a NOTICE that logs how many
-- scoreable beaches lack a station. The canon's noaa_station_check
-- phase becomes a coverage-reporting checkpoint, not a halt.

create or replace function public.assert_scoreable_have_noaa_for_state(p_state text)
returns boolean
language plpgsql
stable
as $$
declare
  v_total   int;
  v_missing int;
begin
  select count(*) filter (where is_scoreable),
         count(*) filter (where is_scoreable and noaa_station_id is null)
    into v_total, v_missing
    from public.beaches_gold
   where state = p_state and is_active;

  if v_missing > 0 then
    raise notice 'state %: % of % scoreable beaches lack noaa_station_id (inland — '
                 'tide will be skipped in scoring)', p_state, v_missing, v_total;
  end if;
  return true;
end $$;


grant execute on function public.align_is_scoreable_to_tier(text)            to anon, authenticated, service_role;
grant execute on function public.assert_scoreable_have_noaa_for_state(text)  to anon, authenticated, service_role;

commit;
