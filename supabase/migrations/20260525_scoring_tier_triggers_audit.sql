-- 20260525_scoring_tier_triggers_audit.sql
--
-- Closes three classifier wiring gaps discovered 2026-05-25 LATE while
-- debugging Seal Beach (fid=8270) being silently classified as
-- scoring_tier='none' for weeks.
--
-- Existing state:
--   ✓ tg_after_change_refresh_scoring_tier on beach_dog_policy (INSERT+UPDATE)
--     fires refresh_scoring_tier() — works.
--
-- Gaps:
--   1. No trigger on beaches_gold.catchment_state_pct changes. When the
--      catchment cascade fills in a previously-null state_pct, scoring_tier
--      stays 'none' until manually re-fired.
--   2. No periodic schedule. New beaches whose dog_policy / catchment
--      arrived in different orders may slip through both triggers.
--   3. tg_compute_weather_grid_cell only fires on lat/lon change — so
--      manually bumping scoring_tier doesn't trigger weather_grid_lat/lon
--      population. Lazy-fill via the weather_grid_lat IS NULL check.
--
-- Plus W3 follow-up:
--   4. Audit view that surfaces stuck 'none' beaches as review-needed.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. Trigger on beaches_gold.catchment_state_pct → refresh_scoring_tier
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.tg_refresh_scoring_tier_on_catchment()
returns trigger
language plpgsql
as $$
begin
  if new.catchment_state_pct is distinct from old.catchment_state_pct then
    perform public.refresh_scoring_tier(new.fid);
  end if;
  return new;
end $$;

drop trigger if exists tg_after_catchment_refresh_scoring_tier on public.beaches_gold;
create trigger tg_after_catchment_refresh_scoring_tier
  after update of catchment_state_pct on public.beaches_gold
  for each row execute function public.tg_refresh_scoring_tier_on_catchment();

comment on function public.tg_refresh_scoring_tier_on_catchment is
  'Fires refresh_scoring_tier() when a beach''s catchment_state_pct changes. Closes the lazy-classification gap where null state_pct → forced ''none'' until catchment is computed.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. Daily pg_cron schedule — refresh_scoring_tier() for all fids
-- ───────────────────────────────────────────────────────────────────────────
-- Belt + suspenders alongside the per-fid triggers. Catches any beach
-- whose upstream signal updates didn't fire (e.g., manual SQL bypass).

do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'daily_refresh_scoring_tier') then
    perform cron.unschedule('daily_refresh_scoring_tier');
  end if;
  perform cron.schedule(
    'daily_refresh_scoring_tier',
    '0 7 * * *',
    $$ select public.refresh_scoring_tier(); $$
  );
end $cron$;

-- ───────────────────────────────────────────────────────────────────────────
-- 3. Patch tg_compute_weather_grid_cell — lazy-fill on null cached cols
-- ───────────────────────────────────────────────────────────────────────────
-- Add `OR weather_grid_lat IS NULL` to the change check. Means any UPDATE
-- to a row missing the cached cols will populate them, regardless of which
-- field actually changed. Idempotent for rows that already have cols.

create or replace function public.tg_compute_weather_grid_cell()
returns trigger
language plpgsql
as $$
declare
  v_changed boolean;
begin
  v_changed := tg_op = 'INSERT'
            or new.lat is distinct from old.lat
            or new.lon is distinct from old.lon
            or new.weather_grid_lat is null;  -- lazy-fill if cached cols missing

  if v_changed and new.lat is not null and new.lon is not null then
    new.weather_grid_lat := public.weather_grid_bin_lat(new.lat);
    new.weather_grid_lon := public.weather_grid_bin_lon(new.lon);

    insert into public.weather_grid (grid_lat, grid_lon, geom, state)
    values (
      new.weather_grid_lat,
      new.weather_grid_lon,
      st_setsrid(st_makepoint(new.weather_grid_lon::float, new.weather_grid_lat::float), 4326)::geography,
      new.state
    )
    on conflict (grid_lat, grid_lon) do nothing;
  end if;

  return new;
end $$;

-- ───────────────────────────────────────────────────────────────────────────
-- 4. Audit view — active beaches stuck on scoring_tier='none'
-- ───────────────────────────────────────────────────────────────────────────
-- Caller (pipeline_health_audit_job or ad-hoc) can SELECT FROM this view
-- to surface beaches that need curation review.

create or replace view public.beaches_active_unscored AS
select g.fid, g.name, g.state, g.county_name, g.lat, g.lon,
       g.catchment_state_pct,
       bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
       public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start) as computed_tier,
       case
         when bdp.arena_group_id is null then 'no_dog_policy'
         when g.catchment_state_pct is null then 'null_catchment_state_pct'
         when public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start) = '4_no_dogs' then 'classified_no_dogs'
         when public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start) = '3_limited_access'
              and g.catchment_state_pct < 0.40 then 'tier_3_low_catchment'
         else 'other'
       end as gap_reason
from public.beaches_gold g
left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
where g.is_active and g.scoring_tier = 'none';

comment on view public.beaches_active_unscored is
  'Active beaches stuck on scoring_tier=''none''. Surface as review-needed via pipeline_health_audit. Per [[weather-grid-reference-layer]] W3 follow-up.';

grant select on public.beaches_active_unscored to anon, authenticated;

commit;
