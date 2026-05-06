-- 20260506_verdict_polygon_triggers.sql
--
-- Trigger-wire what was scheduled an hour ago:
--   - verdict cascade: per-fid compute_dogs_verdict_by_origin(fid)
--     fires on beach_dog_policy change. Per-fid is cheap (one beach
--     at a time) so it's safe to run inline. Replaces the nightly
--     cron.
--   - polygon resolver: refresh_beach_polygons() is full-table
--     rebuild but only the source tables (osm_beach_polys, cpad_units)
--     change rarely. Statement-level trigger fires once per INSERT/
--     UPDATE/DELETE statement -- bulk load = one rebuild.
--
-- Drops the daily verdict_cascade_nightly cron and weekly
-- beach_polygons_weekly cron in favor of these.

begin;

-- ----------------------------------------------------------------------
-- 1. Verdict cascade per-fid trigger on beach_dog_policy
-- ----------------------------------------------------------------------
create or replace function public.tg_recompute_dog_verdict_for_fid()
returns trigger
language plpgsql
as $$
begin
  if NEW.arena_group_id is null then return NEW; end if;
  perform public.compute_dogs_verdict_by_origin(NEW.arena_group_id::bigint);
  return NEW;
end $$;

drop trigger if exists tg_after_change_recompute_verdict on public.beach_dog_policy;

create trigger tg_after_change_recompute_verdict
  after insert or update on public.beach_dog_policy
  for each row
  when (NEW.arena_group_id is not null)
  execute function public.tg_recompute_dog_verdict_for_fid();

-- Drop the daily cron — trigger covers it now
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'verdict_cascade_nightly') then
    perform cron.unschedule('verdict_cascade_nightly');
  end if;
end $cron$;

-- ----------------------------------------------------------------------
-- 2. Polygon resolver statement-level trigger on source tables
-- ----------------------------------------------------------------------
-- refresh_beach_polygons() is a full rebuild (TRUNCATE + INSERT-from-CTE
-- per migration 20260505_beach_polygons_resolver.sql). Cheap enough at
-- ~800 beaches × few-ms-per-row to fire on every source change. Source
-- changes are rare (manual loader runs) so volume is low.
--
-- Statement-level (FOR EACH STATEMENT) means a bulk INSERT of N rows
-- fires the trigger ONCE, not N times.

create or replace function public.tg_refresh_beach_polygons_after_source_change()
returns trigger
language plpgsql
as $$
begin
  perform public.refresh_beach_polygons();
  return null;  -- statement-level, return value ignored
end $$;

-- osm_beach_polys
drop trigger if exists tg_after_change_refresh_polygons on public.osm_beach_polys;
create trigger tg_after_change_refresh_polygons
  after insert or update or delete on public.osm_beach_polys
  for each statement
  execute function public.tg_refresh_beach_polygons_after_source_change();

-- cpad_units
drop trigger if exists tg_after_change_refresh_polygons on public.cpad_units;
create trigger tg_after_change_refresh_polygons
  after insert or update or delete on public.cpad_units
  for each statement
  execute function public.tg_refresh_beach_polygons_after_source_change();

-- Drop the weekly cron — triggers cover it now
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'beach_polygons_weekly') then
    perform cron.unschedule('beach_polygons_weekly');
  end if;
end $cron$;

commit;
