-- 20260506_auto_scoring_metadata_backfill.sql
--
-- When a new gold row is created and flagged is_scoreable=true, the
-- daily-beach-refresh edge function needs noaa_station_id, timezone,
-- besttime_venue_id, open_time, close_time. Without those it skips
-- the beach entirely (no recommendation row → stuck at 05).
--
-- This trigger backfills those fields from the nearest existing
-- scoreable beach in the same county that already has them. Cheap
-- proxy that works fine for SoCal where coverage is dense; for
-- isolated coverage the operator can curate later.

begin;

create or replace function public.tg_backfill_scoring_metadata_for_socal()
returns trigger
language plpgsql
as $$
declare
  donor record;
begin
  -- Only fill if scoreable + at least one scoring field is null
  if NEW.is_scoreable is not true then return NEW; end if;
  if NEW.noaa_station_id is not null and NEW.timezone is not null then
    return NEW;
  end if;

  -- Pick the nearest scoreable beach in the same county that has
  -- a noaa_station_id populated. ORDER BY KNN for distance.
  select d.noaa_station_id, d.besttime_venue_id, d.timezone,
         d.open_time, d.close_time
    into donor
    from public.beaches_gold d
   where d.fid <> NEW.fid
     and d.county_name = NEW.county_name
     and d.is_scoreable
     and d.noaa_station_id is not null
   order by d.geom <-> NEW.geom
   limit 1;

  if found then
    NEW.noaa_station_id   := coalesce(NEW.noaa_station_id,   donor.noaa_station_id);
    NEW.besttime_venue_id := coalesce(NEW.besttime_venue_id, donor.besttime_venue_id);
    NEW.timezone          := coalesce(NEW.timezone,          donor.timezone);
    NEW.open_time         := coalesce(NEW.open_time,         donor.open_time);
    NEW.close_time        := coalesce(NEW.close_time,        donor.close_time);
  end if;
  return NEW;
end $$;

drop trigger if exists tg_before_insert_backfill_scoring_metadata
  on public.beaches_gold;

create trigger tg_before_insert_backfill_scoring_metadata
  before insert on public.beaches_gold
  for each row execute function public.tg_backfill_scoring_metadata_for_socal();

commit;
