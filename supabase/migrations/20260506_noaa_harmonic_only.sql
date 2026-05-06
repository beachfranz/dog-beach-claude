-- 20260506_noaa_harmonic_only.sql
--
-- daily-beach-refresh fails on subordinate NOAA stations (the ones with
-- reference_id IS NOT NULL — these reference another station for tide
-- predictions and the predictions endpoint returns "No Predictions data
-- was found").
--
-- 183 NorCal/Central Coast beaches got assigned subordinate stations by
-- today's backfill trigger and silently failed scoring. Fix:
--
-- 1. Update tg_backfill_scoring_metadata_for_socal to filter NOAA
--    candidates to harmonic stations only (reference_id IS NULL).
-- 2. Retroactively replace any subordinate station assignments in
--    beaches_gold with the nearest harmonic.

begin;

create or replace function public.tg_backfill_scoring_metadata_for_socal()
returns trigger
language plpgsql
as $$
declare
  donor record;
  nearest_station text;
begin
  if NEW.is_scoreable is not true then return NEW; end if;
  if NEW.noaa_station_id is not null and NEW.timezone is not null then
    return NEW;
  end if;

  -- Tier 1: nearest scoreable neighbor in same county
  select d.noaa_station_id, d.besttime_venue_id, d.timezone,
         d.open_time, d.close_time
    into donor
    from public.beaches_gold d
   where d.fid <> NEW.fid
     and d.county_name = NEW.county_name
     and d.is_scoreable
     and d.noaa_station_id is not null
     and exists (
       select 1 from public.noaa_stations s
        where s.station_id = d.noaa_station_id
          and s.reference_id is null   -- harmonic only
     )
     and NEW.geom is not null and d.geom is not null
   order by d.geom <-> NEW.geom limit 1;

  if found then
    NEW.noaa_station_id   := coalesce(NEW.noaa_station_id,   donor.noaa_station_id);
    NEW.besttime_venue_id := coalesce(NEW.besttime_venue_id, donor.besttime_venue_id);
    NEW.timezone          := coalesce(NEW.timezone,          donor.timezone);
    NEW.open_time         := coalesce(NEW.open_time,         donor.open_time);
    NEW.close_time        := coalesce(NEW.close_time,        donor.close_time);
    return NEW;
  end if;

  -- Tier 2: spatial nearest harmonic NOAA station
  if NEW.noaa_station_id is null and NEW.geom is not null then
    select s.station_id
      into nearest_station
      from public.noaa_stations s
     where s.state = 'CA'
       and s.reference_id is null    -- harmonic only
       and s.geom is not null
     order by s.geom <-> NEW.geom limit 1;
    NEW.noaa_station_id := nearest_station;
  end if;

  if NEW.timezone is null  then NEW.timezone  := 'America/Los_Angeles'; end if;
  if NEW.open_time is null  then NEW.open_time  := '05:00'; end if;
  if NEW.close_time is null then NEW.close_time := '22:00'; end if;
  return NEW;
end $$;

-- ----------------------------------------------------------------------
-- Retroactive: re-assign harmonic stations to beaches currently using
-- subordinate ones.
-- ----------------------------------------------------------------------
update public.beaches_gold bg
   set noaa_station_id = (
     select s.station_id from public.noaa_stations s
      where s.state = 'CA'
        and s.reference_id is null
        and s.geom is not null
      order by s.geom <-> bg.geom limit 1
   )
 where bg.is_active and bg.is_scoreable
   and exists (
     select 1 from public.noaa_stations s2
      where s2.station_id = bg.noaa_station_id
        and s2.reference_id is not null
   );

commit;
