-- 20260506_expand_scoring_to_all_ca.sql
--
-- Broaden the scoring scope from SoCal-only (5 counties) to all of CA.
-- OR seeds stay manually curated — no auto-promotion outside CA.
--
-- Three changes:
--
-- 1. tg_auto_scoreable_socal_gold (BEFORE INSERT on beaches_gold)
--    Expanded from 5 SoCal counties to every CA coastal + island
--    + lake-county that's plausible scoreable territory.
--
-- 2. tg_backfill_scoring_metadata_for_socal (BEFORE INSERT on beaches_gold)
--    Same-county donor lookup stays first; falls back to nearest
--    NOAA station from public.noaa_stations when no donor exists.
--    NorCal counties (Mendocino, Humboldt, Del Norte) currently
--    have no scoreable beaches so the donor lookup fails — the
--    spatial NOAA fallback handles it.
--    Timezone defaults to America/Los_Angeles (uniform across CA).
--
-- 3. Retroactive flip: any active CA beach in beaches_gold not
--    currently scoreable AND not in the dogs_allowed='no' set gets
--    is_scoreable=true + scoring metadata backfilled if missing.

begin;

-- ----------------------------------------------------------------------
-- 1. Expand auto-scoreable scope: all CA counties
-- ----------------------------------------------------------------------
create or replace function public.tg_auto_scoreable_socal_gold()
returns trigger
language plpgsql
as $$
begin
  if NEW.is_active is true
     and NEW.state = 'CA'
     and (NEW.is_scoreable is null or NEW.is_scoreable = false)
  then
    NEW.is_scoreable := true;
  end if;
  return NEW;
end $$;

-- ----------------------------------------------------------------------
-- 2. Scoring metadata backfill: NOAA spatial fallback
-- ----------------------------------------------------------------------
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
     and NEW.geom is not null
     and d.geom is not null
   order by d.geom <-> NEW.geom
   limit 1;

  if found then
    NEW.noaa_station_id   := coalesce(NEW.noaa_station_id,   donor.noaa_station_id);
    NEW.besttime_venue_id := coalesce(NEW.besttime_venue_id, donor.besttime_venue_id);
    NEW.timezone          := coalesce(NEW.timezone,          donor.timezone);
    NEW.open_time         := coalesce(NEW.open_time,         donor.open_time);
    NEW.close_time        := coalesce(NEW.close_time,        donor.close_time);
    return NEW;
  end if;

  -- Tier 2: spatial nearest NOAA station (covers NorCal counties
  -- that have no scoreable peers yet)
  if NEW.noaa_station_id is null and NEW.geom is not null then
    select s.station_id
      into nearest_station
      from public.noaa_stations s
     where s.state = 'CA'
       and s.geom is not null
     order by s.geom <-> NEW.geom
     limit 1;
    NEW.noaa_station_id := nearest_station;
  end if;

  -- Uniform CA defaults (same timezone + reasonable park hours)
  if NEW.timezone is null  then NEW.timezone  := 'America/Los_Angeles'; end if;
  if NEW.open_time is null  then NEW.open_time  := '05:00'; end if;
  if NEW.close_time is null then NEW.close_time := '22:00'; end if;

  return NEW;
end $$;

-- ----------------------------------------------------------------------
-- 3. Retroactive flip — CA beaches active + not is_scoreable + not 'no dogs'
-- ----------------------------------------------------------------------
-- a. Backfill scoring metadata via same logic the trigger uses
update public.beaches_gold bg
   set noaa_station_id = coalesce(bg.noaa_station_id, donor.noaa_station_id),
       besttime_venue_id = coalesce(bg.besttime_venue_id, donor.besttime_venue_id),
       timezone = coalesce(bg.timezone, donor.timezone, 'America/Los_Angeles'),
       open_time = coalesce(bg.open_time, donor.open_time, '05:00'),
       close_time = coalesce(bg.close_time, donor.close_time, '22:00')
  from (
    select bg2.fid,
           (select d.noaa_station_id from public.beaches_gold d
             where d.fid <> bg2.fid and d.county_name = bg2.county_name
               and d.is_scoreable and d.noaa_station_id is not null
             order by d.geom <-> bg2.geom limit 1) as noaa_station_id,
           (select d.besttime_venue_id from public.beaches_gold d
             where d.fid <> bg2.fid and d.county_name = bg2.county_name
               and d.is_scoreable and d.besttime_venue_id is not null
             order by d.geom <-> bg2.geom limit 1) as besttime_venue_id,
           (select d.timezone from public.beaches_gold d
             where d.fid <> bg2.fid and d.county_name = bg2.county_name
               and d.is_scoreable and d.timezone is not null
             order by d.geom <-> bg2.geom limit 1) as timezone,
           (select d.open_time from public.beaches_gold d
             where d.fid <> bg2.fid and d.county_name = bg2.county_name
               and d.is_scoreable and d.open_time is not null
             order by d.geom <-> bg2.geom limit 1) as open_time,
           (select d.close_time from public.beaches_gold d
             where d.fid <> bg2.fid and d.county_name = bg2.county_name
               and d.is_scoreable and d.close_time is not null
             order by d.geom <-> bg2.geom limit 1) as close_time
      from public.beaches_gold bg2
     where bg2.is_active and bg2.state = 'CA' and bg2.geom is not null
       and (bg2.is_scoreable is false or bg2.is_scoreable is null)
       and not exists (
         select 1 from public.beach_dog_policy p
          where p.arena_group_id = bg2.fid and p.dogs_allowed = 'no'
       )
  ) donor
 where bg.fid = donor.fid;

-- b. NOAA spatial fallback for CA beaches still without a station
update public.beaches_gold bg
   set noaa_station_id = (
         select s.station_id from public.noaa_stations s
          where s.state = 'CA' and s.geom is not null
          order by s.geom <-> bg.geom limit 1
       )
 where bg.is_active and bg.state = 'CA' and bg.geom is not null
   and bg.noaa_station_id is null
   and (bg.is_scoreable is false or bg.is_scoreable is null)
   and not exists (
     select 1 from public.beach_dog_policy p
      where p.arena_group_id = bg.fid and p.dogs_allowed = 'no'
   );

-- c. Flip is_scoreable=true for the eligible set
update public.beaches_gold bg
   set is_scoreable = true
 where bg.is_active and bg.state = 'CA'
   and (bg.is_scoreable is false or bg.is_scoreable is null)
   and bg.noaa_station_id is not null
   and not exists (
     select 1 from public.beach_dog_policy p
      where p.arena_group_id = bg.fid and p.dogs_allowed = 'no'
   );

commit;
