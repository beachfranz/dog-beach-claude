-- Path 3a step 3+4: add scoring-relevant columns to beaches_gold and
-- backfill them from public.beaches for the 13 active beaches that
-- already have arena_group_id wired up.
--
-- These columns travel with identity in the new spine. Edge functions
-- (in step 7) will read from beaches_gold instead of public.beaches.

begin;

-- 3. Add columns (all nullable; null = "not configured for scoring yet")
alter table public.beaches_gold
  add column if not exists noaa_station_id        text,
  add column if not exists besttime_venue_id      text,
  add column if not exists open_time              text,    -- matches public.beaches type
  add column if not exists close_time             text,
  add column if not exists timezone               text,
  add column if not exists display_name_override  text;

comment on column public.beaches_gold.noaa_station_id is
  'NOAA CO-OPS station id used for tide predictions. NULL → engine treats tide as neutral 0.5.';
comment on column public.beaches_gold.besttime_venue_id is
  'BestTime.app venue id for crowd/busyness fetch. NULL → crowd score is neutral 0.5 (path 1: skip BestTime by default).';
comment on column public.beaches_gold.timezone is
  'IANA timezone (e.g. America/Los_Angeles). NULL → caller picks state default.';
comment on column public.beaches_gold.display_name_override is
  'Friendly name for the consumer UI when arena.name is too literal. Falls back to name.';

-- 4. Backfill from public.beaches for any row with arena_group_id
update public.beaches_gold g
   set noaa_station_id       = b.noaa_station_id,
       besttime_venue_id     = b.besttime_venue_id,
       open_time             = b.open_time,
       close_time            = b.close_time,
       timezone              = b.timezone,
       display_name_override = case
                                  when b.display_name is not null
                                       and b.display_name <> g.name
                                  then b.display_name
                                  else null
                               end
  from public.beaches b
 where b.arena_group_id is not null
   and b.arena_group_id = g.fid;

commit;
