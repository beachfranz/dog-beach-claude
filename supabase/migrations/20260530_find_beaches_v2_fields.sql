-- 20260530_find_beaches_v2_fields.sql
--
-- Adds day_status_v2 + composite_score_v2 to find_beaches RPC return.
-- Task #6 of v1-retirement track. Additive — keeps v1 day_status in
-- the response during transition so find.html / get-beaches-find can
-- swap consumers incrementally.
--
-- v2 backing columns added by 20260530_v2_day_level_columns.sql.

begin;

drop function if exists public.find_beaches(date, double precision, double precision, text, integer, boolean);

create function public.find_beaches(
  p_date date,
  p_lat double precision default null,
  p_lng double precision default null,
  p_leash text default 'any',
  p_limit integer default null,
  p_scored_only boolean default false
)
returns table(
  arena_group_id        bigint,
  location_id           text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  location_tier         text,
  distance_m            double precision,
  day_status            text,
  day_status_v2         text,
  composite_score_v2    int,
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric
)
language sql
stable
security definer
set search_path to 'public'
as $$
  select
    g.fid as arena_group_id,
    g.location_id,
    coalesce(g.display_name_override, g.name) as display_name,
    g.lat::double precision as latitude,
    g.lon::double precision as longitude,
    dp.access_rule,
    dp.has_on_leash,
    dp.has_off_leash,
    dp.dogs_allowed,
    dp.dogs_prohibited_start::text as dogs_prohibited_start,
    public.beach_location_tier(
      dp.dogs_allowed, dp.has_off_leash, dp.has_on_leash,
      dp.dogs_prohibited_start::text
    ) as location_tier,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end as distance_m,
    dr.day_status,
    dr.day_status_v2,
    dr.composite_score_v2,
    dr.best_window_label,
    dr.best_window_status,
    dr.bacteria_risk,
    dr.summary_weather,
    dr.weather_code,
    dr.lowest_tide_height,
    dr.avg_temp,
    dr.avg_wind,
    dr.busyness_category,
    dr.go_hours_count,
    dr.avg_tide_height
  from public.beaches_gold g
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.fid
        and dr.local_date     = p_date
  left join public.beach_dog_policy dp
         on dp.arena_group_id = g.fid
  where g.is_active = true
    and (p_leash = 'any' or dp.access_rule = p_leash or dp.access_rule is null)
    and (not p_scored_only or dr.day_status is not null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$$;

commit;
