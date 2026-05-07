-- 20260507_binary_leash_phase_2_find_beaches.sql
--
-- Binary leash schema migration — Phase 2 (find_beaches RPC).
--
-- Extends find_beaches to return has_on_leash and has_off_leash
-- alongside the legacy access_rule. Leaves the existing p_leash filter
-- semantics intact for backward compat (it filters on access_rule);
-- find.html / get-beaches-find can adopt the binaries client-side
-- without coordinated migration.
--
-- Phase 3 will retire the categorical access_rule from the return
-- tuple and replace the p_leash filter with two boolean params.

begin;

drop function if exists public.find_beaches(date, double precision, double precision, text, integer, boolean);

create or replace function public.find_beaches(
  p_date date,
  p_lat double precision default null,
  p_lng double precision default null,
  p_leash text default 'any',
  p_limit integer default null,
  p_scored_only boolean default true
)
returns table(
  arena_group_id     bigint,
  location_id        text,
  display_name       text,
  latitude           double precision,
  longitude          double precision,
  access_rule        text,
  has_on_leash       boolean,
  has_off_leash      boolean,
  distance_m         double precision,
  day_status         text,
  best_window_label  text,
  best_window_status text,
  bacteria_risk      text,
  summary_weather    text,
  weather_code       integer,
  lowest_tide_height numeric,
  avg_temp           numeric,
  avg_wind           numeric,
  busyness_category  text,
  go_hours_count     integer,
  avg_tide_height    numeric
)
language sql stable security definer
set search_path to 'public'
as $function$
  select
    g.fid as arena_group_id,
    g.location_id,
    coalesce(g.display_name_override, g.name) as display_name,
    g.lat as latitude,
    g.lon as longitude,
    dp.access_rule,
    dp.has_on_leash,
    dp.has_off_leash,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end as distance_m,
    dr.day_status,
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
$function$;

commit;
