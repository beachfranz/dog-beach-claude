-- Path 3b-3: rewrite find_beaches RPC to read from beaches_gold spine.
--
-- Replaces the version that reads public.beaches. The new shape:
--   - Joins beaches_gold (identity) + beach_day_recommendations (scoring)
--     + beach_dog_policy (access_rule) + public.beaches (legacy slug, BC)
--   - Adds arena_group_id to the return shape
--   - Adds p_scored_only param (default true) — when true, only returns
--     beaches with a beach_day_recommendations row for p_date. UI toggle
--     lets users opt into the full catalog when they want completeness.
--
-- Old signatures stay alive until callers migrate; the 5-arg version
-- (with p_limit) is the one frontend calls today via get-beaches-find.
-- We DROP+CREATE that to add the new param.

begin;

drop function if exists public.find_beaches(
  p_date date,
  p_lat double precision,
  p_lng double precision,
  p_leash text,
  p_limit integer
);

create or replace function public.find_beaches(
  p_date         date,
  p_lat          double precision default null,
  p_lng          double precision default null,
  p_leash        text             default 'any',
  p_limit        integer          default null,
  p_scored_only  boolean          default true
)
returns table(
  arena_group_id     bigint,
  location_id        text,
  display_name       text,
  latitude           double precision,
  longitude          double precision,
  access_rule        text,
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
language sql
stable security definer
set search_path to 'public'
as $function$
  select
    g.fid as arena_group_id,
    pb.location_id,
    coalesce(g.display_name_override, g.name) as display_name,
    g.lat as latitude,
    g.lon as longitude,
    dp.access_rule,
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
  left join public.beaches pb
         on pb.arena_group_id = g.fid
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
