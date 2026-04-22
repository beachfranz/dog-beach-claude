-- Add spatial KNN ordering + limit to find_beaches RPC.
--
-- Existing behavior (preserved when p_lat/p_lng NULL):
--   Return all active beaches for the date, unordered. Edge function
--   handles sorting by composite_score.
--
-- New behavior (when p_lat/p_lng present):
--   ORDER BY location <-> point — uses the beaches_location_gist GIST
--   index (created 20260422) for O(log N + limit) bounded queries.
--
-- New parameter p_limit (default NULL = unlimited):
--   When provided along with lat/lng, returns only the nearest p_limit
--   beaches. Enables the bounded-set pattern at 10k+ beach scale.
--
-- Backward compatible: existing callers without p_limit continue to work
-- (default is NULL → no limit applied).

create or replace function public.find_beaches(
  p_date  date,
  p_lat   double precision default null,
  p_lng   double precision default null,
  p_leash text default 'any',
  p_limit integer default null
)
returns table (
  location_id        text,
  display_name       text,
  latitude           numeric,
  longitude          numeric,
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
stable
security definer
set search_path to 'public'
as $$
  select
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    b.access_rule,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(b.location, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end                              as distance_m,
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
  from public.beaches b
  left join public.beach_day_recommendations dr
         on dr.location_id = b.location_id
        and dr.local_date  = p_date
  where b.is_active = true
    and (p_leash = 'any' or b.access_rule = p_leash or b.access_rule is null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then b.location <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$$;
