-- 20260530_nearest_dog_parks_distance_first.sql
--
-- Bug: nearest_dog_parks ordered by v2 score DESC primarily, with distance
-- only as tiebreaker. With p_limit=10 (find.html's NEAREST_LIMIT), a user
-- in Orem UT saw the top-10 highest-scoring parks globally — all in CA at
-- 500+ miles — and zero UT parks. Client-side "Closest First" sort could
-- only re-rank the result set it received.
--
-- Fix: when lat/lng provided, order by distance ASC primarily. The function
-- is named "nearest" — should actually return nearest. Score becomes a
-- tiebreaker within the same distance. Client-side "Best Conditions" sort
-- then ranks the nearby pool by score, which matches user intent
-- ("of the parks within reach, which is best today?").
--
-- When lat/lng not provided, fall back to v2 score DESC ordering as before.

begin;

create or replace function public.nearest_dog_parks(
  p_lat   double precision default null,
  p_lng   double precision default null,
  p_limit integer          default 10,
  p_state text             default null,
  p_date  date             default null
)
returns table (
  fid bigint, name text, state text, address_city text,
  lat double precision, lon double precision,
  distance_miles double precision,
  has_fence boolean, has_drinking_water boolean,
  surface text,
  leash_policy text, hours_text text, hours_open_time text, hours_close_time text,
  additional_rules text,
  source text, source_url text,
  consensus_confidence numeric,
  operator_id bigint, operator_name text, operator_short text,
  composite_score numeric,
  day_status text, best_window_label text, best_window_status text,
  go_hours_count integer,
  summary_weather text, weather_code integer,
  avg_temp numeric, avg_wind numeric,
  score_v2 integer, score_v2_components jsonb,
  drive_minutes integer
)
language sql
stable parallel safe
as $$
  with user_pt as (
    select case when p_lat is not null and p_lng is not null
                then st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography
                else null::geography end as pt,
           coalesce(p_date, current_date) as d
  )
  select
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    case when up.pt is not null
         then st_distance(up.pt, g.geom::geography) / 1609.344
         else null::double precision
    end as distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.name as operator_name, o.short_name as operator_short,
    hs.composite_score,
    dr.day_status, dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    (sv.payload->>'score')::int           as score_v2,
    sv.payload                             as score_v2_components,
    (sv.payload->>'drive_minutes')::int    as drive_minutes
  from public.dog_parks_gold g
  join public.dog_park_dog_policy p on p.dog_park_fid = g.fid
  cross join user_pt up
  left join public.operator o on o.id = p.operator_id
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = up.d
  left join lateral (
    select round(avg(h.hour_score)::numeric, 0) as composite_score
      from public.dog_park_day_hourly_scores h
     where h.dog_park_fid = g.fid
       and h.local_date = up.d
       and h.hour_score is not null
  ) hs on true
  left join lateral (
    select public.compute_dog_park_score_v2(g.fid, p_lat, p_lng, up.d) as payload
  ) sv on true
  where g.is_active = true
    and (p_state is null or g.state = p_state)
  order by
    -- Distance-first when location given (matches function name "nearest").
    -- v2 score is the tiebreaker within the same distance, and the global
    -- fallback when no location is provided.
    case when up.pt is not null then st_distance(up.pt, g.geom::geography) end nulls last,
    coalesce((sv.payload->>'score')::int, 0) desc,
    g.name
  limit greatest(coalesce(p_limit, 10), 1);
$$;

comment on function public.nearest_dog_parks is
  'Returns dog parks ordered by distance ASC when lat/lng given (matches function name); falls back to v2 score DESC when no location provided. Client picks sort mode within the returned pool. Fixed 2026-05-30: previously ordered by score primarily, which starved long-tail states like UT/MD when a CA user opened find.html.';

commit;
