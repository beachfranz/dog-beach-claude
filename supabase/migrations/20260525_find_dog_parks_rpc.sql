-- 20260525_find_dog_parks_rpc.sql
--
-- B7 of dog-park sub-pipeline Phase B. Mirrors public.find_beaches
-- shape; adapted for dog-park surface (dog_parks_gold + dog_park_dog_policy
-- + dog_park_day_recommendations). No tide/bacteria/busyness cols.
--
-- Param shape mirrors find_beaches for client-code symmetry:
--   p_date         — date to look up day_status for
--   p_lat, p_lng   — optional, for distance sort
--   p_leash        — 'off' / 'on' / 'any' (chip "Off-leash beaches" passes 'off')
--   p_limit        — optional row cap
--   p_scored_only  — only return parks with scoring for p_date (default false)
--   p_surface      — optional surface filter (e.g. 'grass')
--   p_fence        — optional has_fence filter (true/false/null)
--   p_water        — optional has_drinking_water filter
--
-- Returns shape: composite of dog_parks_gold identity + dog_park_dog_policy
-- + dog_park_day_recommendations. distance_m computed when lat/lng given.

begin;

create or replace function public.find_dog_parks(
  p_date          date,
  p_lat           double precision default null,
  p_lng           double precision default null,
  p_leash         text default 'any',
  p_limit         integer default null,
  p_scored_only   boolean default false,
  p_surface       text default null,
  p_fence         boolean default null,
  p_water         boolean default null
)
returns table (
  dog_park_fid        bigint,
  name                text,
  display_name        text,
  latitude            double precision,
  longitude           double precision,
  surface             text,
  has_fence           boolean,
  has_drinking_water  boolean,
  double_gate         boolean,
  small_dog_area      boolean,
  lighting            boolean,
  leash_policy        text,
  off_leash_flag      boolean,
  hours_text          text,
  distance_m          double precision,
  day_status          text,
  best_window_label   text,
  best_window_status  text,
  summary_weather     text,
  weather_code        integer,
  avg_temp            numeric,
  avg_wind            numeric,
  avg_uv              numeric,
  go_hours_count      integer,
  composite_score     numeric
)
language sql
stable
security definer
set search_path to 'public'
as $$
  select
    g.fid                                                       as dog_park_fid,
    g.name                                                      as name,
    coalesce(g.display_name_override, g.name)                   as display_name,
    g.lat                                                       as latitude,
    g.lon                                                       as longitude,
    g.surface                                                   as surface,
    coalesce(dp.has_fence, g.has_fence)                         as has_fence,
    coalesce(dp.has_drinking_water, g.has_drinking_water)       as has_drinking_water,
    dp.double_gate,
    dp.small_dog_area,
    dp.lighting,
    dp.leash_policy,
    dp.off_leash_flag,
    dp.hours_text,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end                                                         as distance_m,
    dr.day_status,
    dr.best_window_label,
    dr.best_window_status,
    dr.summary_weather,
    dr.weather_code,
    dr.avg_temp,
    dr.avg_wind,
    dr.avg_uv,
    dr.go_hours_count,
    -- best_window-anchored composite (mirrors how beach card displays score)
    case
      when dr.go_hours_count is null then null
      else round(
        100.0 * dr.go_hours_count
        / nullif(dr.go_hours_count + dr.advisory_hours_count
               + dr.caution_hours_count + dr.no_go_hours_count, 0),
        0
      )::numeric
    end                                                         as composite_score
  from public.dog_parks_gold g
  left join public.dog_park_dog_policy dp
         on dp.dog_park_fid = g.fid
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid
        and dr.local_date  = p_date
  where g.is_active = true
    and g.is_scoreable = true
    and (
      p_leash = 'any'
      or (p_leash = 'off' and (dp.off_leash_flag is true or dp.leash_policy in ('off_leash', 'off_leash_voice_control')))
      or (p_leash = 'on'  and dp.leash_policy = 'on_leash')
    )
    and (p_surface is null or g.surface = p_surface)
    and (p_fence   is null or coalesce(dp.has_fence,          g.has_fence)          = p_fence)
    and (p_water   is null or coalesce(dp.has_drinking_water, g.has_drinking_water) = p_water)
    and (not p_scored_only or dr.day_status is not null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$$;

comment on function public.find_dog_parks is
  'Find dog parks within scope. Mirrors find_beaches arg shape + adds surface/fence/water filters. Reads dog_parks_gold + dog_park_dog_policy + dog_park_day_recommendations. composite_score is best-window % of go-hours per day (rough cross-type comparability with beach hour_score; see [[dog-park-subpipeline-scope]] R5).';

grant execute on function public.find_dog_parks to anon, authenticated;

commit;
