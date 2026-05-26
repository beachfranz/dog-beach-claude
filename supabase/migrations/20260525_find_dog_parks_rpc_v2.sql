-- 20260525_find_dog_parks_rpc_v2.sql
--
-- Refined per Franz working-backward pass 2026-05-25 LATE:
--   - Add large_dog_area + surface_overlay + description_overlay + source +
--     source_url + operator info to return shape
--   - Surface returned as coalesce(overlay ?? raw) so consumer sees winning value
--   - Leash filter retained (any/off/on) for backward-compat but defaults to 'any'
--     since dog parks are off-leash by definition (consumer no longer surfaces toggle)

begin;

drop function if exists public.find_dog_parks(date, double precision, double precision,
                                              text, integer, boolean, text, boolean, boolean);

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
  dog_park_fid          bigint,
  name                  text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  surface               text,                  -- overlay ?? raw OSM
  has_fence             boolean,
  has_drinking_water    boolean,
  double_gate           boolean,
  small_dog_area        boolean,
  large_dog_area        boolean,
  lighting              boolean,
  leash_policy          text,                  -- always 'off_leash' for dog parks (definitional)
  off_leash_flag        boolean,               -- always true
  hours_text            text,
  hours_open_time       text,
  hours_close_time      text,
  additional_rules      text,
  description           text,                  -- overlay ?? raw
  source                text,
  source_url            text,
  operator_id           bigint,
  operator_short        text,
  distance_m            double precision,
  day_status            text,
  best_window_label     text,
  best_window_status    text,
  summary_weather       text,
  weather_code          integer,
  avg_temp              numeric,
  avg_wind              numeric,
  avg_uv                numeric,
  go_hours_count        integer,
  composite_score       numeric
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
    coalesce(dp.surface_overlay, g.surface)                     as surface,
    coalesce(dp.has_fence, g.has_fence)                         as has_fence,
    coalesce(dp.has_drinking_water, g.has_drinking_water)       as has_drinking_water,
    dp.double_gate,
    dp.small_dog_area,
    dp.large_dog_area,
    dp.lighting,
    coalesce(dp.leash_policy, 'off_leash')                      as leash_policy,
    coalesce(dp.off_leash_flag, true)                           as off_leash_flag,
    dp.hours_text,
    dp.hours_open_time,
    dp.hours_close_time,
    dp.additional_rules,
    coalesce(dp.description_overlay, g.description)             as description,
    dp.source,
    dp.source_url,
    dp.operator_id,
    op.name                                                     as operator_short,
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
  left join public.dog_park_dog_policy dp on dp.dog_park_fid = g.fid
  left join public.operator op on op.id = dp.operator_id
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = p_date
  where g.is_active = true
    and g.is_scoreable = true
    -- p_leash retained for backward-compat; default 'any' is the right thing
    -- since dog parks are off-leash by definition.
    and (
      p_leash = 'any'
      or (p_leash = 'off' and coalesce(dp.off_leash_flag, true) is true)
      or (p_leash = 'on'  and dp.leash_policy = 'on_leash')
    )
    -- Surface filter coalesces overlay ?? raw
    and (p_surface is null or coalesce(dp.surface_overlay, g.surface) = p_surface)
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
  'Returns dog parks within scope. Refined 2026-05-25 LATE: adds large_dog_area, surface_overlay coalesce, description coalesce, operator_short, source/source_url. Reads dog_parks_gold + dog_park_dog_policy + operator + dog_park_day_recommendations. composite_score is best-window go-hour % per day.';

grant execute on function public.find_dog_parks to anon, authenticated;

commit;
