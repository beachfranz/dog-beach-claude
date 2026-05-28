-- 20260527_dog_park_scoring_v2_asphalt_bands.sql
--
-- Switch asphalt-temp penalty from a linear curve to a banded curve
-- matching the Weave pavement chart's reference tiers (Franz 2026-05-27 LATE):
--
--   <115°F        safe                            → 0
--   115-125°F     warm, paws OK for short visits  → 2
--   125-135°F     paw burn risk (gate fires)      → 8
--   135-145°F     "skin destruction in 60s"        → 12
--   145°F+        egg-fries / extreme              → 15 (cap)
--
-- Gate threshold (asphalt ≥125 → no_go cap 30) unchanged. The penalty
-- value above 125 mainly matters for the chart visualization since the
-- gate already caps the score there.

begin;

create or replace function public.compute_dog_park_score_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
declare
  v_gold record; v_policy record; v_now record; v_today record;
  v_view_date date := coalesce(p_date, current_date);
  v_pos jsonb; v_neg jsonb;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_gate_fired boolean := false;
  v_temp_pos integer := 0; v_wind_pos integer := 0; v_sky_pos integer := 0;
  v_weather_pos integer := 0;
  v_fence_pos integer := 0; v_water_pos integer := 0; v_comfort_pos integer := 0;
  v_features_pos integer := 0;
  v_window_pos integer := 0;
  v_heat_neg integer := 0; v_harsh_neg integer := 0; v_gotchas_neg integer := 0;
  v_fence boolean; v_water boolean; v_surface text; v_cap integer := 100;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_now from public.dog_park_day_hourly_scores where dog_park_fid = p_fid and is_now = true limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  if v_now.feels_like is not null then
    v_temp_pos := greatest(0, round(10 * (1 - least(abs(v_now.feels_like - 72), 20) / 20.0))::int);
  end if;
  if v_now.wind_speed is not null then
    if v_now.wind_speed <= 5 then
      v_wind_pos := greatest(0, round(v_now.wind_speed * 6 / 5.0)::int);
    else
      v_wind_pos := greatest(0, round(6 - (v_now.wind_speed - 5) * 0.6)::int);
    end if;
  end if;
  if v_now.weather_code is not null then
    v_sky_pos := case v_now.weather_code
      when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
      when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
      when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
      when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
      when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
      when 95 then 0 when 96 then 0 when 99 then 0 else 2 end;
  end if;
  v_weather_pos := least(22, v_temp_pos + v_wind_pos + v_sky_pos);

  if v_fence = true             then v_fence_pos := v_fence_pos + 6; end if;
  if v_policy.double_gate = true then v_fence_pos := v_fence_pos + 3; end if;
  if v_water = true             then v_water_pos := v_water_pos + 5; end if;
  if v_surface = 'grass'        then v_comfort_pos := v_comfort_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true then v_comfort_pos := v_comfort_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047  then v_comfort_pos := v_comfort_pos + 1; end if;
  v_features_pos := v_fence_pos + v_water_pos + v_comfort_pos;

  if v_today.best_window_label is not null then v_window_pos := 5;
  elsif coalesce(v_today.go_hours_count, 0) > 0 then v_window_pos := 2; end if;

  -- Asphalt: banded curve aligned with Weave pavement-chart tiers
  if v_now.asphalt_temp is not null then
    v_heat_neg := v_heat_neg + case
      when v_now.asphalt_temp < 115 then 0
      when v_now.asphalt_temp < 125 then 2
      when v_now.asphalt_temp < 135 then 8
      when v_now.asphalt_temp < 145 then 12
      else 15
    end;
    if v_now.asphalt_temp >= 125 then v_gate_fired := true; end if;
  end if;
  -- UV: banded curve (EPA tiers)
  if v_now.uv_index is not null then
    v_heat_neg := v_heat_neg + case
      when v_now.uv_index < 6 then 0
      when v_now.uv_index < 8 then 1
      when v_now.uv_index < 10 then 3
      when v_now.uv_index < 11 then 4
      else 10
    end;
    if v_now.uv_index >= 11 then v_gate_fired := true; end if;
  end if;
  if v_now.feels_like is not null and v_now.feels_like > 90 then
    v_heat_neg := v_heat_neg + least(5, greatest(0, round((v_now.feels_like - 90) * 0.5)::int));
  end if;
  v_heat_neg := least(v_heat_neg, 25);

  if v_now.wind_speed is not null and v_now.wind_speed > 20 then
    v_harsh_neg := v_harsh_neg + least(10, greatest(0, round((v_now.wind_speed - 20) * 0.5)::int));
  end if;
  if v_now.precip_chance is not null and v_now.precip_chance > 30 then
    v_harsh_neg := v_harsh_neg + least(10, greatest(0, round((v_now.precip_chance - 30) / 7.0)::int));
  end if;
  if v_now.feels_like is not null and v_now.feels_like < 50 then
    v_harsh_neg := v_harsh_neg + least(10, greatest(0, round((50 - v_now.feels_like) * 0.4)::int));
    if v_now.feels_like <= 20 then v_gate_fired := true; end if;
  end if;
  v_harsh_neg := least(v_harsh_neg, 15);

  if v_fence = false then v_gotchas_neg := v_gotchas_neg + 5; end if;
  if v_water = false then v_gotchas_neg := v_gotchas_neg + 3; end if;
  if v_surface in ('asphalt','concrete') and coalesce(v_now.uv_index, 0) >= 6 then
    v_gotchas_neg := v_gotchas_neg + 3;
  end if;
  v_gotchas_neg := least(v_gotchas_neg, 10);

  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    if v_drive_min <= 10 then v_drive := 3;
    elsif v_drive_min <= 25 then v_drive := 0;
    elsif v_drive_min <= 45 then v_drive := -3;
    elsif v_drive_min <= 75 then v_drive := -6;
    else v_drive := -10; end if;
  end if;

  if v_gate_fired then v_cap := 30; else v_cap := 100; end if;

  v_pos := jsonb_build_object(
    'weather', jsonb_build_object('value', v_weather_pos, 'max', 22),
    'features', jsonb_build_object('value', v_features_pos, 'max', 20),
    'window', jsonb_build_object('value', v_window_pos, 'max', 5));
  v_neg := jsonb_build_object(
    'heat_uv', jsonb_build_object('value', v_heat_neg, 'max', 25),
    'weather_harsh', jsonb_build_object('value', v_harsh_neg, 'max', 15),
    'gotchas', jsonb_build_object('value', v_gotchas_neg, 'max', 10));

  return public._score_v2_compose(
    p_baseline := 50, p_positives := v_pos, p_negatives := v_neg,
    p_drive_factor := v_drive,
    p_status_gate_fired := v_gate_fired,
    p_status_gate_cap := v_cap
  ) || jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'now_available', v_now.dog_park_fid is not null,
    'today_available', v_today.dog_park_fid is not null,
    'hour_status', v_now.hour_status,
    'best_window_label', v_today.best_window_label,
    'pos_temp', v_temp_pos, 'pos_wind', v_wind_pos, 'pos_sky', v_sky_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos);
end;
$$;

create or replace function public.compute_dog_park_hourly_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
declare
  v_gold record; v_policy record; v_today record;
  v_view_date date := coalesce(p_date, current_date);
  v_fence_pos integer := 0; v_water_pos integer := 0; v_comfort_pos integer := 0;
  v_features_pos integer := 0;
  v_static_gotchas integer := 0;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_fence boolean; v_water boolean; v_surface text;
  v_open_h integer; v_close_h integer; v_hours jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  if v_fence = true              then v_fence_pos := v_fence_pos + 6; end if;
  if v_policy.double_gate = true  then v_fence_pos := v_fence_pos + 3; end if;
  if v_water = true              then v_water_pos := v_water_pos + 5; end if;
  if v_surface = 'grass'         then v_comfort_pos := v_comfort_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true then v_comfort_pos := v_comfort_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047   then v_comfort_pos := v_comfort_pos + 1; end if;
  v_features_pos := v_fence_pos + v_water_pos + v_comfort_pos;

  if v_fence = false then v_static_gotchas := v_static_gotchas + 5; end if;
  if v_water = false then v_static_gotchas := v_static_gotchas + 3; end if;

  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    if v_drive_min <= 10 then v_drive := 3;
    elsif v_drive_min <= 25 then v_drive := 0;
    elsif v_drive_min <= 45 then v_drive := -3;
    elsif v_drive_min <= 75 then v_drive := -6;
    else v_drive := -10; end if;
  end if;

  v_open_h := coalesce(extract(hour from v_policy.hours_open_time::time)::int, 6);
  v_close_h := coalesce(extract(hour from v_policy.hours_close_time::time)::int, 22);
  if v_close_h <= v_open_h then v_close_h := 22; end if;

  with per_hour as (
    select h.local_hour, h.hour_label, h.hour_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      case when h.wind_speed is null then 0
           when h.wind_speed <= 5 then greatest(0, round(h.wind_speed * 6 / 5.0)::int)
           else greatest(0, round(6 - (h.wind_speed - 5) * 0.6)::int) end as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      -- Heat/UV negative: asphalt + UV both banded; feels_like hot ramp
      least(25,
        case
          when h.asphalt_temp is null or h.asphalt_temp < 115 then 0
          when h.asphalt_temp < 125 then 2
          when h.asphalt_temp < 135 then 8
          when h.asphalt_temp < 145 then 12
          else 15
        end
        + case
            when h.uv_index is null or h.uv_index < 6 then 0
            when h.uv_index < 8 then 1
            when h.uv_index < 10 then 3
            when h.uv_index < 11 then 4
            else 10
          end
        + case when h.feels_like > 90 then least(5, greatest(0, round((h.feels_like - 90) * 0.5)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        case when h.wind_speed > 20 then least(10, greatest(0, round((h.wind_speed - 20) * 0.5)::int)) else 0 end
        + case when h.precip_chance > 30 then least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(10, greatest(0, round((50 - h.feels_like) * 0.4)::int)) else 0 end
      ) as harsh_neg,
      (case when v_surface in ('asphalt','concrete') and coalesce(h.uv_index, 0) >= 6 then 3 else 0 end) as noshade_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      (coalesce(h.asphalt_temp, 0) >= 125
       or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired,
      case
        when coalesce(h.asphalt_temp, 0) >= 125
          or coalesce(h.uv_index, 0) >= 11
          or coalesce(h.feels_like, 99) <= 20 then 30
        when coalesce(h.asphalt_temp, 0) >= 115
          or coalesce(h.feels_like, 70) >= 95
          or coalesce(h.feels_like, 70) <= 32
          or coalesce(h.precip_chance, 0) >= 80 then 60
        else 100
      end as status_cap
    from public.dog_park_day_hourly_scores h
    where h.dog_park_fid = p_fid and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(22, temp_pos + wind_pos + sky_pos) as weather_pos,
      least(10, v_static_gotchas + noshade_neg) as gotchas_neg
    from per_hour
  ),
  scored as (
    select *,
      greatest(0, least(
        status_cap,
        50 + v_features_pos + weather_pos + window_pos
           - heat_uv_neg - harsh_neg - gotchas_neg + v_drive
      )) as score from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label, 'status', hour_status,
    'score', score, 'gate_fired', gate_fired, 'status_cap', status_cap,
    'pos_weather', weather_pos, 'pos_features', v_features_pos,
    'pos_window', window_pos,
    'neg_heat_uv', heat_uv_neg, 'neg_harsh', harsh_neg, 'neg_gotchas', gotchas_neg,
    'drive', v_drive,
    'pos_temp', temp_pos, 'pos_wind', wind_pos, 'pos_sky', sky_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp,
    'weather_code', weather_code, 'precip_chance', precip_chance
  ) order by local_hour) into v_hours from scored;

  return jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'open_hour', v_open_h, 'close_hour', v_close_h,
    'features_positive', v_features_pos, 'static_gotchas', v_static_gotchas,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos,
    'drive_factor', v_drive, 'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label', v_today.best_window_label,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$$;

commit;
notify pgrst, 'reload schema';
