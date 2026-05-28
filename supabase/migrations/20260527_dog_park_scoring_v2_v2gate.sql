-- 20260527_dog_park_scoring_v2_v2gate.sql
--
-- Stop using legacy v1 hour_status as the v2 cap-driver. The v1 status
-- field reflects beach-scoring tier definitions (60°F is "caution-cold"
-- on the shore) and produces force-rounded 60/60/30 scores on mild
-- dog-park hours.
--
-- Replacement: derive the cap tier from v2-domain numeric conditions.
--
--   no_go  cap 30 : asphalt ≥ 125°F · UV ≥ 11 · feels_like ≤ 20°F
--   caution cap 60: asphalt ≥ 115°F · UV ≥ 9  · feels_like ≥ 95 or ≤ 32°F
--                   · precip_chance ≥ 80% (heavy rain)
--   else          : no cap
--
-- Caution is still meaningful — it fires on genuinely concerning
-- numbers (very hot pavement, near-extreme UV, freezing, downpour) —
-- but a 60°F-with-light-breeze morning is no longer artificially capped.
-- Franz 2026-05-27 LATE.

begin;

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
      least(25,
        least(15, greatest(0, round((coalesce(h.asphalt_temp, 0) - 100) / 2.0)::int))
        + least(10, greatest(0, round((coalesce(h.uv_index, 0) - 5) * 10 / 6.0)::int))
        + case when h.feels_like > 85 then least(5, greatest(0, round((h.feels_like - 85) / 3.0)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        case when h.wind_speed > 20 then least(10, greatest(0, round((h.wind_speed - 20) * 0.5)::int)) else 0 end
        + case when h.precip_chance > 30 then least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(10, greatest(0, round((50 - h.feels_like) * 0.4)::int)) else 0 end
      ) as harsh_neg,
      (case when v_surface in ('asphalt','concrete') and coalesce(h.uv_index, 0) >= 6 then 3 else 0 end) as noshade_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      -- V2-DOMAIN gate (no_go): asphalt ≥125, UV ≥11, feels ≤20°F
      (coalesce(h.asphalt_temp, 0) >= 125
       or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired,
      -- V2-DOMAIN cap tier
      case
        when coalesce(h.asphalt_temp, 0) >= 125
          or coalesce(h.uv_index, 0) >= 11
          or coalesce(h.feels_like, 99) <= 20 then 30
        when coalesce(h.asphalt_temp, 0) >= 115
          or coalesce(h.uv_index, 0) >= 9
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
