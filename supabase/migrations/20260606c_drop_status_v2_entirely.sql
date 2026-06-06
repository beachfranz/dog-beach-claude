-- 2026-06-06 — Drop the v2 4-tier status system end-to-end.
--
-- 20260606_drop_v2_caps removed the gate_fired/status_cap hard ceilings
-- and replaced them with a score-band derived status_v2. Franz: the 30/60
-- band thresholds are still old-logic lineage. Drop the entire 4-tier
-- (clear/advisory/caution/no_go) concept.
--
-- After this migration:
--   * compute_*_hourly_v2 emits NO `status` field in per-hour JSON
--   * compute_dog_park_score_v2 returns NO `hour_status_v2`
--   * apply_v2_best_window_to_* stop writing day_status_v2
--   * _find_best_window_v2 filters on is_closed only
--   * Status helpers dropped: _v2_status_from_score, v2_compute_hour_status,
--     v2_compute_hour_status_dog_park, v2_compute_day_status, v2_signal_status
--
-- Columns day_status_v2 / hour_status_v2 are NOT dropped yet (just no longer
-- written). Frontend + edge functions in follow-up commits stop reading them.
-- Column drop is a separate migration once all consumers are clean.

-- 1. compute_dog_park_hourly_v2 — drop status field from JSON
CREATE OR REPLACE FUNCTION public.compute_dog_park_hourly_v2(
  p_fid bigint,
  p_user_lat double precision DEFAULT NULL,
  p_user_lng double precision DEFAULT NULL,
  p_date date DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $function$
declare
  v_gold record; v_policy record; v_today record;
  v_view_date date := coalesce(p_date, current_date);
  v_fence_pos integer := 0; v_water_pos integer := 0; v_comfort_pos integer := 0;
  v_agility_pos integer := 0;
  v_fence boolean; v_water boolean; v_shade boolean;
  v_agility boolean; v_water_play boolean;
  v_surface text;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_open_h integer; v_close_h integer; v_hours jsonb;
  v_bw jsonb;
  v_closures jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb; v_b_asphalt_neg jsonb;
  v_b_uv_neg   jsonb; v_b_uv_need        jsonb; v_b_temp_need   jsonb;
  v_b_drive    jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;
  v_closures := coalesce(v_policy.closures, '[]'::jsonb);
  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type='dog_park' and signal_key='wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type='dog_park' and signal_key='wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type='dog_park' and signal_key='asphalt_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type='dog_park' and signal_key='uv_neg';
  select bands into v_b_uv_need        from public.scoring_config_v2 where entity_type='dog_park' and signal_key='uv_need';
  select bands into v_b_temp_need      from public.scoring_config_v2 where entity_type='dog_park' and signal_key='temp_need';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type='dog_park' and signal_key='drive_factor';
  v_fence := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_shade := v_policy.has_shade;
  v_agility := v_policy.has_agility;
  v_water_play := v_policy.has_water_play;
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));
  if v_fence = true              then v_fence_pos := v_fence_pos + 6; end if;
  if v_policy.double_gate = true  then v_fence_pos := v_fence_pos + 2; end if;
  if v_water = true              then v_water_pos := v_water_pos + 4; end if;
  if v_surface = 'grass'         then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.has_picnic_tables = true then v_comfort_pos := v_comfort_pos + 2; end if;
  if v_agility = true            then v_agility_pos := 3; end if;
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    v_drive := public._v2_lookup_band(v_drive_min::numeric, v_b_drive);
  end if;
  v_open_h := coalesce(extract(hour from v_policy.hours_open_time::time)::int, 6);
  v_close_h := coalesce(extract(hour from v_policy.hours_close_time::time)::int, 22);
  if v_close_h <= v_open_h then v_close_h := 22; end if;

  with per_hour as (
    select h.local_hour, h.hour_label,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_pos) as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      least(6, greatest(
        public._v2_lookup_band(coalesce(h.uv_index, -1),  v_b_uv_need),
        public._v2_lookup_band(coalesce(h.feels_like, -1), v_b_temp_need)
      )) as shade_need,
      least(25,
        public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90 then least(5, greatest(0, round((h.feels_like - 90) * 0.5)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(10, greatest(0, round((50 - h.feels_like) * 0.4)::int)) else 0 end
      ) as harsh_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos
    from public.dog_park_day_hourly_scores h
    where h.dog_park_fid = p_fid and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(22, temp_pos + wind_pos + sky_pos) as weather_pos,
      case when v_shade = true then shade_need else 0 end as shade_pos,
      case when v_water_play = true then shade_need else 0 end as water_play_pos,
      case when v_shade = false then shade_need else 0 end as gotchas_neg,
      v_fence_pos + v_water_pos + v_comfort_pos as features_static
    from per_hour
  ),
  scored as (
    select *,
      case when is_closed then 0
           else greatest(0, least(100,
             50 + features_static + shade_pos + weather_pos + window_pos
                + v_agility_pos + water_play_pos
                - heat_uv_neg - harsh_neg - gotchas_neg + v_drive))
      end as score
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'score', score,
    'is_closed', is_closed,
    'pos_weather', case when is_closed then 0 else weather_pos end,
    'pos_features', case when is_closed then 0 else features_static + shade_pos end,
    'pos_enrichment', case when is_closed then 0 else v_agility_pos + water_play_pos end,
    'pos_window', case when is_closed then 0 else window_pos end,
    'neg_heat_uv', case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',   case when is_closed then 0 else harsh_neg end,
    'neg_gotchas', case when is_closed then 0 else gotchas_neg end,
    'drive', case when is_closed then 0 else v_drive end,
    'pos_temp', case when is_closed then 0 else temp_pos end,
    'pos_wind', case when is_closed then 0 else wind_pos end,
    'pos_sky',  case when is_closed then 0 else sky_pos end,
    'pos_fence', case when is_closed then 0 else v_fence_pos end,
    'pos_water', case when is_closed then 0 else v_water_pos end,
    'pos_shade', case when is_closed then 0 else shade_pos end,
    'pos_comfort', case when is_closed then 0 else v_comfort_pos end,
    'pos_agility', case when is_closed then 0 else v_agility_pos end,
    'pos_water_play', case when is_closed then 0 else water_play_pos end,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp,
    'weather_code', weather_code, 'precip_chance', precip_chance
  ) order by local_hour) into v_hours from scored;

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 0, 2, 5);

  return jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'open_hour', v_open_h, 'close_hour', v_close_h,
    'features_positive', v_fence_pos + v_water_pos + v_comfort_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos,
    'pos_agility', v_agility_pos,
    'drive_factor', v_drive, 'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label', v_today.best_window_label,
    'best_window_v2_label', v_bw->>'label',
    'best_window_v2_start', nullif(v_bw->>'start', '')::int,
    'best_window_v2_end',   nullif(v_bw->>'end',   '')::int,
    'best_window_v2_score', nullif(v_bw->>'score', '')::int,
    'closures', v_closures,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$function$;

-- 2. compute_beach_hourly_v2 — drop status field from JSON
CREATE OR REPLACE FUNCTION public.compute_beach_hourly_v2(
  p_fid bigint,
  p_user_lat double precision DEFAULT NULL,
  p_user_lng double precision DEFAULT NULL,
  p_date date DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $function$
declare
  v_gold record; v_policy record; v_today record; v_amenities record;
  v_view_date date := coalesce(p_date, current_date);
  v_restroom_pos integer := 0; v_lifeguard_pos integer := 0;
  v_parking_pos integer := 0; v_picnic_pos integer := 0; v_shower_pos integer := 0;
  v_food_pos integer := 0; v_drinking_pos integer := 0; v_disabled_pos integer := 0;
  v_amenities_pos integer := 0; v_dog_access_pos integer := 0;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_open_h integer; v_close_h integer; v_hours jsonb;
  v_bw jsonb; v_closures jsonb := '[]'::jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg jsonb; v_b_sand_neg jsonb; v_b_uv_neg jsonb;
  v_b_tide_neg jsonb; v_b_crowd_neg jsonb; v_b_drive jsonb;
begin
  select * into v_gold from public.beaches_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','beach not found','fid',p_fid); end if;
  select * into v_policy from public.beach_dog_policy where arena_group_id = p_fid limit 1;
  select * into v_today from public.beach_day_recommendations
    where location_id = v_gold.location_id and local_date = v_view_date limit 1;
  select * into v_amenities from public.beach_amenities where arena_group_id = p_fid limit 1;
  if v_policy.arena_group_id is not null then
    v_closures := public._beach_closures_from_policy(v_policy.dogs_allowed, v_policy.zone_rules);
  end if;
  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type='beach' and signal_key='wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type='beach' and signal_key='wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type='beach' and signal_key='asphalt_neg';
  select bands into v_b_sand_neg       from public.scoring_config_v2 where entity_type='beach' and signal_key='sand_temp_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type='beach' and signal_key='uv_neg';
  select bands into v_b_tide_neg       from public.scoring_config_v2 where entity_type='beach' and signal_key='tide_neg';
  select bands into v_b_crowd_neg      from public.scoring_config_v2 where entity_type='beach' and signal_key='crowd_neg';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type='beach' and signal_key='drive_factor';
  if v_amenities.has_restrooms       = true then v_restroom_pos := 4; end if;
  if v_amenities.has_lifeguards      = true then v_lifeguard_pos := 4; end if;
  if v_amenities.has_parking         = true then v_parking_pos := 3; end if;
  if v_amenities.has_showers         = true then v_shower_pos := 2; end if;
  if v_amenities.has_picnic_area     = true then v_picnic_pos := 2; end if;
  if v_amenities.has_food            = true then v_food_pos := 1; end if;
  if v_amenities.has_drinking_water  = true then v_drinking_pos := 1; end if;
  if v_amenities.has_disabled_access = true then v_disabled_pos := 1; end if;
  v_amenities_pos := v_restroom_pos + v_lifeguard_pos + v_parking_pos
                   + v_shower_pos + v_picnic_pos + v_food_pos
                   + v_drinking_pos + v_disabled_pos;
  if v_policy.off_leash_flag = true then v_dog_access_pos := 5; end if;
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    v_drive := public._v2_lookup_band(v_drive_min::numeric, v_b_drive);
  end if;
  v_open_h := 6; v_close_h := 21;

  with per_hour as (
    select h.local_hour, h.hour_label,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.sand_temp, h.tide_height, h.precip_chance,
      h.busyness_score, h.is_in_best_window, h.is_daylight,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
      public._v2_first_closure_section(v_closures, v_view_date, h.local_hour) as closed_section,
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_pos) as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      least(30,
        public._v2_lookup_band(coalesce(h.sand_temp, -1), v_b_sand_neg)
        + public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90 then least(4, greatest(0, round((h.feels_like - 90) * 0.4)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int)) else 0 end
      ) as harsh_neg,
      public._v2_lookup_band(coalesce(h.tide_height, -1), v_b_tide_neg) as tide_neg,
      public._v2_lookup_band(coalesce(h.busyness_score, -1), v_b_crowd_neg) as crowd_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos
    from public.beach_day_hourly_scores h
    where h.location_id = v_gold.location_id
      and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(22, temp_pos + wind_pos + sky_pos) as weather_pos,
      v_amenities_pos as amenities_static,
      v_dog_access_pos as dog_access_static
    from per_hour
  ),
  scored as (
    select *,
      case when is_closed then 0
           else greatest(0, least(100,
             50 + amenities_static + dog_access_static + weather_pos + window_pos
                - heat_uv_neg - harsh_neg - tide_neg - crowd_neg + v_drive))
      end as score
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'score', score,
    'is_closed', is_closed, 'closed_section', closed_section,
    'pos_weather',   case when is_closed then 0 else weather_pos end,
    'pos_features',  case when is_closed then 0 else amenities_static end,
    'pos_enrichment', case when is_closed then 0 else dog_access_static end,
    'pos_window',    case when is_closed then 0 else window_pos end,
    'neg_heat_uv',   case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',     case when is_closed then 0 else harsh_neg end,
    'neg_gotchas',   case when is_closed then 0 else tide_neg + crowd_neg end,
    'drive',         case when is_closed then 0 else v_drive end,
    'pos_temp', case when is_closed then 0 else temp_pos end,
    'pos_wind', case when is_closed then 0 else wind_pos end,
    'pos_sky',  case when is_closed then 0 else sky_pos end,
    'pos_restroom',  case when is_closed then 0 else v_restroom_pos end,
    'pos_lifeguard', case when is_closed then 0 else v_lifeguard_pos end,
    'pos_parking',   case when is_closed then 0 else v_parking_pos end,
    'pos_shower',    case when is_closed then 0 else v_shower_pos end,
    'pos_picnic',    case when is_closed then 0 else v_picnic_pos end,
    'pos_food',      case when is_closed then 0 else v_food_pos end,
    'pos_drinking',  case when is_closed then 0 else v_drinking_pos end,
    'pos_disabled',  case when is_closed then 0 else v_disabled_pos end,
    'pos_dog_access', case when is_closed then 0 else v_dog_access_pos end,
    'neg_tide',  case when is_closed then 0 else tide_neg end,
    'neg_crowd', case when is_closed then 0 else crowd_neg end,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp, 'sand_temp', sand_temp,
    'tide_height', tide_height, 'busyness_score', busyness_score,
    'weather_code', weather_code, 'precip_chance', precip_chance,
    'is_daylight', is_daylight
  ) order by local_hour) into v_hours from scored;

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 0, 2, 5);

  return jsonb_build_object(
    'fid', p_fid, 'location_id', v_gold.location_id, 'view_date', v_view_date,
    'open_hour', v_open_h, 'close_hour', v_close_h,
    'amenities_positive', v_amenities_pos,
    'pos_restroom', v_restroom_pos, 'pos_lifeguard', v_lifeguard_pos,
    'pos_parking', v_parking_pos, 'pos_shower', v_shower_pos,
    'pos_picnic', v_picnic_pos, 'pos_food', v_food_pos,
    'pos_drinking', v_drinking_pos, 'pos_disabled', v_disabled_pos,
    'pos_dog_access', v_dog_access_pos,
    'drive_factor', v_drive, 'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label', v_today.best_window_label,
    'best_window_v2_label', v_bw->>'label',
    'best_window_v2_start', nullif(v_bw->>'start', '')::int,
    'best_window_v2_end',   nullif(v_bw->>'end',   '')::int,
    'best_window_v2_score', nullif(v_bw->>'score', '')::int,
    'closures', v_closures,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$function$;

-- 3. compute_dog_park_score_v2 — drop hour_status_v2 from output
CREATE OR REPLACE FUNCTION public.compute_dog_park_score_v2(
  p_fid bigint,
  p_user_lat double precision DEFAULT NULL,
  p_user_lng double precision DEFAULT NULL,
  p_date date DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $function$
declare
  v_gold record; v_policy record; v_now record; v_today record;
  v_view_date date := coalesce(p_date, current_date);
  v_pos jsonb; v_neg jsonb; v_composed jsonb;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_temp_pos integer := 0; v_wind_pos integer := 0; v_sky_pos integer := 0;
  v_weather_pos integer := 0;
  v_fence_pos integer := 0; v_water_pos integer := 0;
  v_shade_pos integer := 0; v_comfort_pos integer := 0;
  v_features_pos integer := 0;
  v_agility_pos integer := 0; v_water_play_pos integer := 0;
  v_enrichment_pos integer := 0;
  v_window_pos integer := 0;
  v_heat_neg integer := 0; v_harsh_neg integer := 0; v_gotchas_neg integer := 0;
  v_fence boolean; v_water boolean; v_shade boolean;
  v_agility boolean; v_water_play boolean;
  v_surface text;
  v_uv_need integer := 0; v_temp_need integer := 0; v_shade_need integer := 0;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_now from public.dog_park_day_hourly_scores where dog_park_fid = p_fid and is_now = true limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_shade := v_policy.has_shade;
  v_agility := v_policy.has_agility;
  v_water_play := v_policy.has_water_play;
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  if v_now.feels_like is not null then
    v_temp_pos := greatest(0, round(10 * (1 - least(abs(v_now.feels_like - 72), 20) / 20.0))::int);
  end if;
  if v_now.wind_speed is not null then
    v_wind_pos := case when v_now.wind_speed < 6 then 2 when v_now.wind_speed < 13 then 6 else 0 end;
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

  v_uv_need := case when v_now.uv_index is null then 0
                    when v_now.uv_index < 6 then 0
                    when v_now.uv_index < 8 then 2
                    when v_now.uv_index < 10 then 4
                    else 6 end;
  v_temp_need := case when v_now.feels_like is null then 0
                      when v_now.feels_like < 80 then 0
                      when v_now.feels_like < 90 then 2
                      when v_now.feels_like < 95 then 4
                      else 6 end;
  v_shade_need := least(6, greatest(v_uv_need, v_temp_need));

  if v_fence = true             then v_fence_pos := v_fence_pos + 6; end if;
  if v_policy.double_gate = true then v_fence_pos := v_fence_pos + 2; end if;
  if v_water = true             then v_water_pos := v_water_pos + 4; end if;
  if v_shade = true             then v_shade_pos := v_shade_need; end if;
  if v_surface = 'grass'        then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.has_picnic_tables = true then v_comfort_pos := v_comfort_pos + 2; end if;
  v_features_pos := v_fence_pos + v_water_pos + v_shade_pos + v_comfort_pos;

  if v_agility = true    then v_agility_pos := 3; end if;
  if v_water_play = true then v_water_play_pos := v_shade_need; end if;
  v_enrichment_pos := v_agility_pos + v_water_play_pos;

  if v_today.best_window_label is not null then v_window_pos := 5;
  elsif coalesce(v_today.go_hours_count, 0) > 0 then v_window_pos := 2; end if;

  if v_now.asphalt_temp is not null then
    v_heat_neg := v_heat_neg + case
      when v_now.asphalt_temp < 115 then 0
      when v_now.asphalt_temp < 125 then 2
      when v_now.asphalt_temp < 135 then 8
      when v_now.asphalt_temp < 145 then 12
      else 15 end;
  end if;
  if v_now.uv_index is not null then
    v_heat_neg := v_heat_neg + case
      when v_now.uv_index < 6 then 0 when v_now.uv_index < 8 then 1
      when v_now.uv_index < 10 then 3 when v_now.uv_index < 11 then 4 else 10 end;
  end if;
  if v_now.feels_like is not null and v_now.feels_like > 90 then
    v_heat_neg := v_heat_neg + least(5, greatest(0, round((v_now.feels_like - 90) * 0.5)::int));
  end if;
  v_heat_neg := least(v_heat_neg, 25);

  if v_now.wind_speed is not null then
    v_harsh_neg := v_harsh_neg + case
      when v_now.wind_speed < 13 then 0
      when v_now.wind_speed < 19 then 2
      when v_now.wind_speed < 25 then 4
      when v_now.wind_speed < 35 then 6
      else 10 end;
  end if;
  if v_now.precip_chance is not null and v_now.precip_chance > 30 then
    v_harsh_neg := v_harsh_neg + least(10, greatest(0, round((v_now.precip_chance - 30) / 7.0)::int));
  end if;
  if v_now.feels_like is not null and v_now.feels_like < 50 then
    v_harsh_neg := v_harsh_neg + least(10, greatest(0, round((50 - v_now.feels_like) * 0.4)::int));
  end if;
  v_harsh_neg := least(v_harsh_neg, 15);

  if v_shade = false then v_gotchas_neg := v_shade_need; end if;

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

  v_pos := jsonb_build_object(
    'weather', jsonb_build_object('value', v_weather_pos, 'max', 22),
    'features', jsonb_build_object('value', v_features_pos, 'max', 22),
    'enrichment', jsonb_build_object('value', v_enrichment_pos, 'max', 9),
    'window', jsonb_build_object('value', v_window_pos, 'max', 5));
  v_neg := jsonb_build_object(
    'heat_uv', jsonb_build_object('value', v_heat_neg, 'max', 25),
    'weather_harsh', jsonb_build_object('value', v_harsh_neg, 'max', 15),
    'gotchas', jsonb_build_object('value', v_gotchas_neg, 'max', 6));

  v_composed := public._score_v2_compose(
    p_baseline := 50, p_positives := v_pos, p_negatives := v_neg,
    p_drive_factor := v_drive
  );

  return v_composed || jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'now_available', v_now.dog_park_fid is not null,
    'today_available', v_today.dog_park_fid is not null,
    'best_window_label', v_today.best_window_label,
    'pos_temp', v_temp_pos, 'pos_wind', v_wind_pos, 'pos_sky', v_sky_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos,
    'pos_shade', v_shade_pos, 'pos_comfort', v_comfort_pos,
    'pos_agility', v_agility_pos, 'pos_water_play', v_water_play_pos);
end;
$function$;

-- 4. _find_best_window_v2 — filter on is_closed only (no status reference)
CREATE OR REPLACE FUNCTION public._find_best_window_v2(
  p_hours jsonb,
  p_min_score integer DEFAULT 0,
  p_min_len integer DEFAULT 2,
  p_max_len integer DEFAULT 5,
  p_from_hour integer DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $function$
declare
  v_start int; v_end int; v_score int; v_label text;
begin
  with hours_data as (
    select (h->>'local_hour')::int as hr,
           (h->>'score')::int as score,
           coalesce((h->>'is_closed')::boolean, false) as is_closed
      from jsonb_array_elements(p_hours) h
  ),
  qualifying as (
    select hr, score, hr - row_number() over (order by hr) as grp
      from hours_data
     where score >= p_min_score
       and not is_closed
       and (p_from_hour is null or hr >= p_from_hour)
  ),
  runs as (
    select min(hr) as start_h, max(hr) as end_h,
           count(*)::int as len,
           max(score)::int as max_score,
           avg(score)::numeric as avg_score
      from qualifying
     group by grp
     having count(*) >= p_min_len
  ),
  best_run as (
    select start_h, end_h, len, max_score from runs
     order by avg_score desc, len desc, start_h
     limit 1
  )
  select start_h, end_h, max_score into v_start, v_end, v_score from best_run;

  if v_start is null then
    return jsonb_build_object('label', null, 'start', null, 'end', null, 'score', null);
  end if;

  if (v_end - v_start + 1) > p_max_len then
    select sub_start into v_start
      from (
        select s.sub_start,
               (select sum((h2->>'score')::int)
                  from jsonb_array_elements(p_hours) h2
                 where (h2->>'local_hour')::int between s.sub_start
                                                   and s.sub_start + p_max_len - 1
                   and not coalesce((h2->>'is_closed')::boolean, false)
               ) as window_sum
        from generate_series(v_start, v_end - p_max_len + 1) s(sub_start)
      ) candidates
      order by window_sum desc nulls last
      limit 1;
    v_end := v_start + p_max_len - 1;
  end if;

  v_label := public._format_hour_range(v_start, v_end + 1);
  return jsonb_build_object('label', v_label, 'start', v_start, 'end', v_end, 'score', v_score);
end;
$function$;

-- 5. apply_v2_best_window_to_recommendations — stop writing day_status_v2
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations(
  p_fid bigint, p_date date DEFAULT CURRENT_DATE
) RETURNS void LANGUAGE plpgsql AS $function$
declare
  v_result jsonb;
  v_label text; v_start int; v_end int; v_v2_score int;
  v_composite_v2 int;
  h jsonb;
begin
  v_result := public.compute_dog_park_hourly_v2(p_fid, null, null, p_date);
  v_label    := v_result->>'best_window_v2_label';
  v_start    := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end      := nullif(v_result->>'best_window_v2_end',   '')::int;
  v_v2_score := nullif(v_result->>'best_window_v2_score', '')::int;

  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.dog_park_day_hourly_scores
       set hour_score_v2 = case when (h->>'is_closed')::boolean = true
                                then null
                                else nullif(h->>'score', '')::int end
     where dog_park_fid = p_fid
       and local_date   = p_date
       and local_hour   = (h->>'local_hour')::int;
  end loop;

  if v_v2_score is not null then
    v_composite_v2 := v_v2_score;
  else
    select avg(hour_score_v2)::int
      into v_composite_v2
      from public.dog_park_day_hourly_scores
     where dog_park_fid = p_fid
       and local_date   = p_date
       and is_daylight  = true
       and hour_score_v2 is not null;
  end if;

  update public.dog_park_day_recommendations
     set best_window_label    = coalesce(v_label, best_window_label),
         best_window_start_ts = case when v_start is not null
                                     then (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC'
                                     else best_window_start_ts end,
         best_window_end_ts   = case when v_end is not null
                                     then (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
                                     else best_window_end_ts end,
         day_status_v2        = NULL,
         composite_score_v2   = v_composite_v2
   where dog_park_fid = p_fid and local_date = p_date;
end;
$function$;

-- 6. apply_v2_best_window_to_beach_recommendations — stop writing day_status_v2
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations(
  p_fid bigint, p_date date DEFAULT CURRENT_DATE
) RETURNS void LANGUAGE plpgsql AS $function$
declare
  v_result jsonb;
  v_label text; v_start int; v_end int; v_v2_score int;
  v_location_id text;
  v_composite_v2 int;
  h jsonb;
begin
  select location_id into v_location_id from public.beaches_gold where fid = p_fid;
  if v_location_id is null then return; end if;

  v_result := public.compute_beach_hourly_v2(p_fid, null, null, p_date);
  v_label    := v_result->>'best_window_v2_label';
  v_start    := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end      := nullif(v_result->>'best_window_v2_end',   '')::int;
  v_v2_score := nullif(v_result->>'best_window_v2_score', '')::int;

  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.beach_day_hourly_scores
       set hour_score_v2 = case when (h->>'is_closed')::boolean = true
                                then null
                                else nullif(h->>'score', '')::int end
     where arena_group_id = p_fid
       and local_date     = p_date
       and local_hour     = (h->>'local_hour')::int;
  end loop;

  if v_v2_score is not null then
    v_composite_v2 := v_v2_score;
  else
    select avg(hour_score_v2)::int
      into v_composite_v2
      from public.beach_day_hourly_scores
     where arena_group_id = p_fid
       and local_date     = p_date
       and is_daylight    = true
       and hour_score_v2 is not null;
  end if;

  update public.beach_day_recommendations
     set best_window_label    = v_label,
         best_window_start_ts = case when v_start is not null
                                     then (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC'
                                     else null end,
         best_window_end_ts   = case when v_end is not null
                                     then (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
                                     else null end,
         day_status_v2        = NULL,
         composite_score_v2   = v_composite_v2
   where location_id = v_location_id and local_date = p_date;
end;
$function$;

-- 7. Drop orphaned status helpers.
DROP FUNCTION IF EXISTS public._v2_status_from_score(integer);
DROP FUNCTION IF EXISTS public.v2_compute_hour_status(numeric, numeric, numeric, numeric, numeric, numeric, numeric, numeric, boolean);
DROP FUNCTION IF EXISTS public.v2_compute_hour_status_dog_park(numeric, numeric, numeric, numeric, numeric, boolean);
DROP FUNCTION IF EXISTS public.v2_compute_day_status(bigint, date);
DROP FUNCTION IF EXISTS public.v2_signal_status(text, text, numeric);
