-- compute_dog_park_hourly_v2: emit advisory_components + pos_features_v3 so
-- dog-park.html's v3 chart mirrors beach.html (per [[paired-functions-port-
-- fixes-both-sides]]). Same approach as 20260618d on the beach side.
--
-- advisory_drill: inline whitelist drill on dog_park_advisory (INNER JOIN
-- config, enabled=true) so SUM(components) == total == score_v3 deduction.
-- pos_features_v3: the v3 rolled-up park-feature positive (fence+water+
-- comfort+shade+agility+water_play) for the single "features" chart segment.
-- score_v3 math, gates, v2 path unchanged.

BEGIN;

CREATE OR REPLACE FUNCTION public.compute_dog_park_hourly_v2(p_fid bigint, p_user_lat double precision DEFAULT NULL::double precision, p_user_lng double precision DEFAULT NULL::double precision, p_date date DEFAULT NULL::date)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
AS $function$
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
      -- v2 inline negs
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
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      -- NEW: advisory drill {total, components[]}, whitelist semantics matching score_v3.
      (
        WITH active AS (
          SELECT dpa.event_type, dpa.severity,
                 (public._advisory_base_for_severity(dpa.severity) * asc_row.weight)::numeric AS contribution
            FROM public.dog_park_advisory dpa
            JOIN public.advisory_score_config asc_row
              ON asc_row.entity_type = 'dog_park'
             AND asc_row.event_type  = dpa.event_type
             AND asc_row.enabled     = true
           WHERE dpa.dog_park_fid = p_fid
             AND (v_view_date::timestamp + (h.local_hour || ' hours')::interval)
                 AT TIME ZONE h.timezone BETWEEN dpa.valid_from AND dpa.valid_to
        )
        SELECT jsonb_build_object(
          'total', COALESCE(SUM(contribution), 0),
          'components', COALESCE(jsonb_agg(
            jsonb_build_object('event_type', event_type, 'severity', severity, 'contribution', contribution)
            ORDER BY contribution DESC, event_type
          ) FILTER (WHERE contribution > 0), '[]'::jsonb)
        ) FROM active
      ) AS advisory_drill
    from public.dog_park_day_hourly_scores h
    where h.dog_park_fid = p_fid and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      COALESCE((advisory_drill->>'total')::numeric, 0) as advisory_penalty,
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
      end as score,
      -- v3 NEW: gates + advisory_penalty + v_drive (singleton has drive)
      case
        when is_closed             then 0
        when asphalt_temp >= 125   then 0
        when uv_index     >= 11    then 0
        when feels_like   <= 20    then 0
        else greatest(0, least(100,
          50 + features_static + shade_pos + weather_pos
             + v_agility_pos + water_play_pos
             - COALESCE(advisory_penalty, 0) + v_drive))::int
      end as score_v3
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'score', score,
    'score_v3', score_v3,
    'status_v3', case
      when is_closed then 'closed'
      when asphalt_temp >= 125 or uv_index >= 11 or feels_like <= 20
        then 'no_go'
      else public._v2_status_from_score(score_v3::int)
    end,
    'advisory_penalty_v3', COALESCE(advisory_penalty, 0),
    'advisory_components', case when is_closed then '[]'::jsonb
                               else COALESCE(advisory_drill->'components', '[]'::jsonb) end,
    'is_closed', is_closed,
    'pos_weather', case when is_closed then 0 else weather_pos end,
    'pos_features', case when is_closed then 0 else features_static + shade_pos end,
    -- v3 rolled-up park features (fence+water+comfort+shade+agility+water_play)
    'pos_features_v3', case when is_closed then 0
                           else features_static + shade_pos + v_agility_pos + water_play_pos end,
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

commit;
notify pgrst, 'reload schema';
