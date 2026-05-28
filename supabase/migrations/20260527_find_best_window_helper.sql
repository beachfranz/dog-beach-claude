-- 20260527_find_best_window_helper.sql
--
-- Pre-beach infra item 3/4. Extract best-window finder from
-- compute_dog_park_hourly_v2 into a shared helper so beach v2 (and
-- future entity types) can reuse the same logic.
--
-- Algorithm:
--   1. Input: hours jsonb array, each element with {local_hour, score, gate_fired}
--   2. Filter to hours where score >= min_score AND not gate_fired
--   3. Find longest contiguous run (gap = 1 hour break)
--   4. If run > max_len hours, pick the max_len-sized subwindow with highest score-sum
--   5. Return label + start + end + max_score
--
-- Output shape: {label, start, end, score} — null all if no qualifying window.

begin;

create or replace function public._find_best_window_v2(
  p_hours      jsonb,
  p_min_score  integer default 70,
  p_min_len    integer default 2,
  p_max_len    integer default 5
) returns jsonb language plpgsql stable as $$
declare
  v_start int; v_end int; v_score int; v_label text;
begin
  with hours_data as (
    select (h->>'local_hour')::int as hr,
           (h->>'score')::int as score,
           coalesce((h->>'gate_fired')::boolean, false) as gate_fired
    from jsonb_array_elements(p_hours) h
  ),
  qualifying as (
    select hr, score, hr - row_number() over (order by hr) as grp
    from hours_data
    where score >= p_min_score and not gate_fired
  ),
  runs as (
    select min(hr) as start_h, max(hr) as end_h,
           count(*)::int as len, max(score)::int as max_score
    from qualifying
    group by grp
    having count(*) >= p_min_len
  ),
  best_run as (
    select start_h, end_h, len, max_score from runs
    order by max_score desc, len desc, start_h
    limit 1
  )
  select start_h, end_h, max_score into v_start, v_end, v_score from best_run;

  if v_start is null then
    return jsonb_build_object('label', null, 'start', null, 'end', null, 'score', null);
  end if;

  -- Cap to max_len: pick the contiguous subwindow with highest score-sum.
  if (v_end - v_start + 1) > p_max_len then
    select sub_start into v_start
      from (
        select s.sub_start,
               (select sum((h2->>'score')::int)
                  from jsonb_array_elements(p_hours) h2
                 where (h2->>'local_hour')::int between s.sub_start
                                                   and s.sub_start + p_max_len - 1) as window_sum
        from generate_series(v_start, v_end - p_max_len + 1) s(sub_start)
      ) candidates
      order by window_sum desc nulls last
      limit 1;
    v_end := v_start + p_max_len - 1;
  end if;

  v_label := public._format_hour_range(v_start, v_end + 1);
  return jsonb_build_object(
    'label', v_label,
    'start', v_start,
    'end',   v_end,
    'score', v_score
  );
end;
$$;

comment on function public._find_best_window_v2 is
  'Generic best-window finder for v2 scoring. Takes an hours jsonb '
  'array, returns the longest qualifying contiguous run (score >= '
  'min_score, not gate_fired), clipped to max_len with subwindow-sum '
  'selection. Reused by all entity-type v2 scorers.';


-- ── Refactor compute_dog_park_hourly_v2 to use the shared helper ──

create or replace function public.compute_dog_park_hourly_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
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
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb; v_b_asphalt_neg jsonb;
  v_b_uv_neg   jsonb; v_b_uv_need        jsonb; v_b_temp_need   jsonb;
  v_b_drive    jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'asphalt_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'uv_neg';
  select bands into v_b_uv_need        from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'uv_need';
  select bands into v_b_temp_need      from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'temp_need';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'drive_factor';

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
    select h.local_hour, h.hour_label, h.hour_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
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
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      (coalesce(h.asphalt_temp, 0) >= 125 or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired,
      case when coalesce(h.asphalt_temp, 0) >= 125 or coalesce(h.uv_index, 0) >= 11
                or coalesce(h.feels_like, 99) <= 20 then 30
           when coalesce(h.asphalt_temp, 0) >= 115 or coalesce(h.feels_like, 70) >= 95
                or coalesce(h.feels_like, 70) <= 32 or coalesce(h.precip_chance, 0) >= 80 then 60
           else 100 end as status_cap
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
    select *, greatest(0, least(status_cap,
        50 + features_static + shade_pos + weather_pos + window_pos
           + v_agility_pos + water_play_pos
           - heat_uv_neg - harsh_neg - gotchas_neg + v_drive)) as score
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label, 'status', hour_status,
    'score', score, 'gate_fired', gate_fired, 'status_cap', status_cap,
    'pos_weather', weather_pos,
    'pos_features', features_static + shade_pos,
    'pos_enrichment', v_agility_pos + water_play_pos,
    'pos_window', window_pos,
    'neg_heat_uv', heat_uv_neg, 'neg_harsh', harsh_neg, 'neg_gotchas', gotchas_neg,
    'drive', v_drive,
    'pos_temp', temp_pos, 'pos_wind', wind_pos, 'pos_sky', sky_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos,
    'pos_shade', shade_pos, 'pos_comfort', v_comfort_pos,
    'pos_agility', v_agility_pos, 'pos_water_play', water_play_pos,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp,
    'weather_code', weather_code, 'precip_chance', precip_chance
  ) order by local_hour) into v_hours from scored;

  -- Best window via shared helper
  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 70, 2, 5);

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
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$$;

commit;
notify pgrst, 'reload schema';
