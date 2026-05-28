-- 20260527_v2_best_window.sql
--
-- Best-window computation from v2 scores (Franz 2026-05-27 LATE).
--
-- Until now compute_dog_park_hourly_v2 returned per-hour v2 scores but
-- the "Best time today" headline on dog-park.html came from the v1
-- daily-refresh write to dog_park_day_recommendations.best_window_label
-- — a different scoring engine. This migration:
--
--   1. Adds v2 best-window fields to compute_dog_park_hourly_v2's return.
--      Best = longest contiguous run of hours where score >= 70 (within
--      park open/close, no_go gates excluded). Clamped to 2-5 hours.
--      Falls back to "single best go hour" if no qualifying run.
--
--   2. Adds public.apply_v2_best_window_to_recommendations(fid, date)
--      which calls (1) and writes the v2 best-window fields into
--      dog_park_day_recommendations (overwriting the v1 values).
--      daily-dog-park-refresh will call this per fid.

begin;

-- ════════════════════════════════════════════════════════════════════
-- Extend the hourly v2 RPC to also return best-window fields.
-- ════════════════════════════════════════════════════════════════════

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
  -- Best window result
  v_bw_label  text;
  v_bw_start  integer;
  v_bw_end    integer;
  v_bw_score  integer;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

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
      case when h.wind_speed is null then 0 when h.wind_speed < 6 then 2
           when h.wind_speed < 13 then 6 else 0 end as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      least(6, greatest(
        case when h.uv_index is null then 0
             when h.uv_index < 6 then 0 when h.uv_index < 8 then 2
             when h.uv_index < 10 then 4 else 6 end,
        case when h.feels_like is null then 0
             when h.feels_like < 80 then 0 when h.feels_like < 90 then 2
             when h.feels_like < 95 then 4 else 6 end
      )) as shade_need,
      least(25,
        case when h.asphalt_temp is null or h.asphalt_temp < 115 then 0
             when h.asphalt_temp < 125 then 2 when h.asphalt_temp < 135 then 8
             when h.asphalt_temp < 145 then 12 else 15 end
        + case when h.uv_index is null or h.uv_index < 6 then 0
               when h.uv_index < 8 then 1 when h.uv_index < 10 then 3
               when h.uv_index < 11 then 4 else 10 end
        + case when h.feels_like > 90 then least(5, greatest(0, round((h.feels_like - 90) * 0.5)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        case when h.wind_speed is null or h.wind_speed < 13 then 0
             when h.wind_speed < 19 then 2
             when h.wind_speed < 25 then 4
             when h.wind_speed < 35 then 6
             else 10 end
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

  -- ── Best window (v2): longest contiguous run of hours where
  -- score >= 70 AND not gate_fired. Clamped to length 2-5 hours.
  -- If no qualifying run, fall back to single highest-scoring go-hour.
  with hours_data as (
    select (h->>'local_hour')::int as hr,
           (h->>'score')::int as score,
           (h->>'gate_fired')::boolean as gate_fired
    from jsonb_array_elements(v_hours) h
  ),
  qualifying as (
    select hr, score,
           hr - row_number() over (order by hr) as grp
    from hours_data
    where score >= 70 and not gate_fired
  ),
  runs as (
    select min(hr) as start_h, max(hr) as end_h,
           count(*)::int as len,
           max(score)::int as max_score
    from qualifying
    group by grp
    having count(*) >= 2
  ),
  best_run as (
    select start_h, end_h, len, max_score
    from runs
    order by max_score desc, len desc, start_h
    limit 1
  )
  select start_h, end_h, max_score into v_bw_start, v_bw_end, v_bw_score from best_run;

  -- Cap window to 5 hours max — slide to the highest-scoring sub-window.
  if v_bw_start is not null and (v_bw_end - v_bw_start + 1) > 5 then
    -- Pick the 5-hour subwindow with highest sum of scores within the run
    select sub_start into v_bw_start
      from (
        select hr as sub_start,
               sum((h->>'score')::int) over (
                 order by hr rows between current row and 4 following
               ) as window_sum
        from (
          select (h->>'local_hour')::int as hr, h
          from jsonb_array_elements(v_hours) h
          where (h->>'local_hour')::int between v_bw_start and v_bw_end - 4
        ) sub
      ) ranked
      order by window_sum desc nulls last
      limit 1;
    v_bw_end := v_bw_start + 4;
  end if;

  if v_bw_start is not null then
    v_bw_label := public._format_hour_range(v_bw_start, v_bw_end + 1);
  end if;

  return jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'open_hour', v_open_h, 'close_hour', v_close_h,
    'features_positive', v_fence_pos + v_water_pos + v_comfort_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos,
    'pos_agility', v_agility_pos,
    'drive_factor', v_drive, 'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label', v_today.best_window_label,
    -- NEW: v2 best window
    'best_window_v2_label', v_bw_label,
    'best_window_v2_start', v_bw_start,
    'best_window_v2_end',   v_bw_end,
    'best_window_v2_score', v_bw_score,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$$;


-- ════════════════════════════════════════════════════════════════════
-- Helper: format an hour range like "10am–2pm" (end-exclusive).
-- ════════════════════════════════════════════════════════════════════

create or replace function public._format_hour_range(p_start int, p_end int)
returns text language plpgsql immutable as $$
declare
  v_start_label text;
  v_end_label   text;
  function_local_label text;
begin
  v_start_label := case
    when p_start = 0 then '12am'
    when p_start < 12 then p_start::text || 'am'
    when p_start = 12 then '12pm'
    else (p_start - 12)::text || 'pm'
  end;
  v_end_label := case
    when p_end = 0 then '12am'
    when p_end < 12 then p_end::text || 'am'
    when p_end = 12 then '12pm'
    when p_end = 24 then '12am'
    else (p_end - 12)::text || 'pm'
  end;
  return v_start_label || '–' || v_end_label;
end;
$$;


-- ════════════════════════════════════════════════════════════════════
-- Apply v2 best window to dog_park_day_recommendations
-- ════════════════════════════════════════════════════════════════════

create or replace function public.apply_v2_best_window_to_recommendations(
  p_fid bigint, p_date date default current_date
) returns void language plpgsql as $$
declare
  v_result jsonb;
  v_label  text;
  v_start  int;
  v_end    int;
begin
  v_result := public.compute_dog_park_hourly_v2(p_fid, null, null, p_date);
  v_label := v_result->>'best_window_v2_label';
  v_start := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end   := nullif(v_result->>'best_window_v2_end',   '')::int;

  if v_label is null then return; end if;

  update public.dog_park_day_recommendations
     set best_window_label    = v_label,
         best_window_start_ts = (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC',
         best_window_end_ts   = (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
   where dog_park_fid = p_fid and local_date = p_date;
end;
$$;

comment on function public.apply_v2_best_window_to_recommendations is
  'Recompute v2 best window for a dog park + write the label and start/end into '
  'dog_park_day_recommendations. Called by daily-dog-park-refresh after the v1 '
  'hourly rows are populated, so the consumer-surface best_window_label aligns '
  'with the v2 chart scores.';

commit;
notify pgrst, 'reload schema';
