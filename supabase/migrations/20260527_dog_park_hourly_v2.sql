-- 20260527_dog_park_hourly_v2.sql
--
-- Per-hour v2 scoring. Returns one row per hour from park opening to
-- park closing, with score + per-component contributions. Powers the
-- vertical bar chart on dog-park.html (Franz 2026-05-27).
--
-- Computes static parts once (features positive, drive factor, base
-- gotchas) and per-hour parts from dog_park_day_hourly_scores rows
-- (weather positive, heat/UV negative, harsh negative, no-shade gotcha,
-- window-now positive, status gate). Same weights as compute_dog_park_score_v2.

begin;

create or replace function public.compute_dog_park_hourly_v2(
  p_fid       bigint,
  p_user_lat  double precision default null,
  p_user_lng  double precision default null,
  p_date      date default null
) returns jsonb
language plpgsql stable
as $$
declare
  v_gold       record;
  v_policy     record;
  v_today      record;
  v_view_date  date := coalesce(p_date, current_date);
  v_features_pos integer := 0;
  v_static_gotchas integer := 0;
  v_drive      integer := 0;
  v_drive_miles numeric;
  v_drive_min   integer;
  v_fence      boolean;
  v_water      boolean;
  v_surface    text;
  v_open_h     integer;
  v_close_h    integer;
  v_hours      jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then
    return jsonb_build_object('error', 'park not found', 'fid', p_fid);
  end if;

  select * into v_policy from public.dog_park_dog_policy
   where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations
   where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence   := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water   := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  -- ── Static features positive (constant across hours) ─────────────
  if v_fence = true                       then v_features_pos := v_features_pos + 6; end if;
  if v_water = true                       then v_features_pos := v_features_pos + 5; end if;
  if v_policy.double_gate = true          then v_features_pos := v_features_pos + 3; end if;
  if v_surface = 'grass'                  then v_features_pos := v_features_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true    then v_features_pos := v_features_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047            then v_features_pos := v_features_pos + 1; end if;

  -- ── Static gotchas (per-hour adds no-shade adjunct) ──────────────
  if v_fence = false then v_static_gotchas := v_static_gotchas + 5; end if;
  if v_water = false then v_static_gotchas := v_static_gotchas + 3; end if;

  -- ── Drive time (constant) ────────────────────────────────────────
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography
    ) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    if v_drive_min <= 10 then v_drive := 3;
    elsif v_drive_min <= 25 then v_drive := 0;
    elsif v_drive_min <= 45 then v_drive := -3;
    elsif v_drive_min <= 75 then v_drive := -6;
    else                        v_drive := -10;
    end if;
  end if;

  -- ── Open/close hours (HH from policy times; defaults 6am-10pm) ───
  v_open_h  := coalesce(extract(hour from v_policy.hours_open_time::time)::int, 6);
  v_close_h := coalesce(extract(hour from v_policy.hours_close_time::time)::int, 22);
  if v_close_h <= v_open_h then v_close_h := 22; end if;

  -- ── Per-hour scoring (one row per hour in [open_h, close_h]) ─────
  with per_hour as (
    select
      h.local_hour, h.hour_label, h.hour_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      -- Weather positive (max 22)
      (case when h.feels_like between 60 and 75 then 10 else 0 end
       + case when h.wind_speed < 8 then 6 else 0 end
       + case when h.weather_code between 0 and 2 then 6 else 0 end) as weather_pos,
      -- Heat/UV negative (max 25)
      least(25,
        (case when h.asphalt_temp >= 125 then 15
              when h.asphalt_temp >= 115 then 15
              when h.asphalt_temp >= 105 then 8
              else 0 end)
        + (case when h.uv_index >= 11 then 10
                when h.uv_index >= 8 then 5
                else 0 end)
        + (case when h.feels_like >= 95 then 5 else 0 end)
      ) as heat_uv_neg,
      -- Weather harsh negative (max 15)
      least(15,
        (case when h.wind_speed >= 25 then 10
              when h.wind_speed >= 15 then 5 else 0 end)
        + (case when h.precip_chance >= 70 then 10 else 0 end)
        + (case when h.feels_like <= 20 then 10
                when h.feels_like <= 35 then 5 else 0 end)
      ) as harsh_neg,
      -- Per-hour no-shade gotcha addend
      (case when v_surface in ('asphalt','concrete')
            and coalesce(h.uv_index, 0) >= 6
            then 3 else 0 end) as noshade_neg,
      -- Window-now positive (per hour: bar shows when window IS THIS hour)
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      -- Status gate
      (h.hour_status = 'no_go'
       or coalesce(h.asphalt_temp, 0) >= 125
       or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired
    from public.dog_park_day_hourly_scores h
    where h.dog_park_fid = p_fid
      and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(10, v_static_gotchas + noshade_neg) as gotchas_neg,
      v_features_pos as features_pos
    from per_hour
  ),
  scored as (
    select *,
      greatest(0, least(100,
        case when gate_fired
             then least(50 + features_pos + weather_pos + window_pos
                          - heat_uv_neg - harsh_neg - gotchas_neg + v_drive, 30)
             else 50 + features_pos + weather_pos + window_pos
                    - heat_uv_neg - harsh_neg - gotchas_neg + v_drive
        end
      )) as score
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour',   local_hour,
    'hour_label',   hour_label,
    'status',       hour_status,
    'score',        score,
    'gate_fired',   gate_fired,
    'pos_weather',  weather_pos,
    'pos_features', features_pos,
    'pos_window',   window_pos,
    'neg_heat_uv',  heat_uv_neg,
    'neg_harsh',    harsh_neg,
    'neg_gotchas',  gotchas_neg,
    'drive',        v_drive,
    'feels_like',   feels_like,
    'wind_speed',   wind_speed,
    'uv_index',     uv_index,
    'asphalt_temp', asphalt_temp,
    'weather_code', weather_code,
    'precip_chance', precip_chance
  ) order by local_hour) into v_hours
    from scored;

  return jsonb_build_object(
    'fid',                p_fid,
    'view_date',          v_view_date,
    'open_hour',          v_open_h,
    'close_hour',         v_close_h,
    'features_positive',  v_features_pos,
    'static_gotchas',     v_static_gotchas,
    'drive_factor',       v_drive,
    'drive_minutes',      v_drive_min,
    'drive_miles',        case when v_drive_miles is not null
                               then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label',  v_today.best_window_label,
    'hours',              coalesce(v_hours, '[]'::jsonb)
  );
end;
$$;

comment on function public.compute_dog_park_hourly_v2 is
  'Per-hour v2 scoring across park open->close. Returns {hours: [{score, '
  'components}]} for vertical bar chart on dog-park.html. Same weights as '
  'compute_dog_park_score_v2; static contributions (features, drive, base '
  'gotchas) computed once, hourly contributions joined from '
  'dog_park_day_hourly_scores. Per Franz 2026-05-27 (vertical hour bars '
  'over horizontal stacked).';

commit;
notify pgrst, 'reload schema';
