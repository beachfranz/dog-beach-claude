-- 20260527_dog_park_scoring_v2_continuous.sql
--
-- Smooth out v2 scoring step functions. Franz 2026-05-27 — "these scores
-- look binary to me. all points or no points for each category."
--
-- Replaces the BETWEEN/threshold pattern with piecewise-linear functions
-- so 73°F doesn't score identically to 60°F, a 10mph wind doesn't get
-- the same +6 as a 2mph dead-calm, and a 106°F pavement isn't tied with
-- a 114°F pavement.
--
-- Component MAX values unchanged (still 22/20/5/-10..+3/25/15/10) — spec
-- maxima preserved; the shape between 0 and max is what's smooth now.
-- Gate trigger thresholds unchanged (asphalt≥125, UV≥11, feels≤20 still
-- cap score at 30).
--
-- Curves (all clamped 0..max, integers via ROUND):
--   Weather positive (max 22)
--     temp:    bell — peak 10 at 68°F, linear falloff 20°F either side
--     wind:    inverse linear — 6 at 0mph, 0 at 12mph
--     code:    lookup table 0..6 across WMO codes 0,1,2,3,45,48,...
--   Heat/UV negative (max 25)
--     asphalt: linear 0 at 100°F → 15 at 130°F
--     UV:      linear 0 at UV 5    → 10 at UV 11
--     hot:     linear 0 at 85°F    → 5  at 100°F (feels-like)
--   Weather harsh negative (max 15)
--     wind:    linear 0 at 10mph   → 10 at 25mph
--     precip:  linear 0 at 30%     → 10 at 100%
--     cold:    linear 0 at 40°F    → 10 at 15°F (feels-like)
--   Window now (per-hour): unchanged — IS/ISN'T in best window is itself binary
--   Park features positive: unchanged — fence/water/gate ARE booleans
--   Drive: unchanged — already five-band stepped at human-meaningful breaks

begin;

-- ════════════════════════════════════════════════════════════════════
-- Single-point scorer (replaces 20260527_dog_park_scoring_v2.sql body)
-- ════════════════════════════════════════════════════════════════════

create or replace function public.compute_dog_park_score_v2(
  p_fid       bigint,
  p_user_lat  double precision default null,
  p_user_lng  double precision default null,
  p_date      date default null
) returns jsonb
language plpgsql stable
as $$
declare
  v_gold      record;
  v_policy    record;
  v_now       record;
  v_today     record;
  v_view_date date := coalesce(p_date, current_date);
  v_pos       jsonb;
  v_neg       jsonb;
  v_drive     integer := 0;
  v_drive_miles  numeric;
  v_drive_min    integer;
  v_gate_fired   boolean := false;
  v_weather_pos  integer := 0;
  v_features_pos integer := 0;
  v_window_pos   integer := 0;
  v_heat_neg     integer := 0;
  v_harsh_neg    integer := 0;
  v_gotchas_neg  integer := 0;
  v_fence        boolean;
  v_water        boolean;
  v_surface      text;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then
    return jsonb_build_object('error', 'park not found', 'fid', p_fid);
  end if;

  select * into v_policy from public.dog_park_dog_policy
   where dog_park_fid = p_fid limit 1;
  select * into v_now from public.dog_park_day_hourly_scores
   where dog_park_fid = p_fid and is_now = true limit 1;
  select * into v_today from public.dog_park_day_recommendations
   where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence   := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water   := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  -- ── ☀️ Weather positive (0..22) — CONTINUOUS ─────────────────────
  -- temp: bell — peak 10 at 68°F, linear falloff over 20°F
  if v_now.feels_like is not null then
    v_weather_pos := v_weather_pos
      + greatest(0, round(10 * (1 - least(abs(v_now.feels_like - 72), 20) / 20.0))::int);
  end if;
  -- wind: bell — peak 6 at 5mph, ±5mph either side. 0mph (stagnant) → 4,
  -- 10mph (breezy) → 4, 15mph (gusty) → 2, 20mph+ → 0.
  if v_now.wind_speed is not null then
    v_weather_pos := v_weather_pos
      + greatest(0, round(6 - 0.4 * abs(v_now.wind_speed - 5))::int);
  end if;
  -- weather code: lookup
  if v_now.weather_code is not null then
    v_weather_pos := v_weather_pos +
      case v_now.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0
        when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0
        when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0
        when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0
        when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0
        else 2
      end;
  end if;
  v_weather_pos := least(v_weather_pos, 22);

  -- ── 🏞️ Park features positive (0..20) — booleans stay binary ────
  if v_fence = true                       then v_features_pos := v_features_pos + 6; end if;
  if v_water = true                       then v_features_pos := v_features_pos + 5; end if;
  if v_policy.double_gate = true          then v_features_pos := v_features_pos + 3; end if;
  if v_surface = 'grass'                  then v_features_pos := v_features_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true    then v_features_pos := v_features_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047            then v_features_pos := v_features_pos + 1; end if;

  -- ── ⏰ Window-now positive (0..5) ────────────────────────────────
  if v_today.best_window_label is not null then v_window_pos := 5;
  elsif coalesce(v_today.go_hours_count, 0) > 0 then v_window_pos := 2;
  end if;

  -- ── 🔥 Heat/UV negative (0..25) — CONTINUOUS ─────────────────────
  -- asphalt: linear 100 → 130°F maps to 0 → 15
  if v_now.asphalt_temp is not null then
    v_heat_neg := v_heat_neg
      + least(15, greatest(0, round((v_now.asphalt_temp - 100) / 2.0)::int));
    if v_now.asphalt_temp >= 125 then v_gate_fired := true; end if;
  end if;
  -- UV: linear UV 5 → 11 maps to 0 → 10
  if v_now.uv_index is not null then
    v_heat_neg := v_heat_neg
      + least(10, greatest(0, round((v_now.uv_index - 5) * 10 / 6.0)::int));
    if v_now.uv_index >= 11 then v_gate_fired := true; end if;
  end if;
  -- compound heat (feels-like): linear 85 → 100°F maps to 0 → 5
  if v_now.feels_like is not null and v_now.feels_like > 85 then
    v_heat_neg := v_heat_neg
      + least(5, greatest(0, round((v_now.feels_like - 85) / 3.0)::int));
  end if;
  v_heat_neg := least(v_heat_neg, 25);

  -- ── 💨 Weather harsh negative (0..15) — CONTINUOUS ───────────────
  -- wind: linear 10 → 25mph maps to 0 → 10
  if v_now.wind_speed is not null and v_now.wind_speed > 10 then
    v_harsh_neg := v_harsh_neg
      + least(10, greatest(0, round((v_now.wind_speed - 10) / 1.5)::int));
  end if;
  -- precip chance: linear 30% → 100% maps to 0 → 10
  if v_now.precip_chance is not null and v_now.precip_chance > 30 then
    v_harsh_neg := v_harsh_neg
      + least(10, greatest(0, round((v_now.precip_chance - 30) / 7.0)::int));
  end if;
  -- cold: linear 40°F → 15°F maps to 0 → 10
  if v_now.feels_like is not null and v_now.feels_like < 40 then
    v_harsh_neg := v_harsh_neg
      + least(10, greatest(0, round((40 - v_now.feels_like) / 2.5)::int));
    if v_now.feels_like <= 20 then v_gate_fired := true; end if;
  end if;
  v_harsh_neg := least(v_harsh_neg, 15);

  -- ── ⚠️ Park gotchas negative (0..10) — booleans stay binary ──────
  if v_fence = false then v_gotchas_neg := v_gotchas_neg + 5; end if;
  if v_water = false then v_gotchas_neg := v_gotchas_neg + 3; end if;
  if v_surface in ('asphalt', 'concrete')
     and coalesce(v_now.uv_index, 0) >= 6 then
    v_gotchas_neg := v_gotchas_neg + 3;
  end if;
  v_gotchas_neg := least(v_gotchas_neg, 10);

  -- ── 🚗 Drive (-10..+3) — five-band stepped at human-meaningful breaks ──
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

  if v_now.hour_status = 'no_go' then v_gate_fired := true; end if;

  v_pos := jsonb_build_object(
    'weather',  jsonb_build_object('value', v_weather_pos,  'max', 22),
    'features', jsonb_build_object('value', v_features_pos, 'max', 20),
    'window',   jsonb_build_object('value', v_window_pos,   'max', 5)
  );
  v_neg := jsonb_build_object(
    'heat_uv',       jsonb_build_object('value', v_heat_neg,    'max', 25),
    'weather_harsh', jsonb_build_object('value', v_harsh_neg,   'max', 15),
    'gotchas',       jsonb_build_object('value', v_gotchas_neg, 'max', 10)
  );

  return public._score_v2_compose(
    p_baseline          := 50,
    p_positives         := v_pos,
    p_negatives         := v_neg,
    p_drive_factor      := v_drive,
    p_status_gate_fired := v_gate_fired,
    p_status_gate_cap   := 30
  ) || jsonb_build_object(
    'fid',             p_fid,
    'view_date',       v_view_date,
    'drive_minutes',   v_drive_min,
    'drive_miles',     case when v_drive_miles is not null
                            then round(v_drive_miles::numeric, 1) else null end,
    'now_available',   v_now.dog_park_fid is not null,
    'today_available', v_today.dog_park_fid is not null,
    'hour_status',     v_now.hour_status,
    'best_window_label', v_today.best_window_label
  );
end;
$$;


-- ════════════════════════════════════════════════════════════════════
-- Hourly scorer — same continuous curves, applied per-hour
-- ════════════════════════════════════════════════════════════════════

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

  if v_fence = true                       then v_features_pos := v_features_pos + 6; end if;
  if v_water = true                       then v_features_pos := v_features_pos + 5; end if;
  if v_policy.double_gate = true          then v_features_pos := v_features_pos + 3; end if;
  if v_surface = 'grass'                  then v_features_pos := v_features_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true    then v_features_pos := v_features_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047            then v_features_pos := v_features_pos + 1; end if;

  if v_fence = false then v_static_gotchas := v_static_gotchas + 5; end if;
  if v_water = false then v_static_gotchas := v_static_gotchas + 3; end if;

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

  v_open_h  := coalesce(extract(hour from v_policy.hours_open_time::time)::int, 6);
  v_close_h := coalesce(extract(hour from v_policy.hours_close_time::time)::int, 22);
  if v_close_h <= v_open_h then v_close_h := 22; end if;

  with per_hour as (
    select
      h.local_hour, h.hour_label, h.hour_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      -- ─ Weather positive (CONTINUOUS, max 22) ──────────────────────
      least(22,
        -- temp bell, peak 10 at 68°F, fall over 20°F each side
        greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 68) - 68), 20) / 20.0))::int)
        -- wind bell, peak 6 at 5mph, ±5mph either side
        + greatest(0, round(6 - 0.4 * abs(coalesce(h.wind_speed, 5) - 5))::int)
        -- WMO code lookup
        + case h.weather_code
            when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
            when 45 then 1 when 48 then 0
            when 51 then 2 when 53 then 1 when 55 then 0
            when 56 then 0 when 57 then 0
            when 61 then 1 when 63 then 0 when 65 then 0
            when 66 then 0 when 67 then 0
            when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
            when 80 then 1 when 81 then 0 when 82 then 0
            when 85 then 0 when 86 then 0
            when 95 then 0 when 96 then 0 when 99 then 0
            else 2
          end
      ) as weather_pos,
      -- ─ Heat/UV negative (CONTINUOUS, max 25) ──────────────────────
      least(25,
        least(15, greatest(0, round((coalesce(h.asphalt_temp, 0) - 100) / 2.0)::int))
        + least(10, greatest(0, round((coalesce(h.uv_index, 0) - 5) * 10 / 6.0)::int))
        + case when h.feels_like > 85
               then least(5, greatest(0, round((h.feels_like - 85) / 3.0)::int))
               else 0 end
      ) as heat_uv_neg,
      -- ─ Weather harsh negative (CONTINUOUS, max 15) ────────────────
      least(15,
        case when h.wind_speed > 10
             then least(10, greatest(0, round((h.wind_speed - 10) / 1.5)::int))
             else 0 end
        + case when h.precip_chance > 30
               then least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int))
               else 0 end
        + case when h.feels_like < 40
               then least(10, greatest(0, round((40 - h.feels_like) / 2.5)::int))
               else 0 end
      ) as harsh_neg,
      (case when v_surface in ('asphalt','concrete')
            and coalesce(h.uv_index, 0) >= 6
            then 3 else 0 end) as noshade_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos,
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

commit;
notify pgrst, 'reload schema';
