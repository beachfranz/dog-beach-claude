-- 20260528_beach_v2_compute.sql
--
-- Beach v2 scoring functions:
--   compute_beach_hourly_v2(fid, lat, lng, date) → jsonb { hours, best_window_v2_*, ... }
--   compute_beach_score_v2(fid, lat, lng, date)  → jsonb (single-point convenience)
--
-- Bucket shape:
--   POSITIVES (max 50):
--     weather    22  — temp (10) + wind (6) + sky (6)
--     amenities  18  — restrooms 4 + lifeguards 4 + parking 3 + showers 2 + picnic 2
--                      + food 1 + drinking 1 + disabled 1
--     dog_access 5   — off-leash flag bonus
--     window     5
--   NEGATIVES (max 60):
--     heat_uv          30  — sand_temp 10 + asphalt 8 + UV 8 + feels-hot 4
--     weather_harsh    15  — wind 10 + precip 5 + cold 5  (slightly milder than dog-park)
--     tide             10  — high tide eats the sand
--     crowd            5
--   DRIVE ±10  — same as dog-park
--
-- Closures: honored via beach_dog_policy.dogs_prohibited_start/end (time-of-day
-- prohibition) AND dogs_allowed='no' (full block). zone_rules.time_windows
-- parsing is a follow-up.

begin;

-- ── Helper: derive a "closures-shaped" jsonb from beach_dog_policy ─
-- Wraps the existing _v2_is_hour_closed helper. Builds the closures
-- array from the simpler beach fields. zone_rules deep-parsing deferred.

create or replace function public._beach_closures_from_policy(
  p_dogs_allowed     text,
  p_prohibited_start text,
  p_prohibited_end   text
) returns jsonb language plpgsql immutable as $$
declare
  v_closures jsonb := '[]'::jsonb;
  v_day text;
begin
  if p_dogs_allowed = 'no' then
    return '[{"kind":"weekly","weekday":"mon","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"tue","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"wed","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"thu","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"fri","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"sat","all_day":true,"reason":"dogs_prohibited"},
             {"kind":"weekly","weekday":"sun","all_day":true,"reason":"dogs_prohibited"}]'::jsonb;
  end if;
  if p_prohibited_start is not null and p_prohibited_end is not null then
    foreach v_day in array array['mon','tue','wed','thu','fri','sat','sun'] loop
      v_closures := v_closures || jsonb_build_array(
        jsonb_build_object('kind','weekly_time','weekday', v_day,
          'start', to_char(p_prohibited_start::time, 'HH24:MI'),
          'end',   to_char(p_prohibited_end::time,   'HH24:MI'),
          'reason','dogs_prohibited_daily')
      );
    end loop;
  end if;
  return v_closures;
end;
$$;


-- ── compute_beach_hourly_v2 ─────────────────────────────────────────

create or replace function public.compute_beach_hourly_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
declare
  v_gold record;
  v_policy record;
  v_today record;
  v_loc_meta record;             -- locations_stage amenities
  v_view_date date := coalesce(p_date, current_date);
  -- Amenity sub-bucket totals
  v_restroom_pos integer := 0; v_lifeguard_pos integer := 0;
  v_parking_pos integer := 0; v_picnic_pos integer := 0; v_shower_pos integer := 0;
  v_food_pos integer := 0; v_drinking_pos integer := 0; v_disabled_pos integer := 0;
  v_amenities_pos integer := 0;
  -- Dog access
  v_dog_access_pos integer := 0;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_open_h integer; v_close_h integer; v_hours jsonb;
  v_bw jsonb;
  v_closures jsonb := '[]'::jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg jsonb; v_b_sand_neg jsonb; v_b_uv_neg jsonb;
  v_b_tide_neg jsonb; v_b_crowd_neg jsonb; v_b_drive jsonb;
begin
  select * into v_gold from public.beaches_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','beach not found','fid',p_fid); end if;
  select * into v_policy from public.beach_dog_policy where arena_group_id = p_fid limit 1;
  select * into v_today from public.beach_day_recommendations
    where location_id = v_gold.location_id and local_date = v_view_date limit 1;
  select * into v_loc_meta from public.locations_stage where fid = p_fid limit 1;

  -- Closures (dog-prohibited windows from beach_dog_policy)
  if v_policy.arena_group_id is not null then
    v_closures := public._beach_closures_from_policy(
      v_policy.dogs_allowed,
      v_policy.dogs_prohibited_start::text,
      v_policy.dogs_prohibited_end::text);
  end if;

  -- Load bands (entity_type='beach' rows)
  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'asphalt_neg';
  select bands into v_b_sand_neg       from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'sand_temp_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'uv_neg';
  select bands into v_b_tide_neg       from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'tide_neg';
  select bands into v_b_crowd_neg      from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'crowd_neg';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type = 'beach' and signal_key = 'drive_factor';

  -- ── Amenities (static across hours) ────────────────────────────────
  if v_loc_meta.has_restrooms     = true then v_restroom_pos := 4; end if;
  if v_loc_meta.has_lifeguards    = true then v_lifeguard_pos := 4; end if;
  if v_loc_meta.has_parking       = true then v_parking_pos := 3; end if;
  if v_loc_meta.has_showers       = true then v_shower_pos := 2; end if;
  if v_loc_meta.has_picnic_area   = true then v_picnic_pos := 2; end if;
  if v_loc_meta.has_food          = true then v_food_pos := 1; end if;
  if v_loc_meta.has_drinking_water= true then v_drinking_pos := 1; end if;
  if v_loc_meta.has_disabled_access = true then v_disabled_pos := 1; end if;
  v_amenities_pos := v_restroom_pos + v_lifeguard_pos + v_parking_pos
                   + v_shower_pos + v_picnic_pos + v_food_pos
                   + v_drinking_pos + v_disabled_pos;

  -- ── Dog access (off-leash flag bonus) ─────────────────────────────
  if v_policy.off_leash_flag = true then v_dog_access_pos := 5; end if;

  -- ── Drive ─────────────────────────────────────────────────────────
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    v_drive := public._v2_lookup_band(v_drive_min::numeric, v_b_drive);
  end if;

  -- Beach hours: sunrise→sunset is "daylight" but we score the full 24h
  -- to cover early-morning + evening visits. is_daylight handles dawn/dusk.
  v_open_h := 6;
  v_close_h := 21;

  with per_hour as (
    select h.local_hour, h.hour_label, h.hour_status as v1_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.sand_temp, h.tide_height, h.precip_chance,
      h.busyness_score, h.is_in_best_window, h.is_daylight,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
      -- Weather positives
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_pos) as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      -- Heat/UV negatives (max 30)
      least(30,
        public._v2_lookup_band(coalesce(h.sand_temp, -1), v_b_sand_neg)
        + public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90 then least(4, greatest(0, round((h.feels_like - 90) * 0.4)::int)) else 0 end
      ) as heat_uv_neg,
      -- Weather harsh (max 15) — slightly milder than dog-park (wind 10, precip 5, cold 5)
      least(15,
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int)) else 0 end
      ) as harsh_neg,
      -- NEW: tide negative
      public._v2_lookup_band(coalesce(h.tide_height, -1), v_b_tide_neg) as tide_neg,
      -- NEW: crowd negative
      public._v2_lookup_band(coalesce(h.busyness_score, -1), v_b_crowd_neg) as crowd_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      -- Gate triggers (beach-specific)
      (coalesce(h.sand_temp, 0) >= 145
       or coalesce(h.asphalt_temp, 0) >= 125
       or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired,
      case when coalesce(h.sand_temp, 0) >= 145 or coalesce(h.asphalt_temp, 0) >= 125
                or coalesce(h.uv_index, 0) >= 11 or coalesce(h.feels_like, 99) <= 20 then 30
           when coalesce(h.sand_temp, 0) >= 125 or coalesce(h.asphalt_temp, 0) >= 115
                or coalesce(h.uv_index, 0) >= 9 or coalesce(h.feels_like, 70) >= 95
                or coalesce(h.feels_like, 70) <= 32 or coalesce(h.precip_chance, 0) >= 80
                or coalesce(h.tide_height, 0) >= 7 then 60
           else 100 end as status_cap
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
           else greatest(0, least(status_cap,
             50 + amenities_static + dog_access_static + weather_pos + window_pos
                - heat_uv_neg - harsh_neg - tide_neg - crowd_neg + v_drive))
      end as score,
      case when is_closed then 'closed' else v1_status end as status_v2
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'status', status_v2,
    'score', score, 'gate_fired', gate_fired, 'status_cap', status_cap,
    'is_closed', is_closed,
    'pos_weather',   case when is_closed then 0 else weather_pos end,
    'pos_features',  case when is_closed then 0 else amenities_static end,
    'pos_enrichment', case when is_closed then 0 else dog_access_static end,
    'pos_window',    case when is_closed then 0 else window_pos end,
    'neg_heat_uv',   case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',     case when is_closed then 0 else harsh_neg end,
    'neg_gotchas',   case when is_closed then 0 else tide_neg + crowd_neg end,
    'drive',         case when is_closed then 0 else v_drive end,
    -- Sub-component drill (matches dog-park keys where possible)
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

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 70, 2, 5);

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
$$;


-- ── compute_beach_score_v2 (single-point convenience) ───────────────

create or replace function public.compute_beach_score_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
declare
  v_hourly jsonb;
  v_now_hour jsonb;
  v_nowH int := extract(hour from now() at time zone 'America/Los_Angeles')::int;
begin
  v_hourly := public.compute_beach_hourly_v2(p_fid, p_user_lat, p_user_lng, p_date);
  if v_hourly ? 'error' then return v_hourly; end if;
  -- Find current-hour entry (or best window if NOW out of range)
  select h into v_now_hour
    from jsonb_array_elements(v_hourly->'hours') h
    where (h->>'local_hour')::int = v_nowH
    limit 1;
  if v_now_hour is null then
    -- pick first hour as fallback
    v_now_hour := (v_hourly->'hours')->0;
  end if;
  return jsonb_build_object(
    'fid', p_fid,
    'view_date', v_hourly->>'view_date',
    'now_hour', v_now_hour,
    'best_window_v2_label', v_hourly->>'best_window_v2_label',
    'drive_minutes', v_hourly->>'drive_minutes',
    'closures', v_hourly->'closures'
  );
end;
$$;


-- ── apply_v2_best_window_to_beach_recommendations ───────────────────

create or replace function public.apply_v2_best_window_to_beach_recommendations(
  p_fid bigint, p_date date default current_date
) returns void language plpgsql as $$
declare
  v_result jsonb;
  v_label text; v_start int; v_end int;
  v_location_id text;
begin
  select location_id into v_location_id from public.beaches_gold where fid = p_fid;
  if v_location_id is null then return; end if;

  v_result := public.compute_beach_hourly_v2(p_fid, null, null, p_date);
  v_label := v_result->>'best_window_v2_label';
  v_start := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end   := nullif(v_result->>'best_window_v2_end',   '')::int;

  if v_label is null then return; end if;

  update public.beach_day_recommendations
     set best_window_label    = v_label,
         best_window_start_ts = (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC',
         best_window_end_ts   = (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
   where location_id = v_location_id and local_date = p_date;
end;
$$;

commit;
notify pgrst, 'reload schema';
