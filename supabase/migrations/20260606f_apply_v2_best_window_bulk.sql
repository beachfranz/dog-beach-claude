-- 2026-06-06 — Bulk port of apply_v2_best_window_to_*_recommendations.
--
-- Today every beach/park × every date does its own RPC round-trip from the
-- daily-*-refresh edge functions:
--
--   for (const d of dates) {
--     await supabase.rpc("apply_v2_best_window_to_beach_recommendations",
--                        { p_fid: beach.arena_group_id, p_date: d });
--   }
--
-- With chunked-cron firing 50 beaches per call × 7 dates × 30 daily passes,
-- that's ~600k pooler hops per day just for best-window apply on the beach
-- side, plus another ~500k on dog-park.
--
-- Bulk functions:
--   apply_v2_best_window_to_beach_recommendations_bulk(p_state, p_fids, p_dates)
--   apply_v2_best_window_to_recommendations_bulk(p_state, p_fids, p_dates)  -- dog park
--
-- Same template as 20260606e (refresh_*_advisories):
--   _scope (fid × date), _gold-joined, _hours, _scored, _windows
--   → single set-based UPDATE on *_day_hourly_scores
--   → single set-based UPDATE on *_day_recommendations
--   → return counts as TABLE
--
-- Drop-in replacement: callers can flip from N RPCs to 1 RPC. The existing
-- per-fid/per-date functions stay (other callers use them for single-row
-- refresh from get-beach-now etc.).

-- ─────────────────────────────────────────────────────────────────────────
-- 1. BEACH bulk apply
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations_bulk(
  p_state text   DEFAULT NULL,
  p_fids  bigint[] DEFAULT NULL,
  p_dates date[]   DEFAULT NULL,
  p_days_ahead int DEFAULT 6      -- ignored when p_dates is supplied
) RETURNS TABLE(
  beaches_processed bigint,
  fid_date_pairs    bigint,
  hour_scores_written bigint,
  recs_written      bigint,
  windows_picked    bigint
) LANGUAGE plpgsql AS $function$
DECLARE
  v_b_wind_pos       jsonb;
  v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg    jsonb;
  v_b_sand_neg       jsonb;
  v_b_uv_neg         jsonb;
  v_b_tide_neg       jsonb;
  v_b_crowd_neg      jsonb;
  v_dates            date[];
BEGIN
  -- Self-clean temp tables (see note in refresh_beach_advisories).
  DROP TABLE IF EXISTS _scope, _gold, _per_hour, _scored, _windows;

  -- Resolve date range
  IF p_dates IS NOT NULL THEN
    v_dates := p_dates;
  ELSE
    SELECT array_agg(d::date)
      INTO v_dates
      FROM generate_series(CURRENT_DATE, CURRENT_DATE + p_days_ahead, '1 day'::interval) AS d;
  END IF;

  -- One-shot band lookups
  SELECT bands INTO v_b_wind_pos       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='wind_pos';
  SELECT bands INTO v_b_wind_harsh_neg FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='wind_harsh_neg';
  SELECT bands INTO v_b_asphalt_neg    FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='asphalt_neg';
  SELECT bands INTO v_b_sand_neg       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='sand_temp_neg';
  SELECT bands INTO v_b_uv_neg         FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='uv_neg';
  SELECT bands INTO v_b_tide_neg       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='tide_neg';
  SELECT bands INTO v_b_crowd_neg      FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='crowd_neg';

  -- 1. Scope = (fid × date) cross
  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT bg.fid, d AS local_date
  FROM public.beaches_gold bg
  CROSS JOIN unnest(v_dates) AS d
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

  -- 2. Gold + policy + amenities joined once per fid (not per fid×date).
  --    Computes the static positive-feature contributions.
  CREATE TEMP TABLE _gold ON COMMIT DROP AS
  SELECT
    bg.fid,
    bg.location_id,
    (CASE WHEN COALESCE(ba.has_restrooms,       false) THEN 4 ELSE 0 END) AS v_restroom_pos,
    (CASE WHEN COALESCE(ba.has_lifeguards,      false) THEN 4 ELSE 0 END) AS v_lifeguard_pos,
    (CASE WHEN COALESCE(ba.has_parking,         false) THEN 3 ELSE 0 END) AS v_parking_pos,
    (CASE WHEN COALESCE(ba.has_showers,         false) THEN 2 ELSE 0 END) AS v_shower_pos,
    (CASE WHEN COALESCE(ba.has_picnic_area,     false) THEN 2 ELSE 0 END) AS v_picnic_pos,
    (CASE WHEN COALESCE(ba.has_food,            false) THEN 1 ELSE 0 END) AS v_food_pos,
    (CASE WHEN COALESCE(ba.has_drinking_water,  false) THEN 1 ELSE 0 END) AS v_drinking_pos,
    (CASE WHEN COALESCE(ba.has_disabled_access, false) THEN 1 ELSE 0 END) AS v_disabled_pos,
    (CASE WHEN COALESCE(dp.off_leash_flag,      false) THEN 5 ELSE 0 END) AS v_dog_access_pos,
    -- Closures jsonb computed once per fid (not per row). _v2_is_hour_closed
    -- in _per_hour just consumes this.
    public._beach_closures_from_policy(
      COALESCE(dp.dogs_allowed, 'unknown'),
      COALESCE(dp.zone_rules, '[]'::jsonb)
    ) AS closures
  FROM _scope s
  JOIN public.beaches_gold     bg ON bg.fid = s.fid
  LEFT JOIN public.beach_dog_policy dp ON dp.arena_group_id = bg.fid
  LEFT JOIN public.beach_amenities  ba ON ba.arena_group_id = bg.fid
  GROUP BY bg.fid, bg.location_id,
           ba.has_restrooms, ba.has_lifeguards, ba.has_parking,
           ba.has_showers, ba.has_picnic_area, ba.has_food,
           ba.has_drinking_water, ba.has_disabled_access,
           dp.off_leash_flag, dp.zone_rules, dp.dogs_allowed;

  -- 3. Per-hour raw data — one row per (fid, date, hour) for hours 6-21.
  CREATE TEMP TABLE _per_hour ON COMMIT DROP AS
  SELECT
    s.fid, s.local_date, h.local_hour, h.hour_label,
    h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
    h.asphalt_temp, h.sand_temp, h.tide_height, h.precip_chance,
    h.busyness_score, h.is_in_best_window, h.is_daylight,
    g.v_restroom_pos + g.v_lifeguard_pos + g.v_parking_pos
      + g.v_shower_pos + g.v_picnic_pos + g.v_food_pos
      + g.v_drinking_pos + g.v_disabled_pos                            AS amenities_pos,
    g.v_dog_access_pos                                                  AS dog_access_pos,
    -- temp_pos: bell around 72°F with 20° range
    greatest(0, round(10 * (1 - least(abs(COALESCE(h.feels_like, 72) - 72), 20) / 20.0))::int) AS temp_pos,
    -- wind_pos via band lookup
    public._v2_lookup_band(COALESCE(h.wind_speed, -1), v_b_wind_pos) AS wind_pos,
    -- sky_pos hardcoded WMO mapping (mirrors compute_beach_hourly_v2)
    CASE h.weather_code
      WHEN 0 THEN 6 WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 2
      WHEN 45 THEN 1 WHEN 48 THEN 0 WHEN 51 THEN 2 WHEN 53 THEN 1 WHEN 55 THEN 0
      WHEN 56 THEN 0 WHEN 57 THEN 0 WHEN 61 THEN 1 WHEN 63 THEN 0 WHEN 65 THEN 0
      WHEN 66 THEN 0 WHEN 67 THEN 0 WHEN 71 THEN 0 WHEN 73 THEN 0 WHEN 75 THEN 0 WHEN 77 THEN 0
      WHEN 80 THEN 1 WHEN 81 THEN 0 WHEN 82 THEN 0 WHEN 85 THEN 0 WHEN 86 THEN 0
      WHEN 95 THEN 0 WHEN 96 THEN 0 WHEN 99 THEN 0 ELSE 2
    END AS sky_pos,
    -- heat_uv_neg capped at 30
    least(30,
      public._v2_lookup_band(COALESCE(h.sand_temp, -1),    v_b_sand_neg)
      + public._v2_lookup_band(COALESCE(h.asphalt_temp, -1), v_b_asphalt_neg)
      + public._v2_lookup_band(COALESCE(h.uv_index, -1),    v_b_uv_neg)
      + CASE WHEN h.feels_like > 90
             THEN least(4, greatest(0, round((h.feels_like - 90) * 0.4)::int))
             ELSE 0 END
    ) AS heat_uv_neg,
    -- harsh_neg capped at 15
    least(15,
      public._v2_lookup_band(COALESCE(h.wind_speed, -1), v_b_wind_harsh_neg)
      + CASE WHEN h.precip_chance > 30
             THEN least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int))
             ELSE 0 END
      + CASE WHEN h.feels_like < 50
             THEN least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int))
             ELSE 0 END
    ) AS harsh_neg,
    public._v2_lookup_band(COALESCE(h.tide_height, -1),    v_b_tide_neg)  AS tide_neg,
    public._v2_lookup_band(COALESCE(h.busyness_score, -1), v_b_crowd_neg) AS crowd_neg,
    CASE WHEN h.is_in_best_window THEN 5 ELSE 0 END AS window_pos,
    public._v2_is_hour_closed(g.closures, s.local_date, h.local_hour) AS is_closed
  FROM _scope s
  JOIN _gold g ON g.fid = s.fid
  JOIN public.beach_day_hourly_scores h
    ON h.location_id = g.location_id
   AND h.local_date  = s.local_date
   AND h.local_hour BETWEEN 6 AND 21;

  -- 4. Composed score per hour
  CREATE TEMP TABLE _scored ON COMMIT DROP AS
  SELECT
    fid, local_date, local_hour, hour_label, is_closed, is_daylight,
    CASE WHEN is_closed THEN 0
         ELSE greatest(0, least(100,
           50 + amenities_pos + dog_access_pos
              + least(22, temp_pos + wind_pos + sky_pos)
              + window_pos
              - heat_uv_neg - harsh_neg - tide_neg - crowd_neg
         ))
    END AS score
  FROM _per_hour;

  -- 5. Bulk UPDATE hour_score_v2. NULL for closed hours (so day-status
  --    aggregator naturally excludes them).
  WITH upd AS (
    UPDATE public.beach_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score END
      FROM _scored sc, _gold g
     WHERE h.arena_group_id = sc.fid
       AND h.location_id    = g.location_id
       AND h.local_date     = sc.local_date
       AND h.local_hour     = sc.local_hour
       AND g.fid            = sc.fid
    RETURNING 1
  )
  SELECT count(*) INTO hour_scores_written FROM upd;

  -- 6. Best window per (fid, date) — pick the contiguous-run-of-open-hours
  --    with the highest avg score, capped at MAX_LEN=5. For runs > 5 hours,
  --    slide a 5-hour window and take the slice with the highest sum.
  --    Fully set-based via window-function rolling sum (no correlated
  --    subqueries — earlier version timed out at 2 min on 3,700 CA pairs).
  CREATE TEMP TABLE _windows ON COMMIT DROP AS
  WITH eligible AS (
    SELECT fid, local_date, local_hour, score,
           local_hour - row_number() OVER (PARTITION BY fid, local_date ORDER BY local_hour) AS grp
    FROM _scored
    WHERE NOT is_closed
      AND score IS NOT NULL
  ),
  runs AS (
    SELECT fid, local_date, grp,
           min(local_hour) AS run_start,
           max(local_hour) AS run_end,
           count(*)::int   AS run_len,
           avg(score)::numeric AS avg_score,
           max(score)::int     AS max_score
    FROM eligible
    GROUP BY fid, local_date, grp
    HAVING count(*) >= 2
  ),
  best_run AS (
    SELECT DISTINCT ON (fid, local_date)
           fid, local_date, grp, run_start, run_end, run_len, max_score, avg_score
    FROM runs
    ORDER BY fid, local_date, avg_score DESC, run_len DESC, run_start ASC
  ),
  -- Rolling 5-hour sum for each starting position within the best run.
  -- Window-function ROWS BETWEEN CURRENT AND 4 FOLLOWING gives the
  -- next 5 hours within the partition; we partition by (fid, date, grp)
  -- so the sum is bounded by run boundaries.
  rolling AS (
    SELECT
      e.fid, e.local_date, e.local_hour AS slice_start,
      sum(e.score) OVER (
        PARTITION BY e.fid, e.local_date, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_sum,
      count(*) OVER (
        PARTITION BY e.fid, e.local_date, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_count
    FROM eligible e
    JOIN best_run b USING (fid, local_date, grp)
    WHERE b.run_len > 5
  ),
  -- Best 5-hour slice per (fid, date) within long runs
  best_slice AS (
    SELECT DISTINCT ON (fid, local_date)
      fid, local_date, slice_start
    FROM rolling
    WHERE window_count = 5
    ORDER BY fid, local_date, window_sum DESC NULLS LAST, slice_start
  )
  SELECT b.fid, b.local_date,
         CASE WHEN b.run_len <= 5 THEN b.run_start ELSE s.slice_start END     AS window_start_h,
         CASE WHEN b.run_len <= 5 THEN b.run_end   ELSE s.slice_start + 4 END AS window_end_h,
         b.max_score
  FROM best_run b
  LEFT JOIN best_slice s USING (fid, local_date);

  windows_picked := (SELECT count(*) FROM _windows);

  -- 7. Aggregate composite_score per (fid, date). Two paths:
  --    (a) best-window picked → composite = max_score of that window
  --    (b) no best window     → composite = avg of daylight hour_score_v2
  WITH composites AS (
    SELECT s.fid, s.local_date,
           COALESCE(
             w.max_score,
             (SELECT avg(score)::int
                FROM _scored sc
               WHERE sc.fid = s.fid AND sc.local_date = s.local_date
                 AND sc.is_daylight = true AND NOT sc.is_closed
                 AND sc.score IS NOT NULL)
           ) AS composite_score_v2,
           w.window_start_h, w.window_end_h
    FROM _scope s
    LEFT JOIN _windows w USING(fid, local_date)
  ),
  upd AS (
    UPDATE public.beach_day_recommendations r
       SET composite_score_v2  = c.composite_score_v2,
           best_window_label   = CASE
             WHEN c.window_start_h IS NOT NULL
             THEN public._format_hour_range(c.window_start_h, c.window_end_h + 1)
             ELSE r.best_window_label
           END,
           best_window_start_ts = CASE
             WHEN c.window_start_h IS NOT NULL
             THEN (c.local_date::timestamp + (c.window_start_h || ' hours')::interval) AT TIME ZONE 'UTC'
             ELSE r.best_window_start_ts
           END,
           best_window_end_ts  = CASE
             WHEN c.window_end_h IS NOT NULL
             THEN (c.local_date::timestamp + ((c.window_end_h + 1) || ' hours')::interval) AT TIME ZONE 'UTC'
             ELSE r.best_window_end_ts
           END,
           day_status_v2       = NULL   -- explicitly NULL per status_v2-drop sweep
      FROM composites c, _gold g
     WHERE r.location_id  = g.location_id
       AND g.fid          = c.fid
       AND r.local_date   = c.local_date
    RETURNING 1
  )
  SELECT count(*) INTO recs_written FROM upd;

  beaches_processed := (SELECT count(DISTINCT fid) FROM _scope);
  fid_date_pairs    := (SELECT count(*)            FROM _scope);

  RETURN NEXT;
END;
$function$;


-- ─────────────────────────────────────────────────────────────────────────
-- 2. DOG-PARK bulk apply
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations_bulk(
  p_state text   DEFAULT NULL,
  p_fids  bigint[] DEFAULT NULL,
  p_dates date[]   DEFAULT NULL,
  p_days_ahead int DEFAULT 6
) RETURNS TABLE(
  parks_processed   bigint,
  fid_date_pairs    bigint,
  hour_scores_written bigint,
  recs_written      bigint,
  windows_picked    bigint
) LANGUAGE plpgsql AS $function$
DECLARE
  v_b_wind_pos       jsonb;
  v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg    jsonb;
  v_b_uv_neg         jsonb;
  v_b_uv_need        jsonb;
  v_b_temp_need      jsonb;
  v_dates            date[];
BEGIN
  DROP TABLE IF EXISTS _scope, _gold, _per_hour, _scored, _windows;

  IF p_dates IS NOT NULL THEN
    v_dates := p_dates;
  ELSE
    SELECT array_agg(d::date)
      INTO v_dates
      FROM generate_series(CURRENT_DATE, CURRENT_DATE + p_days_ahead, '1 day'::interval) AS d;
  END IF;

  SELECT bands INTO v_b_wind_pos       FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='wind_pos';
  SELECT bands INTO v_b_wind_harsh_neg FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='wind_harsh_neg';
  SELECT bands INTO v_b_asphalt_neg    FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='asphalt_neg';
  SELECT bands INTO v_b_uv_neg         FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='uv_neg';
  SELECT bands INTO v_b_uv_need        FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='uv_need';
  SELECT bands INTO v_b_temp_need      FROM public.scoring_config_v2 WHERE entity_type='dog_park' AND signal_key='temp_need';

  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT g.fid, d AS local_date
  FROM public.dog_parks_gold g
  CROSS JOIN unnest(v_dates) AS d
  WHERE g.is_active AND g.is_scoreable = true
    AND (p_state IS NULL OR g.state = upper(p_state))
    AND (p_fids  IS NULL OR g.fid = ANY(p_fids));

  -- Gold + policy joined once per fid
  CREATE TEMP TABLE _gold ON COMMIT DROP AS
  SELECT
    g.fid,
    -- Features (fence + water + comfort)
    (CASE WHEN COALESCE(p.has_fence,          g.has_fence)          IS TRUE THEN 6 ELSE 0 END)
      + (CASE WHEN COALESCE(p.double_gate,    false) IS TRUE THEN 2 ELSE 0 END)         AS v_fence_pos,
    (CASE WHEN COALESCE(p.has_drinking_water, g.has_drinking_water) IS TRUE THEN 4 ELSE 0 END) AS v_water_pos,
    (CASE WHEN lower(COALESCE(p.surface_overlay, g.surface, '')) = 'grass' THEN 1 ELSE 0 END)
      + (CASE WHEN COALESCE(p.small_dog_area, false) IS TRUE OR COALESCE(p.large_dog_area, false) IS TRUE THEN 1 ELSE 0 END)
      + (CASE WHEN COALESCE(p.has_picnic_tables, false) IS TRUE THEN 2 ELSE 0 END)      AS v_comfort_pos,
    (CASE WHEN COALESCE(p.has_agility, false) IS TRUE THEN 3 ELSE 0 END)                AS v_agility_pos,
    COALESCE(p.has_shade,      false) AS has_shade,
    COALESCE(p.has_water_play, false) AS has_water_play,
    COALESCE(p.closures, '[]'::jsonb) AS closures,
    COALESCE(extract(hour FROM p.hours_open_time::time)::int, 6)  AS open_h,
    COALESCE(extract(hour FROM p.hours_close_time::time)::int, 22) AS close_h
  FROM _scope s
  JOIN public.dog_parks_gold g ON g.fid = s.fid
  LEFT JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
  GROUP BY g.fid, g.has_fence, g.has_drinking_water, g.surface,
           p.has_fence, p.has_drinking_water, p.surface_overlay,
           p.double_gate, p.small_dog_area, p.large_dog_area, p.has_picnic_tables,
           p.has_agility, p.has_shade, p.has_water_play,
           p.closures, p.hours_open_time, p.hours_close_time;

  CREATE TEMP TABLE _per_hour ON COMMIT DROP AS
  SELECT
    s.fid, s.local_date, h.local_hour, h.hour_label,
    h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
    h.asphalt_temp, h.precip_chance, h.is_in_best_window, h.is_daylight,
    g.v_fence_pos + g.v_water_pos + g.v_comfort_pos AS features_pos,
    g.v_agility_pos, g.has_shade, g.has_water_play,
    least(6, greatest(
      public._v2_lookup_band(COALESCE(h.uv_index, -1),  v_b_uv_need),
      public._v2_lookup_band(COALESCE(h.feels_like, -1), v_b_temp_need)
    )) AS shade_need,
    greatest(0, round(10 * (1 - least(abs(COALESCE(h.feels_like, 72) - 72), 20) / 20.0))::int) AS temp_pos,
    public._v2_lookup_band(COALESCE(h.wind_speed, -1), v_b_wind_pos) AS wind_pos,
    CASE h.weather_code
      WHEN 0 THEN 6 WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 2
      WHEN 45 THEN 1 WHEN 48 THEN 0 WHEN 51 THEN 2 WHEN 53 THEN 1 WHEN 55 THEN 0
      WHEN 56 THEN 0 WHEN 57 THEN 0 WHEN 61 THEN 1 WHEN 63 THEN 0 WHEN 65 THEN 0
      WHEN 66 THEN 0 WHEN 67 THEN 0 WHEN 71 THEN 0 WHEN 73 THEN 0 WHEN 75 THEN 0 WHEN 77 THEN 0
      WHEN 80 THEN 1 WHEN 81 THEN 0 WHEN 82 THEN 0 WHEN 85 THEN 0 WHEN 86 THEN 0
      WHEN 95 THEN 0 WHEN 96 THEN 0 WHEN 99 THEN 0 ELSE 2
    END AS sky_pos,
    least(25,
      public._v2_lookup_band(COALESCE(h.asphalt_temp, -1), v_b_asphalt_neg)
      + public._v2_lookup_band(COALESCE(h.uv_index, -1),    v_b_uv_neg)
      + CASE WHEN h.feels_like > 90
             THEN least(5, greatest(0, round((h.feels_like - 90) * 0.5)::int))
             ELSE 0 END
    ) AS heat_uv_neg,
    least(15,
      public._v2_lookup_band(COALESCE(h.wind_speed, -1), v_b_wind_harsh_neg)
      + CASE WHEN h.precip_chance > 30
             THEN least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int))
             ELSE 0 END
      + CASE WHEN h.feels_like < 50
             THEN least(10, greatest(0, round((50 - h.feels_like) * 0.4)::int))
             ELSE 0 END
    ) AS harsh_neg,
    CASE WHEN h.is_in_best_window THEN 5 ELSE 0 END AS window_pos,
    public._v2_is_hour_closed(g.closures, s.local_date, h.local_hour) AS is_closed,
    g.open_h, g.close_h
  FROM _scope s
  JOIN _gold g ON g.fid = s.fid
  JOIN public.dog_park_day_hourly_scores h
    ON h.dog_park_fid = s.fid
   AND h.local_date   = s.local_date
   AND h.local_hour BETWEEN g.open_h AND g.close_h;

  CREATE TEMP TABLE _scored ON COMMIT DROP AS
  SELECT
    fid, local_date, local_hour, hour_label, is_closed, is_daylight,
    CASE WHEN is_closed THEN 0
         ELSE greatest(0, least(100,
           50 + features_pos
              + (CASE WHEN has_shade      THEN shade_need ELSE 0 END)         -- shade_pos
              + least(22, temp_pos + wind_pos + sky_pos)
              + window_pos
              + v_agility_pos
              + (CASE WHEN has_water_play THEN shade_need ELSE 0 END)         -- water_play_pos
              - heat_uv_neg - harsh_neg
              - (CASE WHEN NOT has_shade  THEN shade_need ELSE 0 END)         -- gotchas_neg
         ))
    END AS score
  FROM _per_hour;

  WITH upd AS (
    UPDATE public.dog_park_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score END
      FROM _scored sc
     WHERE h.dog_park_fid = sc.fid
       AND h.local_date   = sc.local_date
       AND h.local_hour   = sc.local_hour
    RETURNING 1
  )
  SELECT count(*) INTO hour_scores_written FROM upd;

  -- Window picking — set-based rolling-sum (no correlated subqueries)
  CREATE TEMP TABLE _windows ON COMMIT DROP AS
  WITH eligible AS (
    SELECT fid, local_date, local_hour, score,
           local_hour - row_number() OVER (PARTITION BY fid, local_date ORDER BY local_hour) AS grp
    FROM _scored
    WHERE NOT is_closed AND score IS NOT NULL
  ),
  runs AS (
    SELECT fid, local_date, grp,
           min(local_hour) AS run_start, max(local_hour) AS run_end,
           count(*)::int AS run_len, avg(score)::numeric AS avg_score,
           max(score)::int AS max_score
    FROM eligible
    GROUP BY fid, local_date, grp
    HAVING count(*) >= 2
  ),
  best_run AS (
    SELECT DISTINCT ON (fid, local_date) fid, local_date, grp,
           run_start, run_end, run_len, max_score, avg_score
    FROM runs
    ORDER BY fid, local_date, avg_score DESC, run_len DESC, run_start ASC
  ),
  rolling AS (
    SELECT
      e.fid, e.local_date, e.local_hour AS slice_start,
      sum(e.score) OVER (
        PARTITION BY e.fid, e.local_date, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_sum,
      count(*) OVER (
        PARTITION BY e.fid, e.local_date, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_count
    FROM eligible e
    JOIN best_run b USING (fid, local_date, grp)
    WHERE b.run_len > 5
  ),
  best_slice AS (
    SELECT DISTINCT ON (fid, local_date) fid, local_date, slice_start
    FROM rolling
    WHERE window_count = 5
    ORDER BY fid, local_date, window_sum DESC NULLS LAST, slice_start
  )
  SELECT b.fid, b.local_date,
         CASE WHEN b.run_len <= 5 THEN b.run_start ELSE s.slice_start END     AS window_start_h,
         CASE WHEN b.run_len <= 5 THEN b.run_end   ELSE s.slice_start + 4 END AS window_end_h,
         b.max_score
  FROM best_run b
  LEFT JOIN best_slice s USING (fid, local_date);

  windows_picked := (SELECT count(*) FROM _windows);

  WITH composites AS (
    SELECT s.fid, s.local_date,
           COALESCE(
             w.max_score,
             (SELECT avg(score)::int FROM _scored sc
              WHERE sc.fid = s.fid AND sc.local_date = s.local_date
                AND sc.is_daylight = true AND NOT sc.is_closed
                AND sc.score IS NOT NULL)
           ) AS composite_score_v2,
           w.window_start_h, w.window_end_h
    FROM _scope s
    LEFT JOIN _windows w USING(fid, local_date)
  ),
  upd AS (
    UPDATE public.dog_park_day_recommendations r
       SET composite_score_v2 = c.composite_score_v2,
           best_window_label  = CASE
             WHEN c.window_start_h IS NOT NULL
             THEN public._format_hour_range(c.window_start_h, c.window_end_h + 1)
             ELSE r.best_window_label
           END,
           best_window_start_ts = CASE
             WHEN c.window_start_h IS NOT NULL
             THEN (c.local_date::timestamp + (c.window_start_h || ' hours')::interval) AT TIME ZONE 'UTC'
             ELSE r.best_window_start_ts
           END,
           best_window_end_ts = CASE
             WHEN c.window_end_h IS NOT NULL
             THEN (c.local_date::timestamp + ((c.window_end_h + 1) || ' hours')::interval) AT TIME ZONE 'UTC'
             ELSE r.best_window_end_ts
           END,
           day_status_v2 = NULL
      FROM composites c
     WHERE r.dog_park_fid = c.fid
       AND r.local_date   = c.local_date
    RETURNING 1
  )
  SELECT count(*) INTO recs_written FROM upd;

  parks_processed := (SELECT count(DISTINCT fid) FROM _scope);
  fid_date_pairs  := (SELECT count(*)            FROM _scope);

  RETURN NEXT;
END;
$function$;

-- Usage:
--   SELECT * FROM public.apply_v2_best_window_to_beach_recommendations_bulk();
--   SELECT * FROM public.apply_v2_best_window_to_beach_recommendations_bulk('CA');
--   SELECT * FROM public.apply_v2_best_window_to_beach_recommendations_bulk(NULL, ARRAY[8347, 8348]);
--   SELECT * FROM public.apply_v2_best_window_to_beach_recommendations_bulk(NULL, NULL, ARRAY[CURRENT_DATE]);
--   SELECT * FROM public.apply_v2_best_window_to_recommendations_bulk();  -- dog park
