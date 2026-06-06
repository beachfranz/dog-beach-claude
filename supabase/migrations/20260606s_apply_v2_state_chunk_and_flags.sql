-- 20260606s_apply_v2_state_chunk_and_flags.sql
--
-- Combined fix for the afternoon-data-stale regression introduced by
-- 88eee2e (cleanup: remove per-fid apply_v2 from daily-*-refresh).
--
-- ROOT CAUSE CHAIN (see investigation around the find.html "No good window
-- remaining" symptom):
--   1. 88eee2e removed per-fid apply_v2 from daily-beach-refresh +
--      daily-dog-park-refresh edge fns, assuming orch_jobs slots
--      apply_v2_best_window_beach + _dog_park (shipped c5d4826) would
--      take over.
--   2. The slot's worker, apply_v2_best_window_to_recommendations_bulk
--      (dog park), passes p_state := NULL → processes all 2,628 active
--      parks × 7 days. The per-hour CREATE TEMP TABLE (joining 780K
--      dog_park_day_hourly_scores rows) exceeds Postgres'
--      statement_timeout = 2 min.
--   3. statement_timeout cancels the entire orch_tick transaction at
--      every multiple of 5 minutes (when these jobs fire). The beach
--      apply_v2's earlier successful UPDATE in the same tick gets
--      rolled back too — that's why apply_v2_best_window_beach has
--      last_attempted_at = NULL despite being enabled+non-shadow.
--   4. The bulk fn ALSO doesn't update is_candidate_window or
--      is_in_best_window on hourly rows. So even when apply_v2 runs
--      successfully, the per-hour flags stay whatever daily-*-refresh
--      wrote (often false for newly-written hours).
--   5. find_beaches' best-remaining-window detection keys off
--      is_candidate_window → finds no candidates → renders "No good
--      window remaining" even for high-scoring beaches.
--
-- THIS MIGRATION:
--   Part A — chunk by state inside the wrappers. Replaces the single
--   all-states query with a per-state loop that calls the bulk fn N
--   times (one per active state). Each per-state pass works on ~10× less
--   data, finishing in well under statement_timeout. Cumulative wrapper
--   runtime drops to ~30s typical from >2min unbounded.
--
--   Part B — extend both bulk fns to update is_candidate_window +
--   is_in_best_window on hourly rows. Criterion (mirrors the existing
--   `eligible` CTE used for window picking):
--     - is_candidate_window = (NOT is_closed AND score IS NOT NULL)
--     - is_in_best_window   = (local_hour BETWEEN window_start_h AND window_end_h)
--   These restore the per-hour flag semantics the per-fid pre-88eee2e
--   pipeline relied on, making the bulk fn a complete replacement.

-- ─── Part B.1 — beach bulk fn now also sets is_candidate_window flags ─────
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations_bulk(
  p_state      text             DEFAULT NULL,
  p_fids       bigint[]         DEFAULT NULL,
  p_dates      date[]           DEFAULT NULL,
  p_days_ahead integer          DEFAULT 6
) RETURNS TABLE(
  beaches_processed   bigint,
  fid_date_pairs      bigint,
  hour_scores_written bigint,
  recs_written        bigint,
  windows_picked      bigint
)
LANGUAGE plpgsql AS $function$
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
  DROP TABLE IF EXISTS _scope, _gold, _per_hour, _scored, _windows;

  IF p_dates IS NOT NULL THEN
    v_dates := p_dates;
  ELSE
    SELECT array_agg(d::date)
      INTO v_dates
      FROM generate_series(CURRENT_DATE, CURRENT_DATE + p_days_ahead, '1 day'::interval) AS d;
  END IF;

  SELECT bands INTO v_b_wind_pos       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='wind_pos';
  SELECT bands INTO v_b_wind_harsh_neg FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='wind_harsh_neg';
  SELECT bands INTO v_b_asphalt_neg    FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='asphalt_neg';
  SELECT bands INTO v_b_sand_neg       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='sand_temp_neg';
  SELECT bands INTO v_b_uv_neg         FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='uv_neg';
  SELECT bands INTO v_b_tide_neg       FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='tide_neg';
  SELECT bands INTO v_b_crowd_neg      FROM public.scoring_config_v2 WHERE entity_type='beach' AND signal_key='crowd_neg';

  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT bg.fid, d AS local_date
  FROM public.beaches_gold bg
  CROSS JOIN unnest(v_dates) AS d
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

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
    COALESCE(dp.zone_rules, '[]'::jsonb)                                 AS zone_rules,
    COALESCE(dp.dogs_allowed, 'unknown')                                 AS dogs_allowed
  FROM _scope s
  JOIN public.beaches_gold     bg ON bg.fid = s.fid
  LEFT JOIN public.beach_dog_policy dp ON dp.arena_group_id = bg.fid
  LEFT JOIN public.beach_amenities  ba ON ba.arena_group_id = bg.fid
  GROUP BY bg.fid, bg.location_id,
           ba.has_restrooms, ba.has_lifeguards, ba.has_parking,
           ba.has_showers, ba.has_picnic_area, ba.has_food,
           ba.has_drinking_water, ba.has_disabled_access,
           dp.off_leash_flag, dp.zone_rules, dp.dogs_allowed;

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
    g.zone_rules, g.dogs_allowed,
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
    least(30,
      public._v2_lookup_band(COALESCE(h.sand_temp, -1),    v_b_sand_neg)
      + public._v2_lookup_band(COALESCE(h.asphalt_temp, -1), v_b_asphalt_neg)
      + public._v2_lookup_band(COALESCE(h.uv_index, -1),    v_b_uv_neg)
      + CASE WHEN h.feels_like > 90
             THEN least(4, greatest(0, round((h.feels_like - 90) * 0.4)::int))
             ELSE 0 END
    ) AS heat_uv_neg,
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
    public._v2_is_hour_closed(
      public._beach_closures_from_policy(g.dogs_allowed, g.zone_rules),
      s.local_date, h.local_hour
    ) AS is_closed
  FROM _scope s
  JOIN _gold g ON g.fid = s.fid
  JOIN public.beach_day_hourly_scores h
    ON h.location_id = g.location_id
   AND h.local_date  = s.local_date
   AND h.local_hour BETWEEN 6 AND 21;

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

  -- Best window picking — moved above the hour UPDATE so we can also
  -- write is_in_best_window in the same pass.
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

  -- Single hourly UPDATE: hour_score_v2 + is_candidate_window + is_in_best_window.
  -- Candidate criterion mirrors the _windows `eligible` CTE: not closed,
  -- has a score. Best-window membership is set from _windows ranges.
  WITH upd AS (
    UPDATE public.beach_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score END,
           is_candidate_window = (NOT sc.is_closed AND sc.score IS NOT NULL),
           is_in_best_window   = COALESCE(
             w.window_start_h IS NOT NULL
             AND h.local_hour BETWEEN w.window_start_h AND w.window_end_h,
             false
           )
      FROM _scored sc
      JOIN _gold g ON g.fid = sc.fid
      LEFT JOIN _windows w ON w.fid = sc.fid AND w.local_date = sc.local_date
     WHERE h.arena_group_id = sc.fid
       AND h.location_id    = g.location_id
       AND h.local_date     = sc.local_date
       AND h.local_hour     = sc.local_hour
    RETURNING 1
  )
  SELECT count(*) INTO hour_scores_written FROM upd;

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
           day_status_v2       = NULL
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

-- ─── Part B.2 — dog_park bulk fn now also sets is_candidate_window flags ───
CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations_bulk(
  p_state      text     DEFAULT NULL,
  p_fids       bigint[] DEFAULT NULL,
  p_dates      date[]   DEFAULT NULL,
  p_days_ahead integer  DEFAULT 6
) RETURNS TABLE(
  parks_processed     bigint,
  fid_date_pairs      bigint,
  hour_scores_written bigint,
  recs_written        bigint,
  windows_picked      bigint
)
LANGUAGE plpgsql AS $function$
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

  CREATE TEMP TABLE _gold ON COMMIT DROP AS
  SELECT
    g.fid,
    (CASE WHEN COALESCE(p.has_fence, g.has_fence) IS TRUE THEN 6 ELSE 0 END)
      + (CASE WHEN COALESCE(p.double_gate, false) IS TRUE THEN 2 ELSE 0 END) AS v_fence_pos,
    (CASE WHEN COALESCE(p.has_drinking_water, g.has_drinking_water) IS TRUE THEN 4 ELSE 0 END) AS v_water_pos,
    (CASE WHEN lower(COALESCE(p.surface_overlay, g.surface, '')) = 'grass' THEN 1 ELSE 0 END)
      + (CASE WHEN COALESCE(p.small_dog_area, false) IS TRUE OR COALESCE(p.large_dog_area, false) IS TRUE THEN 1 ELSE 0 END)
      + (CASE WHEN COALESCE(p.has_picnic_tables, false) IS TRUE THEN 2 ELSE 0 END) AS v_comfort_pos,
    (CASE WHEN COALESCE(p.has_agility, false) IS TRUE THEN 3 ELSE 0 END) AS v_agility_pos,
    COALESCE(p.has_shade, false) AS has_shade,
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
              + (CASE WHEN has_shade      THEN shade_need ELSE 0 END)
              + least(22, temp_pos + wind_pos + sky_pos)
              + window_pos
              + v_agility_pos
              + (CASE WHEN has_water_play THEN shade_need ELSE 0 END)
              - heat_uv_neg - harsh_neg
              - (CASE WHEN NOT has_shade  THEN shade_need ELSE 0 END)
         ))
    END AS score
  FROM _per_hour;

  -- Best window picked first so we can write is_in_best_window in the
  -- same per-hour UPDATE as hour_score_v2.
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

  WITH upd AS (
    UPDATE public.dog_park_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score END,
           is_candidate_window = (NOT sc.is_closed AND sc.score IS NOT NULL),
           is_in_best_window   = COALESCE(
             w.window_start_h IS NOT NULL
             AND h.local_hour BETWEEN w.window_start_h AND w.window_end_h,
             false
           )
      FROM _scored sc
      LEFT JOIN _windows w ON w.fid = sc.fid AND w.local_date = sc.local_date
     WHERE h.dog_park_fid = sc.fid
       AND h.local_date   = sc.local_date
       AND h.local_hour   = sc.local_hour
    RETURNING 1
  )
  SELECT count(*) INTO hour_scores_written FROM upd;

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

-- ─── Part A.1 — beach wrapper iterates per state ─────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_beach(p jsonb)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
  v_state           text;
  v_days_ahead      int;
  v_states          text[];
  v_s               text;
  v_total_beaches   bigint := 0;
  v_total_pairs     bigint := 0;
  v_total_hours     bigint := 0;
  v_total_recs      bigint := 0;
  v_total_windows   bigint := 0;
  v_b               bigint;
  v_p               bigint;
  v_h               bigint;
  v_r               bigint;
  v_w               bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  -- Chunk by state to keep each per-hour TEMP TABLE small. The previous
  -- all-states pass exceeded statement_timeout when the dog-park sibling
  -- ran. Per-state, ~370 beaches × 7 × 14 hours = ~36K rows — well inside.
  IF v_state IS NOT NULL THEN
    v_states := ARRAY[upper(v_state)];
  ELSE
    SELECT array_agg(DISTINCT state ORDER BY state)
      INTO v_states
      FROM public.beaches_gold
     WHERE is_active
       AND scoring_tier IN ('daily','hourly')
       AND state IS NOT NULL;
  END IF;

  FOREACH v_s IN ARRAY v_states LOOP
    SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
      INTO v_b, v_p, v_h, v_r, v_w
      FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
        v_s, NULL, NULL, v_days_ahead);

    v_total_beaches := v_total_beaches + COALESCE(v_b, 0);
    v_total_pairs   := v_total_pairs   + COALESCE(v_p, 0);
    v_total_hours   := v_total_hours   + COALESCE(v_h, 0);
    v_total_recs    := v_total_recs    + COALESCE(v_r, 0);
    v_total_windows := v_total_windows + COALESCE(v_w, 0);
  END LOOP;

  RAISE NOTICE 'bulk_apply_beach: states=% days=% beaches=% pairs=% hours=% recs=% windows=%',
    array_length(v_states, 1), v_days_ahead,
    v_total_beaches, v_total_pairs, v_total_hours, v_total_recs, v_total_windows;
END;
$function$;

-- ─── Part A.2 — dog_park wrapper iterates per state ──────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_dog_park(p jsonb)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
  v_state           text;
  v_days_ahead      int;
  v_states          text[];
  v_s               text;
  v_total_parks     bigint := 0;
  v_total_pairs     bigint := 0;
  v_total_hours     bigint := 0;
  v_total_recs      bigint := 0;
  v_total_windows   bigint := 0;
  v_pp              bigint;
  v_p               bigint;
  v_h               bigint;
  v_r               bigint;
  v_w               bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  IF v_state IS NOT NULL THEN
    v_states := ARRAY[upper(v_state)];
  ELSE
    SELECT array_agg(DISTINCT state ORDER BY state)
      INTO v_states
      FROM public.dog_parks_gold
     WHERE is_active AND is_scoreable = true
       AND state IS NOT NULL;
  END IF;

  FOREACH v_s IN ARRAY v_states LOOP
    SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
      INTO v_pp, v_p, v_h, v_r, v_w
      FROM public.apply_v2_best_window_to_recommendations_bulk(
        v_s, NULL, NULL, v_days_ahead);

    v_total_parks   := v_total_parks   + COALESCE(v_pp, 0);
    v_total_pairs   := v_total_pairs   + COALESCE(v_p, 0);
    v_total_hours   := v_total_hours   + COALESCE(v_h, 0);
    v_total_recs    := v_total_recs    + COALESCE(v_r, 0);
    v_total_windows := v_total_windows + COALESCE(v_w, 0);
  END LOOP;

  RAISE NOTICE 'bulk_apply_dp: states=% days=% parks=% pairs=% hours=% recs=% windows=%',
    array_length(v_states, 1), v_days_ahead,
    v_total_parks, v_total_pairs, v_total_hours, v_total_recs, v_total_windows;
END;
$function$;
