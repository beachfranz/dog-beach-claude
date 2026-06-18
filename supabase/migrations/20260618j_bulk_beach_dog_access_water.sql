-- Bulk writer: mirror compute_beach_hourly_v2 v3 changes (20260618i) —
-- zone_rules dog-access (sand 10/5 + water 3/2 + trails 2/1) + water_score
-- (zone access 6/3 + marine swim-comfort). v2 path unchanged.

BEGIN;

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations_bulk(p_state text DEFAULT NULL::text, p_fids bigint[] DEFAULT NULL::bigint[], p_dates date[] DEFAULT NULL::date[], p_days_ahead integer DEFAULT 6)
 RETURNS TABLE(beaches_processed bigint, fid_date_pairs bigint, hour_scores_written bigint, recs_written bigint, windows_picked bigint)
 LANGUAGE plpgsql
AS $function$
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
  DROP TABLE IF EXISTS _scope, _gold, _per_hour, _adv_penalty, _scored, _windows;

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
    AND bg.open_time IS NOT NULL
    AND bg.close_time IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

  -- _gold: add timezone (needed by advisory CTE for local_hour → UTC conversion)
  CREATE TEMP TABLE _gold ON COMMIT DROP AS
  SELECT
    bg.fid,
    bg.location_id,
    bg.timezone                                                          AS tz,
    extract(hour from bg.open_time::time)::int AS v_open_h,
    extract(hour from bg.close_time::time)::int
      + (CASE WHEN extract(minute from bg.close_time::time) > 0 THEN 1 ELSE 0 END)
      - 1 AS v_close_h,
    (CASE WHEN COALESCE(ba.has_restrooms,       false) THEN 4 ELSE 0 END) AS v_restroom_pos,
    (CASE WHEN COALESCE(ba.has_lifeguards,      false) THEN 4 ELSE 0 END) AS v_lifeguard_pos,
    (CASE WHEN COALESCE(ba.has_parking,         false) THEN 3 ELSE 0 END) AS v_parking_pos,
    (CASE WHEN COALESCE(ba.has_showers,         false) THEN 2 ELSE 0 END) AS v_shower_pos,
    (CASE WHEN COALESCE(ba.has_picnic_area,     false) THEN 2 ELSE 0 END) AS v_picnic_pos,
    (CASE WHEN COALESCE(ba.has_food,            false) THEN 1 ELSE 0 END) AS v_food_pos,
    (CASE WHEN COALESCE(ba.has_drinking_water,  false) THEN 1 ELSE 0 END) AS v_drinking_pos,
    (CASE WHEN COALESCE(ba.has_disabled_access, false) THEN 1 ELSE 0 END) AS v_disabled_pos,
    (CASE WHEN COALESCE(dp.off_leash_flag,      false) THEN 5 ELSE 0 END) AS v_dog_access_pos,
    public._dog_access_score_v3(COALESCE(dp.zone_rules,'[]'::jsonb), dp.off_leash_flag, dp.has_off_leash, dp.has_on_leash) AS v_dog_access_v3,
    public._water_access_base_v3(COALESCE(dp.zone_rules,'[]'::jsonb)) AS v_water_access_base,
    bg.marine_grid_lat AS marine_grid_lat, bg.marine_grid_lon AS marine_grid_lon,
    COALESCE(dp.zone_rules, '[]'::jsonb)                                 AS zone_rules,
    COALESCE(dp.dogs_allowed, 'unknown')                                 AS dogs_allowed
  FROM _scope s
  JOIN public.beaches_gold     bg ON bg.fid = s.fid
  LEFT JOIN public.beach_dog_policy dp ON dp.arena_group_id = bg.fid
  LEFT JOIN public.beach_amenities  ba ON ba.arena_group_id = bg.fid
  GROUP BY bg.fid, bg.location_id, bg.timezone, bg.open_time, bg.close_time,
           ba.has_restrooms, ba.has_lifeguards, ba.has_parking,
           ba.has_showers, ba.has_picnic_area, ba.has_food,
           ba.has_drinking_water, ba.has_disabled_access,
           dp.off_leash_flag, dp.zone_rules, dp.dogs_allowed,
           dp.has_off_leash, dp.has_on_leash, bg.marine_grid_lat, bg.marine_grid_lon;

  CREATE TEMP TABLE _per_hour ON COMMIT DROP AS
  SELECT
    s.fid, s.local_date, h.local_hour, h.hour_label,
    h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
    h.asphalt_temp, h.sand_temp, h.tide_height, h.precip_chance,
    h.busyness_score, h.is_in_best_window, h.is_daylight,
    g.v_restroom_pos + g.v_lifeguard_pos + g.v_parking_pos
      + g.v_shower_pos + g.v_picnic_pos + g.v_food_pos
      + g.v_drinking_pos + g.v_disabled_pos                            AS amenities_pos,
    g.v_restroom_pos + g.v_lifeguard_pos + g.v_parking_pos
      + g.v_shower_pos + g.v_picnic_pos + g.v_food_pos
      + g.v_drinking_pos                                                AS amenities_pos_v3,
    g.v_dog_access_pos                                                  AS dog_access_pos,
    g.v_dog_access_v3                                                   AS dog_access_v3,
    CASE WHEN g.v_water_access_base = 0 THEN 0
         ELSE g.v_water_access_base
              + CASE WHEN m.water_temp_f IS NULL THEN 0
                     WHEN m.water_temp_f BETWEEN 64 AND 75 THEN 3
                     WHEN m.water_temp_f BETWEEN 60 AND 80 THEN 1 ELSE 0 END
              + CASE WHEN m.wave_height_m IS NULL THEN 0
                     WHEN m.wave_height_m * 3.28084 < 2 THEN 2
                     WHEN m.wave_height_m * 3.28084 <= 4 THEN 1 ELSE 0 END
    END                                                                 AS water_score,
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
    -- v2 inline negs (kept for v2 path)
    least(30,
      public._v2_lookup_band(COALESCE(h.sand_temp, -1),    v_b_sand_neg)
      + public._v2_lookup_band(COALESCE(h.asphalt_temp, -1), v_b_asphalt_neg)
      + public._v2_lookup_band(COALESCE(h.uv_index, -1),    v_b_uv_neg)
      + CASE WHEN h.feels_like > 90
             THEN least(2, greatest(0, round((h.feels_like - 90) * 0.2)::int))
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
   AND h.local_hour BETWEEN g.v_open_h AND g.v_close_h
  LEFT JOIN public.marine_grid_hourly m
    ON m.grid_lat = g.marine_grid_lat
   AND m.grid_lon = g.marine_grid_lon
   AND m.forecast_ts = (s.local_date::timestamp + (h.local_hour || ' hours')::interval) AT TIME ZONE g.tz;

  -- NEW: per-hour advisory penalty. Joins beach_advisory active at the
  -- UTC instant matching (local_date, local_hour, tz) for the beach.
  CREATE TEMP TABLE _adv_penalty ON COMMIT DROP AS
  SELECT
    ph.fid, ph.local_date, ph.local_hour,
    COALESCE(SUM(
      public._advisory_base_for_severity(ba.severity) * asc_row.weight
    ), 0)::numeric AS penalty_total,
    COALESCE(jsonb_agg(
      jsonb_build_object(
        'event_type',   ba.event_type,
        'severity',     ba.severity,
        'contribution', (public._advisory_base_for_severity(ba.severity) * asc_row.weight)::numeric
      )
      ORDER BY (public._advisory_base_for_severity(ba.severity) * asc_row.weight) DESC, ba.event_type
    ) FILTER (
      WHERE asc_row.weight IS NOT NULL
        AND public._advisory_base_for_severity(ba.severity) * asc_row.weight > 0
    ), '[]'::jsonb) AS components
  FROM _per_hour ph
  JOIN _gold g ON g.fid = ph.fid
  LEFT JOIN public.beach_advisory ba
    ON ba.beach_fid = ph.fid
   AND (ph.local_date::timestamp + (ph.local_hour || ' hours')::interval)
       AT TIME ZONE g.tz BETWEEN ba.valid_from AND ba.valid_to
  LEFT JOIN public.advisory_score_config asc_row
    ON asc_row.entity_type = 'beach'
   AND asc_row.event_type  = ba.event_type
   AND asc_row.enabled     = true
  GROUP BY ph.fid, ph.local_date, ph.local_hour;

  CREATE TEMP TABLE _scored ON COMMIT DROP AS
  SELECT
    ph.fid, ph.local_date, ph.local_hour, ph.hour_label, ph.is_closed, ph.is_daylight,
    -- v2 unchanged (cap-based negatives + window_pos, no drive in bulk)
    CASE WHEN ph.is_closed THEN 0
         ELSE greatest(0, least(100,
           50 + ph.amenities_pos + ph.dog_access_pos
              + least(22, ph.temp_pos + ph.wind_pos + ph.sky_pos)
              + ph.window_pos
              - ph.heat_uv_neg - ph.harsh_neg - ph.tide_neg - ph.crowd_neg
         ))
    END AS score,
    -- v3 NEW: advisory-driven penalty + gates. Gates force 0 so picker
    -- treats the hour as unattractive without needing a status column.
    CASE
      WHEN ph.is_closed THEN 0
      WHEN ph.sand_temp    >= 145 THEN 0
      WHEN ph.asphalt_temp >= 125 THEN 0
      WHEN ph.uv_index     >= 11  THEN 0
      WHEN ph.feels_like   <= 20  THEN 0
      ELSE greatest(0, least(100,
        50 + ph.amenities_pos_v3 + ph.dog_access_v3
           + least(22, ph.temp_pos + ph.wind_pos + ph.sky_pos)
           + ph.water_score
           - COALESCE(ap.penalty_total, 0)
      ))::int
    END AS score_v3,
    COALESCE(ap.penalty_total, 0) AS advisory_penalty_v3
  FROM _per_hour ph
  LEFT JOIN _adv_penalty ap
    ON ap.fid = ph.fid AND ap.local_date = ph.local_date AND ap.local_hour = ph.local_hour;

  -- v3 best-window picker (unchanged structure; reads new score_v3).
  CREATE TEMP TABLE _windows ON COMMIT DROP AS
  WITH eligible AS (
    SELECT fid, local_date, local_hour, score, score_v3,
           local_hour - row_number() OVER (PARTITION BY fid, local_date ORDER BY local_hour) AS grp
    FROM _scored
    WHERE NOT is_closed AND score_v3 IS NOT NULL AND score_v3 > 0
  ),
  runs AS (
    SELECT fid, local_date, grp,
           min(local_hour) AS run_start, max(local_hour) AS run_end,
           count(*)::int   AS run_len,
           avg(score_v3)::numeric AS avg_score,
           max(score)::int        AS max_score,
           max(score_v3)::int     AS max_score_v3
    FROM eligible
    GROUP BY fid, local_date, grp
    HAVING count(*) >= 2
  ),
  best_run AS (
    SELECT DISTINCT ON (fid, local_date)
           fid, local_date, grp, run_start, run_end, run_len, max_score, max_score_v3, avg_score
    FROM runs
    ORDER BY fid, local_date, avg_score DESC, run_len DESC, run_start ASC
  ),
  rolling AS (
    SELECT
      e.fid, e.local_date, e.local_hour AS slice_start,
      sum(e.score_v3) OVER (
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
         b.max_score,
         b.max_score_v3
  FROM best_run b
  LEFT JOIN best_slice s USING (fid, local_date);

  windows_picked := (SELECT count(*) FROM _windows);

  WITH upd AS (
    UPDATE public.beach_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score    END,
           hour_score_v3 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score_v3 END,
           is_candidate_window = (NOT sc.is_closed AND sc.score_v3 IS NOT NULL AND sc.score_v3 > 0),
           is_in_best_window   = COALESCE(
             w.window_start_h IS NOT NULL
             AND h.local_hour BETWEEN w.window_start_h AND w.window_end_h,
             false
           ),
           -- persist the v3 advisory breakdown under a namespaced key,
           -- preserving the v2 metric scores scoreHours wrote earlier.
           explainability = jsonb_set(
             COALESCE(h.explainability, '{}'::jsonb),
             '{advisory_v3}',
             jsonb_build_object(
               'penalty_total', COALESCE(ap.penalty_total, 0),
               'components',     CASE WHEN sc.is_closed THEN '[]'::jsonb
                                      ELSE COALESCE(ap.components, '[]'::jsonb) END
             )
           )
      FROM _scored sc
      JOIN _gold g ON g.fid = sc.fid
      LEFT JOIN _windows w ON w.fid = sc.fid AND w.local_date = sc.local_date
      LEFT JOIN _adv_penalty ap ON ap.fid = sc.fid AND ap.local_date = sc.local_date AND ap.local_hour = sc.local_hour
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
           COALESCE(
             w.max_score_v3,
             (SELECT avg(score_v3)::int
                FROM _scored sc
               WHERE sc.fid = s.fid AND sc.local_date = s.local_date
                 AND sc.is_daylight = true AND NOT sc.is_closed
                 AND sc.score_v3 IS NOT NULL)
           ) AS composite_score_v3,
           w.window_start_h, w.window_end_h
    FROM _scope s
    LEFT JOIN _windows w USING(fid, local_date)
  ),
  upd AS (
    UPDATE public.beach_day_recommendations r
       SET composite_score_v2  = c.composite_score_v2,
           composite_score_v3  = c.composite_score_v3,
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
$function$

;

NOTIFY pgrst, 'reload schema';
COMMIT;
