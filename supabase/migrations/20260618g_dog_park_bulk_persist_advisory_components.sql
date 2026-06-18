-- Dog-park bulk writer: persist v3 advisory breakdown into
-- dog_park_day_hourly_scores.explainability under 'advisory_v3'
-- {penalty_total, components[]}. Additive jsonb_set merge. Sibling of
-- 20260618f per [[paired-functions-port-fixes-both-sides]].

BEGIN;

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations_bulk(p_state text DEFAULT NULL::text, p_fids bigint[] DEFAULT NULL::bigint[], p_dates date[] DEFAULT NULL::date[], p_days_ahead integer DEFAULT 6)
 RETURNS TABLE(parks_processed bigint, fid_date_pairs bigint, hour_scores_written bigint, recs_written bigint, windows_picked bigint)
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_b_wind_pos       jsonb;
  v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg    jsonb;
  v_b_uv_neg         jsonb;
  v_b_uv_need        jsonb;
  v_b_temp_need      jsonb;
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

  -- dog_parks_gold has no timezone column; tz comes from
  -- dog_park_day_hourly_scores.timezone per row in _per_hour.
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
    COALESCE(p.has_shade, false)     AS has_shade,
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
    h.timezone AS tz,
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
    -- v2 inline negs (kept for v2 path)
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

  -- NEW: per-hour advisory penalty. Joins dog_park_advisory active at the
  -- UTC instant matching (local_date, local_hour, tz) for the park.
  CREATE TEMP TABLE _adv_penalty ON COMMIT DROP AS
  SELECT
    ph.fid, ph.local_date, ph.local_hour,
    COALESCE(SUM(
      public._advisory_base_for_severity(dpa.severity) * asc_row.weight
    ), 0)::numeric AS penalty_total,
    COALESCE(jsonb_agg(
      jsonb_build_object(
        'event_type',   dpa.event_type,
        'severity',     dpa.severity,
        'contribution', (public._advisory_base_for_severity(dpa.severity) * asc_row.weight)::numeric
      )
      ORDER BY (public._advisory_base_for_severity(dpa.severity) * asc_row.weight) DESC, dpa.event_type
    ) FILTER (
      WHERE asc_row.weight IS NOT NULL
        AND public._advisory_base_for_severity(dpa.severity) * asc_row.weight > 0
    ), '[]'::jsonb) AS components
  FROM _per_hour ph
  LEFT JOIN public.dog_park_advisory dpa
    ON dpa.dog_park_fid = ph.fid
   AND (ph.local_date::timestamp + (ph.local_hour || ' hours')::interval)
       AT TIME ZONE ph.tz BETWEEN dpa.valid_from AND dpa.valid_to
  LEFT JOIN public.advisory_score_config asc_row
    ON asc_row.entity_type = 'dog_park'
   AND asc_row.event_type  = dpa.event_type
   AND asc_row.enabled     = true
  GROUP BY ph.fid, ph.local_date, ph.local_hour;

  CREATE TEMP TABLE _scored ON COMMIT DROP AS
  SELECT
    ph.fid, ph.local_date, ph.local_hour, ph.hour_label, ph.is_closed, ph.is_daylight,
    -- v2 unchanged
    CASE WHEN ph.is_closed THEN 0
         ELSE greatest(0, least(100,
           50 + ph.features_pos
              + (CASE WHEN ph.has_shade      THEN ph.shade_need ELSE 0 END)
              + least(22, ph.temp_pos + ph.wind_pos + ph.sky_pos)
              + ph.window_pos
              + ph.v_agility_pos
              + (CASE WHEN ph.has_water_play THEN ph.shade_need ELSE 0 END)
              - ph.heat_uv_neg - ph.harsh_neg
              - (CASE WHEN NOT ph.has_shade  THEN ph.shade_need ELSE 0 END)
         ))
    END AS score,
    -- v3 NEW: advisory-driven + gates (no sand_temp gate; dog parks don't
    -- have sand). UV ≥ 11 + asphalt ≥ 125 + feels ≤ 20 still apply.
    CASE
      WHEN ph.is_closed THEN 0
      WHEN ph.asphalt_temp >= 125 THEN 0
      WHEN ph.uv_index     >= 11  THEN 0
      WHEN ph.feels_like   <= 20  THEN 0
      ELSE greatest(0, least(100,
        50 + ph.features_pos
           + (CASE WHEN ph.has_shade      THEN ph.shade_need ELSE 0 END)
           + least(22, ph.temp_pos + ph.wind_pos + ph.sky_pos)
           + ph.v_agility_pos
           + (CASE WHEN ph.has_water_play THEN ph.shade_need ELSE 0 END)
           - COALESCE(ap.penalty_total, 0)
      ))::int
    END AS score_v3,
    COALESCE(ap.penalty_total, 0) AS advisory_penalty_v3
  FROM _per_hour ph
  LEFT JOIN _adv_penalty ap
    ON ap.fid = ph.fid AND ap.local_date = ph.local_date AND ap.local_hour = ph.local_hour;

  -- Best-window picker reads v3. Keeps v3's preferences as canonical.
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
           count(*)::int AS run_len, avg(score_v3)::numeric AS avg_score,
           max(score)::int    AS max_score,
           max(score_v3)::int AS max_score_v3
    FROM eligible
    GROUP BY fid, local_date, grp
    HAVING count(*) >= 2
  ),
  best_run AS (
    SELECT DISTINCT ON (fid, local_date) fid, local_date, grp,
           run_start, run_end, run_len, max_score, max_score_v3, avg_score
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
    SELECT DISTINCT ON (fid, local_date) fid, local_date, slice_start
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
    UPDATE public.dog_park_day_hourly_scores h
       SET hour_score_v2 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score    END,
           hour_score_v3 = CASE WHEN sc.is_closed THEN NULL ELSE sc.score_v3 END,
           is_candidate_window = (NOT sc.is_closed AND sc.score_v3 IS NOT NULL AND sc.score_v3 > 0),
           is_in_best_window   = COALESCE(
             w.window_start_h IS NOT NULL
             AND h.local_hour BETWEEN w.window_start_h AND w.window_end_h,
             false
           ),
           -- persist the v3 advisory breakdown under a namespaced key,
           -- preserving any v2 metric scores written earlier.
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
      LEFT JOIN _windows w ON w.fid = sc.fid AND w.local_date = sc.local_date
      LEFT JOIN _adv_penalty ap ON ap.fid = sc.fid AND ap.local_date = sc.local_date AND ap.local_hour = sc.local_hour
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
           COALESCE(
             w.max_score_v3,
             (SELECT avg(score_v3)::int FROM _scored sc
              WHERE sc.fid = s.fid AND sc.local_date = s.local_date
                AND sc.is_daylight = true AND NOT sc.is_closed
                AND sc.score_v3 IS NOT NULL)
           ) AS composite_score_v3,
           w.window_start_h, w.window_end_h
    FROM _scope s
    LEFT JOIN _windows w USING(fid, local_date)
  ),
  upd AS (
    UPDATE public.dog_park_day_recommendations r
       SET composite_score_v2 = c.composite_score_v2,
           composite_score_v3 = c.composite_score_v3,
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
$function$

;

NOTIFY pgrst, 'reload schema';
COMMIT;
