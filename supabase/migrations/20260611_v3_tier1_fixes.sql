-- 20260611_v3_tier1_fixes.sql
--
-- v3 beach scoring — Tier 1 fixes (3 items in one migration):
--
-- ITEM 1: Beach hours — replace hardcoded 6-21 window with per-beach
--         open_time/close_time. Skip beaches with NULL hours entirely
--         (don't fake a score when we don't know when the beach is open).
--         99.3% of active beaches had hours outside the hardcoded window;
--         most lose dawn (5-6am) and dusk (9-10pm) — often the best dog
--         hours in summer.
--
-- ITEM 2: Cascade rebalance — halve direct UV and feels_like>90 penalties.
--         These signals also drive sand_temp/asphalt_temp scoring, so the
--         direct penalty was on top of the surface-temp penalty. Halving
--         the direct path acknowledges both contribute (sunburn AND paw
--         burn are different harms) without compounding to 30+ pt drops.
--         Net effect: max cascade penalty 30 → 22 pts; La Jolla evening
--         v2/v3 gap closes from ~6 to ~2 pts.
--
-- ITEM 3: v3 picks its own best window — the bulk writer's _windows CTE
--         chain previously used v2 score for stability. With Item 2's
--         smaller v2/v3 gap, picker flap is minimal and v3 should own
--         its own selection for coherent semantics.
--
-- Item 4 (closure consolidation) deferred — affects 0 beaches in practice.

begin;

-- ─── Item 2a: uv_neg band rebalance ───────────────────────────────────
-- Old: 0 (<6) → 1 (6-8) → 3 (8-10) → 4 (10-11) → 8 (≥11)   max 8
-- New: 0 (<6) → 1 (6-8) → 2 (8-10) → 3 (10-11) → 4 (≥11)   max 4
update public.scoring_config_v2
   set bands = jsonb_build_array(
     jsonb_build_object('max', 6,             'score', 0),
     jsonb_build_object('min', 6, 'max', 8,   'score', 1),
     jsonb_build_object('min', 8, 'max', 10,  'score', 2),
     jsonb_build_object('min', 10, 'max', 11, 'score', 3),
     jsonb_build_object('min', 11,            'score', 4)
   )
 where entity_type = 'beach' and signal_key = 'uv_neg';


-- ─── apply_v2_best_window_to_beach_recommendations_bulk ──────────────
-- Items 1+2b+3.

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations_bulk(
  p_state text DEFAULT NULL::text,
  p_fids bigint[] DEFAULT NULL::bigint[],
  p_dates date[] DEFAULT NULL::date[],
  p_days_ahead integer DEFAULT 6
)
RETURNS TABLE(
  beaches_processed bigint,
  fid_date_pairs bigint,
  hour_scores_written bigint,
  recs_written bigint,
  windows_picked bigint
)
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

  -- ITEM 1: skip beaches with NULL open_time/close_time. Stricter than v1
  -- (which defaults NULL→00:00/23:59) — we don't fake a score.
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

  -- ITEM 1: load per-beach v_open_h / v_close_h.
  -- open_time / close_time are TEXT columns (HH:MM strings); cast to time
  -- before extract. v_close_h ceils to inclusive integer hour matching v1's
  -- `hourMinutes < closeMinutes` semantics: close_time '22:00' → last
  -- scored hour starts at 21:00; close_time '22:30' → hour starting at
  -- 22:00 still scored (open until :30).
  CREATE TEMP TABLE _gold ON COMMIT DROP AS
  SELECT
    bg.fid,
    bg.location_id,
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
    COALESCE(dp.zone_rules, '[]'::jsonb)                                 AS zone_rules,
    COALESCE(dp.dogs_allowed, 'unknown')                                 AS dogs_allowed
  FROM _scope s
  JOIN public.beaches_gold     bg ON bg.fid = s.fid
  LEFT JOIN public.beach_dog_policy dp ON dp.arena_group_id = bg.fid
  LEFT JOIN public.beach_amenities  ba ON ba.arena_group_id = bg.fid
  GROUP BY bg.fid, bg.location_id, bg.open_time, bg.close_time,
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
    g.v_restroom_pos + g.v_lifeguard_pos + g.v_parking_pos
      + g.v_shower_pos + g.v_picnic_pos + g.v_food_pos
      + g.v_drinking_pos                                                AS amenities_pos_v3,
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
    -- ITEM 2b: feels_like > 90 extra halved (0.4 → 0.2, cap 4 → 2)
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
    -- ITEM 2b: raw variant also halved
    (
      public._v2_lookup_band(COALESCE(h.sand_temp, -1),    v_b_sand_neg)
      + public._v2_lookup_band(COALESCE(h.asphalt_temp, -1), v_b_asphalt_neg)
      + public._v2_lookup_band(COALESCE(h.uv_index, -1),    v_b_uv_neg)
      + CASE WHEN h.feels_like > 90
             THEN least(2, greatest(0, round((h.feels_like - 90) * 0.2)::int))
             ELSE 0 END
    ) AS heat_uv_neg_raw,
    (
      public._v2_lookup_band(COALESCE(h.wind_speed, -1), v_b_wind_harsh_neg)
      + CASE WHEN h.precip_chance > 30
             THEN least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int))
             ELSE 0 END
      + CASE WHEN h.feels_like < 50
             THEN least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int))
             ELSE 0 END
    ) AS harsh_neg_raw,
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
   -- ITEM 1: use per-beach open/close instead of hardcoded 6-21
   AND h.local_hour BETWEEN g.v_open_h AND g.v_close_h;

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
    END AS score,
    CASE WHEN is_closed THEN 0
         ELSE greatest(0, least(100,
           50 + amenities_pos_v3 + dog_access_pos
              + least(22, temp_pos + wind_pos + sky_pos)
              - heat_uv_neg_raw - harsh_neg_raw - tide_neg - crowd_neg
         ))
    END AS score_v3
  FROM _per_hour;

  -- ITEM 3: best window picker switches from v2 score → v3 score.
  -- _windows now reflects v3's preferences; composite_score_v2 below
  -- becomes "what v2 scores the v3-picked window" (shadow stat).
  CREATE TEMP TABLE _windows ON COMMIT DROP AS
  WITH eligible AS (
    SELECT fid, local_date, local_hour, score, score_v3,
           local_hour - row_number() OVER (PARTITION BY fid, local_date ORDER BY local_hour) AS grp
    FROM _scored
    WHERE NOT is_closed AND score_v3 IS NOT NULL
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
           is_candidate_window = (NOT sc.is_closed AND sc.score_v3 IS NOT NULL),
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
$function$;


-- ─── compute_beach_hourly_v2 (per-fid display) ────────────────────────
-- Items 1 + 2b. Item 3 doesn't apply — this fn doesn't pick the canonical
-- window written to beach_day_recommendations; the bulk writer does.

CREATE OR REPLACE FUNCTION public.compute_beach_hourly_v2(
  p_fid bigint,
  p_user_lat double precision DEFAULT NULL::double precision,
  p_user_lng double precision DEFAULT NULL::double precision,
  p_date date DEFAULT NULL::date
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $function$
declare
  v_gold record; v_policy record; v_today record; v_amenities record;
  v_view_date date := coalesce(p_date, current_date);
  v_restroom_pos integer := 0; v_lifeguard_pos integer := 0;
  v_parking_pos integer := 0; v_picnic_pos integer := 0; v_shower_pos integer := 0;
  v_food_pos integer := 0; v_drinking_pos integer := 0; v_disabled_pos integer := 0;
  v_amenities_pos integer := 0;
  v_amenities_pos_v3 integer := 0;
  v_dog_access_pos integer := 0;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_open_h integer; v_close_h integer; v_hours jsonb;
  v_bw jsonb; v_closures jsonb := '[]'::jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb;
  v_b_asphalt_neg jsonb; v_b_sand_neg jsonb; v_b_uv_neg jsonb;
  v_b_tide_neg jsonb; v_b_crowd_neg jsonb; v_b_drive jsonb;
begin
  select * into v_gold from public.beaches_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','beach not found','fid',p_fid); end if;

  -- ITEM 1: skip beaches with NULL hours. Stricter than v1 — don't fake a
  -- score when we don't know when the beach is open.
  if v_gold.open_time is null or v_gold.close_time is null then
    return jsonb_build_object('error', 'beach has no operating hours', 'fid', p_fid);
  end if;

  select * into v_policy from public.beach_dog_policy where arena_group_id = p_fid limit 1;
  select * into v_today from public.beach_day_recommendations
    where location_id = v_gold.location_id and local_date = v_view_date limit 1;
  select * into v_amenities from public.beach_amenities where arena_group_id = p_fid limit 1;
  if v_policy.arena_group_id is not null then
    v_closures := public._beach_closures_from_policy(v_policy.dogs_allowed, v_policy.zone_rules);
  end if;
  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type='beach' and signal_key='wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type='beach' and signal_key='wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type='beach' and signal_key='asphalt_neg';
  select bands into v_b_sand_neg       from public.scoring_config_v2 where entity_type='beach' and signal_key='sand_temp_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type='beach' and signal_key='uv_neg';
  select bands into v_b_tide_neg       from public.scoring_config_v2 where entity_type='beach' and signal_key='tide_neg';
  select bands into v_b_crowd_neg      from public.scoring_config_v2 where entity_type='beach' and signal_key='crowd_neg';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type='beach' and signal_key='drive_factor';
  if v_amenities.has_restrooms       = true then v_restroom_pos := 4; end if;
  if v_amenities.has_lifeguards      = true then v_lifeguard_pos := 4; end if;
  if v_amenities.has_parking         = true then v_parking_pos := 3; end if;
  if v_amenities.has_showers         = true then v_shower_pos := 2; end if;
  if v_amenities.has_picnic_area     = true then v_picnic_pos := 2; end if;
  if v_amenities.has_food            = true then v_food_pos := 1; end if;
  if v_amenities.has_drinking_water  = true then v_drinking_pos := 1; end if;
  if v_amenities.has_disabled_access = true then v_disabled_pos := 1; end if;
  v_amenities_pos := v_restroom_pos + v_lifeguard_pos + v_parking_pos
                   + v_shower_pos + v_picnic_pos + v_food_pos
                   + v_drinking_pos + v_disabled_pos;
  v_amenities_pos_v3 := v_restroom_pos + v_lifeguard_pos + v_parking_pos
                     + v_shower_pos + v_picnic_pos + v_food_pos
                     + v_drinking_pos;
  if v_policy.off_leash_flag = true then v_dog_access_pos := 5; end if;
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    v_drive := public._v2_lookup_band(v_drive_min::numeric, v_b_drive);
  end if;

  -- ITEM 1: read per-beach open/close (TEXT cols → ::time cast) instead of
  -- hardcoded 6-21
  v_open_h := extract(hour from v_gold.open_time::time)::int;
  v_close_h := extract(hour from v_gold.close_time::time)::int
             + (case when extract(minute from v_gold.close_time::time) > 0 then 1 else 0 end)
             - 1;

  with per_hour as (
    select h.local_hour, h.hour_label,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.sand_temp, h.tide_height, h.precip_chance,
      h.busyness_score, h.is_in_best_window, h.is_daylight,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
      public._v2_first_closure_section(v_closures, v_view_date, h.local_hour) as closed_section,
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_pos) as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      -- ITEM 2b: feels_like > 90 extra halved (0.4 → 0.2, cap 4 → 2)
      least(30,
        public._v2_lookup_band(coalesce(h.sand_temp, -1), v_b_sand_neg)
        + public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90
               then least(2, greatest(0, round((h.feels_like - 90) * 0.2)::int))
               else 0 end
      ) as heat_uv_neg,
      least(15,
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int)) else 0 end
      ) as harsh_neg,
      -- ITEM 2b: raw variant also halved
      (
        public._v2_lookup_band(coalesce(h.sand_temp, -1), v_b_sand_neg)
        + public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90
               then least(2, greatest(0, round((h.feels_like - 90) * 0.2)::int))
               else 0 end
      ) as heat_uv_neg_raw,
      (
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(5, greatest(0, round((h.precip_chance - 30) / 14.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(5, greatest(0, round((50 - h.feels_like) * 0.2)::int)) else 0 end
      ) as harsh_neg_raw,
      public._v2_lookup_band(coalesce(h.tide_height, -1), v_b_tide_neg) as tide_neg,
      public._v2_lookup_band(coalesce(h.busyness_score, -1), v_b_crowd_neg) as crowd_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos
    from public.beach_day_hourly_scores h
    where h.location_id = v_gold.location_id
      and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(22, temp_pos + wind_pos + sky_pos) as weather_pos,
      v_amenities_pos as amenities_static,
      v_amenities_pos_v3 as amenities_static_v3,
      v_dog_access_pos as dog_access_static
    from per_hour
  ),
  scored as (
    select *,
      case when is_closed then 0
           else greatest(0, least(100,
             50 + amenities_static + dog_access_static + weather_pos + window_pos
                - heat_uv_neg - harsh_neg - tide_neg - crowd_neg + v_drive))
      end as score,
      case when is_closed then 0
           else greatest(0, least(100,
             50 + amenities_static_v3 + dog_access_static + weather_pos
                - heat_uv_neg_raw - harsh_neg_raw - tide_neg - crowd_neg + v_drive))
      end as score_v3
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'score', score,
    'score_v3', score_v3,
    'is_closed', is_closed, 'closed_section', closed_section,
    'pos_weather',   case when is_closed then 0 else weather_pos end,
    'pos_features',  case when is_closed then 0 else amenities_static end,
    'pos_enrichment', case when is_closed then 0 else dog_access_static end,
    'pos_window',    case when is_closed then 0 else window_pos end,
    'neg_heat_uv',   case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',     case when is_closed then 0 else harsh_neg end,
    'neg_gotchas',   case when is_closed then 0 else tide_neg + crowd_neg end,
    'drive',         case when is_closed then 0 else v_drive end,
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

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 0, 2, 5);

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
$function$;

commit;
notify pgrst, 'reload schema';
