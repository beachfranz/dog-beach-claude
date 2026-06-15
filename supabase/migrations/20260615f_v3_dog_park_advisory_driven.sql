-- v3 dog-park score: shadow columns + advisory-driven penalty.
--
-- Phase D + E of the v3 refactor, combined. Plan delegated-splashing-lynx.md
-- spec'd these as two migrations (20260615f shadow, 20260615g advisory swap)
-- but the dog-park v3 path has zero current consumers (no hour_score_v3 /
-- composite_score_v3 columns exist on dog_park tables today), so the
-- intermediate "shadow with baseline" state would just be rewritten by the
-- advisory swap immediately. Combining keeps it to one CREATE OR REPLACE
-- per function and one ALTER per table without changing the user-visible
-- outcome.
--
-- Mirrors what beach side did in 20260611_v3_tier1_fixes.sql + 20260615e
-- (Phase C) — same formula, same gates (minus sand which dog parks don't
-- have), same whitelist semantics, same paired CTE pattern in bulk path.
--
-- Per [[paired-functions-port-fixes-both-sides]]: this is the dog-park
-- sibling of the beach refactor shipped in 20260615e earlier today.

BEGIN;

-- ─── 1. ALTER tables for shadow v3 columns ───────────────────────────
ALTER TABLE public.dog_park_day_hourly_scores
  ADD COLUMN IF NOT EXISTS hour_score_v3 numeric;
ALTER TABLE public.dog_park_day_recommendations
  ADD COLUMN IF NOT EXISTS composite_score_v3 numeric;

COMMENT ON COLUMN public.dog_park_day_hourly_scores.hour_score_v3 IS
  'v3 advisory-driven score (0-100). Same baseline as v2 minus inline '
  'heat_uv/harsh/gotchas negs, plus advisory-driven penalty from '
  'dog_park_advisory × advisory_score_config. Shadow column — '
  'consumers read v2 today; cutover ships separately.';
COMMENT ON COLUMN public.dog_park_day_recommendations.composite_score_v3 IS
  'Daily composite of hour_score_v3 across best-window hours, same '
  'shape as composite_score_v2.';


-- ─── 2. apply_v2_best_window_to_recommendations_bulk (dog-park) ──────
-- Bulk path used by daily-dog-park-refresh. v3 swap; v2 unchanged.

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations_bulk(
  p_state text DEFAULT NULL::text,
  p_fids bigint[] DEFAULT NULL::bigint[],
  p_dates date[] DEFAULT NULL::date[],
  p_days_ahead integer DEFAULT 6
)
RETURNS TABLE(
  parks_processed bigint,
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
    ), 0)::numeric AS penalty_total
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
$function$;


-- ─── 3. compute_dog_park_hourly_v2 (singleton path) ──────────────────
-- Per-fid display, used by get-dog-park-now. v3 swap + JSON emit of
-- status_v3 + advisory_penalty_v3.

CREATE OR REPLACE FUNCTION public.compute_dog_park_hourly_v2(
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
  v_closures jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb; v_b_asphalt_neg jsonb;
  v_b_uv_neg   jsonb; v_b_uv_need        jsonb; v_b_temp_need   jsonb;
  v_b_drive    jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;
  v_closures := coalesce(v_policy.closures, '[]'::jsonb);
  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type='dog_park' and signal_key='wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type='dog_park' and signal_key='wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type='dog_park' and signal_key='asphalt_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type='dog_park' and signal_key='uv_neg';
  select bands into v_b_uv_need        from public.scoring_config_v2 where entity_type='dog_park' and signal_key='uv_need';
  select bands into v_b_temp_need      from public.scoring_config_v2 where entity_type='dog_park' and signal_key='temp_need';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type='dog_park' and signal_key='drive_factor';
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
    select h.local_hour, h.hour_label,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
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
      -- v2 inline negs
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
      -- NEW: advisory penalty per-hour (v3 path only)
      (
        SELECT COALESCE(SUM(
          public._advisory_base_for_severity(dpa.severity) * asc_row.weight
        ), 0)::numeric
        FROM public.dog_park_advisory dpa
        JOIN public.advisory_score_config asc_row
          ON asc_row.entity_type = 'dog_park'
         AND asc_row.event_type  = dpa.event_type
         AND asc_row.enabled     = true
        WHERE dpa.dog_park_fid = p_fid
          AND (v_view_date::timestamp + (h.local_hour || ' hours')::interval)
              AT TIME ZONE h.timezone BETWEEN dpa.valid_from AND dpa.valid_to
      ) AS advisory_penalty
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
    select *,
      case when is_closed then 0
           else greatest(0, least(100,
             50 + features_static + shade_pos + weather_pos + window_pos
                + v_agility_pos + water_play_pos
                - heat_uv_neg - harsh_neg - gotchas_neg + v_drive))
      end as score,
      -- v3 NEW: gates + advisory_penalty + v_drive (singleton has drive)
      case
        when is_closed             then 0
        when asphalt_temp >= 125   then 0
        when uv_index     >= 11    then 0
        when feels_like   <= 20    then 0
        else greatest(0, least(100,
          50 + features_static + shade_pos + weather_pos
             + v_agility_pos + water_play_pos
             - COALESCE(advisory_penalty, 0) + v_drive))::int
      end as score_v3
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'score', score,
    'score_v3', score_v3,
    'status_v3', case
      when is_closed then 'closed'
      when asphalt_temp >= 125 or uv_index >= 11 or feels_like <= 20
        then 'no_go'
      else public._v2_status_from_score(score_v3::int)
    end,
    'advisory_penalty_v3', COALESCE(advisory_penalty, 0),
    'is_closed', is_closed,
    'pos_weather', case when is_closed then 0 else weather_pos end,
    'pos_features', case when is_closed then 0 else features_static + shade_pos end,
    'pos_enrichment', case when is_closed then 0 else v_agility_pos + water_play_pos end,
    'pos_window', case when is_closed then 0 else window_pos end,
    'neg_heat_uv', case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',   case when is_closed then 0 else harsh_neg end,
    'neg_gotchas', case when is_closed then 0 else gotchas_neg end,
    'drive', case when is_closed then 0 else v_drive end,
    'pos_temp', case when is_closed then 0 else temp_pos end,
    'pos_wind', case when is_closed then 0 else wind_pos end,
    'pos_sky',  case when is_closed then 0 else sky_pos end,
    'pos_fence', case when is_closed then 0 else v_fence_pos end,
    'pos_water', case when is_closed then 0 else v_water_pos end,
    'pos_shade', case when is_closed then 0 else shade_pos end,
    'pos_comfort', case when is_closed then 0 else v_comfort_pos end,
    'pos_agility', case when is_closed then 0 else v_agility_pos end,
    'pos_water_play', case when is_closed then 0 else water_play_pos end,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp,
    'weather_code', weather_code, 'precip_chance', precip_chance
  ) order by local_hour) into v_hours from scored;

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 0, 2, 5);

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
    'closures', v_closures,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$function$;

commit;
notify pgrst, 'reload schema';
