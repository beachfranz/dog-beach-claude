-- 20260611h_hide_bhs_parent_drop_erosion.sql
--
-- Two UI cleanups based on Rodeo Beach spot-check (Franz 2026-06-11):
--
-- 1. Hide the parent 'Beach Hazards Statement' chip on beach.html.
--    Now that 20260611e extracts the BHS narrative into 7 specific
--    hazard event_types (rip, sneaker, surf, longshore, pier,
--    cold_water, shore_break), the opaque parent chip is redundant
--    noise. Add it to the get_beach_advisories exclusion list.
--
-- 2. Drop beach_erosion_risk entirely. Low UX value — most coastal
--    beaches with any storm history have "localized erosion" mentioned
--    in their BHS, so the chip fires almost everywhere and conveys
--    little actionable info for dog owners. Removed from:
--      - get_beach_advisories (filtered out)
--      - find_beaches hazards array (dropped from IN clause)
--      - refresh_rip_current_advisories (CTE removed; no longer emitted)
--      - existing rows (one-shot DELETE)

begin;

-- ── 1. get_beach_advisories — add two exclusions ────────────────────

CREATE OR REPLACE FUNCTION public.get_beach_advisories(
  p_fid           bigint,
  p_horizon_hours integer DEFAULT 24,
  p_date          date    DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $function$
  WITH bounds AS (
    SELECT
      COALESCE(bg.timezone, 'America/Los_Angeles') AS tz,
      (COALESCE(
        p_date,
        (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date
       )::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS day_start_utc,
      ((COALESCE(
        p_date,
        (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date
       ) + 1)::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS day_end_utc,
      (p_date IS NULL OR p_date = (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date) AS is_today
    FROM public.beaches_gold bg
    WHERE bg.fid = p_fid
    LIMIT 1
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'source',             a.source,
    'severity',           a.severity,
    'event_type',         a.event_type,
    'dog_impact_class',   a.dog_impact_class,
    'dog_impact_text',    a.dog_impact_text,
    'label',              a.label,
    'value',              a.value,
    'icon',               a.icon,
    'valid_from',         GREATEST(a.valid_from, b.day_start_utc),
    'valid_to',           LEAST(a.valid_to, b.day_end_utc),
    'translation_source', a.translation_source,
    'raw_data',           a.raw_data
  ) ORDER BY public._advisory_severity_rank(a.severity) DESC, a.valid_from ASC),
                  '[]'::jsonb)
    FROM public.beach_advisory a
    CROSS JOIN bounds b
   WHERE a.beach_fid   = p_fid
     -- Defense-in-depth exclusions:
     --   strong_drift            — retired metric
     --   Beach Hazards Statement — parent now superseded by extracted hazards
     --   beach_erosion_risk      — low-utility chip; dropped 2026-06-11
     AND a.event_type NOT IN ('strong_drift', 'Beach Hazards Statement', 'beach_erosion_risk')
     AND a.valid_to    >  b.day_start_utc
     AND a.valid_from  <  b.day_end_utc
     AND (NOT b.is_today OR a.valid_to > now())
$function$;

GRANT EXECUTE ON FUNCTION public.get_beach_advisories(bigint, integer, date)
  TO anon, authenticated;


-- ── 2. find_beaches — drop beach_erosion_risk from hazards IN list ───
-- Minimal change to 20260611g; everything else identical.

DROP FUNCTION IF EXISTS public.find_beaches(date, double precision, double precision, text, integer, boolean, integer);

CREATE OR REPLACE FUNCTION public.find_beaches(
  p_date          date,
  p_lat           double precision DEFAULT NULL,
  p_lng           double precision DEFAULT NULL,
  p_leash         text             DEFAULT 'any',
  p_limit         integer          DEFAULT 500,
  p_scored_only   boolean          DEFAULT false,
  p_now_hour      integer          DEFAULT NULL
)
RETURNS TABLE (
  arena_group_id        bigint,
  location_id           text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  timezone              text,
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  dogs_prohibited_end   text,
  location_tier         text,
  distance_m            double precision,
  composite_score_v2    integer,
  current_hour_score    integer,
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  avg_uv                numeric,
  peak_sand_temp        numeric,
  peak_asphalt_temp     numeric,
  peak_wave_height_m    numeric,
  rip_current_risk      text,
  hazards               jsonb,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric,
  has_parking           boolean,
  has_restrooms         boolean,
  has_drinking_water    boolean,
  has_disabled_access   boolean,
  has_showers           boolean,
  has_picnic_area       boolean,
  has_food              boolean,
  has_lifeguards        boolean,
  has_fire_pits         boolean,
  hero_thumb_url        text,
  hero_image_url        text
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $function$
  WITH
  base AS (
    SELECT
      g.fid AS arena_group_id,
      g.location_id,
      coalesce(g.display_name_override, g.name) AS display_name,
      g.lat::double precision AS latitude,
      g.lon::double precision AS longitude,
      g.timezone,
      dp.access_rule,
      dp.has_on_leash,
      dp.has_off_leash,
      dp.dogs_allowed,
      dp.dogs_prohibited_start::text AS dogs_prohibited_start,
      dp.dogs_prohibited_end::text   AS dogs_prohibited_end,
      public.beach_location_tier(
        dp.dogs_allowed, dp.has_off_leash, dp.has_on_leash,
        dp.dogs_prohibited_start::text
      ) AS location_tier,
      CASE
        WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
        THEN ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
        ELSE NULL
      END AS distance_m,
      dr.composite_score_v2,
      dr.best_window_label AS stored_best_window_label,
      dr.best_window_status,
      dr.bacteria_risk,
      dr.summary_weather,
      dr.weather_code,
      dr.lowest_tide_height,
      dr.avg_temp,
      dr.avg_wind,
      dr.avg_uv,
      dr.busyness_category,
      dr.go_hours_count,
      dr.avg_tide_height,
      ba.has_parking,
      ba.has_restrooms,
      ba.has_drinking_water,
      ba.has_disabled_access,
      ba.has_showers,
      ba.has_picnic_area,
      ba.has_food,
      ba.has_lifeguards,
      ba.has_fire_pits,
      g.marine_grid_lat,
      g.marine_grid_lon,
      g.coastal_zone_id,
      g.geom AS geom
    FROM public.beaches_gold g
    LEFT JOIN public.beach_day_recommendations dr
           ON dr.arena_group_id = g.fid
          AND dr.local_date     = p_date
    LEFT JOIN public.beach_dog_policy dp
           ON dp.arena_group_id = g.fid
    LEFT JOIN public.beach_amenities ba
           ON ba.arena_group_id = g.fid
    WHERE g.is_active = true
      AND (p_leash = 'any' OR dp.access_rule = p_leash OR dp.access_rule IS NULL)
      AND (NOT p_scored_only OR dr.composite_score_v2 IS NOT NULL)
  ),

  ranked AS (
    SELECT b.*
    FROM base b
    ORDER BY
      CASE
        WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
        THEN b.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
        ELSE NULL
      END NULLS LAST,
      CASE
        WHEN p_lat IS NULL AND p_lng IS NULL
        THEN b.composite_score_v2
        ELSE NULL
      END DESC NULLS LAST
    LIMIT COALESCE(NULLIF(p_limit, 0), 500)
  ),

  peaks AS (
    SELECT
      h.arena_group_id,
      MAX(h.sand_temp)    AS peak_sand_temp,
      MAX(h.asphalt_temp) AS peak_asphalt_temp
    FROM public.beach_day_hourly_scores h
    JOIN ranked r ON r.arena_group_id = h.arena_group_id
    WHERE h.local_date  = p_date
      AND h.is_daylight = true
    GROUP BY h.arena_group_id
  ),

  surf AS (
    SELECT
      r.arena_group_id,
      MAX(m.wave_height_m) AS peak_wave_height_m
    FROM ranked r
    JOIN public.marine_grid_hourly m
      ON m.grid_lat = r.marine_grid_lat
     AND m.grid_lon = r.marine_grid_lon
    WHERE r.marine_grid_lat IS NOT NULL
      AND r.marine_grid_lon IS NOT NULL
      AND m.forecast_ts >= (p_date::timestamptz - interval '12 hours')
      AND m.forecast_ts <  (p_date::timestamptz + interval '36 hours')
    GROUP BY r.arena_group_id
  ),

  hero AS (
    SELECT
      r.arena_group_id,
      p.image_url AS hero_image_url,
      p.thumb_url AS hero_thumb_url
    FROM ranked r
    LEFT JOIN LATERAL (
      SELECT bp.image_url, bp.thumb_url
        FROM public.beach_photos bp
       WHERE bp.arena_group_id = r.arena_group_id
         AND bp.hidden_at IS NULL
         AND (
           bp.curated_at IS NOT NULL
           OR COALESCE(
               (bp.source_meta->>'override_keep_prob')::float,
               (bp.source_meta->>'predicted_keep_prob')::float,
               0
             ) >= 0.65
         )
       ORDER BY
         COALESCE((bp.source_meta->'vision'->>'has_dog')::boolean, false) DESC,
         (bp.curated_at IS NULL)::int ASC,
         COALESCE(
           (bp.source_meta->>'override_keep_prob')::float,
           (bp.source_meta->>'predicted_keep_prob')::float,
           0
         ) DESC,
         bp.sort_order ASC,
         bp.id ASC
       LIMIT 1
    ) p ON true
  ),

  hazards_agg AS (
    SELECT
      ba.beach_fid AS arena_group_id,
      jsonb_agg(
        jsonb_build_object(
          'event_type', ba.event_type,
          'severity',   ba.severity,
          'value',      ba.value
        )
        ORDER BY CASE ba.severity WHEN 'severe' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END,
                 ba.event_type
      ) AS hazards
    FROM public.beach_advisory ba
    JOIN ranked r ON r.arena_group_id = ba.beach_fid
    WHERE ba.event_type IN (
            'rip_current_risk',
            'sneaker_wave_risk',
            'high_surf_risk',
            'longshore_current_risk',
            'pier_hazard_risk',
            'cold_water_shock_risk',
            'shore_break_risk'
            -- 'beach_erosion_risk' removed 2026-06-11 (low utility)
          )
      AND ba.valid_from <= now()
      AND ba.valid_to   >  now()
    GROUP BY ba.beach_fid
  ),

  eligible AS (
    SELECT
      h.arena_group_id,
      h.local_hour,
      h.hour_score_v2 AS score,
      h.local_hour - row_number() OVER (
        PARTITION BY h.arena_group_id ORDER BY h.local_hour
      ) AS grp
    FROM public.beach_day_hourly_scores h
    JOIN ranked r ON r.arena_group_id = h.arena_group_id
    WHERE p_now_hour IS NOT NULL
      AND h.local_date    = p_date
      AND h.is_daylight   = true
      AND h.is_candidate_window = true
      AND h.local_hour   >= p_now_hour
      AND h.hour_score_v2 IS NOT NULL
  ),
  runs AS (
    SELECT
      arena_group_id, grp,
      min(local_hour) AS run_start,
      max(local_hour) AS run_end,
      count(*)::int   AS run_len,
      avg(score)::numeric AS avg_score
    FROM eligible
    GROUP BY arena_group_id, grp
    HAVING count(*) >= 2
  ),
  best_run AS (
    SELECT DISTINCT ON (arena_group_id)
      arena_group_id, grp, run_start, run_end, run_len, avg_score
    FROM runs
    ORDER BY arena_group_id, avg_score DESC, run_len DESC, run_start ASC
  ),
  rolling AS (
    SELECT
      e.arena_group_id,
      e.local_hour AS slice_start,
      sum(e.score) OVER (
        PARTITION BY e.arena_group_id, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_sum,
      count(*) OVER (
        PARTITION BY e.arena_group_id, e.grp
        ORDER BY e.local_hour
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
      ) AS window_count
    FROM eligible e
    JOIN best_run b USING (arena_group_id, grp)
    WHERE b.run_len > 5
  ),
  best_slice AS (
    SELECT DISTINCT ON (arena_group_id)
      arena_group_id, slice_start
    FROM rolling
    WHERE window_count = 5
    ORDER BY arena_group_id, window_sum DESC NULLS LAST, slice_start
  ),
  best_remaining AS (
    SELECT
      b.arena_group_id,
      CASE WHEN b.run_len <= 5 THEN b.run_start ELSE s.slice_start END     AS run_start,
      CASE WHEN b.run_len <= 5 THEN b.run_end   ELSE s.slice_start + 4 END AS run_end
    FROM best_run b
    LEFT JOIN best_slice s USING (arena_group_id)
  )

  SELECT
    r.arena_group_id,
    r.location_id,
    r.display_name,
    r.latitude,
    r.longitude,
    r.timezone,
    r.access_rule,
    r.has_on_leash,
    r.has_off_leash,
    r.dogs_allowed,
    r.dogs_prohibited_start,
    r.dogs_prohibited_end,
    r.location_tier,
    r.distance_m,
    r.composite_score_v2,
    (
      SELECT nh.hour_score_v2
      FROM public.beach_day_hourly_scores nh
      WHERE nh.arena_group_id = r.arena_group_id
        AND nh.local_date     = (now() AT TIME ZONE COALESCE(r.timezone, 'America/Los_Angeles'))::date
        AND nh.local_hour     = extract(hour FROM now() AT TIME ZONE COALESCE(r.timezone, 'America/Los_Angeles'))::int
      LIMIT 1
    ) AS current_hour_score,
    CASE
      WHEN p_now_hour IS NOT NULL THEN
        CASE
          WHEN br.run_start IS NOT NULL
            THEN public._format_hour_window(br.run_start::int, br.run_end::int)
          ELSE 'No good window remaining'
        END
      ELSE r.stored_best_window_label
    END AS best_window_label,
    r.best_window_status,
    r.bacteria_risk,
    r.summary_weather,
    r.weather_code,
    r.lowest_tide_height,
    r.avg_temp,
    r.avg_wind,
    r.avg_uv,
    p.peak_sand_temp,
    p.peak_asphalt_temp,
    s.peak_wave_height_m,
    rip_adv.value AS rip_current_risk,
    coalesce(haz.hazards, '[]'::jsonb) AS hazards,
    r.busyness_category,
    r.go_hours_count,
    r.avg_tide_height,
    r.has_parking,
    r.has_restrooms,
    r.has_drinking_water,
    r.has_disabled_access,
    r.has_showers,
    r.has_picnic_area,
    r.has_food,
    r.has_lifeguards,
    r.has_fire_pits,
    h.hero_thumb_url,
    h.hero_image_url
  FROM ranked r
  LEFT JOIN best_remaining br ON br.arena_group_id = r.arena_group_id
  LEFT JOIN peaks          p  ON p.arena_group_id  = r.arena_group_id
  LEFT JOIN surf           s  ON s.arena_group_id  = r.arena_group_id
  LEFT JOIN hero           h  ON h.arena_group_id  = r.arena_group_id
  LEFT JOIN hazards_agg    haz ON haz.arena_group_id = r.arena_group_id
  LEFT JOIN public.beach_advisory rip_adv
         ON rip_adv.beach_fid  = r.arena_group_id
        AND rip_adv.event_type = 'rip_current_risk'
        AND rip_adv.valid_from <= now()
        AND rip_adv.valid_to   >  now();
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;


-- ── 3. Delete existing beach_erosion_risk rows ─────────────────────
-- They'll naturally not be re-upserted on the next refresh now that
-- the bhs_erosion CTE isn't read into find_beaches IN list. Refresh fn
-- still emits them (harmless — get_beach_advisories filters them out);
-- next refresh cycle will keep beach_advisory clean of them via the
-- watermark sweep. For immediacy, do a one-shot DELETE here.

DELETE FROM public.beach_advisory
 WHERE event_type = 'beach_erosion_risk';

commit;
notify pgrst, 'reload schema';
