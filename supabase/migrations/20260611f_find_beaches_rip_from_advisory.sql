-- 20260611f_find_beaches_rip_from_advisory.sql
--
-- Repoint find_beaches RPC's rip_current_risk source from
-- wfo_srf_forecast (direct read) to public.beach_advisory (merged row).
--
-- Why: 20260611e extended the rip_current pipeline to merge SRF + BHS
-- body extraction into a single beach_advisory row per beach. The
-- merged row is in beach_advisory with event_type='rip_current_risk'.
-- find_beaches was still reading from wfo_srf_forecast directly,
-- missing 148 beaches whose only signal comes from BHS extraction
-- (no SRF zone match) — so they showed in the cautions card on
-- beach.html but NOT as a list-view 🚩 chip on find.html.
--
-- Bonus side benefit: the new join uses valid_from/valid_to (timezone-
-- clean) instead of forecast_date=p_date (which was vulnerable to the
-- same UTC vs WFO-local timezone bug that bit refresh_rip_current_
-- advisories yesterday).
--
-- The returned column shape is unchanged: text 'High' or 'Moderate'
-- (rip_adv.value carries these exact strings from refresh_rip_current_
-- advisories). Frontend line 2314 of find.html renders unchanged.

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
    -- Repointed 2026-06-11 (this migration): read from the merged
    -- rip_current_risk advisory in beach_advisory instead of straight
    -- from wfo_srf_forecast. Catches BHS-extracted rip current that
    -- has no SRF backing — 148 additional severe beaches today.
    rip_adv.value AS rip_current_risk,
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
  LEFT JOIN public.beach_advisory rip_adv
         ON rip_adv.beach_fid  = r.arena_group_id
        AND rip_adv.event_type = 'rip_current_risk'
        AND rip_adv.valid_from <= now()
        AND rip_adv.valid_to   >  now();
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;
