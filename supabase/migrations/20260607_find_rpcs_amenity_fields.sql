-- 20260607_find_rpcs_amenity_fields.sql
--
-- Extend find_beaches + nearest_dog_parks to surface amenity booleans the
-- new find.html consumer reads for its amenity hard-filter + match-dot strip.
--
-- Trigger: find.html ships Parking / Restrooms / Water / Shade chips. They
-- act as hard filters (composite_score_v2 already weights amenities — the
-- chip is "show me only ones with X," not a soft re-rank). Today the RPCs
-- return no amenity flags for beaches, so the chips no-op on the beach side.
--
-- Per HARD rule [[paired-functions-port-fixes-both-sides]] — fixing
-- find_beaches without sweeping nearest_dog_parks would leave the Shade
-- chip half-broken (works on beaches → never matches; works on dog-parks
-- → unknown because RPC doesn't return has_shade). Do both in one migration.
--
-- Beach amenities live on public.beach_amenities (FK arena_group_id):
--   has_restrooms, has_showers, has_lifeguards, has_drinking_water,
--   has_disabled_access, has_food, has_fire_pits, has_picnic_area,
--   has_parking. (Confirmed via information_schema 2026-06-07.) No
--   has_shade — that's dog-park-only.
--
-- Dog-park amenities live on public.dog_park_dog_policy (FK dog_park_fid):
--   has_fence, has_drinking_water, has_picnic_tables, has_agility,
--   has_shade, has_water_play. The RPC already joins this table, so the
--   added columns add zero new joins.
--
-- Return-table signature is changing on both functions → DROP + CREATE
-- (CREATE OR REPLACE refuses signature drift).

-- ─────────────────────────────────────────────────────────────────────────
-- 1. find_beaches — add LEFT JOIN beach_amenities + 9 amenity columns
-- ─────────────────────────────────────────────────────────────────────────
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
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  location_tier         text,
  distance_m            double precision,
  composite_score_v2    integer,
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric,
  -- Amenity booleans (NEW 2026-06-07) — left-joined from beach_amenities.
  -- NULL when no amenity row exists for the beach (i.e., not yet enriched);
  -- consumer treats NULL as "unknown, pass through" on chip filtering.
  has_parking           boolean,
  has_restrooms         boolean,
  has_drinking_water    boolean,
  has_disabled_access   boolean,
  has_showers           boolean,
  has_picnic_area       boolean,
  has_food              boolean,
  has_lifeguards        boolean,
  has_fire_pits         boolean
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
      dp.access_rule,
      dp.has_on_leash,
      dp.has_off_leash,
      dp.dogs_allowed,
      dp.dogs_prohibited_start::text AS dogs_prohibited_start,
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

  remaining_islands AS (
    SELECT
      h.arena_group_id,
      h.local_hour,
      h.local_hour - row_number() OVER (
        PARTITION BY h.arena_group_id ORDER BY h.local_hour
      ) AS grp
    FROM public.beach_day_hourly_scores h
    WHERE p_now_hour IS NOT NULL
      AND h.local_date = p_date
      AND h.is_daylight = true
      AND h.is_candidate_window = true
      AND h.local_hour >= p_now_hour
  ),
  remaining_runs AS (
    SELECT
      arena_group_id,
      grp,
      min(local_hour) AS run_start,
      max(local_hour) AS run_end,
      count(*) AS run_len
    FROM remaining_islands
    GROUP BY arena_group_id, grp
  ),
  best_remaining AS (
    SELECT DISTINCT ON (arena_group_id)
      arena_group_id, run_start, run_end
    FROM remaining_runs
    ORDER BY arena_group_id, run_len DESC, run_start ASC
  )

  SELECT
    b.arena_group_id,
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    b.access_rule,
    b.has_on_leash,
    b.has_off_leash,
    b.dogs_allowed,
    b.dogs_prohibited_start,
    b.location_tier,
    b.distance_m,
    b.composite_score_v2,
    CASE
      WHEN p_now_hour IS NOT NULL THEN
        CASE
          WHEN br.run_start IS NOT NULL
            THEN public._format_hour_window(br.run_start::int, br.run_end::int)
          ELSE 'No good window remaining'
        END
      ELSE b.stored_best_window_label
    END AS best_window_label,
    b.best_window_status,
    b.bacteria_risk,
    b.summary_weather,
    b.weather_code,
    b.lowest_tide_height,
    b.avg_temp,
    b.avg_wind,
    b.busyness_category,
    b.go_hours_count,
    b.avg_tide_height,
    b.has_parking,
    b.has_restrooms,
    b.has_drinking_water,
    b.has_disabled_access,
    b.has_showers,
    b.has_picnic_area,
    b.has_food,
    b.has_lifeguards,
    b.has_fire_pits
  FROM base b
  LEFT JOIN best_remaining br ON br.arena_group_id = b.arena_group_id
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
  LIMIT COALESCE(NULLIF(p_limit, 0), 500);
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;


-- ─────────────────────────────────────────────────────────────────────────
-- 2. nearest_dog_parks — surface has_shade + has_picnic_tables +
--    has_water_play + has_agility from dog_park_dog_policy (already joined)
-- ─────────────────────────────────────────────────────────────────────────
DROP FUNCTION IF EXISTS public.nearest_dog_parks(double precision, double precision, integer, text, date);

CREATE OR REPLACE FUNCTION public.nearest_dog_parks(
  p_lat   double precision DEFAULT NULL,
  p_lng   double precision DEFAULT NULL,
  p_limit integer          DEFAULT 10,
  p_state text             DEFAULT NULL,
  p_date  date             DEFAULT NULL
)
RETURNS TABLE (
  fid                  bigint,
  name                 text,
  state                text,
  address_city         text,
  lat                  double precision,
  lon                  double precision,
  distance_miles       double precision,
  has_fence            boolean,
  has_drinking_water   boolean,
  surface              text,
  leash_policy         text,
  hours_text           text,
  hours_open_time      text,
  hours_close_time     text,
  additional_rules     text,
  source               text,
  source_url           text,
  consensus_confidence numeric,
  operator_id          bigint,
  operator_name        text,
  operator_short       text,
  composite_score      numeric,
  composite_score_v2   integer,
  best_window_label    text,
  best_window_status   text,
  go_hours_count       integer,
  summary_weather      text,
  weather_code         integer,
  avg_temp             numeric,
  avg_wind             numeric,
  score_v2             integer,
  score_v2_components  jsonb,
  drive_minutes        integer,
  -- Amenity booleans (NEW 2026-06-07) — all from dog_park_dog_policy.
  has_shade            boolean,
  has_picnic_tables    boolean,
  has_water_play       boolean,
  has_agility          boolean
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
  WITH user_pt AS (
    SELECT CASE WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
                ELSE NULL::geography END AS pt,
           COALESCE(p_date, CURRENT_DATE) AS d
  )
  SELECT
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    CASE WHEN up.pt IS NOT NULL
         THEN ST_Distance(up.pt, g.geom::geography) / 1609.344
         ELSE NULL::double precision
    END AS distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.canonical_name AS operator_name, o.short_name AS operator_short,
    hs.composite_score,
    dr.composite_score_v2,
    dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    (sv.payload->>'score')::int          AS score_v2,
    sv.payload                            AS score_v2_components,
    (sv.payload->>'drive_minutes')::int  AS drive_minutes,
    p.has_shade,
    p.has_picnic_tables,
    p.has_water_play,
    p.has_agility
  FROM public.dog_parks_gold g
  JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
  CROSS JOIN user_pt up
  LEFT JOIN public.operators o ON o.id = p.operator_id AND o.is_canonical = true
  LEFT JOIN public.dog_park_day_recommendations dr
         ON dr.dog_park_fid = g.fid AND dr.local_date = up.d
  LEFT JOIN LATERAL (
    SELECT round(avg(h.hour_score_v2)::numeric, 0) AS composite_score
      FROM public.dog_park_day_hourly_scores h
     WHERE h.dog_park_fid = g.fid
       AND h.local_date   = up.d
       AND h.hour_score_v2 IS NOT NULL
  ) hs ON true
  LEFT JOIN LATERAL (
    SELECT public.compute_dog_park_score_v2(g.fid, p_lat, p_lng, up.d) AS payload
  ) sv ON true
  WHERE g.is_active = true
    AND (p_state IS NULL OR g.state = p_state)
  ORDER BY
    CASE WHEN up.pt IS NOT NULL THEN ST_Distance(up.pt, g.geom::geography) END NULLS LAST,
    COALESCE((sv.payload->>'score')::int, 0) DESC,
    g.name
  LIMIT GREATEST(COALESCE(p_limit, 10), 1);
$function$;

GRANT EXECUTE ON FUNCTION public.nearest_dog_parks(
  double precision, double precision, integer, text, date
) TO anon, authenticated;
