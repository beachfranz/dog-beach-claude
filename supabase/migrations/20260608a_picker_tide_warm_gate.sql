-- 20260608a — beaches_due_for_refresh: gate on tide-station warmth
--
-- WHY: 1,074 beaches across 13 states landed with NULL tide_height for
-- the full 7-day window. Root cause: race between weekly_tide_refresh
-- and daily-beach-refresh. The weekly cron's orch wrapper sets
-- last_succeeded_at on dispatch (fire-and-forget), but the actual
-- per-station fetch only completed at 2026-06-08 02:39 UTC (~18.5h
-- after the Sunday 08:00 cron fire). daily-beach-refresh ran during
-- the cold window — lookupTidesFromGrid returned new Map() with no
-- fallback, no error logged, NULL tides written for the whole horizon.
--
-- This is the same shape as [[grid-consumers-require-cell-warmth-gate]]
-- — solved for weather/marine cells in 20260606ze/zf/zg, never ported
-- to tide. Mirroring the weather pattern: add a `tide_warm` CTE that
-- requires either no station (legit inland) or `tide_station_is_warm`
-- in the next 24h.
--
-- Threshold: 20 of next-24h rows. Matches tide_station_is_warm() default
-- and the weather gate's ≥22/24h logic. Tide grid has 14-day horizon so
-- the threshold is comfortable except in the post-cron settling window
-- this gate is meant to skip.
--
-- Dog-park sibling check per [[paired-functions-port-fixes-both-sides]]:
-- dog_parks_gold has no noaa_station_id (dog parks don't read tide).
-- Sibling exempt — beach-only fix.
--
-- The bulk SQL fn refresh_beach_day_hourly_scores_bulk (currently
-- shadow_mode) reads from the same picker, so it benefits transparently.

CREATE OR REPLACE FUNCTION public.beaches_due_for_refresh(
  p_target_fids         bigint[] DEFAULT NULL,
  p_target_location_ids text[]   DEFAULT NULL,
  p_skip_recent_hours   integer  DEFAULT NULL,
  p_limit               integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamptz)
LANGUAGE sql
STABLE
AS $function$
  WITH eligible AS (
    SELECT bg.fid,
           bg.weather_grid_lat,
           bg.weather_grid_lon,
           bg.noaa_station_id
    FROM public.beaches_gold bg
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_target_fids IS NULL OR bg.fid = ANY(p_target_fids))
      AND (p_target_location_ids IS NULL
           OR bg.location_id = ANY(p_target_location_ids))
  ),
  grid_warm AS (
    SELECT e.fid
    FROM eligible e
    JOIN public.weather_grid_hourly wgh
      ON wgh.grid_lat = e.weather_grid_lat
     AND wgh.grid_lon = e.weather_grid_lon
     AND wgh.forecast_ts >  now()
     AND wgh.forecast_ts <= now() + interval '24 hours'
    GROUP BY e.fid
    HAVING count(*) >= 22    -- ≥92% of next 24h in this cell
  ),
  tide_warm AS (
    SELECT e.fid
    FROM eligible e
    WHERE e.noaa_station_id IS NULL    -- inland: tide-neutral, gate passes
       OR public.tide_station_is_warm(e.noaa_station_id, 24)
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.beach_day_recommendations r
      ON r.arena_group_id = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  JOIN grid_warm gw ON gw.fid = f.fid
  JOIN tide_warm tw ON tw.fid = f.fid
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$function$;
