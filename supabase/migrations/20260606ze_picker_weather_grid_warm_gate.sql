-- 20260606ze_picker_weather_grid_warm_gate.sql
--
-- Race: weather_grid loader vs daily-beach-refresh / daily-dog-park-refresh.
-- The grid loader fans out per-cell Open-Meteo fetches across batches over
-- minutes-to-hours (retries + tier scheduling). If a refresh fires DURING
-- the load, the per-cell forecast set is partial — daily refresh sees
-- weather_for_point() returning < 168 hours and silently writes partial
-- beach_day_hourly_scores. HI fid 13882 hit this on 2026-06-07: 24-hour
-- hole (UTC 2026-06-08 entirely missing) producing 1PM-end-of-day bar
-- charts.
--
-- W2 architecture (2026-05-25) deliberately collapsed ~16,500 Open-Meteo
-- calls/day → <100/day via the grid reference layer. A fallback to direct
-- per-beach Open-Meteo would undo that on every fresh-state launch (304
-- new HI cells = 304 extra direct fetches). Wrong shape.
--
-- Right shape: gate. The picker that selects beaches/dog-parks for refresh
-- skips entities whose grid cell is mid-load. Self-healing: once the cell
-- finishes loading, the picker schedules the entity next tick. Beach +
-- dog-park siblings updated together per [[paired-functions-port-fixes-both-sides]].
--
-- Threshold = 45 forecast rows in cell within [now, now+48h] = 94% of
-- the next 2 days. Tier-1 (next-48h, hourly cron) is responsible for
-- this near-term window; healthy cells carry all 48 hours within minutes
-- of t1 firing. A 168-row 7-day floor was tried first but flickered on
-- CA / MA cells whose t3 (12h schedule) hadn't yet rolled into the
-- forecast_days=7 horizon — false negatives on cells that were perfectly
-- ready for daily refresh. The near-48h gate is what daily refresh
-- actually depends on (forecast bars on day 1 = today/tomorrow), not
-- forecast horizon tail.
--
-- Brand-new cells (loader hasn't run at all) carry 0 forecast rows → gated.
-- The cron-side _orch_w_chunked_refresh_freshness_audit catches the case
-- where a cell stays cold for more than the staleness threshold.

BEGIN;

-- ──── beaches_due_for_refresh ────
CREATE OR REPLACE FUNCTION public.beaches_due_for_refresh(
  p_target_fids         bigint[] DEFAULT NULL,
  p_target_location_ids text[]   DEFAULT NULL,
  p_skip_recent_hours   integer  DEFAULT NULL,
  p_limit               integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamp with time zone)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT bg.fid, bg.weather_grid_lat, bg.weather_grid_lon
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
     AND wgh.forecast_ts <= now() + interval '48 hours'
    GROUP BY e.fid
    HAVING count(*) >= 45    -- ≥94% of next 48h in this cell
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
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

-- ──── dog_parks_due_for_refresh ────
CREATE OR REPLACE FUNCTION public.dog_parks_due_for_refresh(
  p_target_fids       bigint[] DEFAULT NULL,
  p_state_filter      text[]   DEFAULT NULL,
  p_skip_recent_hours integer  DEFAULT NULL,
  p_limit             integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamp with time zone)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT dpg.fid, dpg.weather_grid_lat, dpg.weather_grid_lon
    FROM public.dog_parks_gold dpg
    WHERE dpg.is_active
      AND dpg.is_scoreable
      AND (p_target_fids IS NULL OR dpg.fid = ANY(p_target_fids))
      AND (p_state_filter IS NULL OR dpg.state = ANY(p_state_filter))
  ),
  grid_warm AS (
    SELECT e.fid
    FROM eligible e
    JOIN public.weather_grid_hourly wgh
      ON wgh.grid_lat = e.weather_grid_lat
     AND wgh.grid_lon = e.weather_grid_lon
     AND wgh.forecast_ts > now()
    GROUP BY e.fid
    HAVING count(*) >= 144
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.dog_park_day_recommendations r
      ON r.dog_park_fid = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  JOIN grid_warm gw ON gw.fid = f.fid
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

COMMIT;
