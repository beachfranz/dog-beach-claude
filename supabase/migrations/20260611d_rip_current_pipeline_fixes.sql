-- 20260611d_rip_current_pipeline_fixes.sql
--
-- Two bugs in the rip-current chain, surfaced when Franz challenged "0
-- rip_current advisories today" earlier:
--
-- BUG A (HIGH): refresh_rip_current_advisories filters
--   `srf.forecast_date = current_date`. SRF rows are tagged with the
--   WFO's LOCAL today date; `current_date` in the DB is UTC. Between
--   UTC midnight and Pacific midnight (~7 hours/day), the filter misses
--   even though valid SRF data exists. Today at UTC 02:24 / Pacific
--   7:24pm: SRF has 145 Moderate + 24 High zones tagged
--   forecast_date='2026-06-11'; current_date='2026-06-12'; 0 matches.
--   Fix: pick the latest SRF row per zone within last 2 UTC days via
--   DISTINCT ON. Also widen the valid_from/valid_to window so the
--   inserted advisory doesn't land already-expired during UTC night.
--
-- BUG B (LOW): _orch_w_rebind_coastal_zones uses ST_DWithin with
--   `z.geom::geography` cast. The GIST index on nws_zones.geom is over
--   geometry, not geography — the cast forces a sequential scan over
--   2,567 forecast zones per beach. At ~0.6 sec/beach, a 200-beach
--   chunk exceeds the 2-min orch_tick statement_timeout silently
--   (orch_tick aborts, never persists the orch_runs row, never updates
--   last_attempted_at on orch_jobs). Caused every :05 fire since deploy
--   to time out. Fix: use pure geometry with 0.0018-degree buffer
--   (≈200m at mid-lat) per [[pip-for-places-uses-200m]] HARD rule, and
--   the `<->` KNN operator that uses the GIST index. Today's manual
--   backfill (geometry mode) drained 1,479 coastal beaches in <1 sec.

begin;

-- ─── Bug A: refresh_rip_current_advisories ────────────────────────────

CREATE OR REPLACE FUNCTION public.refresh_rip_current_advisories(
  p_state text   DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now      timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  WITH
  -- Latest SRF per zone within last 2 UTC days. Covers WFOs west of
  -- UTC (Pacific/HI), whose forecast_date stays at "today local" even
  -- after UTC midnight rolls over.
  latest_srf AS (
    SELECT DISTINCT ON (zone_id)
      zone_id, wfo, forecast_date, period, rip_current_risk
    FROM public.wfo_srf_forecast
    WHERE period = 'current'
      AND forecast_date >= current_date - 1
    ORDER BY zone_id, forecast_date DESC, fetched_at DESC
  ),
  ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      bg.fid,
      format('rip:rip_current_risk:%s', srf.forecast_date),
      'rip_current',
      'rip_current_risk',
      CASE srf.rip_current_risk
        WHEN 'High'     THEN 'severe'
        WHEN 'Moderate' THEN 'moderate'
      END,
      -- Generous valid window: covers WFOs east and west of UTC, plus
      -- handles the hourly refresh cadence (each row stays "current"
      -- for ~12h until the next SRF supersedes it).
      v_now - interval '6 hours',
      v_now + interval '12 hours',
      'skip_swim',
      CASE srf.rip_current_risk
        WHEN 'High'     THEN
          'NWS High rip current risk in effect — life-threatening rip currents likely. Keep her on the dry sand; no swimming or fetching in the surf.'
        WHEN 'Moderate' THEN
          'NWS Moderate rip current risk today — life-threatening rip currents are possible in the surf zone. Stick to the shore.'
      END,
      'rule',
      'Rip current',
      srf.rip_current_risk,
      '🚩',
      jsonb_build_object(
        'zone_id',       srf.zone_id,
        'wfo',           srf.wfo,
        'period',        srf.period,
        'risk',          srf.rip_current_risk,
        'forecast_date', srf.forecast_date
      ),
      v_now
    FROM public.beaches_gold bg
    JOIN latest_srf srf ON srf.zone_id = bg.coastal_zone_id
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND bg.coastal_zone_id IS NOT NULL
      AND bg.coastal_zone_id <> '__no_zone__'
      AND srf.rip_current_risk IN ('Moderate', 'High')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity        = EXCLUDED.severity,
      valid_from      = EXCLUDED.valid_from,
      valid_to        = EXCLUDED.valid_to,
      dog_impact_text = EXCLUDED.dog_impact_text,
      value           = EXCLUDED.value,
      raw_data        = EXCLUDED.raw_data,
      fetched_at      = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- Watermark sweep: drop rip rows not refreshed this pass (zone risk
  -- dropped, beach binding cleared, or SRF stopped publishing).
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source    = 'rip_current'
      AND bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;


-- ─── Bug B: _orch_w_rebind_coastal_zones ─────────────────────────────
-- Switch from geography cast (sequential scan) to pure geometry +
-- ~0.0018 deg buffer (≈200m at mid-lat) + KNN `<->` operator.
-- Both ST_DWithin and the ORDER BY now use the existing GIST index
-- (nws_zones_geom_gix).

CREATE OR REPLACE FUNCTION public._orch_w_rebind_coastal_zones(
  p jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
SET statement_timeout = '8min'
AS $$
DECLARE
  v_total bigint := 0;
  v_chunk bigint;
  v_iter  int := 0;
  v_start timestamptz := clock_timestamp();
BEGIN
  -- 45s wall-clock ceiling for safety inside orch_tick's 2-min cap.
  -- Geometry mode is ~100× faster than the old geography cast — full
  -- coastal backlog (~2,000 beaches) typically drains in one iteration.
  LOOP
    v_iter := v_iter + 1;
    EXIT WHEN v_iter > 200;
    EXIT WHEN clock_timestamp() - v_start > interval '45 sec';

    UPDATE public.beaches_gold bg
       SET coastal_zone_id = COALESCE(sub.zone_id, '__no_zone__')
      FROM (
        SELECT bg2.fid,
               (SELECT z.zone_id
                  FROM public.nws_zones z
                 WHERE z.zone_type = 'forecast'
                   AND z.geom IS NOT NULL
                   AND ST_DWithin(z.geom,
                         ST_SetSRID(ST_MakePoint(bg2.lon::float, bg2.lat::float), 4326),
                         0.0018)
                 ORDER BY z.geom <-> ST_SetSRID(ST_MakePoint(bg2.lon::float, bg2.lat::float), 4326)
                 LIMIT 1) AS zone_id
          FROM public.beaches_gold bg2
         WHERE bg2.is_active = true
           AND bg2.coastal_zone_id IS NULL
           AND bg2.lat IS NOT NULL AND bg2.lon IS NOT NULL
           AND bg2.state IN ('CA','OR','WA','HI','MA','RI','NH','ME','MD','VA','DE',
                             'NC','SC','GA','FL','AL','MS','LA','TX','NJ','NY','CT')
         ORDER BY bg2.state, bg2.fid
         LIMIT 200
      ) sub
     WHERE bg.fid = sub.fid;
    GET DIAGNOSTICS v_chunk = ROW_COUNT;

    v_total := v_total + v_chunk;
    EXIT WHEN v_chunk = 0;
  END LOOP;

  RETURN v_total;
END $$;

COMMENT ON FUNCTION public._orch_w_rebind_coastal_zones(jsonb) IS
  'Orch wrapper for coastal-zone rebind. Geometry mode (GIST-indexed) — '
  '~100× faster than the original 20260610h geography-cast version which '
  'timed out at 2 min inside orch_tick. Chunked-iteration UPDATE (200 '
  'beaches per loop, 45s wall-clock ceiling). Idempotent: scopes to '
  'coastal_zone_id IS NULL. Payload p ignored.';

commit;
notify pgrst, 'reload schema';
