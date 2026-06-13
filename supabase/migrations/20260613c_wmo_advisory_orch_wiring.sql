-- 20260613c_wmo_advisory_orch_wiring.sql
--
-- Wire refresh_beach_wmo_advisories + refresh_dog_park_wmo_advisories
-- (from 20260613a) into the orch tick. Two wrappers + two orch_jobs
-- rows. Cadence + depends_on mirror the existing daily_weather_
-- advisories / daily_dog_park_advisories pattern (both depend on the
-- per-hour score writers since weather_code is read from those tables).
--
-- Minute placement: 15 (beach) / 17 (dog-park) — free slots not used
-- by daily_weather_advisories (10), refresh_rip_current_advisories
-- (10), daily_dog_park_advisories (12), or hourly_marine_advisories
-- (20).

BEGIN;

-- ─── 1. Beach wrapper ────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_refresh_beach_wmo_advisories(p jsonb)
RETURNS bigint
LANGUAGE plpgsql AS $function$
DECLARE
  v_state    text;
  v_only_fid bigint;
  v_upserted bigint;
  v_retired  bigint;
BEGIN
  v_state    := p->>'state';
  v_only_fid := nullif(p->>'only_fid', '')::bigint;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_beach_wmo_advisories(v_state, v_only_fid);

  RAISE NOTICE 'beach_wmo_advisories: state=% fid=% upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_upserted, v_retired;
  RETURN NULL;
END
$function$;

-- ─── 2. Dog-park wrapper ─────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_refresh_dog_park_wmo_advisories(p jsonb)
RETURNS bigint
LANGUAGE plpgsql AS $function$
DECLARE
  v_state    text;
  v_only_fid bigint;
  v_upserted bigint;
  v_retired  bigint;
BEGIN
  v_state    := p->>'state';
  v_only_fid := nullif(p->>'only_fid', '')::bigint;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_dog_park_wmo_advisories(v_state, v_only_fid);

  RAISE NOTICE 'dog_park_wmo_advisories: state=% fid=% upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_upserted, v_retired;
  RETURN NULL;
END
$function$;

-- ─── 3. orch_jobs registrations ──────────────────────────────────────
INSERT INTO public.orch_jobs (
  job_name, description,
  cadence_kind, cadence_param,
  depends_on, depends_max_age,
  worker_kind, worker_target, worker_payload,
  enabled
)
VALUES
  ('daily_beach_wmo_advisories',
   'Categorical WMO weather-code cautions (fog / lightning / snow / heavy_rain / freezing) for beaches. Reads beach_day_hourly_scores.weather_code; writes deterministic_wmo rows to beach_advisory.',
   'hourly_at', '15',
   ARRAY['beach_refresh_chunked'], '36:00:00',
   'sql_function', 'public._orch_w_refresh_beach_wmo_advisories', '{}'::jsonb,
   true),
  ('daily_dog_park_wmo_advisories',
   'Categorical WMO weather-code cautions for dog parks. Reads dog_park_day_hourly_scores.weather_code; writes deterministic_wmo rows to dog_park_advisory.',
   'hourly_at', '17',
   ARRAY['dog_park_refresh_chunked'], '36:00:00',
   'sql_function', 'public._orch_w_refresh_dog_park_wmo_advisories', '{}'::jsonb,
   true)
ON CONFLICT (job_name) DO UPDATE SET
  description     = EXCLUDED.description,
  cadence_kind    = EXCLUDED.cadence_kind,
  cadence_param   = EXCLUDED.cadence_param,
  depends_on      = EXCLUDED.depends_on,
  depends_max_age = EXCLUDED.depends_max_age,
  worker_target   = EXCLUDED.worker_target,
  worker_payload  = EXCLUDED.worker_payload,
  enabled         = EXCLUDED.enabled,
  updated_at      = now();

COMMIT;
