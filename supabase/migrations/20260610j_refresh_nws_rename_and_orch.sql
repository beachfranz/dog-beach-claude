-- 20260610j_refresh_nws_rename_and_orch.sql
--
-- Two changes:
--
--   1. Helper RPCs for the active-alerts PIP. The refresh-nws edge fn
--      doesn't push raw geometry around — it calls these SQL fns which
--      do ST_Intersects DB-side scoped to a state.
--
--   2. Rename orch_jobs.refresh_nws_srf → refresh_nws (matches the new
--      edge fn name). Cadence shifts from daily 14:00 UTC to hourly @:00
--      so AQ / heat / beach-hazard NWS alerts fire within an hour of
--      issuance. Downstream jobs (rebind_coastal_zones,
--      refresh_rip_current_advisories) shift to hourly_at offset so they
--      run after refresh_nws each hour.

begin;

-- ───────────────────────────────────────────────────────────────────────
-- 1. Helper RPCs
-- ───────────────────────────────────────────────────────────────────────

-- Path 1: inline geometry. Caller passes the alert's geometry as a
-- GeoJSON string; we ST_Intersects against active scoreable beaches in
-- the target state.
create or replace function public._nws_alert_pip_beaches(
  p_geom_geojson text,
  p_state        text
) returns bigint[]
language sql
stable
security definer
set search_path = 'public'
as $$
  select coalesce(array_agg(b.fid), array[]::bigint[])
    from public.beaches_gold b
   where b.is_active = true
     and b.scoring_tier in ('daily','hourly')
     and b.state = upper(p_state)
     and b.geom is not null
     and st_intersects(
           b.geom,
           st_setsrid(st_geomfromgeojson(p_geom_geojson), 4326)
         );
$$;

grant execute on function public._nws_alert_pip_beaches(text, text)
  to service_role;

comment on function public._nws_alert_pip_beaches is
  'Return beach_fids in p_state whose geom intersects an alert geometry '
  'passed as GeoJSON. Called by refresh-nws edge fn for alerts with '
  'inline GeoJSON geometry. Scoped to is_active + scoring_tier in '
  '(daily,hourly).';


-- Path 2: zone-coded. Caller passes a zone_id (already cached in
-- nws_zones); we ST_Intersects against the cached polygon.
create or replace function public._nws_alert_pip_via_zone(
  p_zone_id text,
  p_state   text
) returns bigint[]
language sql
stable
security definer
set search_path = 'public'
as $$
  select coalesce(array_agg(b.fid), array[]::bigint[])
    from public.beaches_gold b
    join public.nws_zones z on z.zone_id = p_zone_id
   where b.is_active = true
     and b.scoring_tier in ('daily','hourly')
     and b.state = upper(p_state)
     and b.geom is not null
     and z.geom is not null
     and st_intersects(b.geom, z.geom);
$$;

grant execute on function public._nws_alert_pip_via_zone(text, text)
  to service_role;

comment on function public._nws_alert_pip_via_zone is
  'Return beach_fids in p_state whose geom intersects a cached '
  'nws_zones.geom. Path 2 for refresh-nws when an alert lacks inline '
  'geometry (NWS alert ships affectedZones=[urls] only). Zone is lazily '
  'warmed before this is called.';


-- ───────────────────────────────────────────────────────────────────────
-- 2. orch_jobs: rename + retime
-- ───────────────────────────────────────────────────────────────────────

-- a. The fetcher itself. Rename refresh_nws_srf → refresh_nws + go hourly.
-- orch_runs has an FK to orch_jobs.job_name and the old refresh_nws_srf
-- row is still referenced by historical runs. Can't UPDATE in either
-- order because of the FK. So: INSERT the new row, MIGRATE the run
-- history pointer, DELETE the old row.
insert into public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
)
select
  'refresh_nws',
  'Hourly @:00: unified NWS fetcher. Phase 1 — per-WFO SRF text product → wfo_srf_forecast. Phase 2 — per-state api.weather.gov/alerts/active → beach_advisory source=nws. Phase 3 — watermark sweep on source=nws rows older than this run''s started_at.',
  'hourly_at',
  '00',
  null,
  array[]::text[],
  null,
  cost_check_fn,
  skip_if_succeeded_within,
  worker_kind,
  'refresh-nws',
  '{}'::jsonb,
  scope_states,
  enabled,
  shadow_mode
from public.orch_jobs
where job_name = 'refresh_nws_srf'
on conflict (job_name) do nothing;

-- Repoint history. With both rows present, the FK update is safe.
update public.orch_runs
   set job_name = 'refresh_nws'
 where job_name = 'refresh_nws_srf';

-- Drop the old row (no longer referenced).
delete from public.orch_jobs
 where job_name = 'refresh_nws_srf';

-- b. Coastal-zone rebind shifts hourly @:05 + depends on the new job
update public.orch_jobs
   set depends_on    = array['refresh_nws']::text[],
       cadence_kind  = 'hourly_at',
       cadence_param = '05',
       description   = 'Hourly @:05: rebind beaches_gold.coastal_zone_id for beaches with NULL binding in coastal states. Picks up newly-warmed nws_zones from refresh_nws. Chunked SQL-function worker — sidesteps the PostgREST 60s cap.'
 where job_name = 'rebind_coastal_zones';

-- c. Rip current advisories shift hourly @:10 + depends on the new job
update public.orch_jobs
   set depends_on    = array['refresh_nws', 'rebind_coastal_zones']::text[],
       cadence_kind  = 'hourly_at',
       cadence_param = '10',
       description   = 'Hourly @:10: fire 🚩 Rip Current Risk beach_advisory rows from wfo_srf_forecast. Moderate / High only; Low suppressed.'
 where job_name = 'refresh_rip_current_advisories';

commit;
