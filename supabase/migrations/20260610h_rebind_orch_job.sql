-- 20260610h_rebind_orch_job.sql
--
-- Wire the coastal-zone rebind into orch_jobs so it fires daily after
-- refresh_nws_srf, picking up beaches in newly-warmed coastal zones.
--
-- Why a separate orch step (not inline in refresh-nws-srf): the rebind
-- UPDATE does a geography KNN per unbound beach against ~150 coastal
-- forecast zones. For initial bootstrap (~3,500 unbound beaches across
-- 20+ coastal states), this exceeds both the edge fn 150s timeout AND
-- the PostgREST 60s wall-clock cap. Pure SQL-function workers invoked
-- by orch_tick get the 10min statement_timeout instead — plenty of
-- headroom even on first run.
--
-- Chunked-iteration shape: process 200 beaches at a time inside a loop,
-- bail at 7min elapsed or when an iteration changes 0 rows. Keeps the
-- function below the orch_tick 10min cap and idempotent — re-running on
-- a fully-bound catalog returns 0 quickly.

begin;

create or replace function public._orch_w_rebind_coastal_zones()
returns bigint
language plpgsql
security definer
set search_path = 'public'
set statement_timeout = '8min'
as $$
declare
  v_total bigint := 0;
  v_chunk bigint;
  v_iter  int := 0;
  v_start timestamptz := clock_timestamp();
begin
  -- When invoked via PostgREST (supabase db query) the client-side
  -- connection caps out around 50-60s — anything over that aborts even
  -- though the DB statement_timeout is 8min. So a single call bails at
  -- 45s. Orch_tick (pg_cron path) has no client wall-clock; chained
  -- calls drain the backlog over consecutive daily ticks.
  loop
    v_iter := v_iter + 1;
    exit when v_iter > 200;                                   -- hard ceiling
    exit when clock_timestamp() - v_start > interval '45 sec';

    -- COALESCE to a sentinel '__no_zone__' when no forecast zone is
    -- within 200m. Without this, UPDATE writes NULL→NULL but ROW_COUNT
    -- still increments — the WHERE filter (coastal_zone_id IS NULL)
    -- re-picks the same beaches forever. The sentinel pulls them out
    -- of the work pool. find_beaches still returns NULL rip_current_risk
    -- because no wfo_srf_forecast row will match '__no_zone__'.
    update public.beaches_gold bg
       set coastal_zone_id = coalesce(sub.zone_id, '__no_zone__')
      from (
        select bg2.fid,
               (select z.zone_id
                  from public.nws_zones z
                 where z.zone_type = 'forecast'
                   and z.geom is not null
                   and st_dwithin(
                         z.geom::geography,
                         st_setsrid(st_makepoint(bg2.lon::float, bg2.lat::float), 4326)::geography,
                         200
                       )
                 order by st_distance(
                            z.geom::geography,
                            st_setsrid(st_makepoint(bg2.lon::float, bg2.lat::float), 4326)::geography
                          )
                 limit 1) as zone_id
          from public.beaches_gold bg2
         where bg2.is_active = true
           and bg2.coastal_zone_id is null
           and bg2.lat is not null and bg2.lon is not null
           and bg2.state in ('CA','OR','WA','HI','MA','RI','NH','ME','MD','VA','DE',
                             'NC','SC','GA','FL','AL','MS','LA','TX','NJ','NY','CT')
         order by bg2.state, bg2.fid
         limit 200
      ) sub
     where bg.fid = sub.fid;
    get diagnostics v_chunk = row_count;

    v_total := v_total + v_chunk;
    exit when v_chunk = 0;
  end loop;

  return v_total;
end $$;

grant execute on function public._orch_w_rebind_coastal_zones()
  to service_role;

comment on function public._orch_w_rebind_coastal_zones is
  'Orch wrapper for coastal-zone rebind. Chunked-iteration UPDATE (200 beaches '
  'per loop, capped at 7min wall-clock) — keeps the function inside orch_tick''s '
  '10min statement_timeout. Returns total rows bound. Idempotent: scopes to '
  'coastal_zone_id IS NULL.';


-- ─── orch_jobs row ───────────────────────────────────────────────────────
-- Fires daily 14:30 UTC, 30 minutes after refresh_nws_srf (14:00 UTC) so
-- newly-warmed zones from today's SRF fetch are available for matching.
-- depends_on the SRF refresh so it skips when SRF hasn't run recently.

insert into public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) values (
  'rebind_coastal_zones',
  'Daily 14:30 UTC: rebind beaches_gold.coastal_zone_id for beaches in coastal states whose binding is NULL. Picks up beaches in newly-warmed NWS forecast zones (refresh_nws_srf lazy-loads zones one-shot when a new SRF references them). Chunked SQL-function worker — sidesteps the PostgREST 60s cap that breaks _rebind_beach_coastal_zones(text) under direct invocation.',
  'daily_at',
  '14:30',
  null,
  array['refresh_nws_srf']::text[],
  '24 hours'::interval,
  null,
  null,
  'sql_function',
  'public._orch_w_rebind_coastal_zones',
  '{}'::jsonb,
  null,
  true,
  false
)
on conflict (job_name) do update set
  description    = excluded.description,
  cadence_kind   = excluded.cadence_kind,
  cadence_param  = excluded.cadence_param,
  depends_on     = excluded.depends_on,
  worker_kind    = excluded.worker_kind,
  worker_target  = excluded.worker_target,
  worker_payload = excluded.worker_payload,
  enabled        = excluded.enabled;

commit;
