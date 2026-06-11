-- 20260611_orch_workers_jsonb_signature.sql
--
-- Fix engine-contract mismatch on two newest orch_jobs workers.
--
-- 20260610h created _orch_w_rebind_coastal_zones() with no args, and
-- 20260610i created _orch_w_refresh_rip_current_advisories() the same
-- way. The orch_tick engine invokes sql_function workers as
-- <fn>(jsonb) — passing the worker_payload jsonb at call time. Every
-- other sql_function worker in the table signs as `p jsonb`. PostgREST
-- therefore raises `function ... does not exist` on every tick.
--
-- rebind_coastal_zones has been failing on its hourly :05 schedule
-- since deploy; refresh_rip_current_advisories sat at
-- last_attempt=NULL because it depends_on rebind_coastal_zones (which
-- never succeeded) — same bug, masked by the dep gate.
--
-- Fix: DROP both, recreate with `p jsonb DEFAULT '{}'::jsonb`. The
-- payload is unused inside (rebind reads no params; rip-current calls
-- refresh_rip_current_advisories() with no args). Default keeps direct
-- ad-hoc invocation (`select _orch_w_rebind_coastal_zones();`) working.

begin;

-- ─── rebind_coastal_zones ────────────────────────────────────────────────

drop function if exists public._orch_w_rebind_coastal_zones();

create or replace function public._orch_w_rebind_coastal_zones(
  p jsonb default '{}'::jsonb
)
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
  -- See 20260610h_rebind_orch_job.sql for the chunked-iteration
  -- rationale (45s client wall-clock vs 8min DB statement_timeout).
  loop
    v_iter := v_iter + 1;
    exit when v_iter > 200;
    exit when clock_timestamp() - v_start > interval '45 sec';

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

grant execute on function public._orch_w_rebind_coastal_zones(jsonb)
  to service_role;

comment on function public._orch_w_rebind_coastal_zones(jsonb) is
  'Orch wrapper for coastal-zone rebind. Chunked-iteration UPDATE (200 beaches '
  'per loop, capped at 45s wall-clock). Returns total rows bound. Idempotent: '
  'scopes to coastal_zone_id IS NULL. Payload p ignored.';


-- ─── refresh_rip_current_advisories ──────────────────────────────────────

drop function if exists public._orch_w_refresh_rip_current_advisories();

create or replace function public._orch_w_refresh_rip_current_advisories(
  p jsonb default '{}'::jsonb
)
returns bigint
language plpgsql
security definer
set search_path = 'public'
as $$
declare
  v_upserted bigint;
begin
  select upserted into v_upserted
    from public.refresh_rip_current_advisories();
  return v_upserted;
end $$;

grant execute on function public._orch_w_refresh_rip_current_advisories(jsonb)
  to service_role;

comment on function public._orch_w_refresh_rip_current_advisories(jsonb) is
  'Orch wrapper for rip-current advisory refresh. Calls public.refresh_rip_current_advisories(); '
  'returns rows upserted. Payload p ignored.';

commit;
