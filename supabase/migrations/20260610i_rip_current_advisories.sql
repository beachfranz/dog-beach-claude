-- 20260610i_rip_current_advisories.sql
--
-- Fire rip_current beach_advisory rows from wfo_srf_forecast.
--
-- Source: public.refresh_marine_advisories() (20260607f) — same upsert +
-- watermark-sweep shape, but the rip case is simpler: no thresholds,
-- no aggregation across hours, just one categorical signal per beach
-- per day (Moderate / High; Low suppressed).
--
-- Read path: beach.html Cautions card reads any beach_advisory row via
-- get_beach_advisories RPC — no frontend change needed; the 🚩 tile
-- appears automatically once these rows land.

begin;

create or replace function public.refresh_rip_current_advisories(
  p_state text   default null,
  p_fid   bigint default null
) returns table(upserted bigint, retired bigint)
language plpgsql as $function$
declare
  v_now      timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
begin
  with ins as (
    insert into public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    select
      bg.fid,
      format('rip:rip_current_risk:%s', srf.forecast_date),
      'rip_current',
      'rip_current_risk',
      case srf.rip_current_risk
        when 'High'     then 'severe'
        when 'Moderate' then 'moderate'
      end,
      -- valid_from = midnight of forecast_date in beach's local tz (best-
      -- effort: SRFs are issued in WFO local time and cover the calendar
      -- day; passing UTC-anchored bounds is close enough — get_beach_advisories
      -- chops at beach-local today anyway).
      srf.forecast_date::timestamptz,
      (srf.forecast_date + interval '1 day')::timestamptz,
      'skip_swim',
      case srf.rip_current_risk
        when 'High'     then
          'NWS High rip current risk in effect — life-threatening rip currents likely. Keep her on the dry sand; no swimming or fetching in the surf.'
        when 'Moderate' then
          'NWS Moderate rip current risk today — life-threatening rip currents are possible in the surf zone. Stick to the shore.'
      end,
      'rule',
      'Rip current',
      srf.rip_current_risk,
      '🚩',
      jsonb_build_object(
        'zone_id',  srf.zone_id,
        'wfo',      srf.wfo,
        'period',   srf.period,
        'risk',     srf.rip_current_risk
      ),
      v_now
    from public.beaches_gold bg
    join public.wfo_srf_forecast srf on srf.zone_id = bg.coastal_zone_id
    where bg.is_active
      and bg.scoring_tier in ('daily','hourly')
      and bg.coastal_zone_id is not null
      and bg.coastal_zone_id <> '__no_zone__'
      and srf.forecast_date = current_date
      and srf.period = 'current'
      and srf.rip_current_risk in ('Moderate', 'High')
      and (p_state is null or bg.state = upper(p_state))
      and (p_fid   is null or bg.fid   = p_fid)
    on conflict (beach_fid, advisory_key) do update set
      severity        = excluded.severity,
      valid_from      = excluded.valid_from,
      valid_to        = excluded.valid_to,
      dog_impact_text = excluded.dog_impact_text,
      value           = excluded.value,
      raw_data        = excluded.raw_data,
      fetched_at      = excluded.fetched_at
    returning 1
  )
  select count(*) into v_upserted from ins;

  -- Watermark sweep: drop rip rows that didn't get refreshed (zone risk
  -- dropped back to Low, beach binding cleared, or SRF stopped publishing
  -- for the zone). Mirrors refresh_marine_advisories shape.
  with del as (
    delete from public.beach_advisory ba
    using public.beaches_gold bg
    where ba.beach_fid = bg.fid
      and ba.source    = 'rip_current'
      and bg.is_active
      and bg.scoring_tier in ('daily','hourly')
      and (p_state is null or bg.state = upper(p_state))
      and (p_fid   is null or bg.fid   = p_fid)
      and ba.fetched_at < v_now - interval '5 seconds'
    returning 1
  )
  select count(*) into v_retired from del;

  return query select v_upserted, v_retired;
end;
$function$;

comment on function public.refresh_rip_current_advisories is
  'Daily refresh of rip_current beach_advisory rows from wfo_srf_forecast. '
  'Source: SRF text product per WFO via refresh-nws-srf edge fn. Only emits '
  'rows for Moderate/High risk; Low suppressed. Watermark sweep drops stale '
  'rows that didn''t refresh in the current pass. Mirrors '
  'refresh_marine_advisories shape (20260607f).';


-- ─── Orch wrapper ────────────────────────────────────────────────────────
create or replace function public._orch_w_refresh_rip_current_advisories()
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

grant execute on function public._orch_w_refresh_rip_current_advisories()
  to service_role;


-- ─── orch_jobs row ───────────────────────────────────────────────────────
-- Fires daily at 14:35 UTC — after refresh_nws_srf (14:00) AND
-- rebind_coastal_zones (14:30), so newly-warmed zones AND newly-bound
-- beaches are both reflected before this fires.
insert into public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) values (
  'refresh_rip_current_advisories',
  'Daily 14:35 UTC: fire 🚩 Rip Current Risk beach_advisory rows from wfo_srf_forecast. Moderate / High only; Low suppressed.',
  'daily_at',
  '14:35',
  null,
  array['refresh_nws_srf', 'rebind_coastal_zones']::text[],
  '48 hours'::interval,
  null,
  null,
  'sql_function',
  'public._orch_w_refresh_rip_current_advisories',
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
