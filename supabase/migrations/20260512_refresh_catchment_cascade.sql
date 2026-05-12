-- 20260512_refresh_catchment_cascade.sql
--
-- Wires the three refresh steps (catchment_score → catchment_state_pct →
-- scoring_tier) into one wrapper so callers don't have to remember the
-- chain. Catchment is bulk-Python-orchestrated, no per-row trigger, so
-- the natural seam is a single function the pipeline calls at end-of-run.
--
-- refresh_catchment_cascade(p_state):
--   1. recompute_beach_catchment(p_state)         → catchment_score
--   2. refresh_catchment_state_pct(p_state)       → state percentile
--   3. refresh_scoring_tier(null)                 → scoring_tier (whole table —
--      it's a cheap single UPDATE, no need to scope)
--
-- Returns a single row with counts for each step.

begin;

create or replace function public.refresh_catchment_cascade(p_state text default null)
returns table(
  catchment_recomputed integer,
  state_pct_updated    bigint,
  scoring_tier_updated bigint
)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_catchment int := 0;
  v_state_pct bigint := 0;
  v_tier      bigint := 0;
begin
  v_catchment := public.recompute_beach_catchment(p_state);

  select coalesce(sum(n_updated), 0) into v_state_pct
    from public.refresh_catchment_state_pct(p_state);

  select coalesce(sum(n_updated), 0) into v_tier
    from public.refresh_scoring_tier(null);

  return query select v_catchment, v_state_pct, v_tier;
end;
$function$;

grant execute on function public.refresh_catchment_cascade(text) to service_role;

commit;
