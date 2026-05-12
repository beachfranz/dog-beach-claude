-- 20260512_catchment_state_pct.sql
--
-- Adds a state-relative percentile rank for catchment_score on
-- beaches_gold. The current scoring tier rule (`catchment >= 50 → daily`)
-- is implicitly national: it works for CA/WA/MA but excludes 0% of
-- Delaware's beaches and only 8% of Oregon's. State-relative rank lets
-- us promote "top of state" beaches even when their absolute catchment
-- looks middling on the national scale.
--
-- This migration is Phase 1: storage + refresh function only. The actual
-- scoring_tier rule rewrite (Phase 2) waits for Franz to pick the
-- threshold once he sees what state_pct ≥ 0.75 vs 0.80 vs 0.90 does.
--
-- Refresh model: function `refresh_catchment_state_pct(p_state)` recomputes
-- percentile for one state (or all). Manual / pipeline-orchestrated. We
-- intentionally avoid a per-row trigger — percent_rank() is a whole-state
-- recompute, so per-row triggers would be expensive on bulk updates.

begin;

alter table public.beaches_gold
  add column if not exists catchment_state_pct numeric(5,4),
  add column if not exists catchment_state_pct_updated_at timestamptz;

comment on column public.beaches_gold.catchment_state_pct is
  'Percentile rank (0..1) of catchment_score within the beach''s own state, '
  'computed via percent_rank(). Refreshed by refresh_catchment_state_pct(). '
  'Null when catchment_score is null OR state has fewer than 5 scored beaches.';

drop function if exists public.refresh_catchment_state_pct(text);
create or replace function public.refresh_catchment_state_pct(p_state text default null)
returns table(state_code text, n_updated bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
begin
  -- Compute per-state percentile rank. Only states with at least 5
  -- scored beaches get ranked — below that, percent_rank is too coarse
  -- (with 4 beaches you get only 4 possible values: 0, 0.33, 0.67, 1.0).
  -- Low-N states stay NULL — caller decides fallback (cluster, national).
  return query
  with eligible as (
    select g.fid, g.state as st, g.catchment_score
      from public.beaches_gold g
     where g.is_active and g.catchment_score is not null
       and g.state is not null
       and (p_state is null or g.state = p_state)
  ),
  state_counts as (
    select st, count(*) as n from eligible group by st
  ),
  scored as (
    select e.fid, e.st,
           percent_rank() over (partition by e.st
                                order by e.catchment_score) as pct
      from eligible e
      join state_counts sc on sc.st = e.st
     where sc.n >= 5
  ),
  upd as (
    update public.beaches_gold g
       set catchment_state_pct = s.pct,
           catchment_state_pct_updated_at = now()
      from scored s
     where g.fid = s.fid
       and (g.catchment_state_pct is distinct from s.pct
            or g.catchment_state_pct_updated_at is null)
    returning g.state as st
  )
  select upd.st, count(*)::bigint from upd group by upd.st;
end;
$function$;

-- Helper view: show what scoring_tier-eligible candidates each state
-- has at multiple state_pct thresholds. Used to pick the right cutoff
-- once Phase 2 lands.
create or replace view public.catchment_state_pct_preview as
with eligible as (
  select state, fid, catchment_score, catchment_state_pct
    from public.beaches_gold
   where is_active and catchment_score is not null
     and catchment_state_pct is not null
),
buckets as (
  select state, count(*) n_total,
         count(*) filter (where catchment_score >= 50)         current_natl_50,
         count(*) filter (where catchment_state_pct >= 0.75)  proposed_p75,
         count(*) filter (where catchment_state_pct >= 0.80)  proposed_p80,
         count(*) filter (where catchment_state_pct >= 0.90)  proposed_p90
    from eligible
   group by state
)
select state, n_total,
       current_natl_50,
       round(100.0 * current_natl_50 / n_total, 1) as current_natl_50_pct,
       proposed_p75,
       round(100.0 * proposed_p75 / n_total, 1) as proposed_p75_pct,
       proposed_p80,
       round(100.0 * proposed_p80 / n_total, 1) as proposed_p80_pct,
       proposed_p90,
       round(100.0 * proposed_p90 / n_total, 1) as proposed_p90_pct
  from buckets order by n_total desc;

grant select on public.catchment_state_pct_preview to anon, authenticated, service_role;
grant execute on function public.refresh_catchment_state_pct(text) to service_role;

commit;
