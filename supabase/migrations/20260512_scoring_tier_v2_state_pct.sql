-- 20260512_scoring_tier_v2_state_pct.sql
--
-- Phase 2 of the state-relative bucketing project. Ships the two-track
-- scoring_tier column (designed-but-not-shipped per the prior memory
-- note) using state-relative percentile rather than the original
-- national catchment_score>=50 cutoff.
--
-- Mapping rule — Matrix C (Franz, 2026-05-12):
--
--   tier \ state_pct       | <0.40  | 0.40–0.72 | 0.72–0.90 | ≥0.90
--   -----------------------+--------+-----------+-----------+-------
--   1_off-leash            | hourly | hourly    | hourly    | hourly
--   2_on-leash             | daily  | daily     | hourly    | hourly
--   3_limited_access       | none   | daily     | daily     | daily
--   4_no_dogs / unknown    | none   | none      | none      | none
--
-- Design notes:
-- - Tier 1 always hourly (the premium off-leash destinations, ~145 beaches).
-- - Tier 2 splits at state_pct ≥ 0.72 (top 28% of its state → hourly).
-- - Tier 3 limited-access gets daily once it has any meaningful catchment
--   (state_pct ≥ 0.40), reflecting that limited-access beaches matter
--   to users only when they're locally present (i.e. people are nearby).
-- - Tier 4 and unknown never get scored.
-- - Null state_pct (low-N states with <5 scored beaches) falls through
--   to 'none' for tiers 2/3 — they need cluster-rank fallback before
--   they can be promoted.
--
-- Targets ~64% compute reduction vs all-tier-1/2-hourly.
--
-- Stored column on beaches_gold; refresh function recomputes for one or
-- all fids. Refresh is manual / pipeline-orchestrated (no per-row
-- trigger — same reasoning as catchment_state_pct: percentile is a
-- whole-state recompute and per-row triggers are wasteful on bulk
-- updates).

begin;

alter table public.beaches_gold
  add column if not exists scoring_tier text
    check (scoring_tier in ('hourly','daily','none')),
  add column if not exists scoring_tier_updated_at timestamptz;

comment on column public.beaches_gold.scoring_tier is
  'Two-track scoring cadence per Matrix C (tier × catchment_state_pct). '
  'hourly: tier 1, or tier 2 at state_pct>=0.72. daily: tier 2 at state_pct<0.72, '
  'or tier 3 at state_pct>=0.40. none: tier 3 below 0.40, tier 4, unknown, or '
  'null state_pct. Refreshed by refresh_scoring_tier().';

drop function if exists public.refresh_scoring_tier(bigint);
create or replace function public.refresh_scoring_tier(p_fid bigint default null)
returns table(scoring_tier_out text, n_updated bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
begin
  return query
  with derived as (
    select g.fid,
           public.beach_location_tier(
             bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
             bdp.dogs_prohibited_start
           ) as tier,
           g.catchment_state_pct as state_pct,
           g.is_active
      from public.beaches_gold g
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
     where p_fid is null or g.fid = p_fid
  ),
  mapped as (
    select fid,
           case
             when not is_active then null
             -- Matrix C
             when tier = '1_off-leash' then 'hourly'
             when tier = '2_on-leash' and state_pct >= 0.72 then 'hourly'
             when tier = '2_on-leash' and state_pct is not null then 'daily'
             when tier = '3_limited_access' and state_pct >= 0.40 then 'daily'
             else 'none'
           end as new_tier
      from derived
  ),
  upd as (
    update public.beaches_gold g
       set scoring_tier = m.new_tier,
           scoring_tier_updated_at = now()
      from mapped m
     where g.fid = m.fid
       and (g.scoring_tier is distinct from m.new_tier
            or g.scoring_tier_updated_at is null)
    returning g.scoring_tier as st
  )
  select coalesce(upd.st, '(cleared)') as scoring_tier_out, count(*)::bigint
    from upd group by upd.st;
end;
$function$;

grant execute on function public.refresh_scoring_tier(bigint) to service_role;

commit;
