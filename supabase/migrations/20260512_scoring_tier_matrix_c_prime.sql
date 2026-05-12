-- 20260512_scoring_tier_matrix_c_prime.sql
--
-- Refines Matrix C: tier 1 (off-leash) now follows the same state_pct
-- gate as tier 2 instead of being unconditionally hourly. Driver: in
-- Oregon, where almost every beach is tier 1, the unconditional rule
-- forced 84% of beaches to hourly even for rural beaches with no
-- traffic worth measuring at hourly cadence.
--
-- New rule (Matrix C'):
--
--   tier \ state_pct     | <0.40 | 0.40–0.72 | 0.72–0.90 | ≥0.90
--   ---------------------+-------+-----------+-----------+-------
--   1_off-leash          | daily | daily     | hourly    | hourly
--   2_on-leash           | daily | daily     | hourly    | hourly
--   3_limited_access     | none  | daily     | daily     | daily
--   4_no_dogs / unknown  | none  | none      | none      | none
--
-- Tier 1 and tier 2 are now identical — the dog-policy signal still
-- matters via the tier 3 cutoff (3 only gets daily at state_pct≥0.40
-- where 1/2 get daily even below). The semantic principle: hourly is
-- traffic-driven, not policy-driven.

begin;

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
             -- Matrix C': hourly is traffic-driven for both tier 1 and 2
             when tier in ('1_off-leash','2_on-leash') and state_pct >= 0.72 then 'hourly'
             when tier in ('1_off-leash','2_on-leash') and state_pct is not null then 'daily'
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

comment on column public.beaches_gold.scoring_tier is
  'Two-track scoring cadence per Matrix C'' (tier × catchment_state_pct). '
  'hourly: tier 1 or 2 at state_pct>=0.72 (top 28% of state). daily: tier 1 or 2 below 0.72, '
  'or tier 3 at state_pct>=0.40. none: tier 3 below 0.40, tier 4, unknown, or null state_pct. '
  'Refreshed by refresh_scoring_tier().';

commit;
