-- 20260606w_scoring_tier_3_threshold_zero.sql
--
-- Drop the tier-3 catchment threshold from 0.40 to "any non-null
-- state_pct", matching tier 1/2 semantics.
--
-- BACKGROUND
-- `catchment_state_pct` is `percent_rank()` of `catchment_score` (count
-- of vets + dog parks + pet shops within 1/3/10 mi rings) within the
-- state. It measures urban dog-infrastructure density, which is biased
-- AGAINST famous remote/wilderness beaches that have zero urban
-- amenities within 10 mi by geography (Pfeiffer Beach in Big Sur,
-- Sculptured Beach in Point Reyes, Tomales Bay beaches in Marin, etc.).
--
-- The prior classifier required `state_pct >= 0.40` to score tier-3
-- (limited_access) beaches as `daily`. That gated out ~359 active
-- tier-3 beaches across all states with `dogs_allowed IN ('yes','mixed')` —
-- legitimate dog-policy beaches that users want surfaced, including
-- 144 at exactly_0 (the wilderness destinations).
--
-- Tier 1/2 always accepted any non-null state_pct → daily; tier 3
-- required >= 0.40 → asymmetric. This migration aligns tier 3 with the
-- tier-1/2 semantics: any non-null state_pct → daily.
--
-- IMPACT
-- ~359 beaches catalog-wide flip from scoring_tier='none' to 'daily':
--   * CA: 130
--   * NH: 83
--   * WA: 72 (some)
--   * AL/MA/MD/DE/UT/MI/OH: remainder
-- These beaches will start showing up on find.html with real day
-- composites + best windows on the next refresh cycle.

CREATE OR REPLACE FUNCTION public.refresh_scoring_tier(p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(scoring_tier_out text, n_updated bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public', 'pg_catalog'
AS $function$
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
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where p_fid is null or g.fid = p_fid
  ),
  mapped as (
    select fid,
           case
             when not is_active then null
             -- Matrix C': hourly is traffic-driven for both tier 1 and 2
             when tier in ('1_off-leash','2_on-leash') and state_pct >= 0.72 then 'hourly'
             when tier in ('1_off-leash','2_on-leash') and state_pct is not null then 'daily'
             -- Tier 3: was `state_pct >= 0.40` (cost gate biased against
             -- wilderness destinations with no urban amenities within 10mi).
             -- Now matches tier 1/2: any non-null state_pct → daily.
             when tier = '3_limited_access' and state_pct is not null then 'daily'
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

-- Apply the new classifier to all beaches.
SELECT * FROM public.refresh_scoring_tier();
