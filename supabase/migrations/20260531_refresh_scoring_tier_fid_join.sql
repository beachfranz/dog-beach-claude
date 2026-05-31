-- 20260531_refresh_scoring_tier_fid_join.sql
--
-- Fix refresh_scoring_tier join. The function was joining
--    beach_dog_policy bdp ON bdp.arena_group_id = g.group_id
-- but per HARD memory pin [[arena_group_id_is_fid_not_group]],
-- beach_dog_policy.arena_group_id is a beach fid, not a group_id.
-- Should be:
--    beach_dog_policy bdp ON bdp.arena_group_id = g.fid
--
-- Symptom that surfaced this:
-- Gray Whale Cove SB (fid 3349) had bdp.dogs_allowed flipped to 'no'
-- by the sand-priority canonical_dogs patch. Expected scoring_tier
-- auto-demote to 'none'. Actual: stayed at 'daily'. Cause: the function
-- looked up bdp at arena_group_id=g.group_id (which for fid 3349 is
-- 8600 — the group-leader fid 8600, a different beach). That beach's
-- bdp still had dogs_allowed='mixed', so tier stayed at daily.
--
-- Blast radius before fix: any beach whose group_id != fid (every
-- non-leader beach in an arena group) was getting its scoring tier
-- computed from a sibling's dog policy. Beaches that happened to share
-- a policy with their group leader looked OK; beaches with different
-- policies than the leader silently got the wrong tier.

CREATE OR REPLACE FUNCTION public.refresh_scoring_tier(p_fid bigint DEFAULT NULL::bigint)
 RETURNS TABLE(scoring_tier_out text, n_updated bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
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

-- Re-run scoring_tier refresh for all active beaches now that the join
-- is correct. Beaches whose tier was wrong (computed from a sibling's
-- policy) will flip to the right tier.
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM public.refresh_scoring_tier(NULL) LOOP
    RAISE NOTICE 'scoring_tier_out=% n_updated=%', r.scoring_tier_out, r.n_updated;
  END LOOP;
END$$;
