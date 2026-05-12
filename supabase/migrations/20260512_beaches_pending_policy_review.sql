-- 20260512_beaches_pending_policy_review.sql
--
-- View surfacing active beaches whose dog policy resolves to 'unknown'
-- (insufficient data). These can't be scored — they fall through to
-- scoring_tier='none' — but they're not "no dogs"; they're "we don't
-- know yet". They need human curation.
--
-- Sorted by catchment_score descending so curators tackle the
-- high-impact unknowns first. Tier 4 (verified no_dogs) is intentionally
-- excluded — those are correctly classified, just not scoreable.

begin;

create or replace view public.beaches_pending_policy_review as
with derived as (
  select g.fid,
         coalesce(g.display_name_override, g.name) as name,
         g.state,
         g.county_name,
         g.catchment_score,
         g.catchment_state_pct,
         g.scoring_tier,
         bdp.dogs_allowed,
         bdp.leash_policy,
         bdp.has_off_leash,
         bdp.has_on_leash,
         bdp.dogs_prohibited_start,
         bdp.source as dog_policy_source,
         bdp.consensus_confidence,
         bdp.disagreement_flag,
         public.beach_location_tier(
           bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
           bdp.dogs_prohibited_start
         ) as tier
    from public.beaches_gold g
    left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
   where g.is_active
)
select fid, name, state, county_name,
       catchment_score, catchment_state_pct, scoring_tier,
       tier, dogs_allowed, leash_policy,
       has_off_leash, has_on_leash, dogs_prohibited_start,
       dog_policy_source, consensus_confidence, disagreement_flag
  from derived
 where tier = 'unknown' or tier is null
 order by catchment_score desc nulls last;

grant select on public.beaches_pending_policy_review to anon, authenticated, service_role;

commit;
