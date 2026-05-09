-- 20260509_tier4_visible_not_scoreable.sql
--
-- Franz: Tier 4 beaches (dogs_allowed='no') should remain visible in
-- the catalog but rendered with muted/no-dogs UI. The previous
-- tg_inactivate_on_no_dogs trigger removed them entirely
-- (is_active=false), preventing them from appearing on the find map
-- even with `scored=false`. That was the wrong place to enforce the
-- constraint.
--
-- New model:
--   - Display gates  → tier-based; Tier 4 visible-but-muted
--   - Cost gates     → is_scoreable; Tier 1+2 only run downstream LLM/photo/refresh
--
-- This migration:
--   1. Drops the trigger so future Tier 4 resolutions don't deactivate.
--   2. Re-activates beaches that were CLEAN trigger casualties:
--      is_active=false AND inactive_reason IS NULL AND dogs_allowed='no'.
--      Skips beaches with inactive_reason set (dedup victims, manual
--      admin holds, rollback markers — those are inactive for other
--      legitimate reasons).
--   3. Re-runs align_is_scoreable_to_tier for affected states so the
--      re-activated rows get is_scoreable=false correctly (Tier 4 not
--      in scoring scope).
--
-- inactivate_tier3_beaches() function is kept as a callable but not
-- triggered automatically. (Manual admin tooling can still use it
-- if needed for some other workflow.)

begin;

-- ── 1. Drop the trigger ────────────────────────────────────────────────
drop trigger if exists tg_beach_dog_policy_inactivate_on_no_dogs
  on public.beach_dog_policy;


-- ── 2. Re-activate clean trigger casualties ────────────────────────────
-- 130 expected: Tier 4 beaches with no inactive_reason (pure trigger
-- side-effect, no other reason for being deactivated).

with reactivated as (
  update public.beaches_gold g
     set is_active = true
    from public.beach_dog_policy bdp
   where bdp.arena_group_id = g.fid
     and bdp.dogs_allowed = 'no'
     and not g.is_active
     and g.inactive_reason is null
  returning g.fid, g.state
)
select state, count(*) reactivated_count
  from reactivated group by state order by count(*) desc;


-- ── 3. Ensure they're not scoreable (align by tier) ────────────────────
-- align_is_scoreable_to_tier already handles this — it demotes any
-- is_scoreable=true beach that resolves to Tier 3/4. Run for affected
-- states.

select state_code, public.align_is_scoreable_to_tier(state_code)
  from (values ('CA'), ('OR'), ('WA')) as s(state_code);


commit;
