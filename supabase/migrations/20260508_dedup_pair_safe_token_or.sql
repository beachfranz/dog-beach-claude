-- 20260508_dedup_pair_safe_token_or.sql
--
-- Extend _dedup_pair_safe to accept token-subset matches as a parallel path.
-- Trigram-OR-token-subset, both still respect dog-mismatch.
--
-- Why: some real dupes have trigram < 0.7 but one name's tokens are a strict
-- subset of the other. Examples (validated):
--   - Fish Rock Beach / Fish Rock Beach at Anchor Bay (sim 0.57)
--   - Lake Arrowhead Village Beach / ...Village Lakefront (sim 0.66)
-- Token-subset catches these structurally where trigram alone can't.
--
-- The token-subset helper is _dedup_token_subset (separate migration). It
-- handles directional mismatch and dog-discrimination internally. Calling
-- it here as an OR path means dedup safety accepts EITHER match style.
--
-- Note: this only changes the SAFETY GATE used by run_late_stage_dedup().
-- Clustering currently uses only the trigram path of _dedup_pair_safe in
-- populate_arena_extras() Strategies 6/7/8. Adding token-subset to clustering
-- (Strategy 10) was attempted tonight and reverted due to cycle bugs in
-- symmetric singleton pairs. Pinned for future work.

begin;

create or replace function public._dedup_pair_safe(name_a text, name_b text, min_sim real default 0.70)
returns boolean
language sql
immutable
as $$
  with norm as (
    select lower(trim(name_a)) as a_lc, lower(trim(name_b)) as b_lc
  ),
  flags as (
    select a_lc, b_lc,
           (a_lc ~ '\mdog\M') as a_has_dog,
           (b_lc ~ '\mdog\M') as b_has_dog
      from norm
  )
  select
    -- Path 1: trigram + dog-match
    (similarity(a_lc, b_lc) >= min_sim and (a_has_dog = b_has_dog))
    OR
    -- Path 2: token-subset (handles dog-mismatch + directional mismatch internally)
    public._dedup_token_subset(name_a, name_b)
  from flags;
$$;

commit;
