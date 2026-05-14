-- 20260513_match_strength_spatial_only.sql
--
-- Removes name_match from _polygon_match_strength. Match confidence is
-- now purely spatial — name overlap influences canonical selection
-- exclusively through the governance resolver's `name_sim_bonus`. No
-- more double-counting.
--
-- BEFORE (4 tiers, mixed spatial + name):
--   4: inside                                    → 0.95
--   3: within 100m                               → 0.85
--   2: name_match >= 2 tokens                    → 0.78
--   1: name_match >= 1 token AND dist <= 2000m   → 0.68
--
-- AFTER (4 tiers, pure spatial):
--   4: inside                                    → 0.95
--   3: within 100m                               → 0.85
--   2: within 500m                               → 0.78
--   1: within 2000m                              → 0.68
--   0: beyond 2km                                → skip
--
-- Populators keep name_token_overlap as a TIEBREAKER in their ranking
-- (a name-matched polygon beats a non-name-matched polygon at the same
-- strength). That's per-source disambiguation, doesn't propagate to the
-- inherited confidence.
--
-- Impact for OR scoreable beaches: ~1 row drops (was name-only matched
-- 2.6km away). Other 4 rows at ms=1 are within 2km and persist. Most
-- attributions (259/263) were already inside or within_100m — unaffected.
--
-- Signature unchanged: `name_tokens` param kept for backward compat with
-- existing populator call sites; ignored internally.

begin;

create or replace function public._polygon_match_strength(
  p_is_inside    boolean,
  p_dist_m       integer,
  p_name_tokens  integer  -- DEPRECATED: ignored. name_match owned by name_sim_bonus.
) returns integer
language sql immutable as $$
  select case
    when p_is_inside                                  then 4   -- inside
    when p_dist_m is not null and p_dist_m <= 100     then 3   -- very close
    when p_dist_m is not null and p_dist_m <= 500     then 2   -- close
    when p_dist_m is not null and p_dist_m <= 2000    then 1   -- nearby
    else                                                   0   -- skip
  end;
$$;

-- Confidence map unchanged — same 4 confidence levels, just remapped
-- onto distance-only criteria.

commit;
