-- 20260507_retire_nesting_data.sql
--
-- Retire nesting-zone signal per Franz 2026-05-07: "too few applicable
-- beaches; put a bullet in it."
--
-- The federally designated critical-habitat layer turned out to be
-- overinclusive (most of the Pacific coast is in WSP/MM critical habitat
-- but actual operational nesting closures are concentrated in specific
-- dunes, captured by operator-published rules). Only 7 beaches had a
-- structured `nesting_zones` section in zone_rules; the spatial overlay
-- adds little value and risks generating false-positive warnings on
-- OR/WA when those beaches load.
--
-- Retired:
--   1. wildlife_critical_habitat table (798 rows; reloadable from REST)
--   2. dog_policy_zones rows where species in ('snowy_plover','least_tern')
--   3. (separately, prompt taxonomy edited in scripts/eval_closed_zones_v3.py)
--
-- Kept:
--   * dog_policy_zones PORE pet-allowed-carveout rows (different signal
--     — where dogs ARE allowed inside an otherwise-prohibited NPS unit)
--
-- Re-loadable. If priorities change, USFWS Critical Habitat is in the
-- external_sources.py registry and re-runs in ~5 min. Pin #7 lives in
-- the archive list with the "why retired" reasoning.

begin;

-- 1. Drop the WSP + LT spatial overlay rows (keep PORE carve-outs).
delete from public.dog_policy_zones
 where species in ('snowy_plover','least_tern');

-- 2. Drop the canonical wildlife_critical_habitat table.
drop table if exists public.wildlife_critical_habitat;

-- Note: the 7 existing `nesting_zones` sections inside beach_dog_policy.zone_rules
-- JSONB are inert (no consumer surface renders them). Leaving in place rather
-- than rewriting JSONB; future text_repass runs won't re-emit per the prompt
-- taxonomy edit.

commit;
