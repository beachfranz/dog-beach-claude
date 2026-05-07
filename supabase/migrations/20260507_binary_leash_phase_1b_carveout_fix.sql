-- 20260507_binary_leash_phase_1b_carveout_fix.sql
--
-- Refine _derive_has_on_leash / _derive_has_off_leash to trust per-section
-- area_evidence over the categorical 'allowed' value.
--
-- BUG: with the original ordering, a source saying allowed='no' (typically
-- the parent-scope wrong-answer that text_repass produces for state-park
-- swim beaches) would assert both binaries = false BEFORE the function
-- got to check areas_evidence. So Doheny SB, with text_repass saying
-- allowed='no' AND areas_evidence='trails: on_leash; campground: on_leash;
-- ...', ended up has_on_leash=false despite the carve-out evidence.
--
-- FIX: when the claim has structural per-area evidence, trust that first.
-- The categorical 'allowed' is a fallback for sources that don't provide
-- per-area detail.

begin;

create or replace function public._derive_has_on_leash(claim jsonb)
returns text language sql immutable as $$
  select case
    when claim is null then null
    -- Trust per-section evidence first — when a source describes specific
    -- areas where on-leash applies, that's a positive signal regardless
    -- of how the source summarizes the categorical 'allowed' field.
    when claim->>'areas_evidence' ~ '\mon_leash\M' then 'true'
    -- Source-asserted positives
    when (claim->>'leash_required') in ('on_leash', 'mixed', 'mixed_by_zone', 'required') then 'true'
    when (claim->>'allowed') = 'mixed' then 'true'
    -- Categorical 'no' is a fallback negative — only fires when no
    -- per-area evidence contradicts.
    when (claim->>'allowed') = 'no' then 'false'
    when (claim->>'allowed') = 'yes' and coalesce(claim->>'leash_required', '') <> 'off_leash' then 'true'
    else null
  end
$$;

create or replace function public._derive_has_off_leash(claim jsonb)
returns text language sql immutable as $$
  select case
    when claim is null then null
    -- Per-section evidence wins
    when claim->>'areas_evidence' ~ '\moff_leash\M' then 'true'
    -- Source-asserted positives
    when (claim->>'leash_required') in ('off_leash', 'mixed', 'mixed_by_zone') then 'true'
    when (claim->>'off_leash_exists') = 'true' then 'true'
    when (claim->>'allowed') = 'mixed' then 'true'
    -- Negatives
    when (claim->>'off_leash_exists') = 'false' then 'false'
    when (claim->>'allowed') = 'no' then 'false'
    else null
  end
$$;

commit;
