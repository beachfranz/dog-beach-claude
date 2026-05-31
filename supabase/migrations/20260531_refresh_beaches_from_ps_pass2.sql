-- 20260531_refresh_beaches_from_ps_pass2.sql
--
-- Patch _refresh_beaches_from_ps to fire BOTH the Pass-1 flat-column promote
-- AND the Pass-2 zone_rules rebuild for every affected fid. Previously the
-- function only ran Pass 1; Pass 2 was reached transitively via
-- tg_after_change_dogs_refire_zone_rules on beach_dog_policy, which is
-- change-gated on dogs_allowed/has_on_leash/has_off_leash deltas.
--
-- Failure mode (silent staleness):
--   1. INSERT operative BPS row whose rule matches what the LLM already
--      inferred (very common for codify backfills — e.g., statute confirms
--      on_leash for a beach already classified on_leash by extraction)
--   2. Pass 1 computes identical flat values → effectively no-op UPDATE
--   3. tg_after_change_dogs_refire_zone_rules sees no flat-column delta → skips
--   4. zone_rules keeps its LLM-inferred sand evidence forever
--
-- Surfaced 2026-05-31 during Goleta codify gap audit. 107 beaches across
-- MVP+ had operative BPS but zone_rules showed inferred=true sand evidence.
-- Bulk-re-promote ran as a separate one-off; this migration prevents
-- the recurrence going forward.
--
-- Scope: all 6 statement triggers on beach_policy_source + beach_policy_source_temporal
-- funnel through this function, so a single update fixes all six.
-- manual_curator protection inside _promote_zone_rules_for_fid handles
-- preserved rows correctly.
--
-- Reversible: prior body was a simple Pass-1-only loop; revert by dropping
-- the v_promote_result line.

CREATE OR REPLACE FUNCTION public._refresh_beaches_from_ps(p_fids bigint[])
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_fid bigint;
  v_count int := 0;
BEGIN
  FOREACH v_fid IN ARRAY p_fids LOOP
    IF v_fid IS NOT NULL THEN
      -- Pass 1: flat columns (dogs_allowed, has_on_leash, has_off_leash, etc.)
      PERFORM public.promote_entity_dogs_to_beach_dog_policy(v_fid);
      -- Pass 2: zone_rules jsonb. Required separately because Pass 1's
      -- transitive trigger (tg_after_change_dogs_refire_zone_rules) is
      -- change-gated on flat-column deltas, and BPS INSERTs whose rule
      -- matches existing inference produce no delta. manual_curator
      -- protection inside this function preserves curated rows.
      PERFORM public._promote_zone_rules_for_fid(v_fid);
      v_count := v_count + 1;
    END IF;
  END LOOP;
  RETURN v_count;
END;
$function$;
