-- Consensus phase B + C — wire trigger on beach_policy_source.
--
-- Closes the integration gap identified by 2026-05-17 discovery
-- (docs/consensus_engine_current_state.md update section). The
-- authority-aware injector (_zr_inject_from_policy_sources) and the
-- entity-aware flat-column promoter (promote_entity_dogs_to_beach_dog_policy)
-- both shipped via 2026-05-16 phase 3 + phase 5.1 migrations and work
-- correctly when invoked manually. They just never fire automatically
-- because no trigger reacts to beach_policy_source changes.
--
-- One trigger function + one trigger closes both Phase B (auto-sync
-- on ps changes) and Phase C (consensus engine reads ps as authority).
-- The flat-column promoter UPSERTs beach_dog_policy, which fires
-- tg_after_change_dogs_refire_zone_rules, which calls
-- _promote_zone_rules_for_fid, which runs the entity injector last.
-- So one call closes the loop for both flat columns AND zone_rules.

CREATE OR REPLACE FUNCTION public.tg_after_change_beach_policy_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_fid bigint;
BEGIN
  v_fid := COALESCE(NEW.beach_fid, OLD.beach_fid);
  IF v_fid IS NULL THEN
    RETURN NULL;
  END IF;
  -- Update flat columns from the canonical entity view. The UPSERT
  -- inside this function fires tg_after_change_dogs_refire_zone_rules,
  -- which calls _promote_zone_rules_for_fid (which runs the entity
  -- injector as its last pass). So this single call closes the loop
  -- for both flat columns AND zone_rules. Manual-curator protection
  -- is inherited from both promote_entity_dogs_to_beach_dog_policy
  -- (skips manual_curator on UPSERT) and _promote_zone_rules_for_fid
  -- (short-circuits on manual_curator source).
  PERFORM public.promote_entity_dogs_to_beach_dog_policy(v_fid);
  RETURN NULL;
END;
$$;

COMMENT ON FUNCTION public.tg_after_change_beach_policy_source() IS
  'Trigger function: on any beach_policy_source INSERT/UPDATE/DELETE, '
  're-promote the affected beach''s entity-derived flat columns. Cascades '
  'via existing tg_after_change_dogs_refire_zone_rules to rebuild '
  'zone_rules with the new authority-tier evidence. Closes the 2026-05-17 '
  'integration gap. Respects manual_curator on both sides.';

DROP TRIGGER IF EXISTS tg_after_change_beach_policy_source
  ON public.beach_policy_source;

CREATE TRIGGER tg_after_change_beach_policy_source
AFTER INSERT OR UPDATE OR DELETE ON public.beach_policy_source
FOR EACH ROW
EXECUTE FUNCTION public.tg_after_change_beach_policy_source();

-- One-shot backfill: re-promote every beach that currently has any
-- beach_policy_source row. Catches the 3 known mismatches (Rat Beach,
-- Port Hueneme, Del Mar Beach) and any others whose ps rows were
-- INSERTed before this trigger existed.
DO $$
DECLARE
  v_fid bigint;
  v_count int := 0;
BEGIN
  FOR v_fid IN
    SELECT DISTINCT beach_fid FROM public.beach_policy_source
  LOOP
    PERFORM public.promote_entity_dogs_to_beach_dog_policy(v_fid);
    v_count := v_count + 1;
  END LOOP;
  RAISE NOTICE 'Re-promoted % beaches from beach_policy_source.', v_count;
END$$;
