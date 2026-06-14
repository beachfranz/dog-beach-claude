-- 20260613n_dogs_bep_orphan_leash_cleanup.sql
--
-- Cleanup orphan leash_required claims. When a BEP row has
-- `allowed=no` AND per-section evidence (area_* keys, sections JSONB,
-- or rules array), the row's flat `leash_required` field carries
-- broader-scope information (typically the city-wide leash rule that
-- would apply IF dogs were allowed somewhere). It's orphan leakage
-- on a row whose beach-level answer is "no."
--
-- Surfaced 2026-06-13: Avalon's Hamilton Beach extraction wrote
-- {allowed:no, leash_required:on_leash, area_sand:not_allowed,
--  area_water:not_allowed, area_trails:not_allowed, notes:"Dogs must
-- be leashed at all times in Avalon; no dogs allowed on beaches..."}
-- The area_* keys correctly captured the beach-level prohibition;
-- leash_required=on_leash was the city-wide rule that would apply
-- elsewhere in Avalon. Not a contradiction — just orphan field.
--
-- This cleanup: when allowed=no AND any per-section evidence exists,
-- delete leash_required from claimed_values and stamp an audit
-- marker `_leash_required_orphan_cleared: true`.
--
-- Scope: catches ~735 of the 740 allowed=no + leash_required rows
-- (the ones with supporting structure). The remaining ~5 are bare
-- city_policy LLM failures — left untouched here; they need
-- re-extraction or curator review, not auto-fold.
--
-- Pattern mirrors 20260613f / 20260613i: extend the normalizer
-- helper, let the existing tg_normalize_dogs_bep_before_write
-- trigger propagate on backfill UPDATE. Downstream consensus +
-- promote re-fire automatically.

BEGIN;

-- Extend the dogs normalizer with the orphan-leash cleanup step.
CREATE OR REPLACE FUNCTION public._normalize_dogs_claimed_values(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $function$
DECLARE
  v_result   jsonb := p;
  v_allowed  text;
  v_leash    text;
  v_sections jsonb;
  v_rules    jsonb;
  v_keys     text[];
  v_k        text;
  v_has_not_allowed boolean := false;
  v_has_other       boolean := false;
  v_section_keys    text[] := ARRAY['sand','water','trails','picnic_area'];
  v_area_keys       text[] := ARRAY['area_sand','area_water','area_trails','area_picnic_area'];
  v_sec_rule        text;
  v_new_sections    jsonb;
  v_new_rules       jsonb;
  v_elem            jsonb;
  v_has_per_section_evidence boolean := false;
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'object' THEN
    RETURN p;
  END IF;

  -- (1a) Fold `allowed`: restricted/seasonal → mixed  [20260613f]
  v_allowed := p->>'allowed';
  IF v_allowed IN ('restricted', 'seasonal') THEN
    v_result := jsonb_set(v_result, '{allowed}', '"mixed"'::jsonb);
    v_allowed := 'mixed';
  END IF;

  -- (1b) Fold `leash_required`: required → on_leash, not_required → off_leash  [20260613f]
  v_leash := v_result->>'leash_required';
  IF v_leash = 'required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"on_leash"'::jsonb);
    v_leash := 'on_leash';
  ELSIF v_leash = 'not_required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"off_leash"'::jsonb);
    v_leash := 'off_leash';
  END IF;

  -- (1c) Fold flat area_* keys: forbidden → not_allowed, off_leash_voice_control → off_leash  [20260613i]
  FOREACH v_k IN ARRAY v_area_keys LOOP
    v_sec_rule := v_result->>v_k;
    IF v_sec_rule IS NOT NULL THEN
      v_result := jsonb_set(v_result, ARRAY[v_k], to_jsonb(public._fold_section_rule_value(v_sec_rule)));
    END IF;
  END LOOP;

  -- (1d) Fold sections.{key}.rule values  [20260613i]
  v_sections := v_result->'sections';
  IF v_sections IS NOT NULL AND jsonb_typeof(v_sections) = 'object' THEN
    v_new_sections := v_sections;
    FOREACH v_k IN ARRAY v_section_keys LOOP
      v_sec_rule := v_sections->v_k->>'rule';
      IF v_sec_rule IS NOT NULL THEN
        v_new_sections := jsonb_set(
          v_new_sections,
          ARRAY[v_k, 'rule'],
          to_jsonb(public._fold_section_rule_value(v_sec_rule))
        );
      END IF;
    END LOOP;
    v_result := jsonb_set(v_result, '{sections}', v_new_sections);
    v_sections := v_new_sections;
  END IF;

  -- (1e) Fold rules[].rule values  [20260613i]
  v_rules := v_result->'rules';
  IF v_rules IS NOT NULL AND jsonb_typeof(v_rules) = 'array' THEN
    v_new_rules := '[]'::jsonb;
    FOR v_elem IN SELECT * FROM jsonb_array_elements(v_rules) LOOP
      v_sec_rule := v_elem->>'rule';
      IF v_sec_rule IS NOT NULL THEN
        v_new_rules := v_new_rules || jsonb_build_array(
          jsonb_set(v_elem, '{rule}', to_jsonb(public._fold_section_rule_value(v_sec_rule)))
        );
      ELSE
        v_new_rules := v_new_rules || jsonb_build_array(v_elem);
      END IF;
    END LOOP;
    v_result := jsonb_set(v_result, '{rules}', v_new_rules);
  END IF;

  -- (1f) NEW: drop orphan leash_required when allowed=no AND per-section evidence exists
  IF v_result->>'allowed' = 'no'
     AND v_result->>'leash_required' IS NOT NULL THEN
    -- Per-section evidence = any area_* key OR sections OR rules
    v_has_per_section_evidence :=
         v_result ? 'area_sand'
      OR v_result ? 'area_water'
      OR v_result ? 'area_trails'
      OR v_result ? 'area_picnic_area'
      OR v_result ? 'area_parking_lot'
      OR v_result ? 'area_campground'
      OR v_result ? 'sections'
      OR v_result ? 'rules';
    IF v_has_per_section_evidence THEN
      v_result := (v_result - 'leash_required')
        || jsonb_build_object('_leash_required_orphan_cleared', true);
    END IF;
  END IF;

  -- (2) Derive `allowed` when missing  [20260613f]
  IF v_allowed IS NULL THEN
    IF v_sections IS NOT NULL AND jsonb_typeof(v_sections) = 'object' THEN
      v_keys := ARRAY(SELECT jsonb_object_keys(v_sections));
      FOREACH v_k IN ARRAY v_keys LOOP
        IF v_sections->v_k->>'rule' = 'not_allowed' THEN
          v_has_not_allowed := true;
        ELSIF v_sections->v_k->>'rule' IS NOT NULL THEN
          v_has_other := true;
        END IF;
      END LOOP;

      IF v_has_not_allowed AND v_has_other THEN
        v_result := v_result
          || jsonb_build_object('allowed', 'mixed', '_allowed_derived', true);
      ELSIF v_has_not_allowed THEN
        v_result := v_result
          || jsonb_build_object('allowed', 'no', '_allowed_derived', true);
      ELSIF v_has_other THEN
        v_result := v_result
          || jsonb_build_object('allowed', 'yes', '_allowed_derived', true);
      END IF;

    ELSIF v_leash IN ('on_leash', 'off_leash', 'mixed') THEN
      v_result := v_result
        || jsonb_build_object('allowed', 'yes', '_allowed_derived', true);
    END IF;
  END IF;

  RETURN v_result;
END
$function$;

-- One-shot backfill: target only rows that match the orphan-leash condition
-- to avoid unnecessary trigger work on the broader dogs BEP set.
UPDATE public.beach_enrichment_provenance
   SET claimed_values = public._normalize_dogs_claimed_values(claimed_values)
 WHERE field_group = 'dogs'
   AND claimed_values->>'allowed' = 'no'
   AND claimed_values->>'leash_required' IS NOT NULL
   AND (claimed_values ? 'area_sand'
     OR claimed_values ? 'area_water'
     OR claimed_values ? 'area_trails'
     OR claimed_values ? 'area_picnic_area'
     OR claimed_values ? 'area_parking_lot'
     OR claimed_values ? 'area_campground'
     OR claimed_values ? 'sections'
     OR claimed_values ? 'rules');

COMMIT;

NOTIFY pgrst, 'reload schema';
