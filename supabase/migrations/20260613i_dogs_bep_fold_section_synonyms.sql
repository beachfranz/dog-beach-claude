-- 20260613i_dogs_bep_fold_section_synonyms.sql
--
-- Extend _normalize_dogs_claimed_values to fold extractor synonyms on
-- per-section rule values:
--
--   forbidden               → not_allowed   (170 rows; city/county dialect
--                                            for the same prohibition concept
--                                            that section_research / codified
--                                            already write as 'not_allowed')
--
--   off_leash_voice_control → off_leash     (200 rows; codified pipeline emits
--                                            this richer value but for binary
--                                            consumer rendering it's just
--                                            off-leash. The voice-control
--                                            constraint is preserved in the
--                                            rule_modifier field of the rules
--                                            array, so this fold is loss-free
--                                            at the headline-rule level.)
--
-- The fold walks all THREE JSONB paths where section rules appear:
--   1. flat top-level keys: area_sand, area_water, etc.
--   2. nested object: sections.{sand,water,trails,picnic_area}.rule
--   3. array: rules[].rule
--
-- Conditional-allowed cluster (nuisance_restriction, collar_tag_required,
-- on_leash_or_voice, carry_leash_required, no_unattended_dogs — 26 rows
-- across 5 distinct values) is intentionally NOT folded. Each value
-- carries a specific constraint distinct enough that collapsing loses
-- information.
--
-- Pattern mirrors 20260613f (which folded restricted/seasonal → mixed and
-- required/not_required → on_leash/off_leash on the top-level allowed +
-- leash_required keys). Same trigger fires automatically on the backfill
-- UPDATE, propagating changes through consensus + promote to
-- beach_dog_policy.

BEGIN;

-- ─── Helper: fold synonyms in a section rule value ────────────────────
CREATE OR REPLACE FUNCTION public._fold_section_rule_value(v text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $function$
  SELECT CASE
    WHEN v = 'forbidden'               THEN 'not_allowed'
    WHEN v = 'off_leash_voice_control' THEN 'off_leash'
    ELSE v
  END;
$function$;

-- ─── Extend the dogs normalizer ──────────────────────────────────────
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
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'object' THEN
    RETURN p;
  END IF;

  -- (1a) Fold `allowed`: restricted/seasonal → mixed  [from 20260613f]
  v_allowed := p->>'allowed';
  IF v_allowed IN ('restricted', 'seasonal') THEN
    v_result := jsonb_set(v_result, '{allowed}', '"mixed"'::jsonb);
    v_allowed := 'mixed';
  END IF;

  -- (1b) Fold `leash_required`: required → on_leash, not_required → off_leash  [from 20260613f]
  v_leash := v_result->>'leash_required';
  IF v_leash = 'required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"on_leash"'::jsonb);
    v_leash := 'on_leash';
  ELSIF v_leash = 'not_required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"off_leash"'::jsonb);
    v_leash := 'off_leash';
  END IF;

  -- (1c) NEW: fold flat area_* keys: forbidden → not_allowed, off_leash_voice_control → off_leash
  FOREACH v_k IN ARRAY v_area_keys LOOP
    v_sec_rule := v_result->>v_k;
    IF v_sec_rule IS NOT NULL THEN
      v_result := jsonb_set(v_result, ARRAY[v_k], to_jsonb(public._fold_section_rule_value(v_sec_rule)));
    END IF;
  END LOOP;

  -- (1d) NEW: fold sections.{key}.rule values
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
    -- Refresh v_sections for downstream derivation step
    v_sections := v_new_sections;
  END IF;

  -- (1e) NEW: fold rules[].rule values
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

  -- (2) Derive `allowed` when missing  [from 20260613f — uses post-fold sections]
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

-- ─── One-shot backfill on existing rows ──────────────────────────────
-- Apply the extended normalizer to every dogs BEP row. The
-- tg_normalize_dogs_bep_before_write trigger from 20260613f catches
-- this UPDATE, but we explicitly re-call the helper to make the
-- change SET-able (UPDATE x = x doesn't recompute via trigger when
-- the same value is supplied).
UPDATE public.beach_enrichment_provenance
   SET claimed_values = public._normalize_dogs_claimed_values(claimed_values)
 WHERE field_group = 'dogs'
   AND claimed_values IS NOT NULL
   AND (
     -- Only update rows that have a foldable value somewhere, to avoid
     -- triggering downstream consensus rebuilds for rows that wouldn't
     -- change. Three OR branches cover the three JSONB paths.
     claimed_values::text ~* '"forbidden"|"off_leash_voice_control"'
   );

COMMIT;

NOTIFY pgrst, 'reload schema';
