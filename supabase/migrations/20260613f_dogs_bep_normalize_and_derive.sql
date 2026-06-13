-- 20260613f_dogs_bep_normalize_and_derive.sql
--
-- Normalize + derive dogs evidence at the pipeline layer. Surfaces:
--
--   1. Synonym collapse on `allowed`:
--        'restricted' → 'mixed'   (140 rows; downstream beach_dog_policy
--                                  already uses {yes,no,mixed,null} only,
--                                  so this is a strict synonym fold —
--                                  no information loss, just consistency.)
--        'seasonal'   → 'mixed'   (8 rows; seasonal restriction is
--                                  semantically a form of mixed access.)
--
--   2. Synonym collapse on `leash_required`:
--        'required'     → 'on_leash'    (2,266 rows)
--        'not_required' → 'off_leash'   (221 rows)
--
--   3. Derive `allowed` when extractor left it null but other fields
--      imply it:
--        - sections present → roll up to yes/no/mixed
--             • all section rules = 'not_allowed' → 'no'
--             • any not_allowed AND any other rule → 'mixed'
--             • no not_allowed rules → 'yes'
--        - else leash_required ∈ {on_leash, off_leash, mixed} → 'yes'
--      Rows where `allowed` was derived (not extracted directly) get a
--      `_allowed_derived: true` marker in the JSONB for audit.
--
-- Shape:
--   a. Helper function _normalize_dogs_claimed_values(jsonb) returns
--      normalized JSONB. IMMUTABLE, pure transform.
--   b. BEFORE INSERT OR UPDATE trigger on beach_enrichment_provenance
--      that calls the helper when field_group='dogs'.
--   c. One-shot UPDATE on the 13,781 existing dogs BEP rows.
--   d. View vw_pbi_dogs_evidence regains a claim_allowed_derived
--      boolean column reading from the new JSONB marker.
--
-- Doesn't touch the dog-park sibling (dog_park_enrichment_provenance)
-- in this pass — pin a follow-up per [[paired-functions-port-fixes-
-- both-sides]] if the same vocabulary applies there.

BEGIN;

-- ─── Helper ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._normalize_dogs_claimed_values(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $function$
DECLARE
  v_result   jsonb := p;
  v_allowed  text;
  v_leash    text;
  v_sections jsonb;
  v_keys     text[];
  v_k        text;
  v_has_not_allowed boolean := false;
  v_has_other       boolean := false;
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'object' THEN
    RETURN p;
  END IF;

  -- (1a) Fold `allowed`: restricted/seasonal → mixed
  v_allowed := p->>'allowed';
  IF v_allowed IN ('restricted', 'seasonal') THEN
    v_result := jsonb_set(v_result, '{allowed}', '"mixed"'::jsonb);
    v_allowed := 'mixed';
  END IF;

  -- (1b) Fold `leash_required`: required → on_leash, not_required → off_leash
  v_leash := v_result->>'leash_required';
  IF v_leash = 'required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"on_leash"'::jsonb);
    v_leash := 'on_leash';
  ELSIF v_leash = 'not_required' THEN
    v_result := jsonb_set(v_result, '{leash_required}', '"off_leash"'::jsonb);
    v_leash := 'off_leash';
  END IF;

  -- (2) Derive `allowed` when missing
  IF v_allowed IS NULL THEN
    v_sections := v_result->'sections';

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
      -- Leash policy exists → dogs are allowed at least somewhere
      v_result := v_result
        || jsonb_build_object('allowed', 'yes', '_allowed_derived', true);
    END IF;
  END IF;

  RETURN v_result;
END
$function$;

-- ─── Trigger ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._tg_normalize_dogs_bep_before_write()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  IF NEW.field_group = 'dogs' THEN
    NEW.claimed_values := public._normalize_dogs_claimed_values(NEW.claimed_values);
  END IF;
  RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS tg_normalize_dogs_bep_before_write
  ON public.beach_enrichment_provenance;
CREATE TRIGGER tg_normalize_dogs_bep_before_write
BEFORE INSERT OR UPDATE OF claimed_values, field_group
  ON public.beach_enrichment_provenance
FOR EACH ROW
EXECUTE FUNCTION public._tg_normalize_dogs_bep_before_write();

-- ─── One-shot backfill on existing rows ──────────────────────────────
-- Touch every dogs row so the trigger fires + writes the normalized
-- JSONB. Set field_group=field_group is a no-op WHERE-clause UPDATE
-- that still fires the BEFORE trigger.
UPDATE public.beach_enrichment_provenance
   SET claimed_values = public._normalize_dogs_claimed_values(claimed_values)
 WHERE field_group = 'dogs'
   AND claimed_values IS NOT NULL;

-- ─── Update view with claim_allowed_derived flag ─────────────────────
DROP VIEW IF EXISTS public.vw_pbi_dogs_evidence;
CREATE VIEW public.vw_pbi_dogs_evidence AS
SELECT
  bep.gold_fid                                AS fid,
  bep.source,
  bep.source_url,
  bep.confidence,
  bep.is_canonical,
  bep.extraction_type,
  bep.cpad_role,
  bep.cpad_unit_name,
  bep.claimed_values->>'allowed'              AS claim_allowed,
  -- TRUE when claim_allowed was filled in by the normalizer (vs.
  -- directly extracted). Helps audit which rows have inferred vs.
  -- asserted values.
  coalesce((bep.claimed_values->>'_allowed_derived')::boolean, false)
                                              AS claim_allowed_derived,
  bep.claimed_values->>'leash_required'       AS claim_leash_required,
  bep.claimed_values->>'default_rule'         AS claim_default_rule,
  (bep.claimed_values->>'off_leash_exists')::boolean AS claim_off_leash_exists,
  bep.claimed_values->>'source_quote'         AS claim_source_quote,
  bep.claimed_values->>'ordinance_ref'        AS claim_ordinance_ref,
  bep.claimed_values->>'scope_notes'          AS claim_scope_notes,
  bep.claimed_values->>'subtype'              AS claim_subtype,
  bep.claimed_values->>'citation'             AS claim_citation,
  bep.claimed_values->>'notes'                AS claim_notes,
  bep.claimed_values->>'operator_name'        AS claim_operator_name,
  bep.claimed_values->>'mng_name'             AS claim_mng_name,
  bep.claimed_values->>'area_sand'            AS claim_area_sand,
  bep.claimed_values->>'area_water'           AS claim_area_water,
  bep.claimed_values->'time_windows'          AS claim_time_windows,
  bep.claimed_values->'rules'                 AS claim_rules,
  bep.claimed_values->'sections'              AS claim_sections,
  bep.claimed_values                          AS claim_raw_jsonb,
  (bep.source ~* '_no_result$')               AS is_sentinel
FROM public.beach_enrichment_provenance bep
JOIN public.beaches_gold b ON b.fid = bep.gold_fid
WHERE bep.field_group = 'dogs'
  AND b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_evidence IS
  'Long-form evidence list for dogs field_group. claim_allowed is post-normalization (restricted→mixed; required/not_required collapsed on leash_required). claim_allowed_derived flags rows where the value was inferred from sections or leash_required by the normalizer (not directly extracted).';

COMMIT;

NOTIFY pgrst, 'reload schema';
