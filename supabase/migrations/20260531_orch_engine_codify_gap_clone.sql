-- 20260531_orch_engine_codify_gap_clone.sql
--
-- PL/pgSQL port of scripts/codify_clone_gap_beaches.py (352 lines Python).
-- The Python is pure SQL queries wrapped in orchestration; the port is
-- one entry function (_orch_w_codify_gap_clone) plus four helpers.
--
-- Behavior:
--   For each gap beach (active, scoreable, no operative sand bps):
--     1. Find smallest jurisdiction containing it (via jurisdictions_buf200m)
--     2. If that yields a sibling beach in same jurisdiction with operative
--        sand bps, copy its bps + temporal rows to the gap beach
--     3. Else fall back to nearest CDPR-managed CPAD unit (within 200m) and
--        copy from a sibling there
--     4. Call _promote_zone_rules_for_fid(fid) to regenerate zone_rules
--   Idempotent: matching (policy_source_id, section, rule, region_name=NULL)
--   rows are UPDATEd in place instead of duplicated. Temporal rows are
--   wholesale-replaced per cloned bps.
--
-- Knobs (in worker_payload):
--   {"states": ["CA","OR","WA","MD","UT"]}   — defaults to MVP+
--   {"limit": 100}                          — cap gap beaches processed
--   {"only_fid": 6190}                      — process a single beach
--
-- Returns: count summary via RAISE NOTICE; no return value.
--
-- Dagster source-of-truth (scripts/codify_clone_gap_beaches.py) remains
-- in repo for local dev / debugging.

BEGIN;

-- ─── Helper: smallest containing jurisdiction (buffered) ──────────────

CREATE OR REPLACE FUNCTION public._orch_codify_clone_smallest_jurisdiction(p_fid bigint)
RETURNS text LANGUAGE sql STABLE AS $$
  SELECT j.name
    FROM public.beaches_gold bg
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, bg.geom)
    JOIN public.jurisdictions j ON j.id = jb.id
   WHERE bg.fid = p_fid
   ORDER BY ST_Area(j.geom) ASC
   LIMIT 1
$$;

-- ─── Helper: nearest CDPR-managed CPAD unit within 200m ───────────────

CREATE OR REPLACE FUNCTION public._orch_codify_clone_nearest_cdpr_unit(p_fid bigint)
RETURNS text LANGUAGE sql STABLE AS $$
  SELECT cu.unit_name
    FROM public.cpad_units cu
    JOIN public.beaches_gold bg ON bg.fid = p_fid
   WHERE ST_DWithin(bg.geom::geography, cu.geom::geography, 200)
     AND (cu.mng_agncy ILIKE '%Department of Parks%'
          OR cu.mng_agncy ILIKE '%CDPR%'
          OR cu.mng_agncy ILIKE '%State Parks%')
   ORDER BY ST_Distance(bg.geom::geography, cu.geom::geography) ASC
   LIMIT 1
$$;

-- ─── Helper: best template sibling in same jurisdiction ───────────────

CREATE OR REPLACE FUNCTION public._orch_codify_clone_template_sibling(
  p_jurisdiction text, p_target_fid bigint
) RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT bps.beach_fid
    FROM public.beach_policy_source bps
    JOIN public.beaches_gold sibling ON sibling.fid = bps.beach_fid
   WHERE bps.operative_status = 'operative'
     AND bps.beach_fid <> p_target_fid
     AND EXISTS (
       SELECT 1 FROM public.jurisdictions j
        WHERE j.name = p_jurisdiction
          AND ST_Intersects(j.geom, sibling.geom)
     )
   GROUP BY bps.beach_fid
   ORDER BY COUNT(*) DESC, bps.beach_fid ASC
   LIMIT 1
$$;

-- ─── Helper: best template sibling in same CPAD unit ──────────────────

CREATE OR REPLACE FUNCTION public._orch_codify_clone_template_sibling_cdpr(
  p_cdpr_unit text, p_target_fid bigint
) RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT bps.beach_fid
    FROM public.beach_policy_source bps
    JOIN public.beaches_gold sibling ON sibling.fid = bps.beach_fid
   WHERE bps.operative_status = 'operative'
     AND bps.beach_fid <> p_target_fid
     AND EXISTS (
       SELECT 1 FROM public.cpad_units cu
        WHERE cu.unit_name = p_cdpr_unit
          AND ST_DWithin(sibling.geom::geography, cu.geom::geography, 200)
     )
   GROUP BY bps.beach_fid
   ORDER BY COUNT(*) DESC, bps.beach_fid ASC
   LIMIT 1
$$;

-- ─── Helper: clone bps + temporal rows from template to gap ───────────

CREATE OR REPLACE FUNCTION public._orch_codify_clone_one(
  p_template_fid bigint, p_gap_fid bigint
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER AS $clone$
DECLARE
  v_template RECORD;
  v_temporal RECORD;
  v_existing_id BIGINT;
  v_new_bps_id BIGINT;
  v_n_bps INT := 0;
  v_n_temporal INT := 0;
BEGIN
  FOR v_template IN
    SELECT id, policy_source_id, section, rule, rule_modifier,
           evidence_verbatim, evidence_url,
           operative_status, status_basis_id, status_note,
           region_anchor, authority_tier, exemption_type, is_global_note
      FROM public.beach_policy_source
     WHERE beach_fid = p_template_fid
       AND operative_status = 'operative'
       AND region_name IS NULL
     ORDER BY id
  LOOP
    -- Idempotent: match by (ps_id, section, rule, region_name NULL)
    SELECT id INTO v_existing_id
      FROM public.beach_policy_source
     WHERE beach_fid = p_gap_fid
       AND policy_source_id = v_template.policy_source_id
       AND section = v_template.section
       AND rule = v_template.rule
       AND region_name IS NULL
     LIMIT 1;

    IF v_existing_id IS NOT NULL THEN
      UPDATE public.beach_policy_source
         SET operative_status   = v_template.operative_status,
             evidence_verbatim  = v_template.evidence_verbatim,
             evidence_url       = v_template.evidence_url,
             status_basis_id    = v_template.status_basis_id,
             status_note        = v_template.status_note,
             authority_tier     = v_template.authority_tier,
             last_verified      = now()
       WHERE id = v_existing_id;
      v_new_bps_id := v_existing_id;
    ELSE
      INSERT INTO public.beach_policy_source (
        beach_fid, policy_source_id, section, rule, rule_modifier,
        evidence_verbatim, evidence_url,
        operative_status, status_basis_id, status_note,
        region_anchor, authority_tier, exemption_type, is_global_note,
        extracted_at, last_verified, created_at
      ) VALUES (
        p_gap_fid, v_template.policy_source_id, v_template.section,
        v_template.rule, v_template.rule_modifier,
        v_template.evidence_verbatim, v_template.evidence_url,
        v_template.operative_status, v_template.status_basis_id,
        v_template.status_note,
        v_template.region_anchor, v_template.authority_tier,
        v_template.exemption_type, v_template.is_global_note,
        now(), now(), now()
      )
      RETURNING id INTO v_new_bps_id;
      v_n_bps := v_n_bps + 1;
    END IF;

    -- Replace temporal rows wholesale (per section under the cloned bps)
    DELETE FROM public.beach_policy_source_temporal
     WHERE bps_id = v_new_bps_id AND section = v_template.section;

    FOR v_temporal IN
      SELECT section, exception_rule, window_kind,
             effective_from_md, effective_to_md,
             anchor_start, anchor_end,
             daily_start, daily_end,
             season_label, extractor_confidence, notes
        FROM public.beach_policy_source_temporal
       WHERE bps_id = v_template.id
    LOOP
      INSERT INTO public.beach_policy_source_temporal (
        bps_id, beach_fid, policy_source_id, section,
        exception_rule, window_kind,
        effective_from_md, effective_to_md,
        anchor_start, anchor_end,
        daily_start, daily_end,
        season_label, extracted_via, extractor_confidence, notes,
        created_at, updated_at
      ) VALUES (
        v_new_bps_id, p_gap_fid, v_template.policy_source_id, v_temporal.section,
        v_temporal.exception_rule, v_temporal.window_kind,
        v_temporal.effective_from_md, v_temporal.effective_to_md,
        v_temporal.anchor_start, v_temporal.anchor_end,
        v_temporal.daily_start, v_temporal.daily_end,
        v_temporal.season_label,
        format('cloned_from_fid_%s', p_template_fid),
        v_temporal.extractor_confidence,
        v_temporal.notes,
        now(), now()
      );
      v_n_temporal := v_n_temporal + 1;
    END LOOP;
  END LOOP;

  RETURN jsonb_build_object('n_bps', v_n_bps, 'n_temporal', v_n_temporal);
END;
$clone$;

-- ─── Entry: _orch_w_codify_gap_clone ──────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_codify_gap_clone(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $entry$
DECLARE
  v_states     TEXT[];
  v_limit      INT;
  v_only_fid   BIGINT;
  v_n_cloned   INT := 0;
  v_n_unhandled INT := 0;
  v_n_failed   INT := 0;
  v_gap        RECORD;
  v_jurisdiction TEXT;
  v_cdpr_unit  TEXT;
  v_template   BIGINT;
  v_match_kind TEXT;
  v_match_label TEXT;
  v_counts     JSONB;
BEGIN
  v_states := COALESCE(
    ARRAY(SELECT jsonb_array_elements_text(p->'states')),
    ARRAY['CA','OR','WA','MD','UT']::TEXT[]
  );
  v_limit := COALESCE((p->>'limit')::INT, 99999);
  v_only_fid := (p->>'only_fid')::BIGINT;

  FOR v_gap IN
    SELECT bg.fid, COALESCE(bg.display_name_override, bg.name) AS name
      FROM public.beaches_gold bg
     WHERE bg.is_active
       AND (v_only_fid IS NOT NULL AND bg.fid = v_only_fid
            OR v_only_fid IS NULL
               AND bg.state = ANY(v_states)
               AND bg.scoring_tier IN ('daily','hourly')
               AND NOT EXISTS (
                 SELECT 1 FROM public.beach_policy_source bps
                  WHERE bps.beach_fid = bg.fid
                    AND bps.operative_status = 'operative'
                    AND bps.section = 'sand'
               ))
     ORDER BY bg.fid
     LIMIT v_limit
  LOOP
    v_template := NULL;
    v_match_kind := NULL;
    v_match_label := NULL;

    v_jurisdiction := public._orch_codify_clone_smallest_jurisdiction(v_gap.fid);
    IF v_jurisdiction IS NOT NULL THEN
      v_template := public._orch_codify_clone_template_sibling(v_jurisdiction, v_gap.fid);
      IF v_template IS NOT NULL THEN
        v_match_kind := 'juri';
        v_match_label := v_jurisdiction;
      END IF;
    END IF;

    IF v_template IS NULL THEN
      v_cdpr_unit := public._orch_codify_clone_nearest_cdpr_unit(v_gap.fid);
      IF v_cdpr_unit IS NOT NULL THEN
        v_template := public._orch_codify_clone_template_sibling_cdpr(v_cdpr_unit, v_gap.fid);
        IF v_template IS NOT NULL THEN
          v_match_kind := 'cdpr';
          v_match_label := v_cdpr_unit;
        END IF;
      END IF;
    END IF;

    IF v_template IS NULL THEN
      v_n_unhandled := v_n_unhandled + 1;
      CONTINUE;
    END IF;

    BEGIN
      v_counts := public._orch_codify_clone_one(v_template, v_gap.fid);
      PERFORM public._promote_zone_rules_for_fid(v_gap.fid);
      v_n_cloned := v_n_cloned + 1;
      RAISE NOTICE 'codify_gap_clone: OK fid=% % match=%=% bps+=% temp+=%',
        v_gap.fid, v_gap.name, v_match_kind, v_match_label,
        v_counts->>'n_bps', v_counts->>'n_temporal';
    EXCEPTION WHEN OTHERS THEN
      v_n_failed := v_n_failed + 1;
      RAISE NOTICE 'codify_gap_clone: FAIL fid=% % %', v_gap.fid, v_gap.name, SQLERRM;
    END;
  END LOOP;

  RAISE NOTICE 'codify_gap_clone done: cloned=% unhandled=% failed=%',
    v_n_cloned, v_n_unhandled, v_n_failed;
END;
$entry$;

COMMENT ON FUNCTION public._orch_w_codify_gap_clone IS
  'orch_jobs worker: clone codified bps+temporal rows from sibling beaches to gap beaches in same jurisdiction or CDPR unit.';

-- ─── Update catalog: noop → sql_function ─────────────────────────────

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_codify_gap_clone',
       worker_payload = jsonb_build_object(
         'states', jsonb_build_array('CA','OR','WA','MD','UT')
       ),
       description = 'Clone operative sand bps + temporal rows from sibling beaches in same jurisdiction or CDPR park unit. PL/pgSQL port of Dagster codify_gap_clone (which wrapped scripts/codify_clone_gap_beaches.py).',
       updated_at = now()
 WHERE job_name = 'weekly_codify_gap_clone';

COMMIT;
