-- 20260601_emit_bep_from_bps_v2_section_priority.sql
--
-- Patch _emit_bep_from_policy_source: prefer SAND-section rules when
-- deriving 'allowed' and 'leash_required' for the BEP claim. The
-- previous version aggregated all rules across sections (sand + global
-- + parking_lot), which over-claimed `has_off_leash=true` on beaches
-- where global municipal code merely MENTIONS off-leash exceptions
-- (e.g. "director may designate", herding/service-dog exemptions).
--
-- Concrete misclassifications spot-checked 2026-06-01:
--   Arroyo Quemada (fid 6102) — SB County §26-49: sand rule is on_leash,
--     global §26-49.1 mentions "community services director may
--     designate off-leash sites". Old emitter -> leash_required='mixed'
--     -> has_off_leash=true. WRONG.
--   Arroyo De La Cruz (fid 6106) — SLO County §9.03.005: sand rule is
--     on_leash; §9.03.005(c) exemption is for herding/service/SAR/LE
--     dogs only. Old emitter elevated to general off-leash. WRONG.
--   Betz Beach (fid 6369) — Riverside County code: sand=on_leash; off_leash
--     reference is generic "may apply per director designation". WRONG.
--   Beer Can Beach (fid 9636) — Sutter County code: same shape. WRONG.
--
-- Abalone Point (fid 6428) was CORRECTLY flipped because its SAND
-- section rule is `off_leash_voice_control` (BLM King Range NCA
-- supplementary rules). The new logic preserves this correct flip.
--
-- After replacing the function, this migration re-emits BEP for every
-- existing (gold_fid, policy_source_id) pair so the corrected logic
-- propagates. Downstream consensus + promote refresh is run separately
-- (Python loop, 1,445 beaches).
--
-- See [[bep-emitted-from-bps-via-trigger]] for the parent design.

BEGIN;

CREATE OR REPLACE FUNCTION public._emit_bep_from_policy_source(
  p_beach_fid        BIGINT,
  p_policy_source_id BIGINT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $fn$
DECLARE
  v_source_tag TEXT := 'codified_v1:ps_' || p_policy_source_id::TEXT;
  v_n_bps      INT;
  v_ps_url     TEXT;
  v_ps_cite    TEXT;
  v_ps_subtype TEXT;
  v_claimed    JSONB;
  v_has_temporal BOOL;
  v_rule_set     JSONB;
  v_temporal_arr JSONB;
  v_verbatim_arr JSONB;
  -- Section-scoped existence flags (sand is the consumer-surface section)
  v_sand_off_leash       BOOL;
  v_sand_on_leash        BOOL;
  v_sand_not_allowed     BOOL;
  v_any_off_leash        BOOL;
  v_any_on_leash         BOOL;
  v_any_not_allowed      BOOL;
BEGIN
  SELECT count(*) INTO v_n_bps
    FROM public.beach_policy_source
   WHERE beach_fid = p_beach_fid
     AND policy_source_id = p_policy_source_id
     AND operative_status = 'operative';

  IF v_n_bps = 0 THEN
    DELETE FROM public.beach_enrichment_provenance
     WHERE gold_fid = p_beach_fid
       AND source   = v_source_tag
       AND field_group = 'dogs';
    RETURN;
  END IF;

  SELECT ps.source_url, ps.citation, ps.subtype
    INTO v_ps_url, v_ps_cite, v_ps_subtype
    FROM public.policy_source ps
   WHERE ps.id = p_policy_source_id;

  SELECT
      jsonb_agg(DISTINCT jsonb_build_object(
        'section',       bps.section,
        'rule',          bps.rule,
        'rule_modifier', bps.rule_modifier,
        'region_name',   bps.region_name
      )),
      jsonb_agg(DISTINCT bps.evidence_verbatim) FILTER (WHERE bps.evidence_verbatim IS NOT NULL)
    INTO v_rule_set, v_verbatim_arr
    FROM public.beach_policy_source bps
   WHERE bps.beach_fid = p_beach_fid
     AND bps.policy_source_id = p_policy_source_id
     AND bps.operative_status = 'operative';

  SELECT
      jsonb_agg(jsonb_build_object(
        'section',           bpst.section,
        'exception_rule',    bpst.exception_rule,
        'window_kind',       bpst.window_kind,
        'effective_from_md', bpst.effective_from_md,
        'effective_to_md',   bpst.effective_to_md,
        'daily_start',       to_char(bpst.daily_start, 'HH24:MI'),
        'daily_end',         to_char(bpst.daily_end,   'HH24:MI'),
        'season_label',      bpst.season_label
      ))
    INTO v_temporal_arr
    FROM public.beach_policy_source_temporal bpst
    JOIN public.beach_policy_source bps ON bps.id = bpst.bps_id
   WHERE bps.beach_fid = p_beach_fid
     AND bps.policy_source_id = p_policy_source_id
     AND bps.operative_status = 'operative';

  v_has_temporal := v_temporal_arr IS NOT NULL AND jsonb_array_length(v_temporal_arr) > 0;

  -- ─── Compute section-scoped existence flags ────────────────────────
  -- The sand section is the consumer-surface answer; global notes are
  -- the broader code context. Sand-section rules MUST take priority
  -- when present, otherwise the noisier global notes pollute the result
  -- (e.g. "director may designate" elevates to has_off_leash=true).

  v_sand_off_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('off_leash','off_leash_voice_control')
  );
  v_sand_on_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' = 'on_leash'
  );
  v_sand_not_allowed := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('not_allowed','prohibited','closed')
  );
  v_any_off_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('off_leash','off_leash_voice_control')
  );
  v_any_on_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' = 'on_leash'
  );
  v_any_not_allowed := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('not_allowed','prohibited','closed')
  );

  v_claimed := jsonb_build_object(
    -- 'allowed' summary: prefer sand-section rules. Temporal exception
    -- (any 'not_allowed' window) bumps to 'mixed' regardless.
    'allowed', CASE
      WHEN v_has_temporal AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
         WHERE t->>'exception_rule' = 'not_allowed'
      ) THEN 'mixed'
      -- Sand has explicit prohibition -> no
      WHEN v_sand_not_allowed AND NOT v_sand_off_leash AND NOT v_sand_on_leash THEN 'no'
      -- Sand has any positive rule -> yes
      WHEN v_sand_off_leash OR v_sand_on_leash THEN 'yes'
      -- Fallback: no sand rule found, use global aggregate
      WHEN v_any_not_allowed AND NOT v_any_off_leash AND NOT v_any_on_leash THEN 'no'
      WHEN v_any_off_leash OR v_any_on_leash THEN 'yes'
      ELSE 'yes'
    END,
    -- 'leash_required' from rule type. SAND-section rules take priority.
    -- Mapping into the downstream derive_has_off_leash vocabulary:
    --   'required'      => has_on_leash=true,  has_off_leash=null/false
    --   'not_required'  => has_on_leash=false, has_off_leash=true
    --   'mixed'         => has_on_leash=true,  has_off_leash=true
    -- So we ONLY emit 'mixed' when the sand-section ITSELF has both
    -- off_leash and on_leash rules (designated off-leash zone within
    -- a generally-on-leash beach). Mere global off_leash mentions do
    -- NOT bump leash_required to 'mixed'.
    'leash_required', CASE
      WHEN v_sand_off_leash AND v_sand_on_leash THEN 'mixed'
      WHEN v_sand_off_leash THEN 'not_required'
      WHEN v_sand_on_leash THEN 'required'
      -- No sand rule: fall back to global aggregate (but only the more
      -- conservative reading; don't infer 'mixed' from global text).
      WHEN v_any_off_leash AND v_any_on_leash THEN 'required'
      WHEN v_any_off_leash THEN 'not_required'
      WHEN v_any_on_leash THEN 'required'
      ELSE 'unknown'
    END,
    'rules',         v_rule_set,
    'time_windows',  v_temporal_arr,
    'evidence',      v_verbatim_arr,
    'citation',      v_ps_cite,
    'subtype',       v_ps_subtype
  );

  INSERT INTO public.beach_enrichment_provenance (
    gold_fid, field_group, source, source_url,
    claimed_values, confidence, is_canonical, policy_source_id,
    notes, updated_at
  ) VALUES (
    p_beach_fid, 'dogs', v_source_tag, v_ps_url,
    v_claimed, 0.95, TRUE, p_policy_source_id,
    'auto-emitted from beach_policy_source by _emit_bep_from_policy_source',
    now()
  )
  ON CONFLICT (gold_fid, field_group, source) DO UPDATE
   SET source_url       = EXCLUDED.source_url,
       claimed_values   = EXCLUDED.claimed_values,
       confidence       = EXCLUDED.confidence,
       is_canonical     = EXCLUDED.is_canonical,
       policy_source_id = EXCLUDED.policy_source_id,
       notes            = EXCLUDED.notes,
       updated_at       = now();
END;
$fn$;

-- ─── Re-emit BEP for every existing (beach, policy_source) pair ───────

DO $reemit$
DECLARE
  r RECORD;
  n INT := 0;
BEGIN
  FOR r IN
    SELECT DISTINCT beach_fid, policy_source_id
      FROM public.beach_policy_source
     WHERE policy_source_id IS NOT NULL
       AND operative_status = 'operative'
  LOOP
    PERFORM public._emit_bep_from_policy_source(r.beach_fid, r.policy_source_id);
    n := n + 1;
  END LOOP;
  RAISE NOTICE 'Re-emitted BEP for % (beach, policy_source) pairs', n;
END;
$reemit$;

COMMIT;
