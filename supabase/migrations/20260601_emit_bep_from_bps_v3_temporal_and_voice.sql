-- 20260601_emit_bep_from_bps_v3_temporal_and_voice.sql
--
-- Three more emitter bugs surfaced after the v2 sand-section-priority fix:
--
-- Bug 1: Temporal `exception_rule='on_leash'` (95 rows) wasn't bumping
--        leash_required to 'mixed'. Beaches normally off-leash with
--        on-leash REQUIRED during peak-hour windows were surfacing as
--        plain off-leash, hiding the time-aware restriction.
--
-- Bug 2: Temporal `exception_rule='off_leash'` (16) and
--        `'off_leash_voice_control'` (17) weren't bumping leash_required
--        to 'mixed' either. Beaches normally on-leash with off-leash
--        time windows were hiding the off-leash opportunity.
--
-- Bug 3: Rule value `on_leash_or_voice` (67 global rows; 2 beaches
--        sand-only) wasn't recognized by sand or aggregate gates.
--        Beaches fell through to leash_required='unknown'. Now treated
--        as on_leash for derivation purposes (the voice-control nuance
--        is preserved in the rules jsonb for downstream renderers).
--
-- v3 also includes a re-emit loop over all (beach, policy_source) pairs
-- so the corrected logic propagates.

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
  v_sand_off_leash       BOOL;
  v_sand_on_leash        BOOL;
  v_sand_not_allowed     BOOL;
  v_any_off_leash        BOOL;
  v_any_on_leash         BOOL;
  v_any_not_allowed      BOOL;
  -- v3 additions
  v_temporal_not_allowed BOOL;
  v_temporal_on_leash    BOOL;
  v_temporal_off_leash   BOOL;
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

  -- ─── Section-scoped rule flags ────────────────────────────────────
  -- v3 — on_leash_or_voice is treated as a flavor of on_leash for the
  -- sand-section detection. The voice-control nuance is preserved in
  -- the rules jsonb for downstream renderers.
  v_sand_off_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('off_leash','off_leash_voice_control')
  );
  v_sand_on_leash := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'section' = 'sand'
       AND r->>'rule' IN ('on_leash','on_leash_or_voice')
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
     WHERE r->>'rule' IN ('on_leash','on_leash_or_voice')
  );
  v_any_not_allowed := EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_rule_set) r
     WHERE r->>'rule' IN ('not_allowed','prohibited','closed')
  );

  -- ─── Temporal-scoped flags (v3) ───────────────────────────────────
  v_temporal_not_allowed := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' = 'not_allowed'
  );
  v_temporal_on_leash := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' = 'on_leash'
  );
  v_temporal_off_leash := v_has_temporal AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
     WHERE t->>'exception_rule' IN ('off_leash','off_leash_voice_control')
  );

  v_claimed := jsonb_build_object(
    -- 'allowed' — temporal not_allowed bumps to 'mixed' (existing behavior)
    'allowed', CASE
      WHEN v_temporal_not_allowed THEN 'mixed'
      WHEN v_sand_not_allowed AND NOT v_sand_off_leash AND NOT v_sand_on_leash THEN 'no'
      WHEN v_sand_off_leash OR v_sand_on_leash THEN 'yes'
      WHEN v_any_not_allowed AND NOT v_any_off_leash AND NOT v_any_on_leash THEN 'no'
      WHEN v_any_off_leash OR v_any_on_leash THEN 'yes'
      ELSE 'yes'
    END,
    -- 'leash_required' — v3 expands to handle temporal on_leash and
    -- off_leash exceptions. A beach with sand=off_leash whose temporal
    -- says on_leash-required-during-peak-hours is leash_required='mixed';
    -- vice versa for sand=on_leash with off_leash temporal windows.
    'leash_required', CASE
      WHEN v_sand_off_leash AND v_sand_on_leash THEN 'mixed'
      WHEN v_sand_off_leash AND v_temporal_on_leash THEN 'mixed'
      WHEN v_sand_on_leash  AND v_temporal_off_leash THEN 'mixed'
      WHEN v_sand_off_leash THEN 'not_required'
      WHEN v_sand_on_leash THEN 'required'
      -- No sand rule — global aggregate, with temporal overrides
      WHEN v_any_off_leash AND v_temporal_on_leash THEN 'mixed'
      WHEN v_any_on_leash  AND v_temporal_off_leash THEN 'mixed'
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
  RAISE NOTICE 'v3 re-emit complete: % (beach, policy_source) pairs', n;
END;
$reemit$;

COMMIT;
