-- 20260601_emit_bep_from_bps.sql
--
-- Closes the BEP/BPS architectural gap surfaced today.
--
-- When a codify run writes beach_policy_source (+ optional
-- beach_policy_source_temporal), we now ALSO emit a canonical BEP row
-- pointing at the same policy_source. That way the BEP-side consensus
-- resolver hears the same authoritative codified rule the BPS-side
-- cascade does. Today LJS's consensus only knew about bringfido +
-- county + state-baseline; the operative SD City Muni Code was
-- invisible to BEP. Same problem applies wherever a codify script
-- writes BPS without touching BEP.
--
-- See [[codify-cascade-reads-bps-not-bep]].
--
-- Architecture:
--   1. _emit_bep_from_policy_source(beach_fid, policy_source_id):
--      Synthesizes one BEP row from all current BPS+BPST data for that
--      (beach, policy_source) pair. Upserts. Deletes if no BPS remain.
--   2. Trigger on beach_policy_source AFTER INSERT/UPDATE/DELETE.
--   3. Backfill: call (1) for every existing distinct pair.
--
-- The BEP row uses source = 'codified_v1:ps_<id>' so multiple codified
-- rules from different policy_sources coexist on one beach without
-- clobbering each other (BEP unique key is (gold_fid, field_group,
-- source)).

BEGIN;

-- ─── Synthesizer ──────────────────────────────────────────────────────

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
BEGIN
  -- Count current operative BPS rows for this pair
  SELECT count(*) INTO v_n_bps
    FROM public.beach_policy_source
   WHERE beach_fid = p_beach_fid
     AND policy_source_id = p_policy_source_id
     AND operative_status = 'operative';

  -- No operative BPS rows → delete any stale BEP row we own
  IF v_n_bps = 0 THEN
    DELETE FROM public.beach_enrichment_provenance
     WHERE gold_fid = p_beach_fid
       AND source   = v_source_tag
       AND field_group = 'dogs';
    RETURN;
  END IF;

  -- Pull policy_source metadata
  SELECT ps.source_url, ps.citation, ps.subtype
    INTO v_ps_url, v_ps_cite, v_ps_subtype
    FROM public.policy_source ps
   WHERE ps.id = p_policy_source_id;

  -- Aggregate distinct rules + verbatims from operative BPS rows
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

  -- Aggregate temporal windows
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

  -- Derive consumer-friendly fields from rules + temporal data
  v_claimed := jsonb_build_object(
    -- 'allowed' summary: if any operative rule is on_leash/off_leash AND there's a
    -- BPST 'not_allowed' window, this is "mixed". Otherwise reflect the rule.
    'allowed', CASE
      WHEN v_has_temporal AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_temporal_arr) t
         WHERE t->>'exception_rule' = 'not_allowed'
      ) THEN 'mixed'
      WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' IN ('off_leash','off_leash_voice_control')
      ) THEN 'yes'
      WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' IN ('not_allowed','prohibited','closed')
      ) THEN 'no'
      ELSE 'yes'
    END,
    -- 'leash_required' from rule type
    'leash_required', CASE
      WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' IN ('off_leash','off_leash_voice_control')
      ) AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' = 'on_leash'
      ) THEN 'mixed'
      WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' IN ('off_leash','off_leash_voice_control')
      ) THEN 'not_required'
      WHEN EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_rule_set) r
         WHERE r->>'rule' = 'on_leash'
      ) THEN 'required'
      ELSE 'unknown'
    END,
    'rules',         v_rule_set,
    'time_windows',  v_temporal_arr,
    'evidence',      v_verbatim_arr,
    'citation',      v_ps_cite,
    'subtype',       v_ps_subtype
  );

  -- Upsert the BEP row. Confidence is high — this is the codified rule
  -- straight from the policy_source. is_canonical=true so the resolver
  -- prefers it over noise.
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

-- ─── Trigger glue ─────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._tg_emit_bep_from_bps()
RETURNS TRIGGER LANGUAGE plpgsql AS $tg$
BEGIN
  -- Handle INSERT/UPDATE: re-emit BEP for the new (beach, policy_source)
  IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.policy_source_id IS NOT NULL THEN
    PERFORM public._emit_bep_from_policy_source(NEW.beach_fid, NEW.policy_source_id);
  END IF;
  -- Handle UPDATE that moved policy_source_id: clean the old one
  IF TG_OP = 'UPDATE' AND OLD.policy_source_id IS DISTINCT FROM NEW.policy_source_id
     AND OLD.policy_source_id IS NOT NULL THEN
    PERFORM public._emit_bep_from_policy_source(OLD.beach_fid, OLD.policy_source_id);
  END IF;
  -- Handle DELETE: re-emit BEP for the old pair (will delete BEP if no
  -- operative BPS rows remain)
  IF TG_OP = 'DELETE' AND OLD.policy_source_id IS NOT NULL THEN
    PERFORM public._emit_bep_from_policy_source(OLD.beach_fid, OLD.policy_source_id);
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$tg$;

DROP TRIGGER IF EXISTS tg_emit_bep_from_bps ON public.beach_policy_source;
CREATE TRIGGER tg_emit_bep_from_bps
  AFTER INSERT OR UPDATE OR DELETE ON public.beach_policy_source
  FOR EACH ROW EXECUTE FUNCTION public._tg_emit_bep_from_bps();

-- ─── Backfill ─────────────────────────────────────────────────────────

DO $backfill$
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
  RAISE NOTICE 'backfilled BEP for % (beach, policy_source) pairs', n;
END;
$backfill$;

COMMIT;
