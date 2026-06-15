-- 20260615a_tide_demote_one_severity.sql
--
-- Demote tide_neg by one severity tier in the v2_signal_status helper:
--   ≥ 7 ft → caution   (was no_go)
--   ≥ 5 ft → advisory  (was caution)
--   ≥ 3 ft → clear     (was advisory)
--   < 3 ft → clear     (unchanged)
--
-- Per Franz 2026-06-15: tide isn't a safety issue per se — high tide
-- limits beach area but doesn't endanger the dog. Capping the maximum
-- severity at caution + dropping the 3-5 ft advisory signal entirely
-- aligns the cascade with how the human reasons about it.
--
-- Pattern matches feels_like_cold / feels_like_hot / precip_chance,
-- which already special-case in the function above the band lookup.
-- The tide_neg band rows in scoring_config_v2 stay as-is for the
-- numeric score → composite calculation; this only changes the
-- severity-label mapping that drives the consumer-surface tile color
-- and the status board on detail.html.
--
-- Sibling change: _shared/scoring.ts (tideStatus) + beach-chat/index.ts
-- (v2StatusFor "tide" case) updated in the same ship per
-- [[v2-signal-status-mapping]].

BEGIN;

CREATE OR REPLACE FUNCTION public.v2_signal_status(
  p_entity_type text,
  p_signal_key  text,
  p_raw_value   numeric
) RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_bands     jsonb;
  v_score     int;
  v_max_score int;
  v_frac      numeric;
BEGIN
  IF p_raw_value IS NULL THEN RETURN NULL; END IF;

  -- ── Non-banded specials ────────────────────────────────────────
  IF p_signal_key = 'feels_like_cold' THEN
    IF p_raw_value <= 20 THEN RETURN 'no_go';    END IF;
    IF p_raw_value <= 32 THEN RETURN 'caution';  END IF;
    IF p_raw_value <= 50 THEN RETURN 'advisory'; END IF;
    RETURN 'clear';
  ELSIF p_signal_key = 'feels_like_hot' THEN
    IF p_raw_value >= 125 THEN RETURN 'no_go';    END IF;
    IF p_raw_value >= 95  THEN RETURN 'caution';  END IF;
    IF p_raw_value >= 85  THEN RETURN 'advisory'; END IF;
    RETURN 'clear';
  ELSIF p_signal_key = 'precip_chance' THEN
    IF p_raw_value >= 80 THEN RETURN 'no_go';    END IF;
    IF p_raw_value >= 50 THEN RETURN 'caution';  END IF;
    IF p_raw_value >= 30 THEN RETURN 'advisory'; END IF;
    RETURN 'clear';
  ELSIF p_signal_key = 'tide_neg' THEN
    -- 20260615a: capped at caution. Tide is an availability signal,
    -- not a safety one — there is no tide value where a dog is in
    -- meaningful danger on a beach. The numeric tide_neg score in
    -- scoring_config_v2 still drives composite score adjustments;
    -- only the severity LABEL is demoted here.
    IF p_raw_value >= 7 THEN RETURN 'caution';  END IF;
    IF p_raw_value >= 5 THEN RETURN 'advisory'; END IF;
    RETURN 'clear';
  END IF;

  -- ── Banded penalty signals ─────────────────────────────────────
  SELECT bands INTO v_bands
    FROM public.scoring_config_v2
   WHERE entity_type = p_entity_type
     AND signal_key  = p_signal_key;
  IF v_bands IS NULL THEN RETURN NULL; END IF;

  IF p_signal_key = 'uv_neg'        AND p_raw_value >= 11  THEN RETURN 'no_go'; END IF;
  IF p_signal_key = 'asphalt_neg'   AND p_raw_value >= 125 THEN RETURN 'no_go'; END IF;
  IF p_signal_key = 'sand_temp_neg' AND p_raw_value >= 145 THEN RETURN 'no_go'; END IF;

  v_score := public._v2_lookup_band(p_raw_value, v_bands);

  SELECT max((b->>'score')::int) INTO v_max_score
    FROM jsonb_array_elements(v_bands) b
   WHERE (b->>'score')::int > 0;

  IF v_score <= 0 OR v_max_score IS NULL OR v_max_score = 0 THEN
    RETURN 'clear';
  END IF;

  v_frac := v_score::numeric / v_max_score::numeric;
  IF v_frac >= 0.67 THEN RETURN 'no_go';   END IF;
  IF v_frac >= 0.34 THEN RETURN 'caution'; END IF;
  RETURN 'advisory';
END;
$$;

COMMIT;

NOTIFY pgrst, 'reload schema';
