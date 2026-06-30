-- 20260630b_offleash_carveout_recurrence.sql
--
-- RECURRENCE PLUG for the R1 off-leash carve-out fix (regression register
-- 2026-06-23). The is_carveout fix was a one-time backfill; codify
-- (extract_and_load_deep.py) keeps inserting off-leash carve-out rows with
-- is_carveout=false (default), so the inflation recurs on the next state run.
--
-- Durable fix (chosen 2026-06-30, classifier-only — NOT a prompt edit): make
-- carve-out classification a mandatory post-codify pipeline phase that reuses
-- the proven deterministic regex + the LLM classifier. Both the INSERT and
-- UPDATE statement triggers on beach_policy_source call _refresh_beaches_from_ps
-- (consensus + promote), so UPDATEing is_carveout AUTO-RE-PROMOTES the beach —
-- no manual re-promote needed.
--
-- This migration adds:
--   1. carveout_classified_at — a marker so the recurring phase only classifies
--      genuinely-new off-leash rows (re-runs are cheap; no LLM re-burn). Replaces
--      the fragile tmp/ checkpoint the classifier script used.
--   2. tag_offleash_carveouts_deterministic(p_state) — the 20260623b high-
--      precision regex pass, now a reusable, state-scopeable function. Runs free
--      before the LLM pass each codify run.
--
-- NO backfill: existing off-leash rows are left carveout_classified_at=NULL on
-- purpose so the first scoped classifier run re-validates them (= W3 validation
-- pass) AND establishes the marker baseline in one go. The trigger re-promote
-- means corrections flow to the consumer surface automatically.

-- 1. marker column ----------------------------------------------------------
ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS carveout_classified_at timestamptz;

COMMENT ON COLUMN public.beach_policy_source.carveout_classified_at IS
  'When this off_leash row was carve-out-classified (deterministic tagger or LLM). NULL = not yet classified → picked up by the post-codify carve-out phase. Lets the recurring phase skip already-classified rows without re-burning LLM cost.';

-- 2. reusable deterministic tagger -----------------------------------------
-- Ports the 20260623b high-precision regex families + genuine-off-leash guards.
-- Tags clear carve-outs (is_carveout=true) and stamps the marker; non-matching
-- off-leash rows stay carveout_classified_at=NULL so the LLM pass adjudicates
-- the ambiguous/genuine boundary. Idempotent (marker gate). Scope by comma-
-- separated p_state (NULL = all active scoring beaches).
CREATE OR REPLACE FUNCTION public.tag_offleash_carveouts_deterministic(p_state text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_n integer;
BEGIN
  UPDATE public.beach_policy_source bps
     SET is_carveout = true,
         carveout_classified_at = now()
    FROM public.beaches_gold g
   WHERE g.fid = bps.beach_fid
     AND g.is_active
     AND g.scoring_tier IN ('daily','hourly')
     AND (p_state IS NULL OR g.state = ANY (string_to_array(p_state, ',')))
     AND bps.rule IN ('off_leash','off_leash_voice_control')
     AND bps.carveout_classified_at IS NULL
     -- guard: never tag a genuine off-leash signal as a carve-out
     AND bps.evidence_verbatim !~* 'voice.?control|reliably responsive|recall command|sufficiently trained|dog beach|leash.?free'
     AND (
          -- dog-park / fenced exercise area
          bps.evidence_verbatim ~* 'dog (exercise|park|run)|fenced.{0,25}(area|enclosure)|confines of (a |an )?(authorized )?dog park|authorized dog park'
          -- activity exemptions (voice-control removed)
       OR bps.evidence_verbatim ~* 'organized obedience|obedience class|service (animal|dog)|guide dog|seeing.?eye|police dog|law enforcement|peace officer|\mK-?9\M|hunting|herding|guarding livestock|field trial|search and rescue'
          -- private property / at home
       OR bps.evidence_verbatim ~* 'private property|owner.?s (premises|property)|\mat home\M|on the property of|premises of (the |its )?owner'
          -- definition of "at large"
       OR bps.evidence_verbatim ~* '\mat large\M.{0,8}(means|includes)|means.{0,25}(not leashed|off the (owner|property))'
          -- supervised event / dog show
       OR bps.evidence_verbatim ~* 'supervised public event|organized (activity|event)|dog show'
     );
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$$;

COMMENT ON FUNCTION public.tag_offleash_carveouts_deterministic(text) IS
  'Post-codify deterministic carve-out tagger (reusable 20260623b regex). Tags high-precision off-leash carve-outs + stamps carveout_classified_at. Run before classify_offleash_carveout.py in the pipeline; the LLM pass handles the rest. Returns rows tagged.';
