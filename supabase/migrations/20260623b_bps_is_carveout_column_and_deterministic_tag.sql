-- 20260623b_bps_is_carveout_column_and_deterministic_tag.sql
--
-- R1 fix, step 1 of 3 (regression audit 2026-06-23). The off-leash flag is
-- inflated because _canonical_dogs_from_policy_sources sets off_leash_flag if
-- ANY operative section is off_leash — so a carve-out clause (off-leash at a
-- dog park / on private property / during an activity / a definition) flips the
-- whole beach off-leash. Fix: tag carve-out off-leash sections, then (step 3)
-- exclude them from the roll-up. The section-level data itself stays intact.
--
-- This migration: add an ADDITIVE `is_carveout` boolean (do NOT touch
-- operative_status, which other code reads), and deterministically tag the
-- HIGH-PRECISION clear-leak families validated against samples 2026-06-23:
--   * dog-park / fenced exercise area / "authorized dog park" exemption
--   * activity exemption: obedience class / service / guide / law-enforcement /
--     police / K-9 / hunting / herding / field trial / search-and-rescue
--   * private-property / "at home" / owner's premises
--   * definition-of-"at large"
--   * supervised public event / dog show
-- with GUARDS excluding genuine-off-leash signals (voice-control / recall /
-- "dog beach" / "leash-free") so real voice-control + dog-beach off-leash
-- beaches are NOT mis-tagged — those go to the LLM pass (step 2).
--
-- Nothing reads is_carveout yet, so the consumer surface is unchanged until the
-- roll-up fix lands in step 3.

ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS is_carveout boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.beach_policy_source.is_carveout IS
  'TRUE when this off_leash section is a carve-out/exemption that does NOT make the BEACH off-leash (dog park, private property, activity exemption, other named place, definition). Set deterministically (20260623b) + LLM (step 2). The dogs roll-up excludes is_carveout rows from the beach-wide off_leash flag.';

UPDATE public.beach_policy_source bps
   SET is_carveout = true
 WHERE bps.rule IN ('off_leash','off_leash_voice_control')
   AND bps.is_carveout = false
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
