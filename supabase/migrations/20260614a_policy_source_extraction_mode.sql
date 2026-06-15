-- 20260614a_policy_source_extraction_mode.sql
--
-- Refactor scope: codified-extraction pipeline shift from per-(beach, ps)
-- LLM calls to per-policy_source extraction with cached output.
--
-- Today extract_and_load_deep.py has a pair-mode loop that calls Sonnet
-- 3,316 times (once per beach_policy_source row) to extract the SAME
-- statute against slightly different (beach_name, jurisdiction) framing.
-- WAC §352-32-060 alone gets 401 calls and lands ~395 "yes" + ~6 "no"
-- due to LLM non-determinism.
--
-- The refactor categorizes each policy_source by exception structure so
-- the extractor knows whether one cached extraction can be applied
-- deterministically (universal), applied with a deterministic name-match
-- check for exceptions (named_exception), or actually needs per-beach LLM
-- because the statute references "designated areas / where posted"
-- (fuzzy_exception), or is structurally per-property (per_beach, e.g. MOU).
--
-- This migration adds the category column. Phase 2 (Sonnet categorizer)
-- and Phase 3 (refactored extractor) consume it.

BEGIN;

ALTER TABLE public.policy_source
  ADD COLUMN IF NOT EXISTS extraction_mode text;

ALTER TABLE public.policy_source
  DROP CONSTRAINT IF EXISTS policy_source_extraction_mode_check;

ALTER TABLE public.policy_source
  ADD CONSTRAINT policy_source_extraction_mode_check
  CHECK (extraction_mode IS NULL
      OR extraction_mode IN ('universal','named_exception','fuzzy_exception','per_beach'));

COMMENT ON COLUMN public.policy_source.extraction_mode IS
  'Refactor 20260614: drives extract_and_load_deep routing. universal = one cached extraction deterministically applied to every beach. named_exception = cached extraction + deterministic name_match against parsed.exceptions. fuzzy_exception = cached base rule + per-beach LLM for "designated areas" carve-outs. per_beach = structurally per-property (e.g. MOU), pair-mode required. NULL = legacy / not yet categorized — extractor falls back to pair-mode for safety.';

-- Index helps the extractor cheaply filter pending policy_sources by mode
CREATE INDEX IF NOT EXISTS policy_source_extraction_mode_idx
  ON public.policy_source (extraction_mode);

COMMIT;

NOTIFY pgrst, 'reload schema';
