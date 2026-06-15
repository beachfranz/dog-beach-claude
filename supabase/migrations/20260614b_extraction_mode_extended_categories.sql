-- 20260614b_extraction_mode_extended_categories.sql
--
-- Extend policy_source.extraction_mode CHECK constraint with three new
-- categories revealed by 2026-06-14 categorizer audit:
--
--   universal_with_fuzzy_carveout — default rule is universal across all
--     in-scope beaches, AND a fuzzy carveout subset needs per-beach LLM.
--     Replaces fuzzy_exception for cases like WAC §352-32-060 ("leash
--     everywhere except designated swimming beaches") where the DEFAULT
--     is universal and only the carveout subset is fuzzy. Today those
--     401 beaches all hit pair-mode; with this category the extractor
--     can apply the cached default deterministically and only LLM the
--     carveout subset (~30 beaches).
--
--   cross_reference — the statute points to another statute/section as
--     the source of exceptions (e.g. "except as provided in §6.16.310").
--     Today ps=8 is misclassified as named_exception with
--     excepted_places=["Section 6.16.310"] — name_match against beach
--     names will never hit, so the carveout is silently lost. With this
--     category the extractor knows to resolve transitively.
--
--   deferred — the statute defers to external state ("where signs
--     posted", "with permission of the parks director") that the LLM
--     cannot resolve from text alone. Today these are fuzzy_exception,
--     and the per-beach disambiguator hallucinates answers. With this
--     category the extractor emits an honest "cannot disambiguate"
--     sentinel that propagates to UI as "check on site / contact
--     operator" instead of fabricating a verdict.
--
-- All three new modes route to pair-mode in extract_and_load_deep for
-- now (same behavior as fuzzy_exception/per_beach). Smarter handlers
-- (carveout-only LLM, cross-ref resolver, sentinel emitter) are
-- follow-ups once the categorizer reclassifies the corpus.

BEGIN;

ALTER TABLE public.policy_source
  DROP CONSTRAINT IF EXISTS policy_source_extraction_mode_check;

ALTER TABLE public.policy_source
  ADD CONSTRAINT policy_source_extraction_mode_check
  CHECK (extraction_mode IS NULL OR extraction_mode IN (
    'universal',
    'named_exception',
    'fuzzy_exception',
    'per_beach',
    'universal_with_fuzzy_carveout',
    'cross_reference',
    'deferred'
  ));

COMMENT ON COLUMN public.policy_source.extraction_mode IS
  'Refactor 20260614: drives extract_and_load_deep routing. '
  'universal = one cached extraction deterministically applied to every beach. '
  'named_exception = cached extraction + deterministic name_match against parsed.exceptions. '
  'fuzzy_exception = cached base rule + per-beach LLM for "designated areas" carve-outs. '
  'per_beach = structurally per-property (e.g. MOU), pair-mode required. '
  'universal_with_fuzzy_carveout = universal default + per-beach LLM only for the carveout subset (20260614b). '
  'cross_reference = exceptions point to another policy_source; resolve transitively (20260614b). '
  'deferred = statute defers to signage/operator permission; emit sentinel rather than hallucinate (20260614b). '
  'NULL = legacy / not yet categorized — extractor falls back to pair-mode for safety.';

COMMIT;

NOTIFY pgrst, 'reload schema';
