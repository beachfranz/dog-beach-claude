-- Add penalty_summary column to policy_source for the enforcement
-- clause (fine amounts, infraction/misdemeanor classification, etc.).
-- Per Franz 2026-05-18: real consumer-surface value — users wonder
-- what happens if they bring their dog to a no-dogs beach.
--
-- 1-2 sentence summary; null when source text doesn't mention enforcement.
-- Backfilled for CA via scripts/extract_penalty_from_policy_source.py.

ALTER TABLE public.policy_source
  ADD COLUMN IF NOT EXISTS penalty_summary text;

COMMENT ON COLUMN public.policy_source.penalty_summary IS
  'Enforcement clause summary: classification (infraction / misdemeanor / civil) + '
  'fine amount(s) if specified. 1-2 sentences. NULL when source text does not '
  'mention enforcement. Extracted by Sonnet from full_text via Step 6 LLM rule '
  'decision (going forward) or extract_penalty_from_policy_source.py (backfill).';
