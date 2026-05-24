-- 20260523_operator_extractions_no_url_sentinel.sql
--
-- Add 'no_url_found' to the source_kind CHECK on
-- operator_policy_extractions. When the URL picker rejects every Tavily
-- hit (common for defunct agencies, school districts, USACE-operated
-- lakes), extract_operator_dogs_policy.py now writes a sentinel row
-- with source_kind='no_url_found', source_url='' so that
-- recently_extracted() caches the no-policy outcome and future runs
-- skip the (Tavily × 2) + (Haiku picker) cost.
--
-- Discovered 2026-05-23 EVENING during NH state launch — 23/66 NH
-- operators had no extraction row and burned ~5-15s LLM cost on every
-- pipeline run. Reusable lift across all states.

BEGIN;

ALTER TABLE public.operator_policy_extractions
  DROP CONSTRAINT IF EXISTS operator_policy_extractions_source_kind_check;

ALTER TABLE public.operator_policy_extractions
  ADD CONSTRAINT operator_policy_extractions_source_kind_check
  CHECK (source_kind = ANY (ARRAY[
    'direct_url',
    'site_search',
    'manual',
    'no_url_found'
  ]));

COMMIT;
