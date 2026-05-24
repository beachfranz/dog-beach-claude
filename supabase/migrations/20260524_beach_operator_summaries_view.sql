-- 20260524_beach_operator_summaries_view.sql
--
-- View that pre-joins the beach → operator_dogs_policy.summary chain
-- so the description generator can fetch operator summaries with a
-- single PostgREST call. Without this, the generator would need 3
-- sequential REST hops across beach_operator → operator (singular) →
-- operators (plural, via canonical_operator_id bridge) →
-- operator_dogs_policy.
--
-- Fills Gap 3 per project_deferred_description_text_reservoir.md —
-- operator summaries (191 substantive rows) carry jurisdictional rule
-- context (e.g. "Cape Lookout off-leash unless near snowy plover
-- nesting Mar 15–Sep 15") that the description prompt was missing.
--
-- One row per (beach_fid, operator_dogs_policy_id). The description
-- generator filters substantive summaries (length >= 80) at query time.

BEGIN;

DROP VIEW IF EXISTS public.beach_operator_summaries;

-- Walks the BEP → operators → operator_dogs_policy chain. BEP.operator_id
-- is the broader "implicit operator" surface — any BEP row tagging the
-- beach with an operator (operator_town, operator_county, operator_pad_us,
-- operator_city, operator_dogs_policy_v1, etc.) becomes a summary candidate.
-- This is wider than beach_operator (only 19 beaches have explicit
-- assignments) — BEP covers thousands of beaches via polygon-derived
-- operator attribution.
--
-- DISTINCT ON (gold_fid, operator_id) so multiple BEP rows for the same
-- operator/beach pair collapse to one summary. Sorted by confidence DESC
-- so the highest-confidence BEP row wins when tied.
CREATE OR REPLACE VIEW public.beach_operator_summaries AS
SELECT DISTINCT ON (bep.gold_fid, bep.operator_id)
    bep.gold_fid                              AS arena_group_id,
    bep.operator_id                           AS extraction_operator_id,
    ops.canonical_name                        AS operator_name,
    ops.level                                 AS operator_level,
    bep.source                                AS bep_source,
    bep.confidence                            AS bep_confidence,
    odp.summary                               AS operator_summary,
    odp.updated_at                            AS summary_updated_at
  FROM public.beach_enrichment_provenance bep
  JOIN public.operators                  ops ON ops.id = bep.operator_id
  JOIN public.operator_dogs_policy       odp ON odp.operator_id = ops.id
 WHERE bep.operator_id IS NOT NULL
   AND odp.summary IS NOT NULL
   AND length(odp.summary) > 0
 ORDER BY bep.gold_fid, bep.operator_id,
          bep.confidence DESC NULLS LAST, bep.updated_at DESC;

COMMENT ON VIEW public.beach_operator_summaries IS
  'beach → operator_dogs_policy.summary chain. One row per '
  '(beach_fid, operator_dogs_policy_id). Filter substantive summaries '
  'via length(operator_summary) >= 80. Read by '
  'scripts/generate_beach_descriptions.py to surface jurisdictional '
  'context (e.g. seasonal closures, named-beach exceptions, code '
  'citations) as grounding for the description prompt.';

-- Grants follow the same anon/auth read pattern as other beach_*
-- consumer views (RLS still enforces the underlying tables).
GRANT SELECT ON public.beach_operator_summaries TO anon, authenticated, service_role;

COMMIT;
