-- Consensus engine rewrite — Phase 4: backfill BEP rows with
-- policy_source_id + relevance_verified.
--
-- DRAFT — NOT YET APPLIED. Review before running.
--
-- Per docs/consensus_engine_rewrite_design.md and the
-- 2026-05-16 audit findings, this phase tags the ~12,033 existing
-- BEP dog-policy rows with:
-- (a) A policy_source_id FK pointing at an anchor row per source
--     class (so the source-authority tier flows through).
-- (b) relevance_verified = TRUE only where the extraction has a
--     verbatim quote AND the source_url is not flagged as
--     high-fanout (cited for >50 distinct beaches — the
--     "generic state page" pattern from the audit).
--
-- Strategy: one anchor `policy_source` row per existing BEP source
-- label, with subtype assigned per the audit-informed mapping
-- below. All BEP rows from that source label get the same FK.
-- This preserves the source-class assertions ("operator_pad_us
-- is tier 3 agency_admin_policy"); within-tier ranking falls back
-- to confidence and updated_at as before.
--
-- IMPORTANT: this does NOT change the operative consensus engine
-- behavior. promote_canonical_dogs_to_beach_dog_policy still reads
-- from beach_field_consensus; the new policy_source_id is
-- consumed by Phase 5's rewritten promoter. Phase 4 is pure
-- data labeling.
--
-- Idempotency: each anchor's metadata.backfill_source_label is
-- unique. UPDATEs are NOT WHERE-filtered to NULL — re-running
-- harmlessly re-writes the same FK on each row. Indices ensure
-- the re-write is cheap.

-- ─── 1. Create anchor policy_source rows for each source class ─────
-- Idempotency: only insert if no anchor for that source_label exists yet.

INSERT INTO public.policy_source (subtype, citation, scope, source_url, metadata)
SELECT * FROM (VALUES
  -- Tier 3: agency administrative policy
  ('agency_administrative_policy'::policy_source_subtype,
   'PAD-US Dogs Policy Extraction (v1)',
   ARRAY['dog_policy'],
   NULL::text,
   '{"backfill_source_label":"pad_us_dogs_policy_v1","backfill_phase":"4","row_count":1175,"avg_confidence":0.55,"note":"PAD-US-derived agency data; tier 3 because it''s derived from federal/state agency datasets, not from the underlying statute"}'::jsonb),

  ('agency_administrative_policy',
   'Operator-PAD-US Cross-Reference (derived)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"operator_pad_us","backfill_phase":"4","row_count":651,"avg_confidence":0.70,"note":"Operator entity matched to PAD-US unit; derived inference, not direct extraction"}'::jsonb),

  ('agency_administrative_policy',
   'City Policy Extraction',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"city_policy","backfill_phase":"4","row_count":424,"avg_confidence":0.65,"note":"City-published dog policy extractions; tier 3 as agency policy"}'::jsonb),

  ('agency_administrative_policy',
   'CPAD Unit Dogs Policy (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"cpad_unit_dogs_policy_v1","backfill_phase":"4","row_count":12,"avg_confidence":0.70,"note":"CPAD unit-specific dog policy extraction"}'::jsonb),

  ('agency_administrative_policy',
   'State Dogs Policy (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"state_dogs_policy_v1","backfill_phase":"4","row_count":0,"note":"Anchor — state-level dog policy extraction"}'::jsonb),

  -- Tier 4: operator-posted policy
  ('operator_posted_policy'::policy_source_subtype,
   'Park URL Extractor (extraction class)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"park_url","backfill_phase":"4","row_count":477,"avg_confidence":0.86,"note":"Extractions from park-specific URLs; broadly tier 3-4 depending on whether the URL is an agency-published or operator-published page. Default tier 4; can be upgraded per-row via walkthrough verification."}'::jsonb),

  ('operator_posted_policy',
   'Section Research Extractor (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"section_research_v1","backfill_phase":"4","row_count":884,"avg_confidence":"constant 0.65 per audit","note":"LLM-derived section research extractions; tier 4 — operator-page-derived structural content"}'::jsonb),

  ('operator_posted_policy',
   'Operator City Extractor (derived)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"operator_city","backfill_phase":"4","row_count":375,"avg_confidence":"constant 0.50 per audit","note":"Operator-city cross-referenced extraction"}'::jsonb),

  ('operator_posted_policy',
   'Operator Policy Exceptions (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"operator_policy_exceptions_v1","backfill_phase":"4","row_count":23,"avg_confidence":0.73,"note":"Operator-published exceptions to general rules"}'::jsonb),

  ('operator_posted_policy',
   'Operator Dogs Policy (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"operator_dogs_policy_v1","backfill_phase":"4","row_count":6,"avg_confidence":0.70,"note":"Operator-published dog policy extraction"}'::jsonb),

  ('operator_posted_policy',
   'Beach Policy V2 Dogs',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"beach_policy_v2_dogs","backfill_phase":"4","row_count":114,"avg_confidence":0.73,"note":"Generation-2 beach policy extraction; tier 4 default — operator-prose-derived"}'::jsonb),

  ('operator_posted_policy',
   'Research (curator)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"research","backfill_phase":"4","row_count":376,"avg_confidence":"constant 0.85 per audit","note":"Curator-marked research extraction"}'::jsonb),

  -- Tier 5: inferred / derived / repass
  ('inferred'::policy_source_subtype,
   'Old-School LLM Extraction',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"old_school_llm","backfill_phase":"4","row_count":744,"avg_confidence":0.74,"note":"Earlier LLM pass; tier 5 — superseded by newer extraction pipelines"}'::jsonb),

  ('inferred',
   'Text Repass (v1)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"text_repass_v1","backfill_phase":"4","row_count":171,"avg_confidence":0.70,"note":"Second-pass extraction; tier 5"}'::jsonb),

  ('inferred',
   'Zone Rules Derived (v1) [FEEDBACK LOOP]',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"zone_rules_derived_v1","backfill_phase":"4","row_count":155,"avg_confidence":0.71,"note":"FEEDBACK LOOP: BEP rows derived from zone_rules that was itself derived from BEP. Tier 5 (inferred). Should not influence canonical resolution."}'::jsonb),

  ('inferred',
   'Unified V1 Merge',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"unified_v1","backfill_phase":"4","row_count":83,"avg_confidence":0.68,"note":"Merge pass across extractors; tier 5"}'::jsonb),

  ('inferred',
   'JSON Explode',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"json_explode","backfill_phase":"4","row_count":7,"avg_confidence":0.70,"note":"Structural transformation, not a real extraction; tier 5"}'::jsonb),

  -- Tier-equivalent: existing manual entries (small counts, treat as agency_admin)
  ('agency_administrative_policy'::policy_source_subtype,
   'Manual entry (legacy)',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"manual","backfill_phase":"4","row_count":1,"note":"Manual entry from pre-curator era"}'::jsonb),

  ('agency_administrative_policy',
   'Manual Curator entry',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"manual_curator","backfill_phase":"4","row_count":1,"note":"Manual curator entry; tier 3 default — curator-verified content"}'::jsonb)
) AS new_anchors(subtype, citation, scope, source_url, metadata)
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source ps
   WHERE ps.metadata->>'backfill_source_label' =
         new_anchors.metadata->>'backfill_source_label'
);

-- ─── 2. Backfill policy_source_id on existing BEP rows ─────────────
-- Idempotent: re-writes the FK; the WHERE on (field_group='dogs')
-- + non-null source guarantees the same outcome on re-runs.

UPDATE public.beach_enrichment_provenance bep
   SET policy_source_id = anchor.id
  FROM public.policy_source anchor
 WHERE bep.field_group = 'dogs'
   AND bep.source IS NOT NULL
   AND anchor.metadata->>'backfill_source_label' = bep.source
   AND (bep.policy_source_id IS NULL OR bep.policy_source_id <> anchor.id);

-- ─── 3. Set relevance_verified ────────────────────────────────────
-- Conservative: only TRUE when (a) verbatim quote exists in
-- claimed_values, (b) source_url is not on the high-fanout
-- blacklist (>50 distinct beaches per audit finding).

-- Build the high-fanout URL blacklist once for efficient JOIN.
CREATE TEMP TABLE IF NOT EXISTS _fanout_blacklist AS
  SELECT source_url
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND source_url IS NOT NULL
   GROUP BY source_url
  HAVING COUNT(DISTINCT gold_fid) > 50;

UPDATE public.beach_enrichment_provenance bep
   SET relevance_verified = TRUE
 WHERE bep.field_group = 'dogs'
   AND bep.policy_source_id IS NOT NULL
   -- Has verbatim quote in some form
   AND (bep.claimed_values ? 'source_quote'
        OR bep.claimed_values ? 'evidence'
        OR bep.claimed_values ? 'quote')
   -- Has a source URL
   AND bep.source_url IS NOT NULL
   -- Source URL is not high-fanout
   AND NOT EXISTS (
     SELECT 1 FROM _fanout_blacklist b
      WHERE b.source_url = bep.source_url
   );

DROP TABLE _fanout_blacklist;

-- ─── 4. Audit summary ─────────────────────────────────────────────
DO $$
DECLARE
  v_total int;
  v_with_ps int;
  v_relevance_verified int;
  v_anchors int;
BEGIN
  SELECT count(*) INTO v_total
    FROM public.beach_enrichment_provenance WHERE field_group = 'dogs';
  SELECT count(*) INTO v_with_ps
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND policy_source_id IS NOT NULL;
  SELECT count(*) INTO v_relevance_verified
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND relevance_verified = TRUE;
  SELECT count(*) INTO v_anchors
    FROM public.policy_source
   WHERE metadata ? 'backfill_source_label';

  RAISE NOTICE 'Phase 4 backfill summary:';
  RAISE NOTICE '  BEP dog rows total: %', v_total;
  RAISE NOTICE '  BEP dog rows with policy_source_id: % (%.1f%%)',
    v_with_ps, 100.0 * v_with_ps / NULLIF(v_total,0);
  RAISE NOTICE '  BEP dog rows relevance_verified: % (%.1f%%)',
    v_relevance_verified, 100.0 * v_relevance_verified / NULLIF(v_total,0);
  RAISE NOTICE '  Backfill anchor policy_source rows: %', v_anchors;
END$$;
