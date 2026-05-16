-- Consensus engine rewrite — Phase 4.1: close Phase 4 mapping gap
-- by adding anchors for operator_county and county_policy.
--
-- Phase 4 missed these two BEP source labels in the initial mapping
-- (2,888 rows unlabeled = 24% of dog BEP). Both are county-level
-- sources parallel to existing city-level mappings; both fit tier 3
-- agency_administrative_policy.
--
-- Idempotent: anchor inserts guarded by NOT EXISTS on
-- backfill_source_label; UPDATE writes the same FK on re-run.

-- ─── 1. Anchor rows for the two missed source labels ──────────────

INSERT INTO public.policy_source (subtype, citation, scope, source_url, metadata)
SELECT * FROM (VALUES
  ('agency_administrative_policy'::policy_source_subtype,
   'Operator-County Cross-Reference (derived)',
   ARRAY['dog_policy'],
   NULL::text,
   '{"backfill_source_label":"operator_county","backfill_phase":"4.1","row_count":1672,"note":"Operator entity cross-referenced to county records; parallel to operator_pad_us / operator_city. Tier 3 — county is a government entity."}'::jsonb),

  ('agency_administrative_policy',
   'County Policy Extraction',
   ARRAY['dog_policy'],
   NULL,
   '{"backfill_source_label":"county_policy","backfill_phase":"4.1","row_count":1216,"note":"County-published dog policy extraction; parallel to city_policy. Tier 3."}'::jsonb)
) AS new_anchors(subtype, citation, scope, source_url, metadata)
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source ps
   WHERE ps.metadata->>'backfill_source_label' =
         new_anchors.metadata->>'backfill_source_label'
);

-- ─── 2. Backfill policy_source_id for the previously unmapped rows ─

UPDATE public.beach_enrichment_provenance bep
   SET policy_source_id = anchor.id
  FROM public.policy_source anchor
 WHERE bep.field_group = 'dogs'
   AND bep.source IN ('operator_county', 'county_policy')
   AND anchor.metadata->>'backfill_source_label' = bep.source
   AND (bep.policy_source_id IS NULL OR bep.policy_source_id <> anchor.id);

-- ─── 3. Set relevance_verified for newly-labeled rows ─────────────
-- Same gate as Phase 4 — verbatim quote present + source_url non-null
-- + source_url not on high-fanout blacklist.

CREATE TEMP TABLE IF NOT EXISTS _fanout_blacklist AS
  SELECT source_url
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND source_url IS NOT NULL
   GROUP BY source_url
  HAVING COUNT(DISTINCT gold_fid) > 50;

UPDATE public.beach_enrichment_provenance bep
   SET relevance_verified = TRUE
 WHERE bep.field_group = 'dogs'
   AND bep.source IN ('operator_county', 'county_policy')
   AND bep.policy_source_id IS NOT NULL
   AND (bep.claimed_values ? 'source_quote'
        OR bep.claimed_values ? 'evidence'
        OR bep.claimed_values ? 'quote')
   AND bep.source_url IS NOT NULL
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
  v_remaining_null int;
BEGIN
  SELECT count(*) INTO v_total
    FROM public.beach_enrichment_provenance WHERE field_group = 'dogs';
  SELECT count(*) INTO v_with_ps
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND policy_source_id IS NOT NULL;
  SELECT count(*) INTO v_relevance_verified
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND relevance_verified = TRUE;
  SELECT count(*) INTO v_remaining_null
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs' AND policy_source_id IS NULL;

  RAISE NOTICE 'Phase 4.1 backfill summary:';
  RAISE NOTICE '  BEP dog rows total: %', v_total;
  RAISE NOTICE '  BEP dog rows with policy_source_id: % (%.1f%%)',
    v_with_ps, 100.0 * v_with_ps / NULLIF(v_total,0);
  RAISE NOTICE '  BEP dog rows relevance_verified: % (%.1f%%)',
    v_relevance_verified, 100.0 * v_relevance_verified / NULLIF(v_total,0);
  RAISE NOTICE '  BEP dog rows still NULL policy_source_id: %', v_remaining_null;
END$$;
