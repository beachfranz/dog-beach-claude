-- Phase 14f — federal Tier 0 bridges (FWS + USFS).
--
-- Same shape as Phase 12 NPS bridge. For each agency with a clean
-- agency-wide entry in pad_us_unit_dogs_policy (default_rule clear,
-- url + citation present), create one policy_source + a beach_policy_source
-- row per beach attributed to that agency's canonical operator via
-- entity_operator (post-Phase 14e).
--
-- Scope (agreed 2026-06-04):
--   FWS  (op 1681, 11 beaches) — default_rule='no' → rule='not_allowed'
--   USFS (op 1682, 45 beaches) — default_rule='mixed'+leash → rule='on_leash'
--
-- Deferred:
--   USACE (op 1676, 8 beaches)   — missing url + citation in pad_us_unit_dogs_policy
--   BLM, DOD, USBR, BIA, NRCS, TVA — default_rule='unknown' (varies by unit;
--                                     Phase 14g unit-specific bridges land these)
--
-- Idempotent: NOT EXISTS guards on both inserts skip already-bridged ops/beaches.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- FWS bridge
-- ─────────────────────────────────────────────────────────────────────
WITH new_fws_ps AS (
  INSERT INTO public.policy_source
    (subtype, issuing_operator_id, citation, source_url, full_text, last_verified)
  SELECT
    'operator_posted_policy'::policy_source_subtype,
    1681,
    'United States Fish and Wildlife Service — 50 CFR §26.21',
    p.url_used,
    p.source_quote,
    now()
  FROM public.pad_us_unit_dogs_policy p
  WHERE p.mng_name = 'FWS' AND p.unit_id IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM public.policy_source ps
      WHERE ps.issuing_operator_id = 1681 AND ps.subtype = 'operator_posted_policy'
    )
  RETURNING id, source_url
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, operative_status)
SELECT
  eo.entity_id,
  ins.id,
  'sand',
  'not_allowed',
  (SELECT source_quote FROM public.pad_us_unit_dogs_policy WHERE mng_name='FWS' AND unit_id IS NULL),
  ins.source_url,
  'operative'
FROM new_fws_ps ins
JOIN public.entity_operator eo
  ON eo.entity_type='beach' AND eo.operator_id = 1681
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source bps
  WHERE bps.beach_fid = eo.entity_id AND bps.policy_source_id = ins.id
);

-- ─────────────────────────────────────────────────────────────────────
-- USFS bridge
-- ─────────────────────────────────────────────────────────────────────
WITH new_usfs_ps AS (
  INSERT INTO public.policy_source
    (subtype, issuing_operator_id, citation, source_url, full_text, last_verified)
  SELECT
    'operator_posted_policy'::policy_source_subtype,
    1682,
    'United States Forest Service — 36 CFR §2.15',
    p.url_used,
    p.source_quote,
    now()
  FROM public.pad_us_unit_dogs_policy p
  WHERE p.mng_name = 'USFS' AND p.unit_id IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM public.policy_source ps
      WHERE ps.issuing_operator_id = 1682 AND ps.subtype = 'operator_posted_policy'
    )
  RETURNING id, source_url
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, operative_status)
SELECT
  eo.entity_id,
  ins.id,
  'sand',
  'on_leash',
  (SELECT source_quote FROM public.pad_us_unit_dogs_policy WHERE mng_name='USFS' AND unit_id IS NULL),
  ins.source_url,
  'operative'
FROM new_usfs_ps ins
JOIN public.entity_operator eo
  ON eo.entity_type='beach' AND eo.operator_id = 1682
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source bps
  WHERE bps.beach_fid = eo.entity_id AND bps.policy_source_id = ins.id
);

-- ─────────────────────────────────────────────────────────────────────
-- Audit
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE
  v_fws_ps int; v_fws_bps int;
  v_usfs_ps int; v_usfs_bps int;
BEGIN
  SELECT count(*) INTO v_fws_ps FROM public.policy_source
   WHERE issuing_operator_id = 1681 AND subtype = 'operator_posted_policy';
  SELECT count(*) INTO v_fws_bps FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
   WHERE ps.issuing_operator_id = 1681 AND ps.subtype = 'operator_posted_policy';

  SELECT count(*) INTO v_usfs_ps FROM public.policy_source
   WHERE issuing_operator_id = 1682 AND subtype = 'operator_posted_policy';
  SELECT count(*) INTO v_usfs_bps FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
   WHERE ps.issuing_operator_id = 1682 AND ps.subtype = 'operator_posted_policy';

  RAISE NOTICE '──────── Phase 14f federal bridges ────────';
  RAISE NOTICE 'FWS policy_source rows:     % (expected 1)', v_fws_ps;
  RAISE NOTICE 'FWS beach_policy_source rows: % (expected ~11)', v_fws_bps;
  RAISE NOTICE 'USFS policy_source rows:    % (expected 1)', v_usfs_ps;
  RAISE NOTICE 'USFS beach_policy_source rows: % (expected ~45)', v_usfs_bps;
END $$;

COMMIT;
