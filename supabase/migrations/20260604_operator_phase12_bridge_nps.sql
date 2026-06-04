-- Operator unification — Phase 12: bridge NPS agency-wide policy.
--
-- pad_us_unit_dogs_policy has an agency-wide row for NPS (unit_id=NULL,
-- mng_name='NPS') with url_used='nps.gov/subjects/pets/index.htm',
-- ordinance_ref='36 CFR §2.15', and verbatim source_quote. This is the
-- universal NPS leash policy that Phase 10 couldn't bridge from
-- operator_dogs_policy (where NPS had applies_to_all=false because its
-- source there was Point Reyes-specific).
--
-- Bridging this single row unlocks Tier 0 for all 130 NPS-attributed beaches
-- (Cape Cod NS, Point Reyes, Golden Gate NRA, Sleeping Bear, etc.).
--
-- Default_rule='mixed' in PAD-US reflects per-unit variation (some allow
-- on leash, some don't allow at all, swimming generally prohibited). For
-- the universal rule we map to 'on_leash' — 36 CFR §2.15's core requirement
-- (≤6 ft leash in developed areas where pets are allowed). Per-unit
-- exceptions (Cape Cod NS no-pets areas, etc.) belong in zone_rules later.

BEGIN;

-- Insert the NPS policy_source row + verify
WITH new_ps AS (
  INSERT INTO public.policy_source
    (subtype, issuing_operator_id, citation, source_url, full_text, last_verified)
  SELECT
    'operator_posted_policy'::policy_source_subtype,
    1683,                                                  -- United States National Park Service
    'United States National Park Service — 36 CFR §2.15',
    p.url_used,
    p.source_quote,
    now()
  FROM public.pad_us_unit_dogs_policy p
  WHERE p.mng_name='NPS' AND p.unit_id IS NULL
  -- Skip if already bridged
  AND NOT EXISTS (
    SELECT 1 FROM public.policy_source ps
    WHERE ps.issuing_operator_id = 1683 AND ps.subtype = 'operator_posted_policy'
  )
  RETURNING id, source_url
),
-- For each NPS-attributed beach without a beach_policy_source row to this PS, insert one
template AS (
  SELECT id AS ps_id, source_url FROM new_ps
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, operative_status)
SELECT
  eo.entity_id,
  t.ps_id,
  'sand',
  'on_leash',
  (SELECT source_quote FROM public.pad_us_unit_dogs_policy WHERE mng_name='NPS' AND unit_id IS NULL),
  t.source_url,
  'operative'
FROM template t
JOIN public.entity_operator eo
  ON eo.entity_type='beach' AND eo.operator_id = 1683
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source bps
  WHERE bps.beach_fid = eo.entity_id AND bps.policy_source_id = t.ps_id
);

COMMIT;
