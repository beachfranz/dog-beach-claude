-- v2 of the legacy operator migration. The original v1
-- (20260516_operator_migrate_ca_legacy.sql) failed on the
-- chk_operator_gov_or_delegated constraint because some legacy rows
-- couldn't be matched to an agency. This v2 fixes the matching logic:
--
-- 1. Strip both PREFIX ("City of "/"County of "/"Town of ") and
--    SUFFIX (", County of"/", City of"/", Town of") from legacy
--    canonical_name when matching agency.name. Five county parks
--    departments live in the agency table with names like "Los Angeles
--    County Department of Beaches and Harbors" but in the legacy table
--    as "Los Angeles County Department of Beaches and Harbors, County
--    of" — the suffix strip lets these match.
--
-- 2. For federal entries that don't match any agency by name, fall
--    back to canonical federal parent agencies:
--      - "National Park", "National Recreation Area", "National
--        Seashore" → National Park Service (id 307)
--      - "California Coastal National Monument" → BLM (id 288)
--      - "Giant Sequoia National Monument" → USFS (id 292)
--      - "United States Coast Guard" → USCG (id 290)
--
-- 3. Skip private/nonprofit operators (10 rows: HOAs, conservancies,
--    land trusts, Nature Conservancy). They have no parent agency by
--    design; they'd need a delegated_authority_via MOU/lease which
--    isn't captured yet. Defer until that data lands.
--
-- Expected result: 150 of 160 CA beach-touching legacy operators
-- migrated to the new public.operator table (94% coverage). 10
-- private/nonprofit operators flagged for follow-up MOU capture.
--
-- Existing public.operator rows (2 from 20260516_operator_nonprofit_seed:
-- PSHDB + Crystal Cove Conservancy) are NOT touched — the
-- WHERE NOT EXISTS guard on (name, type) skips them.

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT DISTINCT ON (lo.canonical_name, mapped_type.t)
       lo.canonical_name,
       lo.short_name,
       mapped_type.t::operator_type,
       resolved.agency_id,
       lo.website,
       lo.dog_policy_url,
       jsonb_build_object(
         'legacy_operator_id', lo.id,
         'slug', lo.slug,
         'aliases', lo.aliases,
         'level', lo.level,
         'subtype', lo.subtype,
         'cpad_agncy_name', lo.cpad_agncy_name,
         'origin_source', lo.origin_source,
         'pad_us_mng_name', lo.pad_us_mng_name
       )
FROM public.operators lo
CROSS JOIN LATERAL (
  SELECT CASE
    WHEN lo.level = 'city' THEN 'city'
    WHEN lo.level = 'county' THEN 'county'
    WHEN lo.level = 'state' THEN 'state'
    WHEN lo.level = 'special-district' THEN 'special_district'
    WHEN lo.level = 'tribal' THEN 'tribal'
    WHEN lo.level = 'private' THEN 'private_company'
    WHEN lo.level = 'federal' AND lo.subtype = 'dod' THEN 'federal_military'
    WHEN lo.level = 'federal' THEN 'federal_department'
    WHEN lo.level = 'joint' THEN 'special_district'
    ELSE 'private_company'
  END AS t
) mapped_type
CROSS JOIN LATERAL (
  -- Normalize legacy name: strip both prefix and suffix
  SELECT regexp_replace(
           regexp_replace(lo.canonical_name, '^(City|County|Town) of ', '', 'i'),
           ', (City|County|Town) of$', '', 'i'
         ) AS norm
) norm
CROSS JOIN LATERAL (
  -- Best-effort agency match: stripped name, then original name, then federal-parent fallback
  SELECT COALESCE(
    -- Direct match (stripped name)
    (SELECT a.id FROM public.agency a
      WHERE a.type::text = (
        CASE mapped_type.t
          WHEN 'city' THEN 'city'
          WHEN 'county' THEN 'county'
          WHEN 'state' THEN 'state_department'
          WHEN 'special_district' THEN 'special_district'
          WHEN 'tribal' THEN 'tribal'
          WHEN 'federal_department' THEN 'federal_department'
          WHEN 'federal_military' THEN 'federal_military'
          ELSE NULL END)
        AND (lower(a.name) = lower(norm.norm) OR lower(a.name) = lower(lo.canonical_name))
      LIMIT 1),
    -- County-department fallback (e.g., "LA County Department of Beaches and Harbors, County of")
    (SELECT a.id FROM public.agency a
      WHERE a.type::text = 'county_department'
        AND lower(a.name) = lower(norm.norm)
      LIMIT 1),
    -- Federal sub-unit fallbacks
    CASE
      WHEN mapped_type.t = 'federal_department' AND (
           lo.canonical_name ILIKE '%National Park'
        OR lo.canonical_name ILIKE '%National Recreation Area'
        OR lo.canonical_name ILIKE '%National Seashore'
      ) THEN (SELECT id FROM public.agency WHERE name = 'National Park Service' AND type = 'federal_department' LIMIT 1)
      WHEN mapped_type.t = 'federal_department' AND lo.canonical_name = 'California Coastal National Monument'
        THEN (SELECT id FROM public.agency WHERE name = 'United States Bureau of Land Management' AND type = 'federal_department' LIMIT 1)
      WHEN mapped_type.t = 'federal_department' AND lo.canonical_name = 'Giant Sequoia National Monument'
        THEN (SELECT id FROM public.agency WHERE name = 'United States Forest Service' AND type = 'federal_department' LIMIT 1)
      WHEN mapped_type.t = 'federal_department' AND lo.canonical_name = 'United States Coast Guard'
        THEN (SELECT id FROM public.agency WHERE name = 'United States Coast Guard' AND type = 'federal_department' LIMIT 1)
      ELSE NULL
    END
  ) AS agency_id
) resolved
WHERE lo.state_code = 'CA'
  AND lo.is_active = true
  AND EXISTS (
    SELECT 1 FROM public.beaches_gold g
     WHERE g.state='CA' AND g.is_active=true AND st_intersects(lo.geom, g.geom)
  )
  -- Skip rows we can't satisfy the check constraint for: NULL agency_id
  -- AND NULL delegated_authority_via (mostly the 10 private/nonprofit
  -- entities: HOAs, Nature Conservancy, etc.).
  AND resolved.agency_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.operator o
     WHERE o.name = lo.canonical_name AND o.type::text = mapped_type.t
  )
ORDER BY lo.canonical_name, mapped_type.t, lo.id;
