-- Consensus engine rewrite — Phase 7: seed the `agency` table from
-- existing PIP signals on beaches_gold + jurisdictions + cpad_units.
--
-- Scope: this migration ONLY populates the agency table with rows.
-- Linking beaches to agencies via beach_agency happens in Phase 7.1.
--
-- Seed sources:
-- 1. Umbrella agencies: State of California, United States
-- 2. Counties: distinct beaches_gold.county_name (CA only this phase)
-- 3. Cities: distinct beaches_gold.c1_jurisdiction_id → jurisdictions.name
-- 4. CPAD-owned agencies (gov only — non-profits / HOAs / private skip)
--    Mapping:
--      'State' + 'State Agency'          → state_department
--      'City'  + 'City Agency'           → city
--      'County'+ 'County Agency'         → county (top-level)
--      'County'+ 'County Agency - *'     → county_department
--      'Federal'+ 'Federal Agency'       → federal_department
--      'Federal'+ DoD/Navy keyword       → federal_military
--      'Special District'                → special_district
--
-- Idempotent via ON CONFLICT (name, type) DO NOTHING from the
-- ux_agency_name_type unique index.

-- ─── 1. Umbrella agencies ─────────────────────────────────────────

INSERT INTO public.agency (name, short_name, type, web_url, code_archive_url, pip_layer)
VALUES
  ('State of California', 'CA', 'state',
   'https://www.ca.gov',
   'https://leginfo.legislature.ca.gov/faces/codes.xhtml',
   NULL),
  ('United States', 'US', 'federal',
   'https://www.usa.gov',
   'https://www.ecfr.gov',
   NULL)
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. CA counties ───────────────────────────────────────────────
-- Each distinct county_name from beaches_gold (CA, active). The
-- parent_agency_id will be State of California, resolved via
-- subquery so this stays idempotent.

INSERT INTO public.agency (name, type, parent_agency_id, pip_layer, default_authority_domains)
SELECT
  county_name || ' County'                                        AS name,
  'county'                                                        AS type,
  (SELECT id FROM public.agency WHERE name='State of California' AND type='state'),
  'counties'                                                      AS pip_layer,
  ARRAY['water_quality','public_health','unincorporated_dog_policy']
                                                                  AS default_authority_domains
FROM (
  SELECT DISTINCT county_name
    FROM public.beaches_gold
   WHERE state='CA' AND is_active=true AND county_name IS NOT NULL
) c
ON CONFLICT (name, type) DO NOTHING;

-- ─── 3. CA cities (from c1_jurisdiction_id) ───────────────────────
-- jurisdictions.place_type uses FIPS class codes:
--   C1 = active incorporated place
--   C5 = active incorporated place serving as county subdivision
--   C9 = independent city
--   M2 = consolidated city (SF, etc.)
--   U1/U2 = CDP (unincorporated — county handles policy, no agency row)

INSERT INTO public.agency (name, type, parent_agency_id, web_url, pip_layer, pip_filter, default_authority_domains)
SELECT
  j.name                                                            AS name,
  'city'                                                            AS type,
  (SELECT id FROM public.agency
    WHERE name = j.county || ' County' AND type='county'
  )                                                                 AS parent_agency_id,
  NULL                                                              AS web_url,
  'jurisdictions'                                                   AS pip_layer,
  'id=' || j.id::text                                               AS pip_filter,
  ARRAY['dog_policy','operations','parking','fire']                 AS default_authority_domains
FROM (
  SELECT DISTINCT j.id, j.name, j.county
    FROM public.beaches_gold g
    JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
   WHERE g.state='CA' AND g.is_active=true
     AND j.place_type IN ('C1','C5','C9','M2')
) j
ON CONFLICT (name, type) DO NOTHING;

-- ─── 4. State departments (DPR, CDFW, State Lands, etc.) ──────────

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
SELECT DISTINCT
  c.agncy_name,
  'state_department'::agency_type,
  (SELECT id FROM public.agency WHERE name='State of California' AND type='state'),
  CASE
    WHEN c.agncy_name ILIKE '%Parks and Recreation%' THEN ARRAY['dog_policy','land_use','operations']
    WHEN c.agncy_name ILIKE '%Fish and Wildlife%'    THEN ARRAY['wildlife_protection','mpa']
    WHEN c.agncy_name ILIKE '%State Lands%'          THEN ARRAY['tidelands','land_use']
    WHEN c.agncy_name ILIKE '%Water Resources%'      THEN ARRAY['water_management']
    WHEN c.agncy_name ILIKE '%Coastal Commission%'   THEN ARRAY['coastal_access','land_use']
    ELSE ARRAY['land_use']::text[]
  END
FROM public.beaches_gold g
JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
WHERE g.state='CA' AND g.is_active=true
  AND c.agncy_lev = 'State'
  AND c.agncy_name IS NOT NULL
ON CONFLICT (name, type) DO NOTHING;

-- ─── 5. CPAD-listed cities (some may overlap with c1_jurisdiction_id) ─

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
SELECT DISTINCT
  c.agncy_name,
  'city'::agency_type,
  -- Try to resolve parent county via the beach's county_name.
  -- May be NULL if the city spans multiple counties / no clear match.
  (SELECT a.id FROM public.agency a
    WHERE a.type='county'
      AND a.name = (SELECT g2.county_name || ' County'
                      FROM public.beaches_gold g2
                      JOIN public.cpad_units c2 ON c2.unit_id = g2.cpad_unit_id
                     WHERE c2.agncy_name = c.agncy_name
                       AND g2.state='CA'
                     LIMIT 1)
    LIMIT 1),
  ARRAY['dog_policy','operations','parking','fire']
FROM public.beaches_gold g
JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
WHERE g.state='CA' AND g.is_active=true
  AND c.agncy_lev = 'City'
  AND c.agncy_name IS NOT NULL
ON CONFLICT (name, type) DO NOTHING;

-- ─── 6. CPAD county departments (LA Beaches & Harbors, OC Parks, etc.) ─

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
SELECT DISTINCT
  c.agncy_name,
  CASE
    WHEN c.agncy_typ = 'County Agency' THEN 'county'
    ELSE 'county_department'
  END::agency_type,
  -- Resolve parent via beach county_name
  (SELECT a.id FROM public.agency a
    WHERE a.type='county'
      AND a.name = (SELECT g2.county_name || ' County'
                      FROM public.beaches_gold g2
                      JOIN public.cpad_units c2 ON c2.unit_id = g2.cpad_unit_id
                     WHERE c2.agncy_name = c.agncy_name
                       AND g2.state='CA'
                     LIMIT 1)
    LIMIT 1),
  CASE
    WHEN c.agncy_name ILIKE '%Parks%'  THEN ARRAY['dog_policy','land_use','operations']
    WHEN c.agncy_name ILIKE '%Beach%'  THEN ARRAY['dog_policy','operations']
    WHEN c.agncy_name ILIKE '%Health%' THEN ARRAY['water_quality','public_health']
    WHEN c.agncy_name ILIKE '%Fire%'   THEN ARRAY['fire','water_safety']
    ELSE ARRAY['land_use']::text[]
  END
FROM public.beaches_gold g
JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
WHERE g.state='CA' AND g.is_active=true
  AND c.agncy_lev = 'County'
  AND c.agncy_name IS NOT NULL
ON CONFLICT (name, type) DO NOTHING;

-- ─── 7. Federal departments (NPS, USFWS, BLM, etc.) ──────────────

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
SELECT DISTINCT
  c.agncy_name,
  CASE
    WHEN c.agncy_name ILIKE '%Department of Defense%' THEN 'federal_military'
    WHEN c.agncy_name ILIKE '%Navy%'                  THEN 'federal_military'
    WHEN c.agncy_name ILIKE '%Army%'                  THEN 'federal_military'
    WHEN c.agncy_name ILIKE '%Coast Guard%'           THEN 'federal_military'
    ELSE 'federal_department'
  END::agency_type,
  (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
  CASE
    WHEN c.agncy_name ILIKE '%National Park%'           THEN ARRAY['dog_policy','land_use','enforcement']
    WHEN c.agncy_name ILIKE '%Fish and Wildlife%'       THEN ARRAY['wildlife_protection','land_use']
    WHEN c.agncy_name ILIKE '%Bureau of Land Mgmt%'     THEN ARRAY['land_use','dog_policy']
    WHEN c.agncy_name ILIKE '%Bureau of Land Management%' THEN ARRAY['land_use','dog_policy']
    WHEN c.agncy_name ILIKE '%Bureau of Reclamation%'   THEN ARRAY['water_management','land_use']
    WHEN c.agncy_name ILIKE '%Forest Service%'          THEN ARRAY['land_use','dog_policy']
    WHEN c.agncy_name ILIKE '%Defense%'
      OR c.agncy_name ILIKE '%Navy%'
      OR c.agncy_name ILIKE '%Army%'
      OR c.agncy_name ILIKE '%Coast Guard%'             THEN ARRAY['access_restriction','enforcement']
    ELSE ARRAY['land_use']::text[]
  END
FROM public.beaches_gold g
JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
WHERE g.state='CA' AND g.is_active=true
  AND c.agncy_lev = 'Federal'
  AND c.agncy_name IS NOT NULL
ON CONFLICT (name, type) DO NOTHING;

-- ─── 8. Special districts (EBRPD, port districts, etc.) ──────────

INSERT INTO public.agency (name, type, default_authority_domains)
SELECT DISTINCT
  c.agncy_name,
  'special_district'::agency_type,
  CASE
    WHEN c.agncy_typ ILIKE '%Park%'   THEN ARRAY['dog_policy','land_use','operations']
    WHEN c.agncy_typ ILIKE '%Port%'   THEN ARRAY['operations','land_use']
    WHEN c.agncy_typ ILIKE '%Harbor%' THEN ARRAY['operations','land_use']
    WHEN c.agncy_typ ILIKE '%Flood%'  THEN ARRAY['water_management']
    ELSE ARRAY['land_use']::text[]
  END
FROM public.beaches_gold g
JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
WHERE g.state='CA' AND g.is_active=true
  AND c.agncy_lev = 'Special District'
  AND c.agncy_name IS NOT NULL
ON CONFLICT (name, type) DO NOTHING;
