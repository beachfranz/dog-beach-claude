-- Phase 14a — Federal + state agency canonical operator seeds.
--
-- Prereq for Phase 14d (resolver rewrite). The new resolver matches
-- PAD-US polygons by their `mng_name` short code (FWS, USACE, DOD, etc.)
-- to a canonical operator's `pad_us_mng_name` array. Existing rows
-- (NPS 1683, USFS 1682, BLM 1677, USCG 1679) hold the LONG-form name in
-- that array — augment them with the short code so the resolver can match
-- either form.
--
-- PAD-US's `loc_mang` field is unreliable (one-letter codes, per-unit
-- names, etc.) so we can't match against it. `mng_name` is the only
-- clean code-shaped field.
--
-- New rows follow Franz's "one national-level record per federal agency"
-- decision (2026-06-04). State_code='CA' is vestigial per the existing
-- model (NPS/USFS/BLM are all tagged CA but used nationally). Matching
-- in the resolver is by `pad_us_mng_name` short code regardless of
-- state_code on the operator row.
--
-- State-park agency seeds: OPRD/WSPRC/DCR/MDDNR for the 4 MVP+ states
-- we serve beyond CA. Each has state_code = its actual state.
--
-- Idempotent via ON CONFLICT-on-slug (operators.slug has UNIQUE constraint).

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Augment existing federal canonicals: add PAD-US short code to array.
--    Also promote 4 federal agencies (FWS/USACE/DOD/USBR) that exist as
--    canonical-shaped but non-canonical bulk-pool rows. Same shape as
--    Phase 6's promotion, just missed because Phase 6 was CA-only PIP.
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators
   SET pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['NPS']::text[])),
       updated_at = now()
 WHERE id = 1683 AND NOT ('NPS' = ANY(pad_us_mng_name));

UPDATE public.operators
   SET pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['USFS']::text[])),
       updated_at = now()
 WHERE id = 1682 AND NOT ('USFS' = ANY(pad_us_mng_name));

UPDATE public.operators
   SET pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['BLM']::text[])),
       updated_at = now()
 WHERE id = 1677 AND NOT ('BLM' = ANY(pad_us_mng_name));

UPDATE public.operators
   SET pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['USCG']::text[])),
       updated_at = now()
 WHERE id = 1679 AND NOT ('USCG' = ANY(pad_us_mng_name));

-- Promote 4 dormant federal-agency rows (FWS / USACE / DOD / USBR) + add short codes
UPDATE public.operators
   SET is_canonical = true,
       is_active    = true,
       pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['FWS']::text[])),
       updated_at = now()
 WHERE id = 1681 AND (is_canonical IS NOT TRUE OR NOT ('FWS' = ANY(pad_us_mng_name)));

UPDATE public.operators
   SET is_canonical = true,
       is_active    = true,
       pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['USACE']::text[])),
       updated_at = now()
 WHERE id = 1676 AND (is_canonical IS NOT TRUE OR NOT ('USACE' = ANY(pad_us_mng_name)));

UPDATE public.operators
   SET is_canonical = true,
       is_active    = true,
       pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['DOD']::text[])),
       updated_at = now()
 WHERE id = 1680 AND (is_canonical IS NOT TRUE OR NOT ('DOD' = ANY(pad_us_mng_name)));

UPDATE public.operators
   SET is_canonical = true,
       is_active    = true,
       pad_us_mng_name = ARRAY(SELECT DISTINCT unnest(pad_us_mng_name || ARRAY['USBR']::text[])),
       updated_at = now()
 WHERE id = 1678 AND (is_canonical IS NOT TRUE OR NOT ('USBR' = ANY(pad_us_mng_name)));

-- ─────────────────────────────────────────────────────────────────────
-- 2. Seed missing federal agency-wide canonicals
-- ─────────────────────────────────────────────────────────────────────
INSERT INTO public.operators
  (canonical_name, slug, state_code, level, is_canonical, is_active,
   origin_source, pad_us_mng_name)
VALUES
  ('United States Fish and Wildlife Service',
   'united-states-fish-and-wildlife-service',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['FWS', 'United States Fish and Wildlife Service']),

  ('United States Army Corps of Engineers',
   'united-states-army-corps-of-engineers',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['USACE', 'United States Army Corps of Engineers']),

  ('United States Department of Defense',
   'united-states-department-of-defense',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['DOD', 'United States Department of Defense']),

  ('United States Bureau of Indian Affairs',
   'united-states-bureau-of-indian-affairs',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['BIA', 'United States Bureau of Indian Affairs']),

  ('Natural Resources Conservation Service',
   'natural-resources-conservation-service',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['NRCS', 'Natural Resources Conservation Service']),

  ('United States Bureau of Reclamation',
   'united-states-bureau-of-reclamation',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['USBR', 'United States Bureau of Reclamation']),

  ('Bureau of Ocean Energy Management',
   'bureau-of-ocean-energy-management',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['BOEM', 'Bureau of Ocean Energy Management']),

  ('National Oceanic and Atmospheric Administration',
   'national-oceanic-and-atmospheric-administration',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['NOAA', 'National Oceanic and Atmospheric Administration']),

  ('Tennessee Valley Authority',
   'tennessee-valley-authority',
   'CA', 'federal', true, true, 'seed_federal',
   ARRAY['TVA', 'Tennessee Valley Authority'])

ON CONFLICT (slug) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────
-- 3. Seed missing state-park canonicals for MVP+ states (OR/WA/MA/MD)
--    Match in resolver will be (state_code, pad_us_mng_name contains 'SPR' or 'SDNR')
-- ─────────────────────────────────────────────────────────────────────
INSERT INTO public.operators
  (canonical_name, slug, state_code, level, is_canonical, is_active,
   origin_source, pad_us_mng_name)
VALUES
  ('Oregon Parks and Recreation Department',
   'oregon-parks-and-recreation-department',
   'OR', 'state', true, true, 'seed_state_pad_us',
   ARRAY['SPR', 'OPRD', 'Oregon Parks and Recreation Department']),

  ('Washington State Parks and Recreation Commission',
   'washington-state-parks-and-recreation-commission',
   'WA', 'state', true, true, 'seed_state_pad_us',
   ARRAY['SPR', 'WSPRC', 'Washington State Parks and Recreation Commission']),

  ('Massachusetts Department of Conservation and Recreation',
   'massachusetts-department-of-conservation-and-recreation',
   'MA', 'state', true, true, 'seed_state_pad_us',
   ARRAY['SPR', 'SDNR', 'DCR', 'Massachusetts Department of Conservation and Recreation']),

  ('Maryland Department of Natural Resources',
   'maryland-department-of-natural-resources',
   'MD', 'state', true, true, 'seed_state_pad_us',
   ARRAY['SPR', 'SDNR', 'SFW', 'MD DNR', 'Maryland Department of Natural Resources'])

ON CONFLICT (slug) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────
-- Audit
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE v_fed int; v_state int;
BEGIN
  SELECT count(*) INTO v_fed FROM public.operators
   WHERE is_canonical = true AND level = 'federal'
     AND canonical_name LIKE 'United States %' OR canonical_name LIKE 'Bureau of %' OR canonical_name LIKE 'National Oceanic%' OR canonical_name LIKE 'Tennessee Valley%' OR canonical_name LIKE 'Natural Resources Conservation%';

  SELECT count(*) INTO v_state FROM public.operators
   WHERE is_canonical = true AND level = 'state'
     AND state_code IN ('OR','WA','MA','MD')
     AND (canonical_name LIKE '%Parks%' OR canonical_name LIKE '%Conservation%' OR canonical_name LIKE '%Natural Resources%');

  RAISE NOTICE 'Federal canonicals (US-style names): %', v_fed;
  RAISE NOTICE 'State-park canonicals for OR/WA/MA/MD: %', v_state;
END $$;

COMMIT;
