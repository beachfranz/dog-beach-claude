-- Walkthrough seeds — round 2: Manhattan Beach (8265), Salt Creek
-- (8316), Coronado (8715), Klamath (561).
--
-- Phase 2 seeded HBDB / Crystal Cove / Fort Funston / Crown Memorial /
-- Dockweiler / Rosie's. The four walkthroughs added later
-- (2026-05-16) had docs in /docs but no DB rows. With Municode now
-- confirmed scriptable (corrected from prior wrong conclusion),
-- this is the right time to backfill.
--
-- Sections covered:
--   1. Missing agencies (Orange County Parks Department,
--      LA County Beaches & Harbors, Yurok Tribe)
--   2. Missing operators — DEFERRED to Phase 8 (operator table empty)
--   3. New policy_source rows per walkthrough
--   4. beach_policy_source rows linking each beach to its sources
--      (Klamath has no operative-rule verbatim — only the agency +
--      policy_source rows are seeded)

-- ─── 1. Missing agencies ─────────────────────────────────────────

INSERT INTO public.agency (name, short_name, type, parent_agency_id, default_authority_domains)
VALUES
  ('Orange County Parks Department', 'OC Parks', 'county_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='Orange County' AND type='county'),
   ARRAY['dog_policy','land_use','operations','enforcement']),
  ('Los Angeles County Department of Beaches and Harbors', 'LA County B&H',
   'county_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='Los Angeles County' AND type='county'),
   ARRAY['dog_policy','operations']),
  ('Yurok Tribe', NULL, 'tribal'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['dog_policy','operations','wildlife_protection','enforcement','cultural_protection'])
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. beach_agency rows for the 4 walkthrough beaches ──────────
-- Update Phase 7.1 defaults to reflect walkthrough findings.
-- Manhattan Beach: LA County B&H runs the sand, not the city directly.
-- Salt Creek: OC Parks (county_department) runs everything.
-- Coronado: city is correct from Phase 7.1.
-- Klamath: Yurok Tribe overrides everything.

-- Manhattan Beach (8265) — promote LA County B&H to rank=1 sand
-- operator; demote existing rank=1 city dog_policy to rank=2.
UPDATE public.beach_agency ba
   SET precedence_rank = 2
  FROM public.agency a
 WHERE ba.beach_fid = 8265
   AND ba.agency_id = a.id
   AND ba.authority_domain = 'dog_policy'
   AND a.type = 'city';
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT 8265, id, d.dom, 1
  FROM public.agency,
       (VALUES ('dog_policy'), ('operations'), ('land_use')) d(dom)
 WHERE name='Los Angeles County Department of Beaches and Harbors'
   AND type='county_department'
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- Salt Creek (8316) — promote OC Parks to rank=1; demote county.
UPDATE public.beach_agency ba
   SET precedence_rank = 2
  FROM public.agency a
 WHERE ba.beach_fid = 8316
   AND ba.agency_id = a.id
   AND ba.authority_domain = 'dog_policy'
   AND a.type = 'county';
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT 8316, id, d.dom, 1
  FROM public.agency,
       (VALUES ('dog_policy'), ('operations'), ('parking'), ('land_use')) d(dom)
 WHERE name='Orange County Parks Department'
   AND type='county_department'
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- Klamath (561) — promote Yurok Tribe to rank=1 dog_policy; demote county.
UPDATE public.beach_agency ba
   SET precedence_rank = 2
  FROM public.agency a
 WHERE ba.beach_fid = 561
   AND ba.agency_id = a.id
   AND ba.authority_domain = 'dog_policy'
   AND a.type IN ('city','county');
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT 561, id, d.dom, 1
  FROM public.agency,
       (VALUES ('dog_policy'), ('operations'), ('enforcement'), ('cultural_protection')) d(dom)
 WHERE name='Yurok Tribe' AND type='tribal'
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 3. New policy_source rows ───────────────────────────────────

-- Manhattan Beach
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy', 'LA County Beaches & Harbors — Rules & Regulations on LA County Beaches',
       (SELECT id FROM public.agency WHERE name='Los Angeles County Department of Beaches and Harbors' AND type='county_department'),
       ARRAY['dog_policy','operations']::text[],
       'https://beaches.lacounty.gov/la-county-beach-rules/',
       'NO Animals allowed on the beach (no cats, dogs, horses, etc.). [Verbatim 2026-05-16. Single rule applies to all LA County operated beaches — Manhattan, Hermosa, Redondo, Dockweiler, Will Rogers, Topanga, Leo Carrillo, Zuma, etc.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE source_url = 'https://beaches.lacounty.gov/la-county-beach-rules/'
);

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url)
SELECT 'municipal_code', 'Manhattan Beach Municipal Code (animal/beach access sections — Title TBD)',
       (SELECT id FROM public.agency WHERE name='Manhattan Beach' AND type='city'),
       ARRAY['dog_policy']::text[],
       NULL
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'Manhattan Beach Municipal Code%'
);

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'community_attestation', 'Hermosa to Manhattan Beach Guide — Manhattan Beach Dog Guide',
       NULL,
       ARRAY['dog_policy']::text[],
       'https://hermosatomanhattanbeach.com/manhattan-beach-dog-guide/',
       'Dogs must be kept on a leash no longer than six feet at all times in public spaces. This includes parks, trails, and city sidewalks. Dogs are not permitted on the beach. [Third-party tourism site paraphrase. Verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE source_url = 'https://hermosatomanhattanbeach.com/manhattan-beach-dog-guide/'
);

-- Salt Creek
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy', 'OC Parks — Regional Park Rules',
       (SELECT id FROM public.agency WHERE name='Orange County Parks Department' AND type='county_department'),
       ARRAY['dog_policy']::text[],
       'https://www.ocparks.com/about-us/park-rules/regional-park-rules',
       'All pets must be on a leash that does not exceed 6 feet. [Verbatim 2026-05-16. OC Parks default is leashed-pets-allowed — opposite of LA County County''s no-animals default.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE source_url = 'https://www.ocparks.com/about-us/park-rules/regional-park-rules'
);

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy', 'OC Parks — Salt Creek Beach Park Rules',
       (SELECT id FROM public.agency WHERE name='Orange County Parks Department' AND type='county_department'),
       ARRAY['dog_policy']::text[],
       'https://www.ocparks.com/beaches/salt-creek-beach/park-rules',
       'Dogs are not permitted on the beach. Dogs are permitted (on a 6-foot leash) on paved walkways and at Bluff Park, the grass area above Salt Creek Beach. Beach hours: 5 am - 12 a.m. daily (includes Strands Beach). [Verbatim 2026-05-16.]',
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/about-us/park-rules/regional-park-rules')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE source_url = 'https://www.ocparks.com/beaches/salt-creek-beach/park-rules'
);

-- Coronado
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url)
SELECT 'municipal_code', 'Coronado Municipal Code Title 32 — Animal Regulations',
       (SELECT id FROM public.agency WHERE name='Coronado' AND type='city'),
       ARRAY['dog_policy']::text[],
       NULL
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation = 'Coronado Municipal Code Title 32 — Animal Regulations'
);

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy', 'City of Coronado — Dogs page',
       (SELECT id FROM public.agency WHERE name='Coronado' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.coronado.ca.us/757/Dogs',
       'The City of Coronado welcomes your furry friends. [Page enumerates dog-permitted areas: Bay View Park, Centennial Park, Harbor View Park, Tidelands Park, Vetter Park, Coronado Cays Park, Coronado Dog Beach (off-leash). Coronado main beach is NOT enumerated; omission = prohibited per page structure. Verbatim 2026-05-16.]',
       (SELECT id FROM public.policy_source WHERE citation='Coronado Municipal Code Title 32 — Animal Regulations')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE source_url = 'https://www.coronado.ca.us/757/Dogs'
);

-- Klamath — Yurok Tribal Code placeholder (no verbatim available yet)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url)
SELECT 'tribal_code', 'Yurok Tribal Code — animal access provisions (section TBD)',
       (SELECT id FROM public.agency WHERE name='Yurok Tribe' AND type='tribal'),
       ARRAY['dog_policy']::text[],
       'https://www.yuroktribe.org'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'Yurok Tribal Code%'
);

-- ─── 4. beach_policy_source rows ─────────────────────────────────

-- Manhattan Beach sand (LA County rule = no animals)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8265,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://beaches.lacounty.gov/la-county-beach-rules/'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'NO Animals allowed on the beach (no cats, dogs, horses, etc.) — applies to all LA County operated beaches including Manhattan.',
       'https://beaches.lacounty.gov/la-county-beach-rules/'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Salt Creek sand (not allowed)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8316,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Dogs are not permitted on the beach.',
       'https://www.ocparks.com/beaches/salt-creek-beach/park-rules'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Salt Creek paved_walkways (on-leash)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8316,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'paved_walkways', 'on_leash', 'operative'::operative_status,
       'Dogs are permitted (on a 6-foot leash) on paved walkways and at Bluff Park, the grass area above Salt Creek Beach.',
       'https://www.ocparks.com/beaches/salt-creek-beach/park-rules'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Coronado main beach sand (omission = prohibited)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8715,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.coronado.ca.us/757/Dogs'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Coronado Beach (main beach in front of Hotel del) is NOT listed on the City of Coronado dogs-permitted-areas page. Page enumerates 6 city parks + Coronado Dog Beach (off-leash, fid 6202); omission of the main beach = prohibited per the page''s enumeration structure.',
       'https://www.coronado.ca.us/757/Dogs',
       'Inference from page-structure omission, not from explicit prohibition language. Confirms with operator-published page logic and walkthrough analysis.'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
