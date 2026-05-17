-- Wave 4: San Juan County (WA) parks dog-policy backfill.
--
-- Source: San Juan County Code §12.08.220 (Animals running at large)
-- — Title 12 / Chapter 12.08 COUNTY PARKS AND LAND BANK PROPERTY.
--
-- Live URL: https://www.codepublishing.com/WA/SanJuanCounty/html/SanJuanCounty12/SanJuanCounty1208.html
-- (codepublishing.com SJ subdomain redirects to ecode360 SA4768;
--  both gate anonymous access, but the section citation is stable.)
--
-- Code structure:
--   Title 6 / Chapter 6.08 Dogs and Cats — defines "at large" as off
--     premises and not under immediate control by voice control,
--     whistle or hand signal, or a leash no longer than 15 feet.
--     This is the countywide off-premises rule (voice-control allowed).
--   Title 12 / Chapter 12.08 County Parks and Land Bank Property —
--     §12.08.220 is the PARKS-SPECIFIC rule with stricter leash.
--
-- Operative rule for parks/beaches (§12.08.220, verbatim from
-- Code Publishing search result 2026-05-16):
--   It is unlawful for any person to allow any dog or pet to run at
--   large in any park, or land bank preserve, and all such animals
--   must be on a leash no greater than 10 feet in length. Any person
--   with a dog or other pet in his or her possession or under his or
--   her control in any park or land bank preserve shall be responsible
--   for the conduct of the animal, shall carry equipment for removing
--   feces, and shall place feces deposited by such animal in an
--   appropriate receptacle.
--
-- "Park" per §12.08 definitions includes "all parks and bodies of
-- water contained therein, drives, trails, beaches, playgrounds,
-- gardens and other park recreation, open space areas, boat ramps
-- and launching facilities, buildings and all other facilities
-- comprising the parks and recreation system of the County."
--
-- Sand rule = on_leash (10-foot leash max in parks/preserves).
-- NOTE: this is stricter than the §6.08 countywide voice-control rule.
-- Parks/beaches operated by San Juan County land bank or parks system
-- require leash. Voice-control under §6.08 applies on private property
-- and roadways but NOT in parks/land-bank land per §12.08.220.
--
-- 44 San Juan County beaches in beaches_gold across the SJ archipelago.

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'San Juan County Code §12.08.220 (Animals running at large) — Title 12 / Chapter 12.08 County Parks and Land Bank Property',
       (SELECT id FROM public.agency WHERE name='San Juan County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/SanJuanCounty/html/SanJuanCounty12/SanJuanCounty1208.html',
       'San Juan County Code §12.08.220 Animals running at large. It is unlawful for any person to allow any dog or pet to run at large in any park, or land bank preserve, and all such animals must be on a leash no greater than 10 feet in length. Any person with a dog or other pet in his or her possession or under his or her control in any park or land bank preserve shall be responsible for the conduct of the animal, shall carry equipment for removing feces, and shall place feces deposited by such animal in an appropriate receptacle. §12.08 definitions: "Park" means all parks and bodies of water contained therein, drives, trails, beaches, playgrounds, gardens and other park recreation, open space areas, boat ramps and launching facilities, buildings and all other facilities comprising the parks and recreation system of the County. [Title 6 Chapter 6.08 has a countywide voice-control "at large" definition allowing leash up to 15 ft OR voice control; §12.08.220 is the stricter PARKS-AND-LAND-BANK rule requiring a leash max 10 ft. The parks-specific rule controls on county park/preserve land. Note: codepublishing.com SJ subdomain redirects to ecode360 SA4768; verbatim text via WebSearch result 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Juan County Code §12.08.220%'
);

-- ─── 2. Link San Juan County beaches ────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='San Juan'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'San Juan County Code §12.08.220%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'San Juan County Code §12.08.220: dogs/pets cannot run at large in any park or land bank preserve; max 10-foot leash. "Park" defined to include beaches.',
       'https://www.codepublishing.com/WA/SanJuanCounty/html/SanJuanCounty12/SanJuanCounty1208.html',
       'Sand rule = on_leash per §12.08.220 (10-foot max) for county parks + land bank preserves. SJ has a separate Title 6 §6.08 countywide voice-control rule for "at large" on general public property — but parks-specific §12.08.220 controls on park/preserve land. Per-beach jurisdiction (county park vs. state park vs. private vs. tideland) refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
