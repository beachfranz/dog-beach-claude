-- Wave 3: City of Laguna Beach — 18 active beaches with Laguna Beach
-- as precedence-1 dog_policy authority.
--
-- Source: Laguna Beach Municipal Code §6.16.010 (leash law) + §6.16.020
-- (dogs on beaches — when and where prohibited), augmented by the
-- City Council resolution hours summarized on the City's
-- lagunabeachcity.net/our-beaches/dogs-on-the-beach page.
--
-- Platforms:
--   - Codified ordinance: qcode.us / ecode360 (LB Municipal Code via
--     library.qcode.us/lib/laguna_beach_ca/pub/municipal_code)
--   - Operative hours (City Council resolution): summarized on the
--     City's own dogs-on-the-beach page; Akamai-blocked from automated
--     fetch but well-corroborated by third-party guides
--
-- Codified leash rule (§6.16.010, verbatim):
--   "No person owning or having charge, care, custody, or control of
--   any dog shall cause or allow ... such dog to be upon any highway,
--   street, alley or any other public property unless such dog be
--   restrained by a substantial chain, or leash not exceeding six feet
--   in length, and is under the charge of a person competent to
--   exercise care, custody and control over such dog ..."
--
-- Codified beach prohibition (§6.16.020, verbatim):
--   (a) ... it is unlawful for any dog to be within the following
--   designated beach and park areas within the city ... whether or not
--   confined by leash, at the following times:
--   (1) At any and all times within Bluebird Park, and within Boat
--   Canyon Park within June 15th and September 15th of each calendar
--   year;
--   (2) At such times and within those areas of Heisler Park as the
--   city council may from time to time fix and designate by resolution;
--   (3) Notwithstanding any other provisions of this code, no person
--   shall take any dog, whether leased or unleashed, upon any public
--   beach in the city between the hours set by city council resolution
--   nor shall any person owning or having charge, care custody or
--   control of any dog cause, permit or allow such dog to be upon such
--   beach at any time that conflicts with the hours set by city council
--   resolution;
--   (4) At any and all times upon any public school grounds, dogs are
--   prohibited, whether or not confined by leash;
--   (5) At any and all times in tot lots or play equipment areas in
--   any city park or beach.
--   (b) Exceptions: dogs in recreation-dept obedience classes; guide
--   dogs for blind persons.
--
-- City Council resolution hours (operative — per City summary page):
--   - SUMMER (June 15 – September 10): dogs only allowed on public
--     beaches BEFORE 9:00 AM and AFTER 6:00 PM, always leashed.
--   - OFF-SEASON (September 11 – June 14): dogs allowed at any time of
--     day, always leashed.
--   - Thousand Steps Beach: dogs prohibited every day of the year.
--
-- 18 of 23 LB-polygon beaches in beaches_gold have LB as
-- precedence-1 dog_policy authority. The other 5 (Arch Beach,
-- La Senda And Bay Drive Beach, Shaw's Cove Beach, Thousand Steps
-- Beach, Victoria Beach) have a different primary authority — not
-- touched here. Thousand Steps in particular is named as prohibited
-- in the city policy; needs governance-resolver follow-up before it
-- can be linked.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Laguna Beach Municipal Code §6.16.010 (Leash required) + §6.16.020 (Dogs on beaches — when and where prohibited)',
       (SELECT id FROM public.agency WHERE name='Laguna Beach' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/42886466',
       'Laguna Beach Municipal Code Chapter 6.16 Animals at Large — Restraints. §6.16.010 Leash required: no person shall allow any dog on public property unless restrained by a substantial chain or leash not exceeding six feet in length, under charge of a competent person. §6.16.020 Dogs on beaches, in parks and school grounds — when and where prohibited: (a)(1) At any and all times within Bluebird Park, and within Boat Canyon Park within June 15th and September 15th; (a)(2) such times and areas of Heisler Park as the city council designates by resolution; (a)(3) no person shall take any dog leashed or unleashed upon any public beach in the city between the hours set by city council resolution; (a)(4) public school grounds prohibited unless prior written authorization; (a)(5) tot lots or play equipment areas in any city park or beach prohibited at all times. (b) Exceptions: recreation-dept obedience classes; guide dogs for blind persons. Operative city council resolution hours (per City of Laguna Beach dogs-on-the-beach summary page): SUMMER (June 15 – September 10): dogs only allowed on public beaches BEFORE 9:00 AM and AFTER 6:00 PM, always leashed. OFF-SEASON (September 11 – June 14): dogs allowed any time of day, always leashed. Thousand Steps Beach: dogs prohibited every day of the year. Bluebird Park and Boat Canyon Park (Jun 15–Sep 15): dogs prohibited per §6.16.020(a)(1). [Codified ordinance fetched from qcode/ecode360 (LB Municipal Code Ch 6.16). Resolution hours sourced from City summary at lagunabeachcity.net/our-beaches/dogs-on-the-beach (Akamai-blocked from automated fetch; values corroborated via web search). Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Laguna Beach Municipal Code §6.16.010%'
);

-- ─── 2. Link 18 LB-authority beaches — on_leash with seasonal hours ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Laguna Beach Municipal Code §6.16.010 + §6.16.020. Dogs always on leash (≤6 ft, in charge of competent person). SUMMER (Jun 15–Sep 10): dogs prohibited on public beaches 9:00 AM – 6:00 PM. OFF-SEASON (Sep 11–Jun 14): dogs allowed any time on leash. Tot lots / play equipment areas prohibited at all times per §6.16.020(a)(5).',
       ps.source_url,
       'Seasonal time-of-day restrictions apply (summer: dogs only before 9am or after 6pm). Hours set by City Council resolution; verify against current City summary page if resolution changes.'
FROM (values (8337),(453),(8336),(218),(8326),(8318),(8323),(8320),(8322),(3570),(8325),(8324),(7398),(8335),(4042),(8334),(8338),(8321)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Laguna Beach Municipal Code §6.16.010%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
