-- Wave 3: City of Santa Barbara — 7 beaches with east/west split per
-- Shoreline Park Staircase exception.
--
-- Source: City of Santa Barbara Police Department — City Animal
-- Regulations page citing the Municipal Code.
-- URL: https://santabarbaraca.gov/government/departments/santa-barbara-police-department/city-animal-regulations
--
-- Operative rule (§6.08.070 Prohibition, verbatim):
--   "It shall be unlawful for any person owning or having possession,
--   charge, custody or control of any animal to cause or permit or
--   allow the same, whether or not on leash or restraint, to be upon a
--   beach or within the Santa Barbara Harbor except that dogs and
--   horses are permitted on the beach at any point between Shoreline
--   Park Staircase and the westerly City limits if restrained in
--   conformance with the provisions of Section 6.08.020."
--
-- §6.08.020(B) Leash law (verbatim):
--   "No dog is permitted upon a street or other public place unless on
--   a leash not in excess of six (6) feet in length and under the
--   immediate care and control of the owner or other person having the
--   care and custody thereof, except during supervised dog training
--   classes, shows or exhibitions held in City Parks when authorized by
--   a Park Use Permit issued by the Parks and Recreation Department."
--
-- Geographic classification — Shoreline Park is at approx (34.4011,
-- -119.7019). West of that point = on_leash permitted exception zone.
-- East of that point = beach prohibition applies (also the Harbor).
--
-- WEST of Shoreline Park (on_leash permitted per §6.08.070 exception):
--   fid 8779 Arroyo Burro Beach   (34.40, -119.744)
--   fid 8780 Hendrys Beach        (34.40, -119.737)  — colloquial name for Arroyo Burro County Beach Park area
--   fid 6817 Mesa Lane Beach      (34.40, -119.731)
--   fid 3877 Thousand Steps Beach (34.40, -119.713)  — SB's Thousand Steps (different from Laguna Beach's), at the staircase from Mesa neighborhood
--
-- EAST of Shoreline Park (not_allowed per §6.08.070 default):
--   fid 6298 Leadbetter Beach     (34.40, -119.699)
--   fid 9316 West Beach           (34.41, -119.690)
--   fid 8610 East Beach           (34.42, -119.671)
--
-- 7 SB-city-polygon beaches; 3 have Santa Barbara as precedence-1
-- dog_policy authority (Arroyo Burro, Hendrys, Thousand Steps), 4 have
-- no resolved authority. All 7 unlinked. All 7 linked per the §6.08.070
-- east/west classification.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Santa Barbara Municipal Code §6.08.070 (Prohibition on beaches/Harbor) + §6.08.020(B) (6-ft leash law)',
       (SELECT id FROM public.agency WHERE name='Santa Barbara' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://santabarbaraca.gov/government/departments/santa-barbara-police-department/city-animal-regulations',
       'Santa Barbara Municipal Code §6.08.070 PROHIBITION: It shall be unlawful for any person owning or having possession, charge, custody or control of any animal to cause or permit or allow the same, whether or not on leash or restraint, to be upon a beach or within the Santa Barbara Harbor except that dogs and horses are permitted on the beach at any point between Shoreline Park Staircase and the westerly City limits if restrained in conformance with the provisions of Section 6.08.020. §6.08.020(B): No dog is permitted upon a street or other public place unless on a leash not in excess of six (6) feet in length and under the immediate care and control of the owner or other person having the care and custody thereof, except during supervised dog training classes, shows or exhibitions held in City Parks when authorized by a Park Use Permit issued by the Parks and Recreation Department. [Codified via the City of Santa Barbara Police Department City Animal Regulations page. The §6.08.070 exception zone (Shoreline Park Staircase west to City limits) covers Arroyo Burro / Hendrys / Mesa Lane / Thousand Steps beaches; beaches east of Shoreline Park (Leadbetter, West Beach, East Beach) and the Harbor remain prohibited. Codified ordinance also accessible via qcode/ecode360 for Santa Barbara city; verbatim text per City page. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Santa Barbara Municipal Code §6.08.070%'
);

-- ─── 2. WEST of Shoreline Park (on_leash exception zone) ────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Santa Barbara Municipal Code §6.08.070 exception zone: dogs and horses permitted on beach between Shoreline Park Staircase and westerly City limits if restrained per §6.08.020(B) (6-ft leash max, immediate care and control).',
       ps.source_url, NULL
FROM (values (8779),(8780),(6817),(3877)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Santa Barbara Municipal Code §6.08.070%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=v.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. EAST of Shoreline Park (default not_allowed) ────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Santa Barbara Municipal Code §6.08.070: unlawful to cause or permit any animal, whether on leash or not, to be upon a beach or within the Santa Barbara Harbor. East of Shoreline Park Staircase — exception zone does not apply.',
       ps.source_url, NULL
FROM (values (6298),(9316),(8610)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Santa Barbara Municipal Code §6.08.070%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=v.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
