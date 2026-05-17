-- Wave 3: City of Dana Point — 6 beaches, blanket no-dogs-on-beach.
--
-- Source: Dana Point Municipal Code §10.04.020 Dogs on Public Property
-- (Title 10 Animal Control, Welfare and Licensing Requirements,
-- Chapter 10.04 Dogs and Cats).
-- URL: https://ecode360.com/42956527
-- Platform: ecode360 (General Code) — found via Franz pointer; not
-- auto-discovered because the city discovery script's amlegal/qcode/
-- municode probes don't include ecode360 as a fallback.
--
-- Operative rule (§10.04.020, verbatim):
--   "No owner or person in charge or in control of any dog, except a
--   blind person with his or her guide dog, shall permit or allow such
--   dog to be within or upon public school property, public beaches,
--   public access ways to the beach, parks, municipal pier or
--   municipal golf course, except where authorized by a city served
--   by the Authority. This Section, however, does not prohibit the use
--   of dogs on school property for teaching other school uses when
--   approved by the school officials."
--
-- §10.04.010 Restraint of Dogs (companion citywide leash rule, verbatim):
--   "(b) Highway, street, alley or other public property unless such
--   dog be restrained by a substantial chain or leash not exceeding
--   six feet in length, and is under the charge of a person competent
--   to exercise care, custody, and control over such dog ..."
--   (Applies off beaches; beaches are flat-prohibited per §10.04.020.)
--
-- 6 DP-city-polygon beaches in beaches_gold:
--   fid 8682 Baby Beach                — unlinked → not_allowed
--   fid 5968 Dana Point Headlands Beach — already linked (skip)
--   fid 9718 Doheny State Beach         — unlinked; STATE BEACH (CA DPR
--                                         likely jurisdiction); link to
--                                         city rule cautiously — state
--                                         park may have separate rules
--   fid 8838 North Day Use Beach        — unlinked → not_allowed
--   fid 8681 Poche Beach                — unlinked → not_allowed
--   fid 8839 South Day Use Beach        — already linked to CA DPR (skip)
--
-- Idempotent — skip any beach with an existing link. Net 4 new links.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Dana Point Municipal Code §10.04.020 (Dogs on Public Property) + §10.04.010 (Restraint of Dogs)',
       (SELECT id FROM public.agency WHERE name='Dana Point' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/42956527',
       'Dana Point Municipal Code Title 10 (Animal Control, Welfare and Licensing Requirements), Chapter 10.04 Dogs and Cats. §10.04.020 Dogs on Public Property: No owner or person in charge or in control of any dog, except a blind person with his or her guide dog, shall permit or allow such dog to be within or upon public school property, public beaches, public access ways to the beach, parks, municipal pier or municipal golf course, except where authorized by a city served by the Authority. This Section, however, does not prohibit the use of dogs on school property for teaching other school uses when approved by the school officials. §10.04.010 Restraint of Dogs (citywide leash law, off beaches): (a) Private property — dog must be restrained by fence/wall/chain/leash not exceeding six feet or under charge of a competent person. (b) Highway, street, alley or other public property — dog must be restrained by substantial chain or leash not exceeding six feet in length, under charge of a competent person, unless owner/operator grants written permission for off-leash. §10.04.030 Females in Season: female cats/dogs strictly confined during breeding period. §10.04.040 Running on Owner''s Premises: dogs may run at large on owner''s premises (other than unspayed females in breeding period). §10.04.050 Working Dogs: herding/hunting/obedience/tracking/show dogs not considered unleashed while performing duties. Ord. 96-01 (1996); amended Ord. 25-16 (2025). [Hosted on ecode360 at ecode360.com/42956527. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Dana Point Municipal Code §10.04.020%'
);

-- ─── 2. Link unlinked DP-polygon beaches → not_allowed ───────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Dana Point Municipal Code §10.04.020: no owner or person in control of any dog (except blind person with guide dog) shall permit such dog to be within or upon public beaches or public access ways to the beach. Citywide blanket prohibition.',
       ps.source_url,
       CASE g.name
         WHEN 'Doheny State Beach' THEN 'Beach is within Doheny State Park — CA DPR rules may layer; verify if DPR allows dogs (parks.ca.gov/Dogs catalog typically prohibits dogs on state-park beach sand).'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Dana Point'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Dana Point Municipal Code §10.04.020%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
