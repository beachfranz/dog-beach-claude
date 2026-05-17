-- Wave 3: City of Solana Beach — 3 beaches with per-beach rule mapping
-- (Seascape Surf not_allowed; Tide Beach Park/Tabletops on_leash).
--
-- Source: Solana Beach Municipal Code §11.12.035 (Dogs on beaches),
-- Chapter 11.12 Public Property and Places.
-- URL: https://www.codepublishing.com/CA/SolanaBeach/html/SolanaBeach11/
--      SolanaBeach1112.html (Code Publishing platform)
-- Plus City Marine Safety page (cityofsolanabeach.ca.gov/en/community/
--   public-safety/marine-safety) for authoritative per-beach interpretation.
--
-- Operative rules:
--
-- §11.12.035 Dogs on beaches (verbatim):
--   A. "No person having the care, custody, charge or control of any
--   dog shall permit or allow that dog to be on any of the beaches
--   within the city except on the beach south of the Del Mar Shore
--   Public Stairway or on the beach north of the Tide Park Public
--   Stairway."
--   B. "It shall be unlawful for any person to bring, allow or permit
--   any dog upon any of the beaches within the city unless such dog is
--   restrained by a leash not in excess of six feet. Every owner of a
--   dog shall restrain his dog from running at large on any of the
--   beaches in the city. No person shall permit any dog to be left
--   unattended on any of the beaches within the city."
--   C. Owners must pick up after their dogs.
--   D. Exception for guide dogs (unsighted or hearing impaired).
--
-- City Marine Safety interpretation (verbatim):
--   "Dogs will not be permitted on the beach or access at Fletcher
--   Cove or Seascape Surf at any time of day year round."
--   Tide Beach Park (Tabletops): dogs permitted on-leash north of
--   stairway access, extending toward Cardiff State Beach.
--   Del Mar Shores (Rock Pile): dogs permitted on-leash south of
--   stairway access, extending toward Del Mar.
--
-- 3 Solana Beach-city-polygon beaches, per-beach mapping:
--   fid 607  Seascape Sur Beach (= "Seascape Surf") → not_allowed
--                                  Per City Marine Safety: "Dogs will
--                                  not be permitted on the beach ... at
--                                  Seascape Surf at any time of day
--                                  year round."
--   fid 8557 Tabletops             → on_leash (north of Tide Park stairway)
--   fid 9721 Tide Beach Park       → on_leash (named carve-out; north of
--                                    the Tide Park stairway is the
--                                    dog-permitted segment)

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Solana Beach Municipal Code §11.12.035 (Dogs on beaches — restricted to north of Tide Park Stairway and south of Del Mar Shore Stairway; 6-foot leash; per-beach map per City Marine Safety)',
       (SELECT id FROM public.agency WHERE name='Solana Beach' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/SolanaBeach/html/SolanaBeach11/SolanaBeach1112.html',
       'Solana Beach Municipal Code Chapter 11.12 PUBLIC PROPERTY AND PLACES, §11.12.035 Dogs on beaches (verbatim, fetched via curl 2026-05-16): A. No person having the care, custody, charge or control of any dog shall permit or allow that dog to be on any of the beaches within the city except on the beach south of the Del Mar Shore Public Stairway or on the beach north of the Tide Park Public Stairway. B. It shall be unlawful for any person to bring, allow or permit any dog upon any of the beaches within the city unless such dog is restrained by a leash not in excess of six feet. Every owner of a dog shall restrain his dog from running at large on any of the beaches in the city. No person shall permit any dog to be left unattended on any of the beaches within the city. C. A person having care, custody, control or possession of a dog ... shall have in his or her possession equipment sufficient to remove and thereafter contain defecation from such dog, and it shall be unlawful to fail to remove and contain the defecation from such dog. D. This section shall not apply to the use of guide dogs for the unsighted or hearing impaired. E. Violations punishable as misdemeanor with administrative fine schedule. Companion §11.12.020(X) carve-out: leashed dogs allowed in Fletcher Cove Park to the top of the beach access ramp and including upper viewpoint and area surrounding Fletcher Cove Community Center, Coastal Rail Trail, La Colonia Park, and Tide Park Viewpoint (these are park/bluff areas, NOT beach sand). City Marine Safety page (cityofsolanabeach.ca.gov/en/community/public-safety/marine-safety) clarifies per-beach scope: "Dogs will not be permitted on the beach or access at Fletcher Cove or Seascape Surf at any time of day year round." Tide Beach Park (Tabletops): on-leash north of stairway access. Del Mar Shores (Rock Pile): on-leash south of stairway access. Dogs must be on 6-foot maximum leash; owners must pick up. [Fetched verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Solana Beach Municipal Code §11.12.035%'
);

-- ─── 2. Seascape Surf → not_allowed ───────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 607, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Solana Beach MC §11.12.035 + City Marine Safety: "Dogs will not be permitted on the beach or access at Fletcher Cove or Seascape Surf at any time of day year round." Seascape Surf is north of the Del Mar Shore Stairway carve-out (the dog-allowed segment runs south toward the Del Mar city limit).',
       ps.source_url,
       'City Marine Safety pages are the authoritative per-beach interpretation of the §11.12.035 stairway boundaries.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Solana Beach Municipal Code §11.12.035%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Tabletops + Tide Beach Park → on_leash ────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Solana Beach MC §11.12.035(A): dogs allowed "on the beach north of the Tide Park Public Stairway". §11.12.035(B): leash not to exceed six feet. City Marine Safety: "Tide Beach Park (Tabletops): dogs permitted on-leash north of stairway access, extending toward Cardiff State Beach."',
       ps.source_url,
       'Named carve-out beach in §11.12.035 (north-of-Tide-Park-Stairway dog-permitted segment). 6-foot max leash; owners must pick up.'
FROM (values (8557),(9721)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Solana Beach Municipal Code §11.12.035%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
