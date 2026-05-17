-- Wave 3: City of Monterey — 4 beaches, on_leash per §23-8 + §23-8.5.
--
-- Source: Monterey City Code §23-8 (Dogs in Parks) + §23-8.5 (Other
-- Animals in Recreation Areas). Codified on municipal.codes
-- (Cloudflare-protected; verbatim fetch blocked, so rule captured via
-- web search and the City of Monterey Parks & Rec leashed-dog areas
-- summary).
-- URLs:
--   https://monterey.municipal.codes/Code/23-8
--   https://monterey.municipal.codes/Code/23-8.5
--   https://monterey.gov/your_city_hall/departments/parks_and_recreation/parks/dog_park.php
--
-- Operative rule (§23-8 Dogs in Parks):
--   "Dogs are permitted in City parks during the hours that such parks
--   are open to the public, and except with respect to any dog used by
--   a law enforcement agency or any dog at a City dog park(s), dogs
--   must at all times be on a leash, no longer than six feet (6'),
--   which must be held by a person capable of restraining the dog at
--   all times. Any excrement deposited by the animal must be promptly
--   removed."
--
-- City of Monterey Parks & Rec lists the following as parks/trails
-- permitting LEASHED DOGS: El Estero Park, Beach area east of Wharf
-- No. 2, Veteran's Memorial Park, and Recreation Trail. Leash ≤6 ft,
-- excrement removal required, current rabies and license required.
--
-- 4 Monterey-city-polygon beaches sorted W to E by longitude. Wharf
-- No. 2 is at approximately lon -121.892:
--
--   WEST of Wharf No. 2 (Cannery Row / New Monterey):
--     fid 8293 Macabee Beach     (lon -121.899)
--     fid 8294 Aeneas Beach      (lon -121.897)
--     fid 8292 San Carlos Beach  (lon -121.896)
--
--   EAST of Wharf No. 2 (designated leashed-dog area per city list):
--     fid 8568 Del Monte Beach   (lon -121.872)
--
-- All 4 get on_leash per §23-8. Del Monte Beach gets an explicit
-- evidence note that it's the city-designated "Beach area east of
-- Wharf No. 2." All 4 are already linked (likely from county/state
-- backfills); these city-policy links supplement.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Monterey City Code §23-8 (Dogs in Parks) + §23-8.5 (Other Animals in Recreation Areas)',
       (SELECT id FROM public.agency WHERE name='Monterey' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://monterey.municipal.codes/Code/23-8',
       'Monterey City Code §23-8 Dogs in Parks: Dogs are permitted in City parks during the hours that such parks are open to the public, and except with respect to any dog used by a law enforcement agency or any dog at a City dog park(s), dogs must at all times be on a leash, no longer than six feet (6''), which must be held by a person capable of restraining the dog at all times. Any excrement deposited by the animal must be promptly removed. §23-8.5 covers Other Animals in Recreation Areas. The City of Monterey Parks & Recreation lists the following parks/trails as permitting LEASHED DOGS: El Estero Park, Beach area east of Wharf No. 2, Veteran''s Memorial Park, and Recreation Trail. Leash ≤6 ft, excrement removal, current rabies and license required. [Codified on municipal.codes (Cloudflare-blocked from automated fetch); operative rule captured via web search of Monterey Code §23-8 + City of Monterey Parks dog page. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Monterey City Code §23-8%'
);

-- ─── 2. Link all 4 Monterey beaches → on_leash ──────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       v.evidence, ps.source_url, v.note
FROM (values
  (8293, 'Monterey City Code §23-8: dogs in city parks must be on leash ≤6 ft, held by competent person. Macabee Beach is a city-jurisdiction pocket beach in the Cannery Row area west of Wharf No. 2.', NULL::text),
  (8294, 'Monterey City Code §23-8: dogs in city parks must be on leash ≤6 ft, held by competent person. Aeneas Beach is a city-jurisdiction pocket beach in the Cannery Row area west of Wharf No. 2.', NULL::text),
  (8292, 'Monterey City Code §23-8: dogs in city parks must be on leash ≤6 ft, held by competent person. San Carlos Beach is at the foot of San Carlos Street, near Coast Guard Pier — popular for scuba diving.', NULL::text),
  (8568, 'Monterey City Code §23-8 + city''s leashed-dog parks list: Del Monte Beach is the "Beach area east of Wharf No. 2" explicitly designated for leashed dogs.', 'Explicitly designated as a leashed-dog beach by the city.')
) as v(fid, evidence, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Monterey City Code §23-8%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
