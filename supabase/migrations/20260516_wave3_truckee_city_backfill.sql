-- Wave 3: Town of Truckee — 3 beaches on Donner Lake, mixed rules
-- (Donner Memorial SP East = on_leash, China Cove = not_allowed,
-- West End Beach = not_allowed).
--
-- Governance note: All 3 Truckee-city-polygon beaches are on Donner
-- Lake. East Beach and China Cove Beach are within Donner Memorial
-- State Park (CA DPR authority). West End Beach is a Truckee-Donner
-- Recreation and Park District facility (special district). The Town
-- of Truckee Municipal Code Title 8 ANIMALS is hosted on Municode at
-- library.municode.com/ca/truckee/codes/municipal_code and on the
-- town's own document center; it provides citywide "at large
-- prohibited" rules (voice + visual control required) but the
-- operative beach-specific rules come from DPR (state park) and the
-- park district (West End Beach).
--
-- Sources:
--   CA DPR / Donner Memorial SP — tahoepublicbeaches.org/beaches/
--     donner-memorial-state-park-beaches/ (and parks.ca.gov/?page_id=503)
--   Truckee-Donner Rec & Park District — tdrpd.org/165/West-End-Beach
--
-- Operative rules (verbatim):
--   Donner Memorial SP (tahoepublicbeaches.org):
--     "Dogs are allowed on the Lakeside Interpretive Trail, Zig Zag
--     Trail, fire roads, and along the shore of Donner Lake except
--     for China Cove Beach."
--     "Dogs must always remain on a six-foot (maximum) leash."
--     "Dogs are not allowed in the visitor center, on the half-mile
--     Nature Trail, or China Cove Beach."
--   West End Beach Park (Truckee-Donner Rec & Park District,
--   tdrpd.org/165/West-End-Beach):
--     "NO DOGS, GLASS, OR SMOKING"
--
-- 3 Truckee-city-polygon beaches:
--   - fid 9580 China Cove Beach (Donner Memorial SP) → not_allowed
--   - fid 8846 East Beach       (Donner Memorial SP) → on_leash
--   - fid 8272 West End Beach   (TDRPD)              → not_allowed

-- ─── 1. Donner Memorial SP policy_source (CA DPR) ─────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks — Donner Memorial State Park dog policy (lakeshore on_leash except China Cove not_allowed)',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.tahoepublicbeaches.org/beaches/donner-memorial-state-park-beaches/',
       'CA State Parks — Donner Memorial State Park dog policy (per Lake Tahoe Public Beaches portal page for Donner Memorial SP beaches, in coordination with parks.ca.gov/?page_id=503). Verbatim: "Dogs are allowed on the Lakeside Interpretive Trail, Zig Zag Trail, fire roads, and along the shore of Donner Lake except for China Cove Beach." "Dogs must always remain on a six-foot (maximum) leash." "Dogs are not allowed in the visitor center, on the half-mile Nature Trail, or China Cove Beach." Operative interpretation: lakeshore including East Beach = on_leash (6-ft max); China Cove Beach = not_allowed. Town of Truckee Municipal Code Title 8 (at-large prohibition, voice-and-visual control citywide except strict leash at Truckee Regional Park May-October) does not override state-park-specific rules. [Fetched verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.tahoepublicbeaches.org/beaches/donner-memorial-state-park-beaches/'
);

-- ─── 2. China Cove Beach → not_allowed ────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9580, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Donner Memorial State Park: "Dogs are not allowed in the visitor center, on the half-mile Nature Trail, or China Cove Beach." Specific exception within an otherwise dog-friendly state park.',
       ps.source_url,
       'China Cove is the named beach exception within Donner Memorial SP. East Beach (sister beach in the same park) does allow leashed dogs.'
FROM public.policy_source ps
WHERE ps.source_url='https://www.tahoepublicbeaches.org/beaches/donner-memorial-state-park-beaches/'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. East Beach → on_leash ─────────────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8846, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Donner Memorial State Park: "Dogs are allowed ... along the shore of Donner Lake except for China Cove Beach." "Dogs must always remain on a six-foot (maximum) leash." East Beach is part of the allowed lakeshore.',
       ps.source_url,
       '6-foot max leash per CA State Parks rule.'
FROM public.policy_source ps
WHERE ps.source_url='https://www.tahoepublicbeaches.org/beaches/donner-memorial-state-park-beaches/'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. West End Beach policy_source (TDRPD) ──────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'Truckee-Donner Recreation and Park District — West End Beach park rules (no dogs)',
       (SELECT id FROM public.agency WHERE name='Truckee-Donner Recreation and Park District' AND type='special_district'),
       ARRAY['dog_policy']::text[],
       'https://www.tdrpd.org/165/West-End-Beach',
       'Truckee-Donner Recreation and Park District — West End Beach Park rules (posted on tdrpd.org/165/West-End-Beach). Verbatim rule: "NO DOGS, GLASS, OR SMOKING". West End Beach is a 12-acre TDRPD day-use beach on the west shore of Donner Lake with American Red Cross Certified Lifeguards. Year-round prohibition (no seasonal qualifier in posted rules). [Fetched verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.tdrpd.org/165/West-End-Beach'
);

-- ─── 5. West End Beach → not_allowed ──────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8272, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Truckee-Donner Rec & Park District — West End Beach posted rules: "NO DOGS, GLASS, OR SMOKING". Year-round prohibition at TDRPD facility.',
       ps.source_url,
       'Special-district facility (TDRPD); operator-posted rule, not codified municipal ordinance.'
FROM public.policy_source ps
WHERE ps.source_url='https://www.tdrpd.org/165/West-End-Beach'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
