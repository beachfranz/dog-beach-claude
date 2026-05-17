-- Wave 3: City of San Diego — 17 active beaches with City of SD as
-- precedence-1 dog_policy authority.
--
-- Source: City of San Diego Parks & Recreation — "Dogs at Beaches and
-- Mission Bay" rules page (with the broader policy summary page).
--   https://www.sandiego.gov/park-and-recreation/parks/dogs
--   https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog
--
-- The City of San Diego Municipal Code is hosted on the City Clerk's
-- own portal (docs.sandiego.gov/municode/) as image-PDF chapters — NOT
-- on Municode, amlegal, or qcode. Chapter 5 (Animal Regulation) and
-- Chapter 63 (Park & Recreation) are the codified backstops; the
-- per-beach designations (which beach is off-leash vs prohibited) live
-- in administrative orders summarized on the Parks Dept page.
--
-- Treating the Parks Dept rules page as `agency_administrative_policy`
-- subtype — it is the operative public-facing summary of where dogs
-- may be on SD City beaches. Each beach is linked with the rule that
-- applies to it per the page.
--
-- Per-beach mapping:
--
--   OFF-LEASH (explicit per the rules page):
--     fid 8358 Dog Beach (Ocean Beach Flood Control Channel)
--     fid 9716 Fiesta Island (Mission Bay; 4am–10pm, with seasonal
--              nesting closures Apr 15–Sep 15)
--
--   ON-LEASH with seasonal time-of-day restrictions (Mission Bay Park
--   sub-beaches per rules: prohibited 9am–4pm Oct 31–Mar 31; prohibited
--   9am–6pm Apr 1–Oct 31; on-leash outside those hours):
--     fid 8554 Crown Point Beach
--     fid 8546 El Carmel Point
--     fid 6054 Mission Bay Beach
--     fid 8680 North Cove Public Beach
--     fid 6274 Playa Pacifica
--     fid 8552 Riviera Beach
--     fid 8679 Ski Beach
--     fid 8540 Spanish Landing Beach (Mission Bay south edge)
--     fid 8355 Top Of The Beach (Mission Beach/MB border)
--     fid 8544 Ventura Cove Park
--
--   NOT ALLOWED (default per rules page: "Dogs are restricted at other
--   beach and bay locations"):
--     fid 8352 Hermosa Terrace Park (La Jolla)
--     fid 306  Kellogg's Beach (Sunset Cliffs)
--     fid 4916 Ocean Beach Tide Pools (Sunset Cliffs)
--     fid 9014 Shoreline Beach Park (far south, near Imperial Beach line)
--     fid 8351 Windansea Beach (La Jolla)
--
-- Default leash length cited city-wide: 8 feet (per summary page —
-- "All dogs shall be maintained on a leash not to exceed 8 feet").
--
-- 15 of 32 SD-city-polygon beaches in beaches_gold have a different
-- primary dog_policy authority (state parks, etc) — not touched here.

-- ─── 1. Insert agency_administrative_policy policy_source ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of San Diego Parks & Recreation — Dogs at Beaches and Mission Bay (rules summary)',
       (SELECT id FROM public.agency WHERE name='San Diego' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog',
       'City of San Diego Parks & Recreation Dog Policy at Beaches and Mission Bay. Citywide default: "All dogs shall be maintained on a leash not to exceed 8 feet, including trails and canyons in all parks." Dog Beach (west end of San Diego River Floodway / Ocean Beach Flood Control Channel): off-leash allowed all day. Fiesta Island (Mission Bay Park): off-leash optional, daily 4:00 a.m.–10:00 p.m.; dogs not permitted leashed or unleashed at the Youth Aquatic Center/Camp; seasonal closures April 15–September 15 in designated buffer zones around nesting sites. All other Mission Bay Park areas: on-leash only with seasonal time-of-day restrictions — dogs prohibited 9:00 a.m.–4:00 p.m. October 31–March 31, and prohibited 9:00 a.m.–6:00 p.m. April 1–October 31; outside restricted hours dogs are permitted but must always be on a leash. All other beach and bay locations: dogs restricted (prohibited). [Source: City of SD Parks Dept Dogs page + Dogs at Beaches sub-page. Underlying codified backstop: SD Municipal Code Chapter 5 (Animal Regulation) + Chapter 63 (Park & Recreation), hosted on docs.sandiego.gov/municode/ as image PDFs — not yet ingested verbatim. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog'
);

-- ─── 2. Off-leash beaches (Dog Beach + Fiesta Island) ───────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'off_leash', 'operative'::operative_status,
       v.evidence,
       ps.source_url, v.note
FROM (values
  (8358, 'Dog Beach (west end of the San Diego River Floodway / Ocean Beach Flood Control Channel): off-leash allowed all day per City of SD Parks & Recreation Dogs at Beaches page.', NULL::text),
  (9716, 'Fiesta Island (Mission Bay Park): off-leash optional, daily 4am–10pm. Dogs prohibited at Youth Aquatic Center; seasonal nesting closures Apr 15–Sep 15 in designated buffer zones.', 'Time-of-day + seasonal restrictions apply — off-leash subject to 4am–10pm window and Apr–Sep nesting buffers.')
) as v(fid, evidence, note)
CROSS JOIN public.policy_source ps
WHERE ps.source_url='https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Mission Bay Park sub-beaches (on-leash with seasonal hrs) ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Mission Bay Park sub-beach: on-leash required (8-foot max). Dogs prohibited 9am–4pm Oct 31–Mar 31, and 9am–6pm Apr 1–Oct 31. Outside restricted hours, on-leash only.',
       ps.source_url,
       'Time-of-day restrictions apply: prohibited 9am-4pm Oct-Mar / 9am-6pm Apr-Oct.'
FROM (values (8554),(8546),(6054),(8680),(6274),(8552),(8679),(8540),(8355),(8544)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url='https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. Default not-allowed (all other SD City beaches) ─────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'City of SD Parks Dogs at Beaches page: "Dogs are restricted at other beach and bay locations." Default applies to all SD City beaches not on the off-leash or Mission Bay lists.',
       ps.source_url,
       NULL
FROM (values (8352),(306),(4916),(9014),(8351)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url='https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
