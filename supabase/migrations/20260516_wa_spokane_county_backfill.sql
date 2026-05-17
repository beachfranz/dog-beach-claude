-- Wave 4: Spokane County (WA) dog-policy backfill.
--
-- Source: Spokane County Code §5.04.070 (Control of dogs) — Title
-- 5 / Chapter 5.04 Dogs and Cats.
--
-- Live URL: https://library.municode.com/wa/spokane_county/codes/code_of_ordinances?nodeId=TIT5AN_CH5.04DOCA_5.04.070COOFDO
--
-- Operative rule (§5.04.070, verbatim via Playwright 2026-05-16):
--   The following dog control regulations are police regulations
--   designed to protect public health and safety. The owner or keeper
--   of a dog is strictly liable to control his or her dog(s) as
--   required herein... It is unlawful for the owner or keeper of a
--   dog(s) to violate any of the following regulations. The owner or
--   keeper of a dog or dogs shall prevent said dog(s) from:
--   (1) Running at large in the county, whether licensed or not;
--       provided, that this subsection shall not: a) prohibit a
--       person from walking or exercising a dog in public when the
--       dog is on a leash, tether or chain not exceeding eight feet
--       in length; or, b) prohibit a person from having a dog
--       off-leash in an area described in section 5.04.045(1) as an
--       off-leash area, provided that the requirements of section
--       5.04.045(2) are met;
--   (2) Entering any place where food is stored, prepared, served or
--       sold to the public or any public building or hall...
--
-- §5.04.040 definitions:
--   (6) "At large dog" means a dog that is physically off the
--       premises of the owner... and which is not secured by a leash
--       that is under the control of the owner... not exceeding
--       eight feet in length.
--
-- Sand rule = on_leash (8-foot max). Per published Spokane County
-- Parks rules: dogs are NOT allowed on any swim beach or in any swim
-- area — designated swim portions = not_allowed.
--
-- 1 Spokane County beach in beaches_gold.

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Spokane County Code §5.04.070 (Control of dogs) — Title 5 / Chapter 5.04 Dogs and Cats',
       (SELECT id FROM public.agency WHERE name='Spokane County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/wa/spokane_county/codes/code_of_ordinances?nodeId=TIT5AN_CH5.04DOCA_5.04.070COOFDO',
       'Spokane County Code §5.04.070 Control of dogs. The following dog control regulations are police regulations designed to protect public health and safety. The owner or keeper of a dog is strictly liable to control his or her dog(s) as required herein... The owner or keeper of a dog or dogs shall prevent said dog(s) from: (1) Running at large in the county, whether licensed or not; provided, that this subsection shall not: a) prohibit a person from walking or exercising a dog in public when the dog is on a leash, tether or chain not exceeding eight feet in length; or, b) prohibit a person from having a dog off-leash in an area described in section 5.04.045(1) as an off-leash area, provided that the requirements of section 5.04.045(2) are met. (2) Entering any place where food is stored, prepared, served or sold to the public or any public building or hall... (Res. No. 2018-0166, 2-20-2018; Res. No. 13-1198, 12-17-2013). §5.04.040(6) "At large dog" means a dog that is physically off the premises of the owner, handler, or keeper of the dog and which is not secured by a leash that is under the control of the owner, handler, or keeper not exceeding eight feet in length. [Per published Spokane County Parks rules: dogs not allowed on any swim beach or swim area. Fetched via Playwright/Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Spokane County Code §5.04.070%'
);

-- ─── 2. Link Spokane County beaches ─────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Spokane'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Spokane County Code §5.04.070%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Spokane County Code §5.04.070(1): no dog at large in county; leash max 8 feet for public walking/exercise. Per Spokane County Parks rules: no dogs on swim beach or swim area.',
       'https://library.municode.com/wa/spokane_county/codes/code_of_ordinances?nodeId=TIT5AN_CH5.04DOCA_5.04.070COOFDO',
       'Sand rule = on_leash per §5.04.070(1) (8-foot max). Designated swim portion = not_allowed per Parks rules; per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
