-- Wave 3: City of Half Moon Bay — supplementary city policy_source +
-- 2 most-likely city-beach matches linked as on_leash.
--
-- Source: Half Moon Bay Municipal Code §9.13.020 (Designated parks and
-- beaches) + §9.13.050 (Unlawful activities designated) — codified on
-- codepublishing.com — and the City of Half Moon Bay beach info page.
--
-- URLs:
--   https://www.codepublishing.com/CA/HalfMoonBay/html/HalfMoonBay09/HalfMoonBay0913.html
--   https://www.halfmoonbay.gov/438/Beaches
--
-- Operative facts:
--   - HMB Municipal Code §9.13.020(B) designates THREE municipal public
--     beaches: (1) Poplar Beach (between Seymour Ditch and Kelly Avenue);
--     (2) Cañada Verde Beach area (west end of Miramontes Point Road);
--     (3) Surfers Beach (westerly extension of Coronado Street, between
--     breakwater and mean high tide).
--   - City of Half Moon Bay beach info page: leashed dogs (6-ft max)
--     permitted on city beaches and on city-managed portions of the
--     Coastal Trail.
--   - State beach areas (Half Moon Bay State Beach — Francis Beach,
--     Venice Beach, Dunes Beach, Roosevelt Beach) prohibit dogs on the
--     sand (per CA DPR Dogs catalog, already linked in those beaches).
--
-- Name-match honesty: our 7 HMB-polygon beaches in beaches_gold do not
-- cleanly map to the 3 city-designated beach names. Best inferential
-- matches (Franz authorized option 3 — link likely matches with
-- name-match uncertainty flagged in status_note):
--   - fid 8571 Miramontes Point Beach → likely Cañada Verde Beach
--     (city code: "west end of Miramontes Point Road")
--   - fid 3838 El Granada Beach → possibly Surfers Beach (city code:
--     "westerly extension of Coronado Street, near El Granada")
--
-- Other 5 fids NOT linked here:
--   - 8570 Mateo Coast State Beaches (already linked to CA DPR)
--   - 2078 Roosevelt Beach (HMB State Beach section, state jurisdiction)
--   - 9243 Pillar Point Harbor Beach (harbor district, not city)
--   - 6932 Pelican Point Beach (unclear; conservatively skipped)
--   - 7606 Wavecrest Beach (near Wavecrest Rd, but Poplar Beach per
--     code is between Seymour Ditch and Kelly Avenue — different
--     streets; not a clean match for the city-designated Poplar Beach)
--
-- The 2 added links supplement existing CA DPR links (do not displace
-- them); the consensus engine can sort precedence based on agency
-- precedence + source authority.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Half Moon Bay Municipal Code §9.13.020 (Designated parks and beaches) + §9.13.050 (Unlawful activities) — leashed dogs permitted on city beaches',
       (SELECT id FROM public.agency WHERE name='Half Moon Bay' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.halfmoonbay.gov/438/Beaches',
       'Half Moon Bay Municipal Code Chapter 9.13 PARK AND BEACH REGULATIONS. §9.13.020(B) designates three municipal public beaches: (1) Poplar Beach, located between Seymour Ditch and Kelly Avenue; (2) Cañada Verde Beach area, located at the west end of Miramontes Point Road; (3) Surfers Beach, located at the westerly extension of Coronado Street (between the breakwater and mean high tide). City of Half Moon Bay beach info page summarizes operative dog policy: leashed dogs (6-foot maximum) permitted on city beaches and on city-managed portions of the Coastal Trail. State-beach areas (Half Moon Bay State Beach — Francis, Venice, Dunes, Roosevelt Beach) prohibit dogs on the sand per CA DPR. §9.13.030 city beaches open 24 hours per day per California Coastal Act. §9.13.050 unlawful activities designated on city parks and beaches (alcohol, glass containers, plastic water bottles, fires — see §9.13.060-075 for specifics). Ord. 12-94, C-2014-03, C-2019-02, C-2020-05. [Source: HMB Municipal Code via codepublishing.com + City of HMB beach info page. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Half Moon Bay Municipal Code §9.13.020%'
);

-- ─── 2. Link 2 most-likely city-beach matches as on_leash ───────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'HMB Municipal Code §9.13.020(B) + City beach info page: leashed dogs (6-ft max) permitted on city beaches.',
       ps.source_url,
       v.note
FROM (values
  (8571, 'NAME-MATCH UNCERTAINTY: "Miramontes Point Beach" likely corresponds to "Cañada Verde Beach area" per HMB §9.13.020(B)(2) location ("west end of Miramontes Point Road"). Walkthrough needed to confirm.'),
  (3838, 'NAME-MATCH UNCERTAINTY: "El Granada Beach" may correspond to "Surfers Beach" per HMB §9.13.020(B)(3) location ("westerly extension of Coronado Street, near El Granada"). Could alternatively be the unincorporated El Granada community beach (state/harbor district). Walkthrough needed to confirm city vs state jurisdiction.')
) as v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Half Moon Bay Municipal Code §9.13.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
