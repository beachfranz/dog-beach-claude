-- Bucket A tier-3/4 — Catalina Island beaches (23 LA County tier-4).
--
-- ALL beaches in LA County (including the entirety of Catalina Island) are
-- governed by LA County Code §17.12.290 "Cats and dogs prohibited":
--
--   "A person shall not bring or maintain on any beach a dog or cat."
--   (Ord. 10025 § 2, 1970: Ord. 9767 Art. 3 § 42, 1969.)
--
-- This is a section-level deep-link inside the Title 17 PARKS, BEACHES AND
-- OTHER PUBLIC AREAS title canonicalized this morning (see
-- 20260517_municode_url_canonicalize.sql). Chapter 17.12 BEACHES Part 3 -
-- Rules and Regulations §17.12.290.
--
-- Source: Catalina Island Conservancy's own published rules confirm:
--   "Los Angeles County Code Sections 17.12.290 and 17.12.300 prohibit
--    dogs, cats and horses on any beach in Los Angeles County, including
--    all beaches on Catalina Island."
--   (catalinaconservancy.org/resources/policies-and-information/)
--
-- Special case — Parsons Landing (fid 8369) has a campground area set back
-- from the beach where the Conservancy permits leashed dogs with a permit.
-- The beach itself remains no-dogs per §17.12.290; campground exception is
-- captured in status_note for that row.
--
-- These 23 beaches all had `(no p=1 gov)` and zero policy_source links
-- pre-migration — surfaced by the 2026-05-17 tier-3/4 audit (bucket A).
-- They're all Catalina Island per coordinate cluster (lat 33.32–33.48,
-- lon −118.30 to −118.59).

-- ─── 1. Insert section-level §17.12.290 policy_source row ─────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'LA County Code §17.12.290 (Cats and dogs prohibited) — Title 17 Ch 17.12 BEACHES Part 3 Rules and Regulations',
       (SELECT id FROM public.agency WHERE name = 'Los Angeles County' AND type = 'county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances?nodeId=TIT17PABEOTPUAR_CH17.12BE_PT3RURE_17.12.290CADOPR',
       'LA County Code §17.12.290 Cats and dogs prohibited: "A person shall not bring or maintain on any beach a dog or cat." (Ord. 10025 § 2, 1970: Ord. 9767 Art. 3 § 42, 1969.) Cross-reference: §17.12.300 prohibits horses, mules and similar animals on any beach. Applies to every beach within unincorporated Los Angeles County, including all beaches on Catalina Island per Catalina Island Conservancy''s published rules (catalinaconservancy.org/resources/policies-and-information/). Fetched verbatim via Playwright 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'LA County Code §17.12.290%'
);

-- ─── 2. Link all 23 Catalina Island beaches → not_allowed ──────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'LA County Code §17.12.290: "A person shall not bring or maintain on any beach a dog or cat." Applies to all LA County beaches including Catalina Island (per Catalina Island Conservancy''s published rules).',
       ps.source_url,
       'Catalina Island beach; LA County jurisdiction (Conservancy owns ~88% of island land but LA County animal code governs beach access). Dogs allowed elsewhere on Catalina only with Conservancy Wildland Permit + leash. Permit dogs allowed only in specific campgrounds (Black Jack, Little Harbor excluding Shark Harbor, Parsons Landing campground area set back from beach) — not on any beach itself.'
FROM (values
  -- Two Harbors / Isthmus cluster
  (7654), -- Two Harbors-the Isthmus of Catalina
  (8363), -- Lorenzo Beach
  (8368), -- Starlight Beach
  (8369), -- Parsons Landing (beach itself; campground exception in status_note above)
  (8370), -- Frankie's Beach
  (8371), -- Johnsons Landing
  (8373), -- Sullivans Beach
  (8379), -- Wells Beach
  (8380), -- Ribbon Beach
  -- Mid-island east coast
  (8382), -- Empire Landing
  (8385), -- Rippers Cove
  (8386), -- Paradise Cove (Catalina, NOT the Malibu Paradise Cove)
  (8389), -- Cottonwood Beach
  (8390), -- Ben Weston Beach
  (8391), -- Gibralter Beach
  (8392), -- Cabrillo Beach (Catalina; the San Pedro Cabrillo Beach is fid 8555 — Agent 2)
  -- East coast / Avalon-adjacent
  (8401), -- LongPoint Beach
  (8403), -- Button Shell Beach
  (8406), -- Gallagher Beach
  (8407), -- Bannings Beach
  (8408), -- Willow Cove Beach
  (8410), -- Moonstone Beach
  -- South end (San Clemente Is side)
  (8422)  -- Silver Canyon Landing
) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'LA County Code §17.12.290%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
