-- Bucket A tier-3/4 — Orange County: Laguna Beach coves + Huntington/Bolsa
-- Chica State Beaches + Seal Beach city. Plus HOA defers.
--
-- 14 OC fids surfaced by the 2026-05-17 tier-3/4 audit with no
-- precedence-1 governance and zero policy_source links pre-migration.
--
-- ─── Cluster breakdown ───────────────────────────────────────────
--
-- A. LAGUNA BEACH coves (8 fids) — all link to existing ps id=144
--    (Laguna Beach Municipal Code §6.16.010 + §6.16.020), shipped in
--    20260516_wave3_laguna_beach_city_backfill.sql. Per the city policy:
--      • SUMMER (Jun 15–Sep 10): dogs prohibited 9 AM – 6 PM on public
--        beaches; otherwise leashed.
--      • OFF-SEASON (Sep 11–Jun 14): dogs allowed any time, leashed.
--      • Thousand Steps Beach: dogs PROHIBITED EVERY DAY (named exception
--        in the city policy summary).
--    No new policy_source rows needed for the seasonal-leash coves.
--
--      fid 6813  Arch Beach           c1=Laguna Beach  → on_leash (seasonal)
--      fid 7729  Shaw's Cove Beach    c1=Laguna Beach  → on_leash (seasonal)
--      fid 8314  La Senda And Bay Dr  c1=Laguna Beach  → on_leash (seasonal)
--      fid 8315  Thousand Steps Beach c1=Laguna Beach  → NOT_ALLOWED (named)
--      fid 8319  Victoria Beach       c1=Laguna Beach  → on_leash (seasonal)
--      fid 8328  Irvine Cove Beach    c1=NULL (gated cove geographically
--                                       inside Laguna Beach city; HOA
--                                       gate but beach is public-trust
--                                       below MHW; city ordinance applies)
--                                                     → on_leash (seasonal)
--      fid 3733  Woods Cove Beach     c1=Laguna Niguel  ⚠ polygon-vs-name
--                                       mismatch (fid 8321 "Woods Cove"
--                                       at the canonical Laguna Beach
--                                       coords IS in Laguna Beach city
--                                       per c1; fid 3733's coords place
--                                       it ~5 km inland). Linking to
--                                       Laguna Beach city policy per name
--                                       and prompt; flagged in status_note.
--                                                     → on_leash (seasonal)
--
-- B. CA STATE BEACHES (2 fids) — 2 new CA DPR per-park policy_source rows.
--    Both have identical posted dog policy:
--      "Yes: Dogs allowed only on the multiuse trail (bike path).
--       Dogs not allowed on sand."
--    Page IDs resolved via parks.ca.gov/?page_id=21805 listing:
--      Bolsa Chica State Beach   → parks.ca.gov/?page_id=642
--      Huntington State Beach    → parks.ca.gov/?page_id=643
--    Two bps rows each: section='sand' → not_allowed, section='bike_path'
--    → on_leash (with leash law from CA PRC §4319-equivalent state-park
--    leash rule). Beach card surfaces NOT_ALLOWED at the sand level.
--
--      fid 8453  Huntington State Beach   → sand=not_allowed + bike_path=on_leash
--      fid 8606  Bolsa Chica State Beach  → sand=not_allowed + bike_path=on_leash
--
-- C. SEAL BEACH city (2 fids) — 1 new ps row.
--    Seal Beach Municipal Code §9.05.090 "Vehicles and Animals":
--      "No person having the care, custody, charge, or control of any
--       animal shall permit or allow that animal to be on the pier,
--       city beach or jetty." (Ord. 1515; Ord. 1708, 2/12/2024.)
--    Narrow exceptions: city-run obedience classes / special-event-permit
--    shows; ADA service dogs on leash with ID; city employees on duty.
--    Platform: eCode360 (custid SE5012). Chapter 9.05 BEACHES AND PIERS.
--      § 9.05.090 guid 43955898 → https://ecode360.com/43955898
--
--      fid 8270  Seal Beach (the city's main beach)  → not_allowed
--      fid 6377  Seal Beach Senior Center            → not_allowed
--                  (park_name="Electric Ave Greenbelt"; c1=Seal Beach;
--                   §9.05.090 covers "city beach" — applies)
--
-- D. DEFERS — Surfside Beach (fid 8253) + 2 HOA lake "beaches":
--
--      fid 8253  Surfside Beach
--        c1=Seal Beach polygon but actually Surfside Colony, a private
--        gated HOA community immediately north of Sunset Beach. Coords
--        (33.727, −118.083) place it between Bolsa Chica SB to the south
--        and the USMC Seal Beach Naval Weapons Station to the north.
--        Ambiguous governance: Surfside Colony HOA likely operative on
--        sand; Seal Beach §9.05.090 explicitly scopes to "city beach"
--        (does it include Surfside?). DEFERRED — needs Surfside Colony
--        Community Association CC&R lookup or Seal Beach city-boundary
--        polygon check. Per [[deferred-canyon-lake]] HOA defer precedent.
--
--      fid 6178  North Lake Beach Club  (Orange, HOA Park – Orange County)
--        Coords (33.681, −117.797) — inland lake HOA beach (Lake Forest /
--        Irvine area). Private POA facility, not public access. Per
--        [[operator-not-pseudo-agency]] + [[deferred-canyon-lake]] HOA
--        defer precedent. NOT a coastal/state-beach concern. DEFERRED.
--
--      fid 8637  East Beach             (Orange, HOA Park – Orange County)
--        Coords (33.631, −117.642) — inland lake HOA beach in Mission
--        Viejo (Lake Mission Viejo HOA). c1=Mission Viejo polygon but
--        the operative authority is the Lake Mission Viejo Association.
--        Same defer rationale as fid 6178. DEFERRED.
--
-- Constraints:
--   - All bps inserts use ON CONFLICT DO NOTHING (uniqueness on
--     (beach_fid, policy_source_id, section)).
--   - Every URL is page-level (per [[page-level-over-agency-level]]).
--   - REUSE: ps 144 (Laguna) covers 8 of the 11 linked fids; only 3 new
--     ps rows created (Huntington SB, Bolsa Chica SB, Seal Beach §9.05.090).
--
-- Fetched verbatim 2026-05-17 via Playwright.

-- ─── A. Laguna Beach coves → reuse ps 144 ─────────────────────────

-- A.1. 6 coves with the seasonal on_leash rule.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, 144, 'sand', 'on_leash', 'operative'::operative_status,
       'Laguna Beach Municipal Code §6.16.010 + §6.16.020. Dogs always on leash (≤6 ft, in charge of competent person). SUMMER (Jun 15–Sep 10): dogs prohibited on public beaches 9:00 AM – 6:00 PM. OFF-SEASON (Sep 11–Jun 14): dogs allowed any time on leash. Tot lots / play equipment areas prohibited at all times per §6.16.020(a)(5).',
       'https://ecode360.com/42886466',
       v.note
FROM (values
  (6813, 'Laguna Beach city polygon; seasonal time-of-day restrictions apply (summer: dogs only before 9am or after 6pm).'),
  (7729, 'Laguna Beach city polygon; seasonal time-of-day restrictions apply (summer: dogs only before 9am or after 6pm).'),
  (8314, 'Laguna Beach city polygon; seasonal time-of-day restrictions apply (summer: dogs only before 9am or after 6pm).'),
  (8319, 'Laguna Beach city polygon; seasonal time-of-day restrictions apply (summer: dogs only before 9am or after 6pm).'),
  (8328, 'Geographically inside Laguna Beach city (Irvine Cove is a gated HOA community on the north end of Laguna Beach; c1 polygon NULL but coords confirm). Beach below MHW is public-trust land governed by city ordinance. Seasonal time-of-day restrictions apply.'),
  (3733, 'Polygon-vs-name mismatch: c1_jurisdiction=Laguna Niguel and coords are ~5 km inland of the Laguna Beach Woods Cove area (fid 8321 is the canonical Woods Cove already linked to ps 144). Linking to Laguna Beach city policy per beach name; revisit during beach-coord-validation pass [[beach-coord-validation]].')
) AS v(fid, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- A.2. Thousand Steps Beach (fid 8315) — named no-dogs exception.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES (
  8315, 144, 'sand', 'not_allowed', 'operative'::operative_status,
  'Laguna Beach Municipal Code §6.16.020 + City Council resolution as summarized on lagunabeachcity.net/our-beaches/dogs-on-the-beach: "Thousand Steps Beach: dogs prohibited every day of the year." This is a named exception to the general seasonal-leash rule that applies to other Laguna Beach public beaches.',
  'https://ecode360.com/42886466',
  'Named no-dogs exception in the City''s dogs-on-the-beach summary page; codified via §6.16.020(a)(3) authority delegated to City Council resolution. Verify against current City summary if resolution changes.'
)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── B. CA State Beaches — 2 new ps rows ──────────────────────────

-- B.1. Bolsa Chica State Beach ps row.
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Bolsa Chica State Beach',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=642',
       'California State Parks per-park dogs policy page for Bolsa Chica State Beach (parks.ca.gov/?page_id=642). DOGS IN PARKS: "Are dogs Allowed? Yes: Dogs allowed only on the multiuse trail (bike path). Dogs not allowed on sand." Baseline state-park leash rule applies on permitted areas (max 6-ft leash; never left unattended) per CA Code of Regulations Title 14 §4312 and parks.ca.gov/Dogs catalog. Fetched verbatim via Playwright 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.parks.ca.gov/?page_id=642'
);

-- B.2. Huntington State Beach ps row.
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Huntington State Beach',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=643',
       'California State Parks per-park dogs policy page for Huntington State Beach (parks.ca.gov/?page_id=643). DOGS IN PARKS: "Are dogs Allowed? Yes: Dogs allowed only on the multiuse trail (bike path). Dogs not allowed on sand." Baseline state-park leash rule applies on permitted areas (max 6-ft leash; never left unattended) per CA Code of Regulations Title 14 §4312 and parks.ca.gov/Dogs catalog. Fetched verbatim via Playwright 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.parks.ca.gov/?page_id=643'
);

-- B.3. Bolsa Chica SB → sand=not_allowed + bike_path=on_leash.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8606, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'CA DPR Bolsa Chica State Beach dogs policy: "Dogs not allowed on sand." Bike path access is captured as a separate row.',
       ps.source_url, NULL
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.parks.ca.gov/?page_id=642'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8606, ps.id, 'bike_path', 'on_leash', 'operative'::operative_status,
       'CA DPR Bolsa Chica State Beach dogs policy: "Dogs allowed only on the multiuse trail (bike path)." State-park leash rule applies (max 6-ft leash, never unattended) per 14 CCR §4312.',
       ps.source_url,
       'Bike path / multiuse trail access only — dogs on sand are prohibited (see sand row).'
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.parks.ca.gov/?page_id=642'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- B.4. Huntington SB → sand=not_allowed + bike_path=on_leash.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8453, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'CA DPR Huntington State Beach dogs policy: "Dogs not allowed on sand." Bike path access is captured as a separate row.',
       ps.source_url, NULL
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.parks.ca.gov/?page_id=643'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8453, ps.id, 'bike_path', 'on_leash', 'operative'::operative_status,
       'CA DPR Huntington State Beach dogs policy: "Dogs allowed only on the multiuse trail (bike path)." State-park leash rule applies (max 6-ft leash, never unattended) per 14 CCR §4312.',
       ps.source_url,
       'Bike path / multiuse trail access only — dogs on sand are prohibited (see sand row).'
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.parks.ca.gov/?page_id=643'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── C. Seal Beach city — 1 new ps row + 2 bps rows ───────────────

-- C.1. Seal Beach Municipal Code §9.05.090 ps row.
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Seal Beach Municipal Code §9.05.090 (Vehicles and Animals — Title 9 Ch 9.05 BEACHES AND PIERS)',
       (SELECT id FROM public.agency WHERE name='Seal Beach' AND type='city' AND pip_layer='jurisdictions'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/43955898',
       'Seal Beach Municipal Code Title 9 Ch 9.05 BEACHES AND PIERS, §9.05.090 Vehicles and Animals: A. "No person having the care, custody, charge, or control of any animal shall permit or allow that animal to be on the pier, city beach or jetty. This prohibition shall not be applicable to: 1. Any animal enrolled and participating in obedience classes offered by the community services department of the city or in any show for which the city has issued a special event permit. 2. Any service dog, as defined by the Americans with Disabilities Act (ADA), or any dog being trained to be a service dog as defined by the ADA pursuant to a recognized program of training, provided the dog is on a leash and the person in charge has an official identification card issued for such purposes. 3. City employees and agents engaged in the discharge of official duties." B. (vehicles/horses prohibition on city beach or pier without city manager authorization). (Ord. 1515; Ord. 1708, 2/12/2024.) [Codified ordinance on eCode360 platform (custid SE5012) at ecode360.com/43955898. Chapter root ecode360.com/43955858. City TOC ecode360.com/SE5012. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Seal Beach Municipal Code §9.05.090%'
);

-- C.2. Seal Beach beach fids → not_allowed.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Seal Beach Municipal Code §9.05.090(A): "No person having the care, custody, charge, or control of any animal shall permit or allow that animal to be on the pier, city beach or jetty." Narrow exceptions: city-run obedience classes / special-event-permit shows; ADA service dogs on leash with ID; city employees on official duty.',
       ps.source_url, v.note
FROM (values
  (8270, NULL),
  (6377, 'park_name="Electric Ave Greenbelt" — this fid is a city park-and-beach feature within the Seal Beach city polygon; §9.05.090 covers "city beach" so the prohibition applies. Greenbelt area outside the city-beach footprint would default to Seal Beach §7.05.055 at-large leash rule, not no-dogs.')
) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Seal Beach Municipal Code §9.05.090%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── D. DEFERS — no inserts ───────────────────────────────────────
--
-- fid 8253 Surfside Beach   — DEFERRED (Surfside Colony HOA; ambiguous
--                              vs. Seal Beach city-beach scope of §9.05.090).
-- fid 6178 North Lake Beach Club — DEFERRED (Orange Co HOA lake beach;
--                              [[operator-not-pseudo-agency]] + [[deferred-canyon-lake]]).
-- fid 8637 East Beach       — DEFERRED (Lake Mission Viejo HOA lake beach;
--                              same rationale as fid 6178).
--
-- No URL fetches attempted for any HOA-controlled fid per
-- [[deferred-canyon-lake]] AV-flag precedent.
