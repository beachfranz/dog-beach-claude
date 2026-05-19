-- Canonical agency row for NOAA (National Oceanic and Atmospheric Administration)
-- Per playbook tenet 4 (canonical bare-name agency).
--
-- Context: The federal coverage script (derive_policy_source_for_jurisdiction.py
-- --list-federal-coverage) flagged that Olympic Coast National Marine Sanctuary
-- (10 WA beaches intersect) has no canonical NOAA agency row in the DB.
-- Creating this row unblocks future references — though Olympic Coast NMS itself
-- does NOT constrain pets on the dry beach (see 2026-05-19 investigation note):
--   - 15 CFR §922.150 sets the OCNMS landward boundary at "mean higher high water
--     line" (federally-managed adjacent) or "mean lower low water line" (adjacent
--     to Indian reservations / State / county lands). Dry beach is OUTSIDE NMS.
--   - 15 CFR §922.152 enumerates prohibitions: oil/gas/minerals, discharges,
--     historical-resources tampering, seabed alteration, taking marine mammals/
--     sea turtles/seabirds, low overflight. NO mention of pets, dogs, or visitor
--     beach use.
--   - Beach pet rules on these 10 beaches come from landward managers: NPS
--     Olympic National Park (Kalaloch, Ruby, Beach 1/2/3, Second Beach),
--     Quinault Indian Nation (Pacific Beach, Seabrook adjacent), Makah Tribe
--     (Shi Shi Beach), etc. — NOT NOAA.
--
-- Agency row created for future NOAA-issued policies (e.g., aquaculture, MPA
-- enforcement, vessel discharge advisories) that might surface in the consensus
-- engine. NOT used to back any beach_policy_source row for OCNMS today.

INSERT INTO public.agency (name, type, short_name, web_url)
SELECT 'National Oceanic and Atmospheric Administration',
       'federal_department'::agency_type,
       'NOAA',
       'https://www.noaa.gov'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
  WHERE name = 'National Oceanic and Atmospheric Administration'
    AND type = 'federal_department'::agency_type
);
