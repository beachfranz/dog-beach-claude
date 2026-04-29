-- Re-attribute 4 GGNRA / Maritime NHP beaches from City of San Francisco
-- (operator_id 335) to United States National Park Service (1683).
--
-- Identified during dry-run of the missing-policy operator extraction:
-- the picker for "City of San Francisco" landed on nps.gov's Ocean Beach
-- page, correctly recognizing that SF's coastal beaches are GGNRA-managed.
-- Rather than write NPS policy under SF's operator row (semantically
-- wrong), point the affected beaches at NPS directly. NPS already has
-- extracted policy (default_rule='restricted'), so the verdict cascade
-- will pick that up via operator_default.
--
-- Affected beaches (CCC objectid → name):
--   414  Kirby Cove                  — GGNRA (Marin Headlands)
--   418  San Francisco Maritime NHP  — NPS
--   419  SF Maritime NHP Visitor Ctr — NPS
--   452  Phillip Burton Memorial     — Point Reyes (NPS), San Mateo county
--
-- Two of them (414, 452) have UBP partners in beach_locations — update
-- us_beach_points.operator_id too so the 805 view reflects the change.
--
-- The 4 remaining SF (335) beaches stay put — Exploratorium, Fisherman's
-- Wharf, SF Marina, South Beach Harbor are Port of SF / city, not NPS.

begin;

update public.ccc_access_points
   set operator_id = 1683
 where objectid in (414, 418, 419, 452);

update public.us_beach_points
   set operator_id = 1683
 where fid in (12743258, 14930948);

-- Recompute verdicts for the 4 reattributed CCC points.
select public.compute_dogs_verdict(414);
select public.compute_dogs_verdict(418);
select public.compute_dogs_verdict(419);
select public.compute_dogs_verdict(452);

commit;
