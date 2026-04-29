-- Reattribute six misattributed state beaches to CDPR (operator_id 551).
--
-- All six surfaced in yesterday's LIKELY_OUR_ERROR_no triage. Each has
-- a "State Beach"-class name that semantically belongs to CDPR but
-- was attributed to a city, county, or CDFW operator from earlier
-- spatial joins (the UBP/CCC point coordinates fell outside the CPAD
-- polygon for the corresponding state beach, so the spatial-attribute
-- step picked the nearest non-CDPR polygon instead).
--
--   Bean Hollow State Beach      San Mateo County (502)        → CDPR
--   Huntington State Beach       City of Huntington Beach (158) → CDPR
--   Mavericks Beach              San Mateo County (502)        → CDPR
--   Montara State Beach          San Mateo County (502)        → CDPR
--   San Onofre State Beach       San Diego County (515)        → CDPR
--   Crescent Beach (Crescent City)  CDFW (552)                  → CDPR
--
-- For each name, ALL ccc_access_points and us_beach_points rows are
-- normalized to operator_id 551, regardless of prior value. Partner
-- rows in the other table get updated too (some Huntington/Montara
-- variants were already at 551; this just ensures all match).
--
-- Pattern matches the SF→NPS reattribution from earlier in the
-- session: per-beach FK update on ccc + us_beach_points only.
-- compute_dogs_verdict_by_origin gets called via the standard
-- recompute_all_dogs_verdicts_by_origin batch job afterward.

begin;

-- CCC access points
update public.ccc_access_points
   set operator_id = 551
 where (archived is null or archived <> 'Yes')
   and name in (
     'Bean Hollow State Beach',
     'Huntington State Beach',
     'Mavericks Beach',
     'Montara State Beach',
     'San Onofre State Beach',
     'San Onofre State Beach ',  -- trailing-space variant in our data
     'Crescent Beach',
     'Crescent Beach (Crescent City)'
   );

-- UBP partners
update public.us_beach_points
   set operator_id = 551
 where name in (
     'Bean Hollow State Beach',
     'Huntington State Beach',
     'Mavericks Beach',
     'Montara State Beach',
     'San Onofre State Beach',
     'San Onofre State Beach ',
     'Crescent Beach',
     'Crescent Beach (Crescent City)'
   );

commit;
