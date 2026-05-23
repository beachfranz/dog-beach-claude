-- 20260523_nh_per_park_dogs_policy_ncr_731_08.sql
--
-- Per-unit dog policy rows for named NH state parks, sourced from
-- NCR 731.08 (NH Department of Natural and Cultural Resources
-- administrative rule on Animals at parks and department properties).
--
-- Authoritative source: NH Code of Administrative Rules — Chapter Ncr 730,
-- Part Ncr 731, Section 731.08. Document #14291, effective 2025-06-25,
-- expires 2035-06-25. Issued by NH Department of Natural and Cultural
-- Resources (NCR), the successor to DRED (abolished 2017-07-01 per RSA
-- 12-A:1; 2017, 156:15).
--
-- Per-unit confidence = 0.85 (per-unit tier per
-- _emit_evidence_from_pad_us_dogs_policy). These rows OVERRIDE class-
-- default rows for the listed unit_ids. mng_name is NULL on per-unit rows
-- per the table's UNIQUE(mng_name) constraint (mng_name uniqueness is
-- only enforced for class-default rows where unit_id IS NULL).
--
-- Encoding scheme:
--   - Year-round prohibited parks (Ncr 731.08(g)): default_rule='no',
--     dogs_allowed='no', all area_*='forbidden'.
--   - Coastal beach state parks with summer prohibition + Oct 1 – Apr 30
--     off-season permit (Ncr 731.08(e)(1) + (f)(1)): default_rule='mixed'
--     so the beach STAYS in scoring scope; area_sand/area_water='forbidden'
--     for the operating-season UX. Off-season nuance flagged in
--     prohibited_areas JSON for downstream seasonal-closure handling.
--   - "Permitted only in certain areas" parks (Ncr 731.08(d)): zone-
--     specific flags per the rule's enumerated areas.
--   - "Beach + campground prohibited" parks (Ncr 731.08(e)(14)/(15)):
--     mixed default with explicit area-level prohibitions.
--
-- All NH state parks share these universal terms (Ncr 731.08(l), (m), (j),
-- (i)): 6-foot max leash, waste cleanup required, hunting/SAR dogs may
-- be off-leash under RSA 466:30-a, remote-area off-leash voice control
-- under RSA 466:30-a.

BEGIN;

-- ── Year-round dog-prohibited parks (Ncr 731.08(g)) ───────────────────

-- (g)(1) Monadnock State Park — year-round prohibited
INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground, area_picnic_area,
   area_parking_lot, source_quote, url_used, url_kind,
   ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  (133335, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g): Animals shall be prohibited year-round on the following department properties: (1) At Monadnock state park.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(1)', 'manual_curator', 1.00,
   'Monadnock State Park — dogs prohibited year-round per NH Dept of Natural and Cultural Resources Ncr 731.08(g)(1). Document #14291 eff 2025-06-25.'),
  (133357, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g): Animals shall be prohibited year-round on the following department properties: (1) At Monadnock state park.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(1)', 'manual_curator', 1.00,
   'Monadnock State Park (duplicate poly with mng_name=UNK) — same year-round prohibition.');

-- (g)(2) Odiorne Point State Park — year-round prohibited
INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground, area_picnic_area,
   area_parking_lot, source_quote, url_used, url_kind,
   ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  (133337, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g): Animals shall be prohibited year-round on the following department properties: (2) At Odiorne Point state park.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(2)', 'manual_curator', 1.00,
   'Odiorne Point State Park — dogs prohibited year-round per NH NCR 731.08(g)(2).'),
  (3247, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g)(2) — Odiorne Point state park year-round prohibition.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(2)', 'manual_curator', 1.00,
   'Odiorne Point State Park MPA designation poly (mng_name=SPR) — same year-round prohibition.'),
  (133355, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g)(2) — Odiorne Point state park year-round prohibition.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(2)', 'manual_curator', 1.00,
   'Odiorne Point State Park (DRED-named duplicate) — same year-round prohibition.');

-- (g)(3) Ossipee Lake Natural Area — year-round prohibited
INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground, area_picnic_area,
   area_parking_lot, source_quote, url_used, url_kind,
   ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  (125827, NULL, 'no', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'Ncr 731.08(g): Animals shall be prohibited year-round on the following department properties: (3) At Ossipee Lake natural area.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(3)', 'manual_curator', 1.00,
   'Ossipee Lake Natural Area — dogs prohibited year-round per NH NCR 731.08(g)(3).');

-- (g)(4) Rhododendron State Park — year-round prohibited, except parking
-- and the section of Rhododendron loop providing access to Little
-- Monadnock Mountain trail.
INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground, area_picnic_area,
   area_parking_lot, prohibited_areas, source_quote, url_used,
   url_kind, ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  (125859, NULL, 'mixed', 'no', TRUE,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'on_leash',
   '{"description": "Year-round prohibited except parking area and Rhododendron loop section providing access to Little Monadnock Mountain trail. Per Ncr 731.08(g)(4)."}'::jsonb,
   'Ncr 731.08(g)(4): At Rhododendron state park, except at the parking area and the section of Rhododendron loop which is not posted as prohibited which provides trail access to and on Little Monadnock Mountain trail.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(4)', 'manual_curator', 1.00,
   'Rhododendron State Park — year-round prohibited EXCEPT parking + Little Monadnock Mountain trail access via Rhododendron loop section.'),
  (125860, NULL, 'mixed', 'no', TRUE,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'on_leash',
   '{"description": "Year-round prohibited except parking area and Rhododendron loop section providing access to Little Monadnock Mountain trail. Per Ncr 731.08(g)(4)."}'::jsonb,
   'Ncr 731.08(g)(4) — Rhododendron State Park year-round prohibition with parking/trail-access carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(g)(4)', 'manual_curator', 1.00,
   'Rhododendron State Park (duplicate poly) — same year-round prohibition with parking/trail carve-out.');

-- ── Coastal-beach state parks: summer prohibition + Oct 1 – Apr 30 carve-out ──
-- Ncr 731.08(e)(1) + (f)(1). Operating season: dogs prohibited on the
-- coastal beach. Off-season Oct 1 – Apr 30: dogs permitted on leash.
-- Encoding: default_rule='mixed' (varies by season, beach stays in scoring
-- scope); area_sand/area_water='forbidden' (the operating-season default
-- consumer-facing rule); seasonal nuance flagged in prohibited_areas for
-- downstream seasonal-closure handling.

INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_picnic_area, area_parking_lot,
   prohibited_areas, source_quote, url_used, url_kind,
   ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  -- Hampton Beach State Park (SDNR poly)
  (125845, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash along Ocean Boulevard and at Hampton Beach South per Ncr 731.08(f)(1). Department may revoke off-season permission for non-compliance per (f)(2)."}'::jsonb,
   'Ncr 731.08(e)(1): At state park coastal beaches, including Rye Harbor also known as "Ragged Neck;". (f)(1): In Hampton Beach state park along Ocean Boulevard and at Hampton Beach South ... animals shall be permitted only from October 1 through April 30.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Hampton Beach State Park — coastal beach. Summer (operating season): dogs prohibited. Winter (Oct 1 – Apr 30): on-leash permitted. 6-foot max leash per (l).'),
  -- Hampton Beach State Park (SPR MPA poly)
  (3246, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per (f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1) + (f)(1) — Hampton Beach SP coastal-beach seasonal rules.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Hampton Beach State Park (MPA designation duplicate poly) — same seasonal rule.'),

  -- Rye Harbor State Park / Ragged Neck (SDNR poly)
  (125863, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per Ncr 731.08(f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1): At state park coastal beaches, including Rye Harbor also known as "Ragged Neck;". (f)(1): ... Rye Harbor/Ragged Neck state park ... animals shall be permitted only from October 1 through April 30.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Rye Harbor State Park (Ragged Neck) — coastal-beach seasonal rule.'),
  -- Rye Harbor SP (SPR MPA poly)
  (3248, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per Ncr 731.08(f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1) + (f)(1) — Rye Harbor/Ragged Neck SP coastal-beach seasonal rules.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Rye Harbor State Park (Ragged Neck) MPA duplicate — same seasonal rule.'),

  -- Jenness Beach State Park
  (125846, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per Ncr 731.08(f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1) + (f)(1) — Jenness state beach coastal-beach seasonal rules.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Jenness Beach State Park — coastal-beach seasonal rule.'),

  -- North Hampton State Park
  (133336, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per Ncr 731.08(f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1) + (f)(1) — North Hampton state beach coastal-beach seasonal rules.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'North Hampton State Park — coastal-beach seasonal rule.'),

  -- Wallis Sands State Park
  (133343, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Coastal beach: dogs PROHIBITED during operating season per Ncr 731.08(e)(1). Off-season Oct 1 – Apr 30: dogs permitted on leash per Ncr 731.08(f)(1)."}'::jsonb,
   'Ncr 731.08(e)(1) + (f)(1) — Wallis Sands state beach coastal-beach seasonal rules.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(1), (f)(1)', 'manual_curator', 1.00,
   'Wallis Sands State Park — coastal-beach seasonal rule.');

-- ── Specific named freshwater + park-area prohibitions (Ncr 731.08(e)) ────

INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_picnic_area, area_campground,
   area_parking_lot, prohibited_areas, source_quote, url_used,
   url_kind, ordinance_ref, extraction_model, extraction_confidence, notes)
VALUES
  -- Ellacoya State Park — entire park prohibited during operating season
  (125840, NULL, 'mixed', 'no', NULL,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden', 'forbidden',
   '{"description": "Entire park prohibited during operating season per Ncr 731.08(e)(7). Off-season ((f) default): permitted unless year-round prohibited."}'::jsonb,
   'Ncr 731.08(e)(7): At Ellacoya state park (entire park during operating season).',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(7)', 'manual_curator', 1.00,
   'Ellacoya State Park — entire park dog-prohibited during operating season. Off-season default per (f) is permitted.'),

  -- Echo Lake State Park — beach + picnic + lake perimeter trail prohibited
  (126015, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'forbidden', 'forbidden', NULL, 'on_leash',
   '{"description": "Beach, picnic area, and lake perimeter hiking trail prohibited during operating season per Ncr 731.08(e)(6). Other trails and areas open with leash."}'::jsonb,
   'Ncr 731.08(e)(6): In Echo Lake state park, at the beach, picnic area, or on the lake perimeter hiking trail.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(6)', 'manual_curator', 1.00,
   'Echo Lake State Park — beach + picnic + lake perimeter trail prohibited operating season.'),

  -- Sunapee State Park (Mount Sunapee) — beach prohibited; arts fest period restricts more
  (125866, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash', 'on_leash',
   '{"description": "Beach prohibited operating season per Ncr 731.08(e)(13). Main park and ski area additionally prohibited during annual arts and crafts festival."}'::jsonb,
   'Ncr 731.08(e)(13): In Mount Sunapee state park, at the beach, and at the main park and ski area, during the annual arts and crafts festival.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(13)', 'manual_curator', 1.00,
   'Mount Sunapee State Park — beach prohibited; arts fest period additional restriction.'),
  (125867, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'on_leash', 'on_leash',
   '{"description": "Beach prohibited operating season per Ncr 731.08(e)(13). Main park and ski area additionally prohibited during annual arts and crafts festival."}'::jsonb,
   'Ncr 731.08(e)(13) — Mount Sunapee SP beach and arts-fest restrictions.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(13)', 'manual_curator', 1.00,
   'Mount Sunapee State Park (duplicate poly) — same.'),

  -- Pawtuckaway State Park — beach + campground prohibited; trails OK
  (133338, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'on_leash', 'forbidden', 'on_leash',
   '{"description": "Beach and campground prohibited operating season per Ncr 731.08(e)(14). Hiking trails, picnic, parking OK on leash."}'::jsonb,
   'Ncr 731.08(e)(14): In Pawtuckaway state park, at the beach and at the campground.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(14)', 'manual_curator', 1.00,
   'Pawtuckaway State Park — beach + campground prohibited operating season; trails permitted on leash.'),

  -- White Lake State Park — beach + campground + picnic + facilities prohibited
  (125877, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'forbidden', 'on_leash',
   '{"description": "Beach, campground, picnic areas, beaches, and near park facilities all prohibited operating season per Ncr 731.08(e)(15). Hiking trails away from park facilities OK on leash."}'::jsonb,
   'Ncr 731.08(e)(15): In White Lake state park, at the beach and at the campground, picnic areas, beaches, and near park facilities.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(15)', 'manual_curator', 1.00,
   'White Lake State Park — beach + campground + picnic + near-facility areas all prohibited operating season.'),
  (125878, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'forbidden', 'on_leash',
   '{"description": "Beach, campground, picnic areas, beaches, near park facilities prohibited operating season per Ncr 731.08(e)(15)."}'::jsonb,
   'Ncr 731.08(e)(15) — White Lake SP beach/campground/picnic prohibition.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(e)(15)', 'manual_curator', 1.00,
   'White Lake State Park (duplicate poly) — same.');

-- ── "Permitted only in certain areas" parks (Ncr 731.08(d)) ──────────
-- These parks broadly allow dogs but ONLY in specified areas. Trails +
-- campgrounds are the typical allowed zones; beaches/water features
-- generally not enumerated as allowed → encoded as forbidden.

INSERT INTO public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground, area_picnic_area,
   area_parking_lot, designated_dog_zones, source_quote,
   url_used, url_kind, ordinance_ref, extraction_model,
   extraction_confidence, notes)
VALUES
  -- Bear Brook SP — campground + hiking trails only
  (133331, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["campground", "hiking_trails"], "description": "Operating-season carve-out per Ncr 731.08(d)(2)."}'::jsonb,
   'Ncr 731.08(d)(2): In Bear Brook state park, at the campground and hiking trails only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(2)', 'manual_curator', 1.00,
   'Bear Brook State Park — dogs permitted at campground + hiking trails only during operating season. Apr 1 – Nov 1 campground rule per (d)(1) compounds.'),

  -- Crawford Notch SP — campground + hiking trails + Willey dog walk
  (125837, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["campground", "hiking_trails", "willey_site_designated_dog_walk"], "description": "Operating-season carve-out per Ncr 731.08(d)(3)."}'::jsonb,
   'Ncr 731.08(d)(3): In Crawford Notch state park, at the campground, park hiking trails, and the designated dog walk area at the Willey site only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(3)', 'manual_curator', 1.00,
   'Crawford Notch State Park — campground + hiking trails + Willey site designated dog walk only.'),

  -- Franconia Notch SP — designated dog walk + hiking trails (not ski trails)
  (125842, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', NULL, 'forbidden', 'on_leash',
   '{"allowed_zones": ["designated_dog_walk", "hiking_trails"], "prohibited_zones": ["ski_trails"], "description": "Operating-season carve-out per Ncr 731.08(d)(4). Ski trails explicitly excluded."}'::jsonb,
   'Ncr 731.08(d)(4): In Franconia Notch state park, at the designated dog walk area and on hiking trails only, but not ski trails.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(4)', 'manual_curator', 1.00,
   'Franconia Notch State Park — designated dog walk + hiking trails only; ski trails excluded.'),

  -- Greenfield SP — campground + hiking trails only
  (125844, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["campground", "hiking_trails"], "description": "Operating-season carve-out per Ncr 731.08(d)(5)."}'::jsonb,
   'Ncr 731.08(d)(5): In Greenfield state park, at the campground and on hiking trails only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(5)', 'manual_curator', 1.00,
   'Greenfield State Park — campground + hiking trails only.'),

  -- Moose Brook SP — campground + hiking trails only
  (125851, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["campground", "hiking_trails"], "description": "Operating-season carve-out per Ncr 731.08(d)(6)."}'::jsonb,
   'Ncr 731.08(d)(6): In Moose Brook state park, at the campground and on hiking trails only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(6)', 'manual_curator', 1.00,
   'Moose Brook State Park — campground + hiking trails only.'),

  -- Mount Washington SP — designated areas only
  (125852, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'forbidden', 'forbidden', 'on_leash',
   '{"allowed_zones": ["designated_areas"], "description": "Operating-season carve-out per Ncr 731.08(d)(7) — dogs permitted at designated areas only."}'::jsonb,
   'Ncr 731.08(d)(7): In Mount Washington state park, at designated areas only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(7)', 'manual_curator', 1.00,
   'Mount Washington State Park — designated areas only (rule does not enumerate specific zones).'),

  -- Umbagog SP (5 polys: 125868, 125869, 125870, 125871, 125872)
  -- Base camp + designated remote campsites only
  (125868, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["base_camp", "designated_remote_campsites"], "description": "Operating-season carve-out per Ncr 731.08(d)(8)."}'::jsonb,
   'Ncr 731.08(d)(8): In Umbagog state park, at base camp and designated remote campsites only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(8)', 'manual_curator', 1.00,
   'Umbagog State Park — base camp + designated remote campsites only.'),
  (125869, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["base_camp", "designated_remote_campsites"]}'::jsonb,
   'Ncr 731.08(d)(8) — Umbagog SP carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(8)', 'manual_curator', 1.00,
   'Umbagog State Park (duplicate poly).'),
  (125870, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["base_camp", "designated_remote_campsites"]}'::jsonb,
   'Ncr 731.08(d)(8) — Umbagog SP carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(8)', 'manual_curator', 1.00,
   'Umbagog State Park (duplicate poly).'),
  (125871, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["base_camp", "designated_remote_campsites"]}'::jsonb,
   'Ncr 731.08(d)(8) — Umbagog SP carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(8)', 'manual_curator', 1.00,
   'Umbagog State Park (duplicate poly).'),
  (125872, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', NULL, 'on_leash', 'forbidden', 'on_leash',
   '{"allowed_zones": ["base_camp", "designated_remote_campsites"]}'::jsonb,
   'Ncr 731.08(d)(8) — Umbagog SP carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(8)', 'manual_curator', 1.00,
   'Umbagog State Park (duplicate poly).'),

  -- Wellington SP — hiker parking + west-side trails only
  (125875, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'forbidden', 'on_leash',
   '{"allowed_zones": ["hiker_parking_area", "trails_west_of_west_shore_road"], "description": "Operating-season carve-out per Ncr 731.08(d)(9). Beach + east-side prohibited."}'::jsonb,
   'Ncr 731.08(d)(9): In Wellington state park, at the hiker parking area and trails on the west side of West Shore Road only.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(9)', 'manual_curator', 1.00,
   'Wellington State Park — hiker parking + west-side trails only. Beach prohibited.'),
  (126016, NULL, 'mixed', 'mixed', TRUE,
   'forbidden', 'forbidden', 'on_leash', 'forbidden', 'forbidden', 'on_leash',
   '{"allowed_zones": ["hiker_parking_area", "trails_west_of_west_shore_road"]}'::jsonb,
   'Ncr 731.08(d)(9) — Wellington SP carve-out.',
   'https://www.nhstateparks.org/NHStateParks/media/NHStateParks/PDFs/Admin%20Rules/NCR-Parks-Animals-1-2026.pdf',
   'agncy_web',
   'NCR 731.08(d)(9)', 'manual_curator', 1.00,
   'Wellington State Park (duplicate poly).');

COMMIT;

-- Sanity check (post-apply diagnostic — not executed by migration):
--   SELECT unit_id, ordinance_ref, dogs_allowed, default_rule,
--          area_sand, area_water
--     FROM public.pad_us_unit_dogs_policy
--    WHERE ordinance_ref LIKE 'NCR 731.08%'
--    ORDER BY ordinance_ref, unit_id;
--
-- Expected: 32 rows across 19 unique parks (some parks have duplicate
-- PAD-US polys; each gets its own row).
