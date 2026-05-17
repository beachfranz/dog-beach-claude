-- Canonicalize Municode + leginfo + ecode360 policy_source URLs to
-- page-level deep links.
--
-- Per the 2026-05-17 URL-quality audit (see docs/wave3_pipeline_integration_design.md
-- and the page-level-over-agency-level memory):
--
--   * 19 CA county ps rows from the Wave 2 remaining_municode_counties batch
--     had source_url pointing at the Municode root for the county
--     (library.municode.com/ca/<county>/codes/code_of_ordinances) instead
--     of the per-chapter ?nodeId=... deep link for the animal-regulation
--     chapter.
--   * 4 single-row ps entries (3, 4, 5, 6) point at code-system TOC roots
--     instead of the section/chapter-level deep link the citation names.
--
-- This migration replaces each row's source_url with the resolved
-- per-chapter (Municode) or per-section (leginfo/ecode360) URL.
--
-- nodeIds were resolved by fetching each county's Municode root with
-- Playwright and extracting the animal-regulation chapter's nodeId; for
-- the four counties whose animal chapter is nested under a parent title
-- (San Joaquin under Title 4 Public Safety, Plumas + Siskiyou under
-- Sanitation/Health) the resolved nodeId is the full nested path.
-- LA County's Title 17 is actually "Parks, Beaches and Other Public Areas"
-- (TIT17PABEOTPUAR), not "Beaches and Harbors" — citation also corrected.
--
-- Sutter and Yuba counties (ps ids 106, 108) DEFERRED: their Municode
-- code roots render no title-level nodeIds even with a 25s wait + all
-- doc-slug variants tried (code_of_ordinances, municipal_code, code).
-- The Wave 2 chapter label "Animal Regulation Chapter" was a placeholder.
-- Leaving those two rows at the catalog URL with a status_note update.

UPDATE public.policy_source ps
   SET source_url = m.url
  FROM (VALUES
    -- Wave 2 Municode counties (18 of 19 resolved)
    ( 97, 'https://library.municode.com/ca/nevada_county/codes/code_of_ordinances?nodeId=COOR_TIT8ANRE'),
    ( 98, 'https://library.municode.com/ca/san_joaquin_county/codes/code_of_ordinances?nodeId=TIT4PUSA_DIV9ANSE'),
    ( 99, 'https://library.municode.com/ca/el_dorado_county/codes/code_of_ordinances?nodeId=PTAGECOOR_TIT6AN'),
    (100, 'https://library.municode.com/ca/mono_county/codes/code_of_ordinances?nodeId=TIT9AN'),
    (101, 'https://library.municode.com/ca/madera_county/codes/code_of_ordinances?nodeId=TIT6ANAG'),
    (102, 'https://library.municode.com/ca/siskiyou_county/codes/code_of_ordinances?nodeId=TIT5SAHE_CH3ANCO'),
    (103, 'https://library.municode.com/ca/shasta_county/codes/code?nodeId=TIT6AN'),
    (104, 'https://library.municode.com/ca/imperial_county/codes/code_of_ordinances?nodeId=TIT6AN'),
    (105, 'https://library.municode.com/ca/butte_county/codes/code_of_ordinances?nodeId=CH4AN'),
    (107, 'https://library.municode.com/ca/calaveras_county/codes/code_of_ordinances?nodeId=COOR_TIT6AN'),
    (109, 'https://library.municode.com/ca/riverside_county/codes/code_of_ordinances?nodeId=TIT6AN'),
    (110, 'https://library.municode.com/ca/lake_county/codes/code_of_ordinances?nodeId=COOR_CH4ANFIFO'),
    (111, 'https://library.municode.com/ca/plumas_county/codes/code_of_ordinances?nodeId=TIT6SAHE_CH1AN'),
    (112, 'https://library.municode.com/ca/fresno_county/codes/code_of_ordinances?nodeId=TIT9AN'),
    (113, 'https://library.municode.com/ca/napa_county/codes/code_of_ordinances?nodeId=TIT6AN'),
    (114, 'https://library.municode.com/ca/alameda_county/codes/code_of_ordinances?nodeId=TIT5AN'),
    (115, 'https://library.municode.com/ca/san_mateo_county/codes/code_of_ordinances?nodeId=TIT6AN'),
    -- LA County Title 17 (Parks, Beaches and Other Public Areas)
    (  6, 'https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances?nodeId=TIT17PABEOTPUAR'),
    -- leginfo section deep links (trailing dot required by leginfo parser)
    (  3, 'https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=6001.'),
    (  4, 'https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=HSC&sectionNum=115880.'),
    -- HB §13.08.070: section-level guid (43799445), not just the chapter (43799422)
    (  5, 'https://ecode360.com/43799445')
  ) AS m(ps_id, url)
 WHERE ps.id = m.ps_id;

-- LA County Title 17 was mis-cited as "Beaches and Harbors" (the department
-- name) — the actual code title is "Parks, Beaches and Other Public Areas".
UPDATE public.policy_source
   SET citation = 'LA County Code Title 17 (Parks, Beaches and Other Public Areas)'
 WHERE id = 6
   AND citation = 'LA County Code Title 17 (Beaches and Harbors)';

-- Sutter (106) and Yuba (108): Municode TOC for both renders no title-level
-- nodeIds even with a 25s wait or alternate doc_slugs. Add a status_note
-- so this gap is documented in the data, not just the audit log.
UPDATE public.beach_policy_source bps
   SET status_note = COALESCE(status_note, '') ||
       CASE WHEN status_note IS NULL OR status_note = '' THEN '' ELSE ' // ' END ||
       'Municode TOC did not expose a per-chapter nodeId for this county; catalog URL retained (2026-05-17 audit).'
 WHERE bps.policy_source_id IN (106, 108)
   AND (status_note IS NULL OR status_note NOT LIKE '%Municode TOC did not expose%');
