-- Long Beach (ps ids 7, 8) — both rows had source_url pointing at
-- §6.16.100 (TIT6AN_CH6.16ANRE_6.16.100DOLERE) which is neither row's
-- cited section. Surfaced by the 2026-05-17 red-flag audit immediately
-- after applying 20260517_municode_url_canonicalize.sql.
--
-- Per [[page-level-over-agency-level]]: each ps row's source_url should
-- deep-link to its OWN cited section.

UPDATE public.policy_source ps
   SET source_url = m.url
  FROM (VALUES
    -- §6.16.310 — Dog exercise area on the beach (Rosie's Dog Beach carve-out)
    (7, 'https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.310DOEXARBE'),
    -- §6.16.090 — Dogs prohibited on beach
    (8, 'https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.090DOPRBESC')
  ) AS m(ps_id, url)
 WHERE ps.id = m.ps_id;
