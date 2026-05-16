-- LBMC §6.16.310 — Dog exercise area on the beach — resolved
--
-- Phase 2/5b walkthrough seeds left LBMC sections as "(section TBD)"
-- because Municode SPA navigation was thought to be bot-protected.
-- Franz pointed at https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.100DOLERE
-- 2026-05-16, revealing that the doc slug is `municipal_code` (not
-- `code_of_ordinances`) and the `.codes-chunks-pg` selector renders
-- the chapter cleanly.
--
-- Confirmed sections (full text fetched via Municode):
--   §6.16.090 — Dogs prohibited on beaches or schoolgrounds (baseline)
--   §6.16.100 — Dog leash required (6 ft leash)
--   §6.16.310 — Dog exercise area on the beach (the Dog Beach Zone
--               authorizing statute that §6.16.090 references as
--               "Except as provided in Section 6.16.310")
--
-- This migration updates the citations + full_text on the existing
-- policy_source rows and tightens Rosie's beach_policy_source
-- evidence_verbatim with the actual statute text.

UPDATE public.policy_source
   SET citation = 'Long Beach Municipal Code §6.16.310 (Dog exercise area on the beach)',
       source_url = 'https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.100DOLERE',
       last_verified = NOW()
 WHERE id = 7;

UPDATE public.policy_source
   SET citation = 'Long Beach Municipal Code §6.16.090 (Dogs prohibited on beaches or schoolgrounds) + §6.16.100 (Dog leash required)',
       source_url = 'https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.100DOLERE',
       last_verified = NOW()
 WHERE id = 8;

-- Update Rosie's beach_policy_source row with the actual statute text
UPDATE public.beach_policy_source
   SET evidence_verbatim = 'LBMC §6.16.310: Notwithstanding any other ordinance or rules of the City of Long Beach, dogs may be permitted, during the below-mentioned times of day, on that part of the beach of the City of Long Beach bounded between Granada Avenue (the eastern boundary) and Roycroft Avenue (the western boundary) from the water line to the designated boundary markers located approximately sixty (60) yards from the water line (the northern boundary) to be designated by appropriate posting by the Department of Parks, Recreation and Marine. Dogs are permitted on this designated part of the beach for the purpose of exercise. The hours that dogs may be so on the beach shall be set at the discretion of the Director of the Department of Parks, Recreation and Marine. [Each dog must be under voice control; one dog per adult; full conditions in subsections A-T. ORD-15-0009 / ORD-09-0022.]',
       evidence_url = 'https://library.municode.com/ca/long_beach/codes/municipal_code?nodeId=TIT6AN_CH6.16ANRE_6.16.100DOLERE',
       status_note = 'Resolved 2026-05-16: LBMC §6.16.310 confirmed. Voice-control + one-dog-per-adult required. §6.16.090 baseline prohibits dogs on city beaches except as provided in §6.16.310.',
       last_verified = NOW()
 WHERE beach_fid = 6411 AND policy_source_id = 7;
