-- Bucket B — City of Goleta: Ellwood Beach (8946) + Haskell's Beach (8947).
--
-- Per the 2026-05-17 URL-quality audit + Wave-3 followup pass.
--
-- Goleta MC Title 6 ("Animals and Fowl Code") is administrative: §6.01.020
-- adopts Santa Barbara County Code Chapter 7 ("Animals and Fowl") by
-- reference, frozen at the version in effect 2019-02-05 (per Ord. 19-05 §1).
-- The substantive leash rule therefore comes from SB County's Animal Code.
-- SB County's modern equivalent is Title 26 Article III §26-49 (already in
-- this database as policy_source id 93 with deep-linked Municode URL).
--
-- Source URL: qcode.us/codes/goleta/view.php?topic=6-01-020 (section deep
-- link per [[url-resolution-field-guide]] qcode pattern; qcode routes to
-- ecode360 under custid GO4931 — both URLs are stable).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Goleta Municipal Code §6.01.020 (County Ordinance Adopted — adopts Santa Barbara County Animals & Fowl Code Chapter 7 by reference, per Ord. 19-05)',
       (SELECT id FROM public.agency WHERE name='Goleta' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://qcode.us/codes/goleta/view.php?topic=6-01-020',
       'Goleta Municipal Code Title 6 (Animals and Fowl Code), §6.01.010: "This chapter shall be known as the ''Animals and Fowl Code'' of the City and may be cited as such." §6.01.020: "Chapter 7, entitled ''Animals and Fowl,'' of the Santa Barbara County Code, as amended and in effect on February 5, 2019, is adopted by reference as the Animals and Fowl Code of the City." Where this Code and Chapter 7 conflict, this Code prevails (§6.01.030). Certain sections of SB County Chapter 7 (re: licensing fees, etc.) are excluded from the Goleta adoption (§6.01.040). The substantive leash + dog-control rule lives in SB County Code Title 26 Article III §26-49 (modern numbering; was Chapter 7 under the 2019 reference snapshot) — see policy_source id 93 for the verbatim SB County text + deep-link. Fetched verbatim via Playwright 2026-05-17. (Ord. 19-05 §1)'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Goleta Municipal Code §6.01.020%'
);

-- Link both Goleta-coastal beaches to the city's adoption row.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Goleta adopts SB County Animals & Fowl Code by reference (Goleta MC §6.01.020 → SB County Code Chapter 7 as of 2019-02-05). The operative leash rule (6-foot leash required when off owner''s property) is in SB County''s code — see policy_source id 93 for the SB County deep-link + verbatim.',
       ps.source_url,
       'Adoption-by-reference chain: Goleta MC §6.01.020 → SB County Code Chapter 7 (modern §26-49). Walkthrough recommended to verify any city-specific beach signage or carve-outs.'
FROM (values (8946),(8947)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Goleta Municipal Code §6.01.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
