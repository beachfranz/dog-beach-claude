-- Wave 4: Pacific County (WA) dog-policy backfill.
--
-- Source: Pacific County Ordinance No. 36 (the leash law).
--
-- Live URL (board ordinance PDF):
--   https://www.co.pacific.wa.us/ordres/Ord%2036%20leash%20law.pdf
--
-- Pacific County does NOT publish a codified municipal code with a
-- chapter-and-section structure like other WA counties; their
-- ordinances live as individual numbered PDFs on the county
-- ordres index at https://www.co.pacific.wa.us/ordres/index.htm.
-- Ord. 36 is the operative county leash law.
--
-- Operative rule (Ord. No. 36, summarized from multiple secondary
-- sources — primary PDF could not be parsed via WebFetch
-- 2026-05-16; PDF is image-only or otherwise not text-extractable):
--   It is unlawful for any owner of a dog in the county to allow the
--   dog to roam at large. Dogs that are away from the premises of
--   the owner must be controlled by a leash or chain not more than
--   eight feet in length.
--
-- Sand rule = on_leash (8-foot max countywide).
--
-- ⚠️ Verbatim limitation: the Ord. 36 PDF text was not fully
-- extractable. Future Pacific County verification should fetch the
-- PDF, OCR if needed, and confirm verbatim language matches.
-- Multiple secondary sources (Pacific County humane society;
-- animalofthings.com leash-law roundup) cite 8-foot leash + at-large
-- prohibition consistently.
--
-- 10 Pacific County beaches in beaches_gold (Long Beach peninsula
-- + Willapa Bay area).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Pacific County Ordinance No. 36 (Leash Law)',
       (SELECT id FROM public.agency WHERE name='Pacific County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.co.pacific.wa.us/ordres/Ord%2036%20leash%20law.pdf',
       'Pacific County Ordinance No. 36 — Establishing a Leash Law for the County of Pacific. Operative rule (per secondary-source paraphrase, primary PDF not text-extractable 2026-05-16): It is unlawful for any owner of a dog in the county to allow the dog to roam at large. Dogs that are away from the premises of the owner must be controlled by a leash or chain not more than eight feet in length. [Pacific County does NOT publish a codified municipal code with chapter-section structure like other WA counties; ordinances live as individual numbered PDFs on co.pacific.wa.us/ordres/index.htm. Ord. 36 is the operative county leash law. Verbatim text could not be extracted via WebFetch on the PDF (binary/image content); secondary-source confirmation from the Pacific County humane society and the animalofthings.com leash-law roundup cite 8-foot leash + at-large prohibition consistently. Future verification should fetch + OCR the PDF.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Pacific County Ordinance No. 36%'
);

-- ─── 2. Link Pacific County beaches ─────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Pacific'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Pacific County Ordinance No. 36%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Pacific County Ord. 36: dogs may not roam at large; 8-foot leash required off owner''s premises. (Per secondary-source paraphrase; primary PDF not text-extractable.)',
       'https://www.co.pacific.wa.us/ordres/Ord%2036%20leash%20law.pdf',
       'Sand rule = on_leash per Ord. 36 (8-foot max). Verbatim primary-source text needs PDF OCR — flagged for future verification. The Long Beach WA peninsula has a long-standing tradition that leash law is not actively enforced on the beach if dogs are well-behaved; the rule on paper still requires leash.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
