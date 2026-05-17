-- Phase B: Del Norte County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer:
--   https://agendas.dnco.org/450966/450979/450980/452243/RevisedAnimalControlOrdinanceCountyCounselAnimalControl452753.pdf
--
-- Source is a Del Norte County Board agenda PDF — "Revised Animal Control
-- Ordinance" presented by County Counsel. The PDF is image-only (no text
-- layer), so Franz hand-transcribed Title 8 / Chapter 02 / Section 80
-- (Leash Law) for ingest. Treating as the current operative codified text
-- (Del Norte County publishes its code only via board ordinances, not via
-- a third-party codifier — Phase A discovery found no Municode/amlegal/
-- qcode/ecode360/county.codes match for Del Norte).
--
-- Code structure (as transcribed):
--   Title 8: Animals
--     Chapter 02: Dogs and other Pets
--       Section 80: Leash Law
--
-- Operative rule (Title 8 Ch 02 §80 Leash Law, verbatim):
--   A. It is unlawful for the owner or keeper of a dog to allow such dog
--   to be unleashed or unattended in any public area not designated for
--   unleashed dogs, or on any public road, or any private road to which
--   the dog owner does not have a right of possession.
--   B. It is unlawful for the owner or keeper of a dog to allow such dog
--   to trespass on private property.
--   C. Any dog found to be unleashed in violation of this section is
--   subject to immediate seizure and impoundment.
--   D. A dog that has strayed from but then returned to the private
--   property of his owner or keeper shall not be seized or impounded, but
--   a citation may be issued. If the owner or keeper is not home the dog
--   may be impounded a notice posted pursuant to Government Code Section
--   53074.
--
-- 11 Del Norte County beaches in beaches_gold (coastal — far-north CA
-- coast, Crescent City environs).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Del Norte County Code Title 8 / Ch 02 / §80 (Leash Law)',
       (SELECT id FROM public.agency WHERE name='Del Norte County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://agendas.dnco.org/450966/450979/450980/452243/RevisedAnimalControlOrdinanceCountyCounselAnimalControl452753.pdf',
       'Del Norte County Code Title 8 (Animals), Chapter 02 (Dogs and other Pets), Section 80 Leash Law. A. It is unlawful for the owner or keeper of a dog to allow such dog to be unleashed or unattended in any public area not designated for unleashed dogs, or on any public road, or any private road to which the dog owner does not have a right of possession. B. It is unlawful for the owner or keeper of a dog to allow such dog to trespass on private property. C. Any dog found to be unleashed in violation of this section is subject to immediate seizure and impoundment. D. A dog that has strayed from but then returned to the private property of his owner or keeper shall not be seized or impounded, but a citation may be issued. If the owner or keeper is not home the dog may be impounded a notice posted pursuant to Government Code Section 53074. [Source: Del Norte County Board agenda PDF "Revised Animal Control Ordinance" (County Counsel item). PDF is image-only — text hand-transcribed by Franz 2026-05-16. Del Norte County has no third-party codifier; live code lives only via board ordinances. Live current-code URL TBD.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Del Norte County Code Title 8%§80%'
);

-- ─── 2. Link Del Norte County beaches ───────────────────────────

WITH dn_id AS (
  SELECT id FROM public.agency WHERE name='Del Norte County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Del Norte'
    AND (ba_dp.agency_id = (SELECT id FROM dn_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Del Norte County Code Title 8 Ch 02 §80(A): unlawful to allow a dog to be unleashed or unattended in any public area not designated for unleashed dogs. Subsection (C): unleashed dog subject to immediate seizure and impoundment.',
       ps.source_url,
       'Source is board-agenda PDF (image-only); text hand-transcribed by Franz 2026-05-16. Live current-code URL TBD.'
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Del Norte County Code Title 8%§80%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
