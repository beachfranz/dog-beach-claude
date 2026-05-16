-- Wave 2 follow-up: BLM California pet policy backfill via
-- 43 CFR §8365.2-1 + §8365.2-5.
--
-- Per [[comprehensive-source-accelerator]]: BLM has a federal pet
-- baseline at 43 CFR §8365.2 (Rules for Developed Recreation Sites
-- and Areas):
--   §8365.2-1(c) — 6-foot leash baseline
--   §8365.2-5(b) — no animals in swimming areas (service dogs OK)
--
-- Same federal-pet-baseline structure as USFS §261.16 — different
-- title, same operative effect.
--
-- 4 BLM California beaches in beaches_gold:
--   6042 Centerville Beach County Park (Humboldt)
--   6165 Sea View Beach (Imperial)
--   8910 Mattole Beach (Humboldt)
--   9314 Pit Stop Beach (El Dorado)
--
-- None are designated swim areas → all default to sand=on_leash
-- per §8365.2-1(c).

-- ─── 1. Insert §8365.2 policy_source ─────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'federal_regulation',
       '43 CFR §8365.2-1 + §8365.2-5 (BLM Developed Recreation Sites — Pet Provisions)',
       (SELECT id FROM public.agency WHERE name='United States Bureau of Land Management' AND type='federal_department'),
       ARRAY['dog_policy']::text[],
       'https://www.ecfr.gov/current/title-43/subtitle-B/chapter-II/subchapter-H/part-8360/subpart-8365',
       '§8365.2-1 Sanitation. On developed recreation sites and areas, no person shall, unless otherwise authorized: (c) Bring an animal into such an area unless the animal is on a leash not longer than 6 feet and secured to a fixed object or under control of a person, or is otherwise physically restricted at all times. §8365.2-5 Public health, safety and comfort. On developed recreation sites and areas, unless otherwise authorized, no person shall: (b) Bring an animal, except a Seeing Eye or Hearing Ear dog, to a swimming area. [43 CFR Part 8360 Subpart 8365 — Public Conduct on Public Lands. Federal baseline for all BLM-administered developed-site dog access.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE '43 CFR §8365.2%'
);

-- ─── 2. Link 4 BLM California beaches to §8365.2 ────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE '43 CFR §8365.2%'),
       'sand', 'on_leash', 'operative'::operative_status,
       '43 CFR §8365.2-1(c): "Bring an animal into such an area unless the animal is on a leash not longer than 6 feet and secured to a fixed object or under control of a person, or is otherwise physically restricted at all times." Federal baseline for BLM developed sites.',
       'https://www.ecfr.gov/current/title-43/subtitle-B/chapter-II/subchapter-H/part-8360/subpart-8365',
       'Per-field-office supplementary rules under §8365.1-6 may modify; verify per beach if specific BLM California field office rules differ.'
  FROM (VALUES (6042), (6165), (8910), (9314)) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
