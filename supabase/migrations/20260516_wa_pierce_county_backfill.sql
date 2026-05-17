-- Wave 4: Pierce County (WA) parks dog-policy backfill.
--
-- Source: Pierce County Code §14.08.070 (Animals) — Title 14 /
-- Chapter 14.08 Park Code.
--
-- Live URL: https://pierce.county.codes/PCC/14.08.070
-- (Hosted on county.codes / General Code platform.)
--
-- Operative rule (§14.08.070, verbatim, multi-subsection):
--   A. Animal Control. Any animal in the park system is subject to
--      Title 6 PCC, Animals. Animals must always be under the
--      physical control of the owner, except in a designated
--      off-leash area. The following additional requirements apply:
--      1. Dogs, pets, or domestic animals shall be kept on a leash
--         no longer than 8 feet or confined and under control at all
--         times, except within a designated off-leash area, where
--         subsection B of this Section shall apply.
--      2. It is unlawful for a person accompanied by an animal in
--         the park system to fail to provide for the removal of
--         their animal's fecal matter.
--      3. Animals are restricted from any designated swimming beach,
--         golf course, athletic fields at Sprinker Recreation Center
--         or Heritage Recreation Center, public buildings, and other
--         areas deemed inappropriate for animals. Users shall follow
--         posted signs.
--   B. Designated Off-Leash Areas. Areas for off-leash dogs are
--      provided in some parks, which allow dogs, under verbal/voice
--      control of their handlers, to be off-leash within the
--      designated confines of the off-leash park.
--
-- Sand rule = on_leash (8-foot leash max).
-- Designated swimming beaches = not_allowed under §14.08.070.A.3;
-- per-beach refinement deferred.
--
-- 33 Pierce County beaches in beaches_gold (Tacoma area + Anderson/
-- Fox/McNeil/Vashon islands' Pierce side + Key Peninsula).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Pierce County Code §14.08.070 (Animals) — Title 14 / Chapter 14.08 Park Code',
       (SELECT id FROM public.agency WHERE name='Pierce County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://pierce.county.codes/PCC/14.08.070',
       'Pierce County Code §14.08.070 Animals. A. Animal Control. Any animal in the park system is subject to Title 6 PCC, Animals. Animals must always be under the physical control of the owner, except in a designated off-leash area. 1. Dogs, pets, or domestic animals shall be kept on a leash no longer than 8 feet or confined and under control at all times, except within a designated off-leash area, where subsection B of this Section shall apply. 2. It is unlawful for a person accompanied by an animal in the park system to fail to provide for the removal of their animal''s fecal matter. 3. Animals are restricted from any designated swimming beach, golf course, athletic fields at Sprinker Recreation Center or Heritage Recreation Center, public buildings, and other areas deemed inappropriate for animals. Users shall follow posted signs. B. Designated Off-Leash Areas. Areas for off-leash dogs are provided in some parks, which allow dogs, under verbal/voice control of their handlers, to be off-leash within the designated confines of the off-leash park. All other provisions of this Title shall apply within off-leash areas. [Hosted on county.codes (General Code). Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Pierce County Code §14.08.070%'
);

-- ─── 2. Link Pierce County beaches ──────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Pierce'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Pierce County Code §14.08.070%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Pierce County Code §14.08.070.A.1: dogs/pets must be kept on a leash no longer than 8 feet or confined and under control at all times in any PC park, except in designated off-leash areas. §14.08.070.A.3 prohibits animals from designated swimming beaches.',
       'https://pierce.county.codes/PCC/14.08.070',
       'Sand rule defaults to on-leash per §14.08.070.A.1 (8-foot max). Designated swim portions = not_allowed under §14.08.070.A.3; per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
