-- Wave 2 follow-up: USFS California pet policy backfill.
--
-- Per [[comprehensive-source-accelerator]]: USFS's federal rule
-- 36 CFR §261.16 (Developed Recreation Sites) is the comprehensive
-- baseline covering ALL USFS-operated beaches/recreation sites.
-- Two key provisions:
--   (j) "Bringing in or possessing an animal, other than a service
--        animal, unless it is crated, caged, or upon a leash not
--        longer than six feet, or otherwise under physical
--        restrictive control."  → 6-foot leash baseline
--   (k) "Bringing in or possessing in a swimming area an animal,
--        other than a service animal."  → no dogs in swim areas
--
-- 22 USFS beaches in beaches_gold spanning 7 national forests:
--   Lassen NF, Angeles NF, Sierra NF, Inyo NF, Los Padres NF,
--   San Bernardino NF, Klamath NF. (Plus 9 more not enumerated above.)

-- ─── 1. Insert §261.16 policy_source ─────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'federal_regulation',
       '36 CFR §261.16 (Developed Recreation Sites — Pet Provisions)',
       (SELECT id FROM public.agency WHERE name='United States Forest Service' AND type='federal_department'),
       ARRAY['dog_policy']::text[],
       'https://www.ecfr.gov/current/title-36/chapter-II/part-261/subpart-A/section-261.16',
       '(j) Bringing in or possessing an animal, other than a service animal, unless it is crated, caged, or upon a leash not longer than six feet, or otherwise under physical restrictive control. (k) Bringing in or possessing in a swimming area an animal, other than a service animal. [36 CFR Part 261 — Prohibitions, §261.16 Developed Recreation Sites. Federal baseline for all USFS developed-site dog access.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE '36 CFR §261.16%'
);

-- ─── 2. Link all 22 USFS beaches to §261.16 ──────────────────────
-- Default rule: on_leash at section='sand' per §261.16(j).
-- "Swim Beach" (9150) is named for and operates as a designated swim
-- area, so §261.16(k) overrides → sand=not_allowed.

WITH usfs_id AS (
  SELECT id FROM public.agency WHERE name='United States Forest Service' AND type='federal_department'
), usfs_beaches AS (
  SELECT DISTINCT g.fid, g.name
  FROM public.beaches_gold g
  LEFT JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
  LEFT JOIN public.beach_agency ba ON ba.beach_fid = g.fid
  WHERE g.state='CA' AND g.is_active=true
    AND (c.agncy_name='United States Forest Service'
         OR ba.agency_id=(SELECT id FROM usfs_id))
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT u.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE '36 CFR §261.16%'),
       'sand',
       CASE WHEN u.name ILIKE 'Swim Beach%' THEN 'not_allowed' ELSE 'on_leash' END,
       'operative'::operative_status,
       CASE
         WHEN u.name ILIKE 'Swim Beach%'
           THEN '36 CFR §261.16(k) prohibits animals (other than service animals) in swimming areas. "Swim Beach" is a named/designated swim area, so the §261.16(k) override applies.'
         ELSE
           '36 CFR §261.16(j) requires dogs to be on a leash not longer than six feet or otherwise physically restrained at USFS developed recreation sites.'
       END,
       'https://www.ecfr.gov/current/title-36/chapter-II/part-261/subpart-A/section-261.16',
       CASE
         WHEN u.name ILIKE 'Swim Beach%'
           THEN 'Designated swim area → §261.16(k) overrides §261.16(j).'
         ELSE
           'Per-forest variations may apply via individual forest orders; verify per beach if specific rules differ from federal baseline.'
       END
FROM usfs_beaches u
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
