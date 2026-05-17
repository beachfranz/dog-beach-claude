-- Wave 4: Thurston County (WA) dog-policy backfill.
--
-- Source: Thurston County Code §9.10.050(C) (Unleashed pet animal on
-- public property) — Title 9 / Chapter 9.10 Animal Control.
--
-- Live URL: https://library.municode.com/wa/thurston_county/codes/code_of_ordinances?nodeId=TIT9AN_CH9.10ANCO
--
-- Operative rule (§9.10.050(C), verbatim via Playwright 2026-05-16):
--   Unleashed Pet Animal on Public Property. Such person's pet animal
--   is on public property such as a public park, beach or school
--   ground and not on a leash held by a person who is able to maintain
--   physical control of the animal, or is in violation of additional
--   specific restrictions which have been posted. Such restrictions
--   shall not apply to cats, guide dogs for the visually impaired,
--   service animals for the physically handicapped where being off
--   leash is necessary to the service, or to dogs on fenced areas of
--   public property specifically designated as areas for dogs without
--   the requirement of a leash. A first violation of unleashed pet
--   animals on public property is a Class 4 infraction. A second and
--   any subsequent violation shall be a Class 3 infraction.
--
-- "Restrained" per §9.10.030 = secured by a leash or lead and under
-- physical control of a person with the strength and judgment to
-- handle the animal.
--
-- §9.10.050(A) also separately criminalizes "Pet Animal at Large" in
-- unincorporated areas using the §9.10.030 "at large" definition
-- (off premises and not under control of a person by means of a
-- leash, carrier or demonstrated voice command).
--
-- Sand rule = on_leash. §9.10.050(C) explicitly names "public park,
-- beach or school ground" and requires a leash held by a person
-- maintaining physical control.
--
-- 5 Thurston County beaches in beaches_gold (south Puget Sound +
-- Capitol Lake / Budd Inlet area).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Thurston County Code §9.10.050(C) (Unleashed pet animal on public property) — Title 9 / Chapter 9.10 Animal Control',
       (SELECT id FROM public.agency WHERE name='Thurston County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/wa/thurston_county/codes/code_of_ordinances?nodeId=TIT9AN_CH9.10ANCO',
       'Thurston County Code §9.10.050(C) Unleashed Pet Animal on Public Property. Such person''s pet animal is on public property such as a public park, beach or school ground and not on a leash held by a person who is able to maintain physical control of the animal, or is in violation of additional specific restrictions which have been posted. Such restrictions shall not apply to cats, guide dogs for the visually impaired, service animals for the physically handicapped where being off leash is necessary to the service, or to dogs on fenced areas of public property specifically designated as areas for dogs without the requirement of a leash. A first violation of unleashed pet animals on public property is a Class 4 infraction. A second and any subsequent violation shall be a Class 3 infraction. §9.10.050(A) also criminalizes "Pet Animal at Large" in unincorporated Thurston County. §9.10.030 "Restrained" means secured by a leash or lead and under physical control of a person with the strength and judgment to handle the animal. [Beach explicitly named in §9.10.050(C). Fetched via Playwright/Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Thurston County Code §9.10.050(C)%'
);

-- ─── 2. Link Thurston County beaches ────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Thurston'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Thurston County Code §9.10.050(C)%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Thurston County Code §9.10.050(C): pet animal on public property "such as a public park, beach or school ground" must be on a leash held by a person maintaining physical control. Beach explicitly named.',
       'https://library.municode.com/wa/thurston_county/codes/code_of_ordinances?nodeId=TIT9AN_CH9.10ANCO',
       'Sand rule = on_leash per §9.10.050(C). Off-leash only at fenced areas specifically designated for dogs without leash.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
