-- Phase B: San Luis Obispo County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer (PDF mirrored on the City of SLO
-- escribemeetings portal but identified by document filename as the SLO
-- COUNTY code Title 9):
--   https://pub-slocity.escribemeetings.com/filestream.ashx?DocumentId=15218
--   filename: "E - San Luis Obispo County Code Title 9.pdf"
--   25 pages, 346KB, Supp. No. 16 Update 2, Ord. No. 3498 11-7-23
--
-- Live platform for the SLO County code not yet resolved during Phase A
-- (not on Municode/amlegal/qcode/ecode360/county.codes). Future Wave 3
-- work can confirm and update source_url.
--
-- Operative rule (§9.03.005 Leash law, verbatim):
--   (a) No person shall permit any dog owned, harbored, or controlled by
--   him to be in or upon any public roadway, walkway, park or other public
--   area unless securely restrained by a leash and under the direct control
--   of a person competent to exercise full care, custody, and charge over
--   such dog, or unless the dog is at a "heel" beside a person and obedient
--   to that person's command.
--   (c) The provisions of this section shall not be applicable: (1) to dogs
--   herding livestock or hunting; (2) within the boundaries of any park or
--   other public area specifically designated and authorized by the
--   controlling agency as an off-leash recreational area for dogs; or
--   (3) registered service / search-and-rescue / law-enforcement dogs.
--
-- §9.01.003(f) "At large" definition (verbatim):
--   "being upon public property or private property which is open to the
--   public while unrestrained by a leash."
--
-- §9.01.003(s) "Heel" definition: "for a dog to walk with its head or body
-- remaining parallel and directly adjacent to the legs of its handler."
--
-- §9.02.001 Animals at large: prohibits non-household-pet animals from
-- running at large on public street/place; (b) household pets are
-- "otherwise restricted" by this title (i.e., the leash mandate in 9.03.005).
--
-- Rule choice: `on_leash` per dominant text. The §9.03.005(a) "heel" clause
-- is a narrow alternative that requires the dog to remain *directly
-- adjacent* and *obedient*; doesn't justify a general voice-control
-- characterization on beaches. §9.03.005(c)(2) acknowledges off-leash zones
-- exist only where "specifically designated and authorized."
--
-- 19 SLO County beaches in beaches_gold (coastal — Pismo, Avila, Morro Bay
-- environs plus county-jurisdiction stretches).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'San Luis Obispo County Code §9.03.005 (Leash law) + §9.01.003(f) (At large definition) + §9.02.001 (Animals at large) — Title 9',
       (SELECT id FROM public.agency WHERE name='San Luis Obispo County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://pub-slocity.escribemeetings.com/filestream.ashx?DocumentId=15218',
       'San Luis Obispo County Code Title 9 (Animals). §9.03.005 Leash law: (a) No person shall permit any dog owned, harbored, or controlled by him to be in or upon any public roadway, walkway, park or other public area unless securely restrained by a leash and under the direct control of a person competent to exercise full care, custody, and charge over such dog, or unless the dog is at a "heel" beside a person and obedient to that person''s command. (c) Exceptions: (1) herding/hunting dogs obedient to command; (2) within boundaries of any park or other public area specifically designated and authorized by the controlling agency as an off-leash recreational area for dogs; (3) registered service/SAR/LE dogs. §9.01.003(f) "At large" means being upon any private property while unrestrained by a leash and without permission, or being upon public property or private property open to the public while unrestrained by a leash. §9.01.003(s) "Heel" means for a dog to walk with its head or body remaining parallel and directly adjacent to the legs of its handler. §9.02.001 Animals at large: (a) no animal other than household pets shall run at large in public; (b) household pets remain restricted under this title. Ord. No. 3498, 11-7-23. [Verbatim per SLO County Code Title 9 PDF (25 pp, Supp. No. 16 Update 2), mirrored on the City of San Luis Obispo escribemeetings portal. Fetched 2026-05-16. Live platform for the current code not yet identified.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Luis Obispo County Code §9.03.005%'
);

-- ─── 2. Link SLO County beaches ──────────────────────────────────

WITH slo_id AS (
  SELECT id FROM public.agency WHERE name='San Luis Obispo County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='San Luis Obispo'
    AND (ba_dp.agency_id = (SELECT id FROM slo_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'SLO County Code §9.03.005(a): no person shall permit dog in any public area unless securely restrained by a leash and under direct control of a competent person, OR unless the dog is at a "heel" beside a person and obedient. §9.01.003(f) "at large" = being on public property unrestrained by a leash.',
       ps.source_url,
       'Heel-exception in §9.03.005(a) is narrow; default on_leash for beaches. Off-leash zones require specific designation per §9.03.005(c)(2).'
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'San Luis Obispo County Code §9.03.005%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
