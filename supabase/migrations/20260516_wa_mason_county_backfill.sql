-- Wave 4: Mason County (WA) dog-policy backfill.
--
-- Source: Mason County Code §4.08.050 (Animals at large) — Title 4 /
-- Chapter 4.08 Animal Code.
--
-- Live URL: https://library.municode.com/wa/mason_county/codes/code_of_ordinances?nodeId=TIT4AN_CH4.08ANCO
--
-- Operative rule (§4.08.050(a), verbatim via Playwright 2026-05-16):
--   It shall be unlawful for the owner or keeper of any animal to
--   negligently allow such animal to enter or trespass onto private
--   property of another without the express permission of the owner
--   or caretaker of said property; or to allow said animal to run at
--   large onto any public property or the public right-of-way within
--   Mason County.
--
-- "At large" defined in §4.08.020 as: "an animal off or outside the
-- premises belonging to its owner or keeper and not in the company
-- of and under direct control of its owner or keeper."
--
-- IMPORTANT — voice-control jurisdiction: Mason County's "at large"
-- prohibition uses "direct control" language with no explicit leash
-- mandate. Like Humboldt CA §541-21, Mason permits voice/physical
-- control without a leash requirement on public property.
--
-- Sand rule = off_leash_voice_control. Owner must keep dog in
-- "company of" and "under direct control of" — voice/whistle/hand
-- signal satisfies the rule if it produces immediate compliance.
--
-- Penalty: $250 civil infraction (§4.08.050(a)(2)); misdemeanor if
-- dog represents a potential threat per §4.08.050(b).
--
-- 13 Mason County beaches in beaches_gold (Hood Canal west shore +
-- Case Inlet area).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Mason County Code §4.08.050 (Animals at large) — Title 4 / Chapter 4.08 Animal Code',
       (SELECT id FROM public.agency WHERE name='Mason County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/wa/mason_county/codes/code_of_ordinances?nodeId=TIT4AN_CH4.08ANCO',
       'Mason County Code §4.08.050 Animals at large. (a) It shall be unlawful for the owner or keeper of any animal to negligently allow such animal to enter or trespass onto private property of another without the express permission of the owner or caretaker of said property; or to allow said animal to run at large onto any public property or the public right-of-way within Mason County. (1) The animal control authority or any commissioned law enforcement officer may cite an owner or keeper upon probable cause that a violation of this subsection (a) has occurred. (2) Any owner or keeper who is found, by a preponderance of the evidence, to have violated any portion of subsection (a) shall be subject to a two hundred fifty dollar civil infraction. (b) It shall be unlawful for the owner or keeper of any animal to knowingly allow that animal to be at large under subsection (a) of this section when such animal represents a potential threat of substantial bodily injury to people or damage to property... punishable as a misdemeanor. §4.08.020 "At large" means an animal off or outside the premises belonging to its owner or keeper and not in the company of and under direct control of its owner or keeper. [Voice-control jurisdiction: "direct control" language permits voice control with no leash mandate. Fetched via Playwright/Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Mason County Code §4.08.050%'
);

-- ─── 2. Link Mason County beaches ───────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Mason'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Mason County Code §4.08.050%'),
       'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'Mason County Code §4.08.050(a): unlawful to allow animal to run at large on any public property. "At large" per §4.08.020 = not in company of and under direct control. Voice control permitted — no explicit leash mandate.',
       'https://library.municode.com/wa/mason_county/codes/code_of_ordinances?nodeId=TIT4AN_CH4.08ANCO',
       'Sand rule = off_leash_voice_control per §4.08.050 + §4.08.020 (voice-control "direct control" language, no leash requirement). Mirrors Humboldt CA §541-21 pattern.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
