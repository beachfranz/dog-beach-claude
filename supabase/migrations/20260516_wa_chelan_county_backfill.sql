-- Wave 4: Chelan County (WA) parks dog-policy backfill.
--
-- Source: Chelan County Code §7.24.010(3) (Animals subsection of
-- park rules) — Title 7 / Chapter 7.24 Park Regulations.
--
-- Live URL: https://www.codepublishing.com/WA/ChelanCounty/html/Chelco07/Chelco0724.html
--
-- Operative rule (§7.24.010(3), verbatim via curl 2026-05-16):
--   Animals. It is unlawful for any person to permit any animal to
--   run at large in any park except in designated areas. All animals
--   must be on a leash and under the direct control of their owner
--   or responsible person at all times. Upon occurrence of excessive
--   barking, vicious, or unruly behavior from any animal, a written
--   warning will be given to the owner or other responsible person.
--   Upon subsequent occurrence of any such conduct, immediate removal
--   from the park of the subject animal is mandatory.
--
-- §7.24.010(1) defines "Park" as "any public grounds under the
-- control, ownership, or supervision of Chelan County."
--
-- §7.24.010(16) Swimming: "Swimming is prohibited except in
-- designated areas."
--
-- Sand rule = on_leash (all animals must be on a leash in any
-- county park; specific leash length not stated in §7.24 but the
-- "general" Chelan County leash guidance is 8-foot max).
--
-- Title 7 / Chapter 7.40 (Animal Control) covers nuisance + dangerous
-- dog enforcement countywide but does not establish a general leash
-- mandate on public property; for parks/beaches the operative rule is
-- §7.24.010(3).
--
-- 9 Chelan County beaches in beaches_gold (Lake Chelan + Columbia
-- River freshwater).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Chelan County Code §7.24.010(3) (Animals — Park Regulations) — Title 7 / Chapter 7.24',
       (SELECT id FROM public.agency WHERE name='Chelan County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/ChelanCounty/html/Chelco07/Chelco0724.html',
       'Chelan County Code §7.24.010(3) Animals. It is unlawful for any person to permit any animal to run at large in any park except in designated areas. All animals must be on a leash and under the direct control of their owner or responsible person at all times. Upon occurrence of excessive barking, vicious, or unruly behavior from any animal, a written warning will be given to the owner or other responsible person. Upon subsequent occurrence of any such conduct, immediate removal from the park of the subject animal is mandatory. §7.24.010(1) "Park" means any public grounds under the control, ownership, or supervision of Chelan County. §7.24.010(16) Swimming: Swimming is prohibited except in designated areas. [Title 7 Chapter 7.40 (Animal Control) handles nuisance + dangerous dogs countywide; §7.24.010(3) is the operative park-and-beach rule. Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Chelan County Code §7.24.010(3)%'
);

-- ─── 2. Link Chelan County beaches ──────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Chelan'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Chelan County Code §7.24.010(3)%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Chelan County Code §7.24.010(3): unlawful to permit any animal to run at large in any county park except designated areas; all animals must be on a leash and under direct control.',
       'https://www.codepublishing.com/WA/ChelanCounty/html/Chelco07/Chelco0724.html',
       'Sand rule = on_leash per §7.24.010(3). Chelan beaches are predominantly Lake Chelan / Columbia River freshwater. Many are in state parks (e.g., Lake Chelan State Park) where the federal/state state-parks rule controls; per-beach jurisdiction refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
