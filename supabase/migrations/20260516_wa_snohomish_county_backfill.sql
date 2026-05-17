-- Wave 4: Snohomish County (WA) dog-policy backfill.
--
-- Source: Snohomish County Code §9.14.030 (Dogs off premises to be on
-- a leash) — Title 9 / Chapter 9.14 DOG LEASH LAW.
--
-- Live URL: https://snohomish.county.codes/SCC/9.14.030
-- (Hosted on county.codes / General Code platform.)
--
-- Operative rule (§9.14.030, verbatim via Playwright 2026-05-16):
--   It is unlawful for the owner or custodian of any dog to cause,
--   permit, or allow such dog to roam, run, stray or be away from the
--   premises of its owner or custodian or to be on any public place
--   or any public property unless the dog is under control, PROVIDED,
--   HOWEVER, that this section does not apply to dogs directly
--   supervised by their owners while using designated off leash areas
--   on county park property. Any dog found roaming, running, straying
--   or being away from such premises and not under control is
--   declared to be a public nuisance and may be seized and impounded
--   subject to redemption in the manner provided by chapter 9.12 SCC.
--
-- "Under control" per §9.14.020 / county pet-rule guidance = restrained
-- by a secure leash or other restraint not more than eight feet in
-- length and under the physical control of a person capable of
-- restricting the dog's movement.
--
-- Exceptions per §9.14.040: lawful herding, hunting, sanctioned
-- competition, training; "open run areas"; search-and-rescue; law
-- enforcement.
--
-- Sand rule = on_leash (8-foot max on any public place/property).
-- Off-leash status applies only at posted county-park OLDAs.
--
-- 20 Snohomish County beaches in beaches_gold (Puget Sound shore
-- between King and Skagit).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Snohomish County Code §9.14.030 (Dogs off premises to be on a leash) — Title 9 / Chapter 9.14 DOG LEASH LAW',
       (SELECT id FROM public.agency WHERE name='Snohomish County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://snohomish.county.codes/SCC/9.14.030',
       'Snohomish County Code §9.14.030 Dogs off premises to be on a leash. It is unlawful for the owner or custodian of any dog to cause, permit, or allow such dog to roam, run, stray or be away from the premises of its owner or custodian or to be on any public place or any public property unless the dog is under control, PROVIDED, HOWEVER, that this section does not apply to dogs directly supervised by their owners while using designated off leash areas on county park property. Any dog found roaming, running, straying or being away from such premises and not under control is declared to be a public nuisance and may be seized and impounded subject to redemption in the manner provided by chapter 9.12 SCC. "Under control" means restrained by a secure leash or other restraint not more than eight feet in length and under the physical control of a person capable of restricting the dog''s movement. §9.14.040 exceptions: lawful herding, hunting, sanctioned competition, training; open run areas; search and rescue; law enforcement. [Hosted on county.codes (General Code). Fetched via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Snohomish County Code §9.14.030%'
);

-- ─── 2. Link Snohomish County beaches ───────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Snohomish'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Snohomish County Code §9.14.030%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Snohomish County Code §9.14.030: dogs must be under control (8-foot leash max per §9.14.020) on any public place or public property. Off-leash status only at posted county-park OLDAs.',
       'https://snohomish.county.codes/SCC/9.14.030',
       'Sand rule = on_leash per §9.14.030 + §9.14.020 (8-foot max). Off-leash only at posted county-park OLDAs.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
