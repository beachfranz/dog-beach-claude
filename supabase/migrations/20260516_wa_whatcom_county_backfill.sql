-- Wave 4: Whatcom County (WA) dog-policy backfill.
--
-- Source: Whatcom County Code §6.04 (Animal Control) — Title 6 /
-- Chapter 6.04, hosted on ecode360 (WH4863) following codepublishing
-- redirect.
--
-- Live URL: https://ecode360.com/WH4863 (Whatcom moved to ecode360
-- from codepublishing.com; the old codepublishing URL now redirects
-- to ecode360 with the WH4863 custid.)
--
-- Code structure (§6.04 Animal Control):
--   §6.04.030 Dog control zones — all unincorporated Whatcom County
--     declared dog control zones EXCEPT areas zoned R-5, R-10, or AG.
--   §6.04.020 "Under control" definition — leash restraining dog to
--     owner's immediate proximity.
--   §6.04 leash law — unlawful to allow dog to run/stray off premises
--     within a dog control zone on public property or another's
--     private property, unless under control by means of a leash.
--
-- Operative rule (§6.04 leash provision, verbatim from WebSearch
-- 2026-05-16):
--   It shall be unlawful for the owner or keeper of any dog to cause,
--   permit or allow his/her dog to run, stray or otherwise to be
--   away from the premises of the owner or keeper within a dog
--   control zone and to be on public property or the private property
--   of another without their permission, unless such dog be under
--   control by means of a leash.
--
-- §6.04.020 "Under control" — by means of a leash, restrains the dog
-- to the owner's immediate proximity, preventing the dog from
-- trespassing upon property or annoying or chasing other persons,
-- animals, or vehicles of any sort.
--
-- Exceptions: sanctioned search-and-rescue (sheriff's office); lawful
-- competition sanctioned by a nationally recognized organization.
--
-- Sand rule = on_leash (under-control means leash per §6.04.020
-- definition). Note: in R-5/R-10/AG zones (rural), county is NOT a
-- dog control zone — Whatcom is more lenient in rural areas. Most
-- coastal beaches are NOT in those zones, so the leash rule applies.
--
-- 13 Whatcom County beaches in beaches_gold (NW Washington coast
-- + Bellingham Bay area + Lummi/Sandy Point).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Whatcom County Code §6.04 (Animal Control) — Dog Control Zones + Leash Law',
       (SELECT id FROM public.agency WHERE name='Whatcom County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/WH4863',
       'Whatcom County Code §6.04 Animal Control. §6.04.030 Dog control zones: All of the unincorporated areas of Whatcom County are hereby declared to be dog control zones except areas designated R-5, R-10, and AG. Leash law: It shall be unlawful for the owner or keeper of any dog to cause, permit or allow his/her dog to run, stray or otherwise to be away from the premises of the owner or keeper within a dog control zone and to be on public property or the private property of another without their permission, unless such dog be under control by means of a leash. §6.04.020 "Under control" means that the owner, by means of a leash, restrains the dog to the owner''s immediate proximity, preventing the dog from trespassing upon property or annoying or chasing other persons, animals, or vehicles of any sort. Exceptions: sanctioned search-and-rescue (sheriff''s office); lawful animal competition sanctioned by a nationally recognized organization or a local chapter thereof. [Whatcom moved to ecode360 (WH4863) from codepublishing.com; verbatim text via WebSearch 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Whatcom County Code §6.04%'
);

-- ─── 2. Link Whatcom County beaches ─────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Whatcom'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Whatcom County Code §6.04%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Whatcom County Code §6.04: within a dog control zone (all unincorporated WC except R-5, R-10, AG zones), unlawful to allow dog to stray off premises onto public property unless under control by leash. §6.04.020 defines "under control" as leash restraining to owner''s immediate proximity.',
       'https://ecode360.com/WH4863',
       'Sand rule = on_leash per §6.04 leash law + §6.04.020 definition. Most coastal beaches are in dog control zones (not R-5/R-10/AG rural zones), so the leash rule applies. Per-beach zone refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
