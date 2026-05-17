-- Phase B: Santa Cruz County code backfill.
--
-- Source: animallaw.info verbatim mirror of Santa Cruz County Code Title 6.
-- URL: https://www.animallaw.info/local/ca-santa-cruz-county-santa-cruz-county-code-title-6-animals
--
-- Staleness: animallaw.info's last-check date is "May, 2012". Their own note
-- states local ordinances are "no longer checked and are kept only for
-- archival and example purposes." The core leash rule (§6.12.020) is a
-- structural ordinance unlikely to have changed materially. Franz authorized
-- ingest with explicit staleness annotation 2026-05-16.
--
-- Operative rule per §6.12.020 (verbatim):
--   "It is unlawful for the owner of any dog, whether licensed or
--   unlicensed, to permit or allow such dog to be away from the premises
--   of its owner at any time if not under actual physical restraint or
--   control, such as a leash, tether, or in the grasp of a person."
--
-- §6.04.020 Definitions — "Animal at large": any dog found off the owner's
-- premises that is not under actual physical restraint or control such as
-- a leash, tether, or in the grasp of a competent person.
--
-- §6.12.010 Dogs at Large Prohibited (companion section).
--
-- 45 Santa Cruz County beaches in beaches_gold.
-- Santa Cruz County code's live platform was not resolved during Phase A
-- discovery (not on Municode/amlegal/qcode/ecode360). The current code is
-- presumably at sccounty01.co.santa-cruz.ca.us or hosted on a vendor not
-- yet probed; per-section verification deferred.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Santa Cruz County Code §6.12.020 (Dogs off premises — leash required) + §6.12.010 (Dogs at large prohibited) + §6.04.020 (Definitions: animal at large)',
       (SELECT id FROM public.agency WHERE name='Santa Cruz County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.animallaw.info/local/ca-santa-cruz-county-santa-cruz-county-code-title-6-animals',
       'Santa Cruz County Code Title 6 (Animals). §6.12.020: It is unlawful for the owner of any dog, whether licensed or unlicensed, to permit or allow such dog to be away from the premises of its owner at any time if not under actual physical restraint or control, such as a leash, tether, or in the grasp of a person. §6.12.010: Dogs at large prohibited — officers may issue citations or impound dogs if the owner is not present to secure the animal. §6.04.020 Definitions — "Animal at large": any dog found off the owner''s premises that is not under actual physical restraint or control, such as a leash, tether, or in the grasp of a competent person. [Source: animallaw.info verbatim mirror, last-checked May 2012. Current platform for the live Santa Cruz County Code not yet resolved; structural leash rule unchanged across CA counties is a strong prior. Verify against current code before relying on procedural details (penalty schedules, dangerous-dog procedures).]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Santa Cruz County Code §6.12.020%'
);

-- ─── 2. Link Santa Cruz County beaches ───────────────────────────

WITH sc_id AS (
  SELECT id FROM public.agency WHERE name='Santa Cruz County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Santa Cruz'
    AND (ba_dp.agency_id = (SELECT id FROM sc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Santa Cruz County Code §6.12.020: unlawful to permit a dog to be away from the premises of its owner if not under actual physical restraint or control, such as a leash, tether, or in the grasp of a person.',
       ps.source_url,
       'Source is animallaw.info May-2012 verbatim mirror; live platform for current code not yet resolved.'
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Santa Cruz County Code §6.12.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
