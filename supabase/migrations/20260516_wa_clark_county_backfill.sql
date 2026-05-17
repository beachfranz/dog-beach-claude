-- Wave 4: Clark County (WA) dog-policy backfill.
--
-- Source: Clark County Code §8.15.020 (Dogs off premises to be on
-- leash) — Title 8 / Chapter 8.15 Dog Leash Areas.
--
-- Live URL: https://www.codepublishing.com/WA/ClarkCounty/html/ClarkCounty08/ClarkCounty0815/ClarkCounty0815020.html
--
-- Operative rule (§8.15.020, verbatim via curl 2026-05-16):
--   (1) Except as provided in subsection (2) of this section, it is
--   unlawful for the owner or custodian of any dog to cause, permit,
--   or allow such dog to roam, run, stray or be away from the
--   premises of such owner or custodian and to be on any public
--   place, any public property or the private property of another
--   within the boundaries of a leash law area as provided for herein
--   unless such dog be controlled by a leash, such control to be
--   exercised by such owner or custodian or other competent and
--   authorized person. Any dog found roaming, running, straying or
--   being away from the owner's or custodian's premises and not on
--   leash as herein provided is hereby declared to be a nuisance and
--   such dog may be seized and impounded subject to redemption in
--   the manner provided by Chapter 8.19 of this title.
--   (2) Dogs may be allowed to run at large in off-leash areas of
--   such properties as may be designated by the public works director
--   or designee, who shall have the authority to establish such rules
--   and regulations as reasonably necessary for the operation of such
--   designated properties. Dogs in designated off-leash areas must be
--   accompanied by their owner, be under vocal control and not cause
--   a public nuisance, safety hazard or harass people, other dogs or
--   wildlife. Except as specifically provided herein, all other
--   applicable provisions of this chapter shall also apply in
--   designated off-leash areas.
--
-- §8.15 establishes mandatory leash areas within urban growth
-- boundaries and other designated areas.
--
-- Additional published park-side rule from Clark County Public Works:
--   Dogs are prohibited from beach + surrounding turf at Vancouver
--   Lake Park between April 1 and October 31.
--   Dogs (except service animals) are not allowed on beach areas at
--   Klineline Pond and Vancouver Lake Regional Park.
--
-- Sand rule = on_leash. Note: dogs prohibited on the specific named
-- beach areas (Vancouver Lake, Klineline) — per-beach refinement
-- deferred for now; baseline leash law links every Clark County beach.
--
-- 6 Clark County beaches in beaches_gold (Columbia River + Vancouver
-- Lake area; freshwater).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Clark County Code §8.15.020 (Dogs off premises to be on leash) — Title 8 / Chapter 8.15 Dog Leash Areas',
       (SELECT id FROM public.agency WHERE name='Clark County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/ClarkCounty/html/ClarkCounty08/ClarkCounty0815/ClarkCounty0815020.html',
       'Clark County Code §8.15.020 Dogs off premises to be on leash. (1) Except as provided in subsection (2) of this section, it is unlawful for the owner or custodian of any dog to cause, permit, or allow such dog to roam, run, stray or be away from the premises of such owner or custodian and to be on any public place, any public property or the private property of another within the boundaries of a leash law area as provided for herein unless such dog be controlled by a leash, such control to be exercised by such owner or custodian or other competent and authorized person. Any dog found roaming, running, straying or being away from the owner''s or custodian''s premises and not on leash as herein provided is hereby declared to be a nuisance and such dog may be seized and impounded subject to redemption in the manner provided by Chapter 8.19 of this title. (2) Dogs may be allowed to run at large in off-leash areas of such properties as may be designated by the public works director or designee... Dogs in designated off-leash areas must be accompanied by their owner, be under vocal control and not cause a public nuisance, safety hazard or harass people, other dogs or wildlife. (Sec. 1 of Res. 1981-04-108; amended by Sec. 1 of Ord. 2005-08-10). [Additional published Public Works rule: dogs prohibited from beach areas at Vancouver Lake Park (Apr 1 – Oct 31) and Klineline Pond. Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Clark County Code §8.15.020%'
);

-- ─── 2. Link Clark County beaches ───────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Clark'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Clark County Code §8.15.020%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Clark County Code §8.15.020(1): within a leash law area, unlawful to allow dog to stray off premises on public property unless controlled by a leash. Off-leash only at designated areas under §8.15.020(2).',
       'https://www.codepublishing.com/WA/ClarkCounty/html/ClarkCounty08/ClarkCounty0815/ClarkCounty0815020.html',
       'Sand rule = on_leash per §8.15.020(1). Specific named beach prohibitions (Vancouver Lake Park Apr-Oct; Klineline Pond) under Public Works director rules per §8.15.020(2) — per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
