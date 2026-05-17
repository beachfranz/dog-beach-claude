-- Wave 4: Island County (WA) dog-policy backfill.
--
-- Source: Island County Code Title VI / Chapter 6.08 §6.08.090
-- (Control off premises — Authorized off-leash areas).
--
-- Live URL: https://library.municode.com/wa/island_county/codes/code_of_ordinances?nodeId=TITVIAN_CH6.08DOLICO
--
-- Operative rule (§6.08.090 — verbatim from Municode):
--   In Island County unincorporated areas, it is unlawful for the
--   owner, keeper, or person having custody or control of any dog to
--   permit a dog to roam, run, stray, or be away from the premises of
--   the owner or custodian and to be in any public place or on any
--   public property or the private property of another in the county,
--   unless such dog is controlled by a leash or chain not more than
--   eight feet in length, such control to be exercised by the owner
--   or custodian or other competent and authorized persons.
--   Dogs may roam and be off-leash in portions of designated Island
--   County Park areas posted by the Island County Parks and
--   Recreation Department Director for such use, provided that the
--   dogs are not in heat and are accompanied by the dog owner or
--   custodian who is in voice control of the dog (i.e. the dog will
--   immediately come when called by the owner/custodian).
--
-- Sand rule = on_leash (8-foot leash baseline; off-leash only in
-- specifically-posted park areas).
--
-- 49 Island County beaches in beaches_gold (Whidbey + Camano islands).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Island County Code §6.08.090 (Control off premises — Authorized off-leash areas) — Title VI / Chapter 6.08 Dog License and Control',
       (SELECT id FROM public.agency WHERE name='Island County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/wa/island_county/codes/code_of_ordinances?nodeId=TITVIAN_CH6.08DOLICO',
       'Island County Code §6.08.090 Control off premises — Authorized off-leash areas. In Island County unincorporated areas, it is unlawful for the owner, keeper, or person having custody or control of any dog to permit a dog to roam, run, stray, or be away from the premises of the owner or custodian and to be in any public place or on any public property or the private property of another in the county, unless such dog is controlled by a leash or chain not more than eight feet in length, such control to be exercised by the owner or custodian or other competent and authorized persons. Dogs may roam and be off-leash in portions of designated Island County Park areas posted by the Island County Parks and Recreation Department Director for such use, provided that the dogs are not in heat and are accompanied by the dog owner or custodian who is in voice control of the dog (i.e. the dog will immediately come when called by the owner/custodian). Any dog found roaming, running, straying, or away from the premises of the owner or custodian and not under control as herein provided may be impounded, subject to redemption. [Title VI hosted on Municode. Fetched via WebSearch 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Island County Code §6.08.090%'
);

-- ─── 2. Link Island County beaches ───────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Island'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Island County Code §6.08.090%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Island County Code §6.08.090: 8-foot leash baseline countywide on public property; off-leash permitted only in specifically-posted areas of Island County Parks with voice control.',
       'https://library.municode.com/wa/island_county/codes/code_of_ordinances?nodeId=TITVIAN_CH6.08DOLICO',
       'Sand rule defaults to on-leash per §6.08.090 8-foot leash baseline. Off-leash status would apply only at parks specifically posted by the Parks & Recreation Department Director; per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
