-- Consensus engine rewrite — Phase 5b (revised): link the existing
-- walkthrough-quality policy_source rows to their issuing agencies.
--
-- Background (from 2026-05-16 conversation): we had drifted from
-- statutes to policy; Franz paused 5b until agency entities existed.
-- Phase 6 + 7 built the agency table and linked beaches to agencies.
-- Phase 5b now closes the loop: every codified law / agency policy
-- that we've already captured gets pointed at its issuing agency
-- via policy_source.issuing_agency_id.
--
-- Scope: 19 walkthrough-quality rows (ids 1-19, 23). The remaining
-- 25 policy_source rows are either:
--   - operator_posted_policy (ids 20, 21, 22) — need operator
--     table seeded first; pinned as Phase 8 gap
--   - Extraction-pipeline artifacts (ids 24-44) — categorical
--     labels not real legal sources; stay NULL on issuing_agency_id
--     and are slated for cleanup in [[deferred-bep-source-cleanup]].
--
-- Mapping (built from the walkthrough docs in /docs):

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='National Park Service' AND type='federal_department'
) WHERE id IN (1, 2, 18, 19, 23);
-- ids: 36 CFR §2.15 (1), 36 CFR §7.97(d) (2), 1979 GGNRA Pet Policy (18),
--      2025 GOGA Compendium (19), withdrawn 2017 rulemaking (23)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='California State Lands Commission' AND type='state_department'
) WHERE id = 3;
-- id 3: PRC §6001 et seq. (tidelands)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='State of California' AND type='state'
) WHERE id = 4;
-- id 4: H&S Code §115880 / AB 411 — water quality. State-level statute,
-- enforced by counties; State is the issuing legislative body.

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='Huntington Beach' AND type='city'
) WHERE id IN (5, 10);
-- id 5: HBMC §13.08.070; id 10: MOU (city is gov party)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='Los Angeles County' AND type='county'
) WHERE id = 6;
-- id 6: LA County Code Title 17 (Beaches and Harbors)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='Long Beach' AND type='city'
) WHERE id IN (7, 8);
-- ids 7, 8: LBMC (dog beach zone + general leash law)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='East Bay Regional Park District' AND type='special_district'
) WHERE id = 9;
-- id 9: EBRPD Ordinance 38

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='State of California' AND type='state'
) WHERE id = 11;
-- id 11: Lease State→LA (state is the lessor)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='Los Angeles' AND type='city'
) WHERE id = 12;
-- id 12: Operating agreement LA→County (city is the lessor of the operating agreement)

UPDATE public.policy_source SET issuing_agency_id = (
  SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'
) WHERE id IN (13, 14, 15, 16, 17);
-- id 13: Lease DPR→EBRPD; ids 14-17: DPR admin policies

-- ─── Sanity check ─────────────────────────────────────────────────

DO $$
DECLARE
  unmapped_count INT;
BEGIN
  SELECT COUNT(*) INTO unmapped_count
    FROM public.policy_source
   WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,23)
     AND issuing_agency_id IS NULL;
  IF unmapped_count > 0 THEN
    RAISE WARNING 'Phase 5b: % walkthrough-quality policy_source rows still NULL — agency name mismatch?', unmapped_count;
  END IF;
END$$;
