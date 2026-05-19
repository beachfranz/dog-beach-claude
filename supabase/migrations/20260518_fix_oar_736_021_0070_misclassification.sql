-- Fix OAR 736-021-0070 (OR Ocean Shore) misclassification — 2026-05-18
--
-- ps id=167 was created 2026-05-17 with WRONG content:
--   - citation said "OPRD state-park 6-ft leash"
--   - full_text said "dogs must be on a leash not longer than six (6) feet at all times"
--   - all 152 bps links classified rule='on_leash'
--
-- Actual OAR 736-021-0070 (verified from oregon.public.law 2026-05-18):
--   (1) "Domestic animals are ALLOWED in the ocean shore state recreation area"
--   (2)(a) "Direct control = within unobstructed sight of handler + responds
--          to voice commands or other methods of control"
--   (2)(b) "Domestic animal handlers must CARRY a leash or restraining device"
--          — CARRY, not be on
--   (2)(c) "Must promptly leash animals at the request or order of a park employee"
--
-- The prior-session row conflated 736-021-0070 (Ocean Shore = voice-control)
-- with 736-010-0030 (General Parks = strict 6-ft leash). Two different OAR
-- rules. The rule for OR Ocean Shore is off_leash_voice_control, not on_leash.
--
-- IMPACT: ~152 OR coastal beach consumer-surface entries currently show
-- on_leash where many should show off_leash_voice_control (Cannon Beach,
-- Manzanita, Bandon, Rockaway, etc. are famously dog-friendly per OPRD's
-- own operator-posted policy at stateparks.oregon.gov).
--
-- This migration:
--   1. UPDATEs ps id=167 with correct citation + full_text + metadata
--   2. UPDATEs all 152 bps rows for ps_id=167 with rule=off_leash_voice_control
--      + corrected evidence_verbatim
--   3. tg_stmt_upd_beach_policy_source fires → cascade through promote_entity_dogs
--      → zone_rules regenerated for 152 affected beaches
--
-- Per playbook tenet #5 — UPDATE triggers also auto-fire the cascade.
-- For the 5 OPRD-internal beaches that ALSO have OAR 736-010-0030 (ps id=256,
-- state-park 6-ft leash), the consensus engine will rank them — strict beats
-- permissive when both authorities exist for the same beach.

BEGIN;

-- 1. Fix the policy_source row
UPDATE public.policy_source
SET
    citation = 'OAR 736-021-0070 (Pets and Other Animals) — Parks and Recreation Department, Ch. 736, Div. 21 Ocean Shore State Recreation Area Rules',
    full_text = $$OAR 736-021-0070 Pets and Other Animals

(1) Domestic animals, including saddle or pack animals, are allowed in the ocean shore state recreation area except as provided in OAR 736-021-0090 (Cultural, Historic, Natural and Wildlife Resources), 736-030-0005 (Prohibition of Horses and Other Livestock), 736-030-0010 (Prohibition of Dogs Off Leash), and as otherwise posted by the department.

(2) The handler of any domestic animal must be responsible for the animal's behavior and must exercise direct control over the animal while in the ocean shore state recreation area.
  (a) "Direct control" means that the animal is within the unobstructed sight of the handler and responds to voice commands or other methods of control.
  (b) Domestic animal handlers must carry a leash or restraining device at all times while in the ocean shore state recreation area.
  (c) Domestic animal handlers must promptly leash animals at the request or order of a park employee.
  (d) Handlers must prevent their animals from harassing people, wildlife and other domestic animals.
  (e) Animals may not be hitched or confined in a manner that may cause damage to any natural resources on the ocean shore.
  (f) Handlers are responsible for the removal of the animal's waste while in the ocean shore state recreation area.

(3) A park manager, ocean shore naturalist, or law enforcement officer may take any measure deemed necessary to protect ocean shore resources or to prevent interference by the animal with the safety, comfort, or well-being of any person.

Operative rule for OR Ocean Shore State Recreation Area beaches:
- off_leash_voice_control (direct control = sight + voice)
- handler must carry leash + leash on park-employee request
- exceptions: OAR 736-021-0090 wildlife (incl. snowy plover seasonal closures), 736-030-0010 specific dogs-off-leash prohibitions, as posted
- park-employee discretion to require leash at any time

This is MORE PERMISSIVE than OAR 736-010-0030 (General Park Area Rules) which mandates strict 6-ft leash inside park boundaries. The Ocean Shore is its own regulatory regime per ORS 390 (Ocean Shore Recreation Area Act).

[Corrected 2026-05-18 — prior row conflated 736-021-0070 (Ocean Shore = voice-control) with 736-010-0030 (Park Areas = 6-ft leash); see metadata.corrected_from for prior values.]$$,
    last_verified = NOW(),
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'corrected_2026_05_18', true,
        'corrected_from_citation', 'Oregon Administrative Rules §736-021-0070 (Pets and Other Animals) — OPRD state-park 6-ft leash',
        'correction_reason', 'Prior row conflated 736-021-0070 (Ocean Shore voice-control) with 736-010-0030 (General Park 6-ft leash). Two different OAR rules; correct citation + full_text now reflect oregon.public.law/rules/oar_736-021-0070.'
    )
WHERE id = 167;

-- 2. Fix the rule classification on all 152 bps rows
UPDATE public.beach_policy_source
SET
    rule = 'off_leash_voice_control',
    evidence_verbatim = 'OAR 736-021-0070(2): handler must exercise direct control = within unobstructed sight + responds to voice commands; must carry leash; must promptly leash at park-employee request.',
    last_verified = NOW(),
    status_note = COALESCE(status_note || E'\n', '') ||
        '[Corrected 2026-05-18: prior classification rule=on_leash was based on misclassified ps row 167; actual OAR 736-021-0070 is off_leash_voice_control. Snowy plover seasonal closures still apply at specific beaches per OPRD posting; park-employee discretion to require leash at any time.]'
WHERE policy_source_id = 167;

-- 3. Verification
SELECT 'ps_167_citation' AS what, citation FROM public.policy_source WHERE id = 167
UNION ALL
SELECT 'bps_count_corrected', COUNT(*)::text FROM public.beach_policy_source
WHERE policy_source_id = 167 AND rule = 'off_leash_voice_control'
UNION ALL
SELECT 'bps_count_still_on_leash', COUNT(*)::text FROM public.beach_policy_source
WHERE policy_source_id = 167 AND rule = 'on_leash';

COMMIT;
