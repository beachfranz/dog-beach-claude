-- Consensus engine rewrite — Phase 5.2: schema additions surfaced
-- by the four walkthroughs preceding Phase 5b (Manhattan Beach,
-- Salt Creek, Klamath, Coronado).
--
-- Step 1 of 2: add the `tribal_code` value to the
-- policy_source_subtype enum. PostgreSQL requires `ADD VALUE` to
-- be committed in its own transaction before the new value can be
-- used — so the function update that consumes it lives in
-- 20260516_consensus_phase5_2_step2_authority_function.sql.
--
-- Walkthroughs that surfaced these additions:
--
-- 1. New policy_source_subtype: `tribal_code`. Tier 1. Distinct
--    from the existing `tribal_resolution` (tier 3, administrative).
--    A codified tribal code has statute-equivalent force on tribal
--    land. Klamath / Yurok was the surfacing walkthrough.
--
-- 2. (step 2 file) Updated policy_source_authority() function to
--    assign tribal_code → tier 1.
--
-- 3. Comments / documentation of new agency-type vocabulary:
--    - `agency.type = 'tribal'` — sovereign-equal to federal;
--      flat hierarchy not nested under state
--    - `agency.type = 'military'` — DoD/Navy/Army; distinct from
--      civilian federal (NPS, USFWS) because of 18 USC §1382 and
--      adjacency-only authority pattern
--    No schema change needed — agency table doesn't exist yet
--    (placeholder bigint on policy_source.issuing_agency_id).
--    When the agency table lands, these types should be included.
--
-- 4. Future beach_operator.scope_section column for Manhattan
--    Beach-style section-split joint-operations. beach_operator
--    doesn't exist yet either; documented for when it does.
--
-- Idempotency: `ADD VALUE IF NOT EXISTS` is supported in modern
-- PostgreSQL.

ALTER TYPE policy_source_subtype ADD VALUE IF NOT EXISTS 'tribal_code';

COMMENT ON TABLE public.policy_source IS
  'First-class entity for legal/policy instruments (statutes, MOUs, '
  'agency policies, operator-published rules, court rulings, etc.). '
  'Authority tier is intrinsic to subtype via policy_source_authority(). '
  'Phase 1 of consensus engine rewrite, 2026-05-16. '
  ''
  'Phase 5.2 additions (2026-05-16): '
  '- tribal_code subtype added at tier 1 (codified tribal law has '
  '  statute-equivalent force on tribal land per the Klamath/Yurok '
  '  walkthrough). '
  '- Existing tribal_resolution stays at tier 3 for administrative '
  '  decisions under the tribal_code umbrella. '
  ''
  'Future agency-table vocabulary (deferred until agency table is built): '
  '- agency.type = ''tribal'' — sovereign-equal to federal; flat '
  '  hierarchy not nested under state; PL-280 jurisdictional split '
  '  applies in CA. '
  '- agency.type = ''military'' — DoD/Navy/Army; distinct from '
  '  civilian federal (NPS, USFWS) because of 18 USC §1382 and '
  '  adjacency-only authority pattern. '
  ''
  'Future beach_operator-table vocabulary (deferred): '
  '- beach_operator.scope_section — section-scoped operators '
  '  (Manhattan Beach pattern: LA County operates sand; city '
  '  operates adjacent infrastructure within the same beach).';
