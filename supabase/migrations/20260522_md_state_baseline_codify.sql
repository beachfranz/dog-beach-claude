-- 20260522_md_state_baseline_codify.sql
--
-- MARYLAND STATE BASELINE CODIFY
--
-- Codifies Maryland's statewide default dog-policy on public lands (state
-- parks) as both:
--   1. policy_source row for COMAR 08.07.06.17 (Pets) — the operative
--      Maryland Park Service regulation issued by MD DNR.
--   2. state_dogs_policy MD seed UPDATE to point ordinance_ref + source_url
--      at the correct COMAR citation (prior 2026-05-14 seed cited
--      COMAR 08.01.07 which is the wrong section; verified citation is
--      COMAR 08.07.06.17 inside Subtitle 07 FORESTS AND PARKS, Chapter
--      08.07.06 USE OF STATE PARKS).
--
-- Source: COMAR 08.07.06.17 Pets, Maryland Department of Natural Resources
-- (Maryland Park Service).
--
-- Primary deep-link URL (regs.maryland.gov — official MD regulations site):
--   https://regs.maryland.gov/us/md/exec/comar/08.07.06.17
--
-- Cross-verified via:
--   - http://mdrules.elaws.us/comar/08.07.06.17 (eLaws mirror)
--   - https://www.law.cornell.edu/regulations/maryland/COMAR-08-07-06-17
--   - https://dnr.maryland.gov/publiclands/pages/pets.aspx (MD DNR plain-English page)
--
-- Verbatim operative text (COMAR 08.07.06.17 "Pets"):
--   A. Developed Areas.
--     (1) Except for guide service dogs, pets of any kind are not allowed
--         on a picnic area, cabin area, camping area, or other restricted
--         area of a State park.
--     (2) The Service reserves the right to designate pet areas in a State
--         park. In these areas pets shall be on a leash with a maximum
--         length of 6 feet.
--   B. Undeveloped Areas.
--     Pets are permitted on a leash with a maximum length of 10 feet.
--
-- Adopted effective October 11, 1993 (20:20 Md. R. 1568); amended.
--
-- Rule classification:
--   - default_rule: on_leash (statewide default — both developed designated
--     pet areas AND undeveloped areas require leashes; only length differs)
--   - has_on_leash: true
--   - has_off_leash: false (no codified statewide off-leash provision;
--     the DNR pets page mentions designated swimming areas + permitted
--     hunting as off-leash, but those are park-level overrides, NOT the
--     COMAR-codified statewide default — confirmed by the 2026-05-14
--     fix migration that explicitly set has_off_leash=false for this
--     reason)
--   - dogs_allowed: yes (with the developed-area restrictions)
--
-- Per [[page-level-over-agency-level]]: source_url points at the §17
-- deep-link, NOT dnr.maryland.gov homepage.
--
-- Per [[citywide-leash-inference]] (territorial-rule-applies-to-beaches-
-- by-default): COMAR 08.07.06.17 governs ALL Maryland state parks including
-- those with beaches (Sandy Point SP, Assateague SP, Janes Island SP, etc.).
-- State-park beaches inherit this baseline; county/municipal beaches handled
-- by lower-tier operator/local-ordinance rows.

begin;

-- ─── 1. Ensure MD DNR agency row (canonical bare name) ─────────────

INSERT INTO public.agency (name, type, short_name, web_url)
VALUES (
  'Maryland Department of Natural Resources',
  'state_department',
  'MD DNR',
  'https://dnr.maryland.gov/'
)
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. Insert codified policy_source for COMAR 08.07.06.17 ────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text, effective_date)
SELECT
  'state_regulation',
  'COMAR 08.07.06.17 (Pets) — Maryland Park Service, Use of State Parks',
  (SELECT id FROM public.agency
    WHERE name='Maryland Department of Natural Resources'
      AND type='state_department'),
  ARRAY['dog_policy']::text[],
  'https://regs.maryland.gov/us/md/exec/comar/08.07.06.17',
  'COMAR 08.07.06.17 Pets. A. Developed Areas. (1) Except for guide service dogs, pets of any kind are not allowed on a picnic area, cabin area, camping area, or other restricted area of a State park. (2) The Service reserves the right to designate pet areas in a State park. In these areas pets shall be on a leash with a maximum length of 6 feet. B. Undeveloped Areas. Pets are permitted on a leash with a maximum length of 10 feet. [Title 08 Department of Natural Resources, Subtitle 07 Forests and Parks, Chapter 08.07.06 Use of State Parks. Adopted effective October 11, 1993 (20:20 Md. R. 1568). Authoritative copy at regs.maryland.gov; mirrored at mdrules.elaws.us/comar/08.07.06.17 and law.cornell.edu/regulations/maryland/COMAR-08-07-06-17. MD DNR plain-English summary at dnr.maryland.gov/publiclands/pages/pets.aspx. Fetched via WebFetch 2026-05-22.]',
  DATE '1993-10-11'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'COMAR 08.07.06.17%'
);

-- ─── 3. Re-seed state_dogs_policy MD row with correct citation ─────
--
-- The 2026-05-14 seed (citing "COMAR 08.01.07") is superseded; correct
-- citation is COMAR 08.07.06.17. Re-point ordinance_ref + source_url at
-- the operative deep-link. Confidence bumped 0.40 → 0.55 because we now
-- have a verified codified backing.

INSERT INTO public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
VALUES
  ('MD', 'Maryland', 'yes', 'mixed',
   true, false,
   'COMAR 08.07.06.17 Pets. A. Developed Areas: (1) Except for guide service dogs, pets of any kind are not allowed on a picnic area, cabin area, camping area, or other restricted area of a State park. (2) In designated pet areas pets shall be on a leash with a maximum length of 6 feet. B. Undeveloped Areas: Pets are permitted on a leash with a maximum length of 10 feet.',
   'https://regs.maryland.gov/us/md/exec/comar/08.07.06.17',
   'COMAR 08.07.06.17',
   'Maryland Park Service codified pet rule (COMAR 08.07.06.17). Default rule statewide: on_leash (6-ft in designated developed-area pet zones; 10-ft in undeveloped areas). Pets prohibited from picnic/cabin/camping/other restricted areas except service dogs. Off-leash mentioned on the DNR plain-English pets page (designated swimming areas + permitted hunting) is a PARK-LEVEL override, NOT the COMAR-codified statewide default — captured here as has_off_leash=false per 2026-05-14 fix. County/municipal beaches handled by lower-tier operator-posted / local-ordinance rows.',
   0.55,
   now())
ON CONFLICT (state_code) DO UPDATE
  SET dogs_allowed  = excluded.dogs_allowed,
      default_rule  = excluded.default_rule,
      has_on_leash  = excluded.has_on_leash,
      has_off_leash = excluded.has_off_leash,
      source_quote  = excluded.source_quote,
      source_url    = excluded.source_url,
      ordinance_ref = excluded.ordinance_ref,
      scope_notes   = excluded.scope_notes,
      confidence    = excluded.confidence,
      scraped_at    = now();

commit;

-- ─── Notes / follow-ups ────────────────────────────────────────────
--
-- No beach_policy_source (bps) rows are inserted here. MD has no
-- beaches_gold beaches yet (state expansion not started); per
-- [[beach-catalog-prereq-for-codify]] the codify step ships the ps + state
-- baseline and per-beach bps wiring lands as part of MD ingestion.
-- The downstream populate_from_state_default_gold() emitter (wired in
-- 20260508_state_dogs_policy_entity.sql) will pick the state_dogs_policy
-- row up automatically when MD beaches are promoted to gold.
--
-- Federal overlay (Assateague Island National Seashore — NPS 36 CFR §2.15
-- + Compendium) and county/municipal codes are out of scope for this
-- migration; tracked separately per the state-expansion playbook.
