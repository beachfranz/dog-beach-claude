-- 20260522_md_state_baseline_codify.sql
--
-- MARYLAND STATE-PARK SYSTEM CODIFY + honest state_dogs_policy fallback.
--
-- ─── Critical framing correction (Franz 2026-05-22) ─────────────────
--
-- Maryland has NO statewide leash law. The prior framing of this
-- migration (and the 2026-05-14 state_dogs_policy seed) incorrectly
-- treated COMAR 08.07.06.17 as a statewide default. It is not. COMAR
-- 08.07.06.17 binds ONLY beaches inside the Maryland state-park system
-- (Sandy Point SP, Greenwell SP, Janes Island SP, Point Lookout SP,
-- Calvert Cliffs SP, Assateague SP — the state-park portion, not the
-- NS). Beyond state parks, leash rules in MD are creatures of local
-- ordinances (counties + cities) which vary widely. Many private
-- community / HOA waterfronts have no codified rule at all.
--
-- This migration has THREE parts, each scoped honestly:
--
--   1. policy_source row for COMAR 08.07.06.17 (Pets) — the MD Park
--      Service regulation. NOT a statewide leash law; only governs
--      state-park lands. Created so per-state-park BPS rows can
--      attach to it.
--
--   2. BPS rows attaching COMAR 08.07.06.17 to the two in-scope MD
--      state-park beaches in this wave: Sandy Point SP (fid 6310347)
--      and Greenwell State - Pavilion (fid 18144392). Both were
--      explicitly EXCLUDED from their parent county migrations
--      (Anne Arundel + St Mary's) precisely so they could attach to
--      the state-park rule here. Other MD state-park beaches will be
--      attached as MD ingestion surfaces them.
--
--   3. state_dogs_policy MD seed UPSERT — honest last-resort fallback
--      that ACKNOWLEDGES no statewide leash law exists. Replaces the
--      2026-05-14 seed which mis-framed COMAR as the statewide
--      default. Cited as "no statewide leash law" + ordinance_ref
--      noting where the state-park rule lives separately. Fired by
--      promote_to_gold's populate_from_state_default_gold() emitter
--      on every MD beach promotion; cascade ordering means city /
--      county / federal / state-park BPS rows override this fallback
--      where they exist. Only uncovered MD beaches inherit the honest
--      "unknown / mixed" fallback — never a falsely-attributed COMAR.
--
-- ─── COMAR 08.07.06.17 source details ──────────────────────────────
--
-- Source: COMAR 08.07.06.17 Pets, MD Department of Natural Resources
--   (Maryland Park Service).
-- Title 08 DNR, Subtitle 07 Forests and Parks, Chapter 08.07.06 Use
--   of State Parks, § 17 Pets.
-- Adopted effective October 11, 1993 (20:20 Md. R. 1568); amended.
--
-- Primary deep-link URL (regs.maryland.gov):
--   https://regs.maryland.gov/us/md/exec/comar/08.07.06.17
-- Cross-verified via:
--   - mdrules.elaws.us/comar/08.07.06.17 (eLaws mirror)
--   - law.cornell.edu/regulations/maryland/COMAR-08-07-06-17
--   - dnr.maryland.gov/publiclands/pages/pets.aspx (DNR plain-English)
--
-- Verbatim operative text (COMAR 08.07.06.17 "Pets"):
--   A. Developed Areas.
--     (1) Except for guide service dogs, pets of any kind are not
--         allowed on a picnic area, cabin area, camping area, or
--         other restricted area of a State park.
--     (2) The Service reserves the right to designate pet areas in
--         a State park. In these areas pets shall be on a leash with
--         a maximum length of 6 feet.
--   B. Undeveloped Areas.
--     Pets are permitted on a leash with a maximum length of 10
--     feet.
--
-- Per [[page-level-over-agency-level]]: source_url points at the §17
-- deep-link, NOT dnr.maryland.gov homepage.

begin;

-- ─── 1. Ensure MD DNR agency row (canonical bare name) ─────────────

INSERT INTO public.agency (name, type, short_name, web_url)
SELECT 'Maryland Department of Natural Resources',
       'state_department'::agency_type,
       'MD DNR',
       'https://dnr.maryland.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
   WHERE name = 'Maryland Department of Natural Resources'
     AND type = 'state_department'::agency_type
);

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
  'COMAR 08.07.06.17 Pets. A. Developed Areas. (1) Except for guide service dogs, pets of any kind are not allowed on a picnic area, cabin area, camping area, or other restricted area of a State park. (2) The Service reserves the right to designate pet areas in a State park. In these areas pets shall be on a leash with a maximum length of 6 feet. B. Undeveloped Areas. Pets are permitted on a leash with a maximum length of 10 feet. [Title 08 Department of Natural Resources, Subtitle 07 Forests and Parks, Chapter 08.07.06 Use of State Parks. Adopted effective October 11, 1993 (20:20 Md. R. 1568). Authoritative copy at regs.maryland.gov; mirrored at mdrules.elaws.us/comar/08.07.06.17 and law.cornell.edu/regulations/maryland/COMAR-08-07-06-17. MD DNR plain-English summary at dnr.maryland.gov/publiclands/pages/pets.aspx. Fetched via WebFetch 2026-05-22.] NOT a statewide leash law — applies only to lands inside the MD state-park system.',
  DATE '1993-10-11'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'COMAR 08.07.06.17%'
);

-- ─── 3. BPS rows: COMAR → Sandy Point SP + Greenwell SP ────────────
--
-- Both beaches explicitly deferred from their county migrations
-- (Anne Arundel: fid 6310347 Sandy Point SP; St Mary's: fid 18144392
-- Greenwell State - Pavilion) so they could attach here.
--
-- Sandy Point: COMAR §A(1) "not allowed on ... other restricted area"
-- supports MD Park Service's seasonal May 1 – Sep 30 prohibition in
-- the main swimming-beach section (operator-posted as a designated
-- restricted area in season). Base rule encoded as on_leash; seasonal
-- not_allowed window is a candidate for beach_policy_source_temporal
-- lift if/when the Park Service publishes a written compendium.
--
-- Greenwell Pavilion: standard COMAR designated-pet-area on_leash
-- (6-ft). No known seasonal carve-out.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       v.evidence,
       ps.source_url,
       v.note,
       NULL
FROM (values
  (6310347::bigint,
   'COMAR 08.07.06.17(A)(2): "The Service reserves the right to designate pet areas in a State park. In these areas pets shall be on a leash with a maximum length of 6 feet." Maryland Park Service operates Sandy Point State Park.',
   'Sandy Point SP main swimming beach has an operator-posted seasonal pet prohibition May 1 – Sep 30 (Memorial Day weekend area is a restricted area in season per §A(1)). Base rule on_leash; seasonal not_allowed window is a candidate for §9.5 temporal extraction if the Park Service publishes the seasonal designation in a written compendium. Walkthrough recommended.'),
  (18144392::bigint,
   'COMAR 08.07.06.17(A)(2): "In these areas pets shall be on a leash with a maximum length of 6 feet." Greenwell State Park is operated by the Maryland Park Service; the Pavilion area is a developed park section subject to the designated-pet-area 6-foot leash rule.',
   'Greenwell SP Pavilion area; no known seasonal carve-out. Walkthrough recommended to confirm posted signage.')
) AS v(fid, evidence, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'COMAR 08.07.06.17%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- ─── 4. Honest state_dogs_policy MD seed (no statewide leash law) ──
--
-- Replaces the 2026-05-14 seed which incorrectly cited "COMAR 08.01.07"
-- AS the statewide default. Maryland has NO statewide leash law. This
-- seed is the LAST-RESORT FALLBACK fired by
-- populate_from_state_default_gold() on every MD beach promotion.
-- Cascade ordering: city / county / federal / state-park BPS rows
-- override this fallback where they exist. For uncovered beaches the
-- fallback honestly says "local rules vary, nothing categorical" —
-- it does NOT falsely assert COMAR applies.
--
-- Mirrors OR / WA / CA pattern: cite the public-access framework
-- rather than a specific rule. MD has no equivalent to Oregon's
-- Beach Bill or California's Coastal Act creating a statewide
-- access regime, so the fallback is "unknown / mixed."

-- NOTE: state_dogs_policy has NO primary key / unique constraint on prod
-- (the 20260508 entity migration declared `state_code text primary key`
-- but it didn't survive — verified 2026-05-22 via pg_indexes query).
-- Using DELETE+INSERT instead of ON CONFLICT. Idempotent: the DELETE
-- is a no-op if no MD row exists; the INSERT always adds the corrected
-- row. Missing-PK + accumulated-dupes (OR has 2 rows in prod) is tracked
-- as a separate data-integrity follow-up.

DELETE FROM public.state_dogs_policy WHERE state_code = 'MD';

INSERT INTO public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
VALUES
  ('MD', 'Maryland', 'mixed', 'mixed',
   false, false,
   'Maryland has no statewide leash law. State-park system governed by COMAR 08.07.06.17 (6-ft developed / 10-ft undeveloped, no off-leash) — but that rule binds only inside state-park boundaries. Beyond state parks, leash rules are entirely creatures of local ordinances (counties + cities) which vary widely. Many private community / HOA waterfronts have only operator-posted rules. Public access on tidal beaches is recognized by common law (Maryland Public Trust Doctrine) but not via a statewide statute analogous to Oregon''s Beach Bill.',
   'https://dnr.maryland.gov/publiclands/pages/pets.aspx',
   '(no statewide leash law; state-park rule at COMAR 08.07.06.17)',
   'Last-resort fallback only. MD beaches inside state-park polygons get the COMAR 08.07.06.17 rule via explicit BPS rows (Sandy Point, Greenwell, etc. — see Part 3 of this migration). Municipal/county beaches via local codify (Anne Arundel, Cecil, St Mary''s, Calvert, Worcester, Ocean City migrations this same date). Federal-land beaches via federal codify. This row fires only for MD beaches not covered by any per-jurisdiction codify.',
   0.30,
   now());

commit;

-- ─── Notes / follow-ups ────────────────────────────────────────────
--
-- 1. Sandy Point SP seasonal prohibition (May 1 – Sep 30) is NOT
--    encoded as a temporal layer here. If MD Park Service publishes
--    a written compendium or signed designation, run
--    `scripts/extract_temporal_from_policy_source.py --ps-ids <id>`
--    against this ps to lift the seasonal not_allowed window into
--    beach_policy_source_temporal. Until then the base on_leash row
--    is conservative + correct (off-season behavior).
--
-- 2. Other MD state-park beaches (Janes Island SP, Point Lookout SP,
--    Calvert Cliffs SP, Assateague SP [state-park portion, not the
--    NS], etc.) likely exist in beaches_gold but were not enumerated
--    in the Phase 1 wave manifest. Add explicit BPS rows attaching
--    them to this COMAR ps as they surface in MD ingestion. Pattern:
--    extend the Part 3 INSERT with additional fids + per-beach
--    evidence quotes.
--
-- 3. Assateague Island specifically: the FEDERAL National Seashore
--    portion is handled by md_federal_codify.sql (NPS authority).
--    The MD STATE PARK portion (different unit, different authority)
--    attaches HERE to COMAR. Don't conflate the two.
--
-- 4. The state_dogs_policy fallback (Part 4 above) deliberately does
--    NOT cite COMAR as the source_url's operative rule. Cascade
--    behavior: any MD beach outside a state-park polygon AND outside
--    a city/county/federal BPS row inherits the honest "unknown /
--    mixed" fallback rather than a misattributed state-park rule.
