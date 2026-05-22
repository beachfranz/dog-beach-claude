-- 20260522_md_ocean_city_codify.sql
--
-- MD OCEAN CITY codify — Chapter 6 Animals, Article II Dogs, §6-34 Unlawful acts.
--
-- Worcester County's animal-control code (Worcester County Code § PS 2-101(d))
-- EXCLUDES the Town of Ocean City; OC is governed by its OWN code, captured
-- here. See sibling migration 20260522_md_worcester_county_codify.sql.
--
-- Source: Town of Ocean City, MD — Code of Ordinances Ch 6 Animals, Art II Dogs.
-- Platform: Municode (library.municode.com/md/ocean_city).
-- Deep-link (§6-34 Unlawful acts):
--   https://library.municode.com/md/ocean_city/codes/code_of_ordinances?nodeId=PTIICOOR_CH6AN_ARTIIDO_S6-34UNAC
-- Chapter root (for context):
--   https://library.municode.com/md/ocean_city/codes/code_of_ordinances?nodeId=PTIICOOR_CH6AN
--
-- Adopted: Code 1972, § 23-5.2; recodified Code 1999, § 6-34. Amended by
--   Ord. No. 1999-13 (4-15-1999) and Ord. No. 2007-15 (7-2-2007).
--   Penalty provisions updated by Ord. No. 2025-27 (11-3-2025) at §6-4.
--   Charter authority: § C-414(20).
--
-- ─── Operative rule (verbatim — §6-34 Unlawful acts) ─────────────────
--
-- It shall be unlawful for any owner or person having possession of a dog,
-- or for any other person, to:
--   ...
--   (19) Allow an unleashed dog to be on the public streets, ways or other
--        public property, except under subsection (25) of this section, or
--        on property of others; and, in no event, shall a leash exceed
--        eight feet in length.
--   ...
--   (23) Permit the person's dog to be in any public recreational area
--        unless such dog is controlled by leash, except in an official
--        off-leash area established by the Mayor and City Council of
--        Ocean City.
--   (24) Allow a dog to be on the public beaches or boardwalk of the
--        municipality during the period from May 1 through September 30
--        of each year.
--   (25) Allow a dog to run at large on the public beaches of the
--        municipality from October 1 through April 30 of each year.
--
-- Cross-reference: §6-36(b) — Any dog on the public beaches or boardwalks
-- of Ocean City, whether leashed, under control or running at large during
-- the period from May 1 through September 30 of each year, may also be
-- seized [by OCPD / OC Beach Patrol / animal control].
--
-- ─── Rule interpretation per beach ──────────────────────────────────
--
-- Ocean City's 12 scoreable beaches (all "public beaches" within OC city
-- limits) share ONE codified rule structure:
--
--   * May 1 – Sep 30  : NOT_ALLOWED  (full prohibition on beach + boardwalk,
--                                     per §6-34(24); seizure authorized by
--                                     §6-36(b))
--   * Oct 1 – Apr 30  : OFF_LEASH    (explicitly permitted to "run at large"
--                                     on public beaches per §6-34(25),
--                                     overriding the citywide 8-ft leash law
--                                     of §6-34(19) for THE BEACH ONLY; the
--                                     boardwalk remains under §6-34(19)
--                                     leash + 8-ft-max year-round)
--
-- Encoding strategy (per playbook §6 + Del Mar walkthrough):
--   * Write ONE bps row per beach with base rule = off_leash + region NULL
--     (the off-season state, codified by §6-34(25)). status_note records
--     the May 1 – Sep 30 prohibition + boardwalk distinction.
--   * Run scripts/extract_temporal_from_policy_source.py --ps-ids <new id>
--     as §9.5 post-apply to lift the seasonal NOT_ALLOWED window (May 1 –
--     Sep 30) into beach_policy_source_temporal.
--   * Per [[citywide-leash-inference]], the city-wide §6-34 rule applies
--     to all public beaches within OC corporate limits by default; explicit
--     per-beach mention is not required.
--
-- 12 beaches mapped (all Worcester County, Town of Ocean City, scoring_tier
-- 'daily'):
--   fid 3277290  Ocean City Beach Maryland (1313 Washington Ln)
--   fid 3835020  The Ocean City beach     (11600–11640 Atlantic Ave)
--   fid 6397096  130th Street Beach       (12902 Assawoman Dr)
--   fid 6798134  110th Street Beach       (11000 Coastal Hwy)
--   fid 9162300  28th Street Beach        (2711 Atlantic Ave)
--   fid 11965328 15th Street Beach        (1405 Atlantic Ave)
--   fid 12306921 The Quay Unit 502        (10700 Coastal Hwy)
--   fid 14910035 Ocean City Beach         (6 Tenth St)
--   fid 16500596 2nd Street Beach         (200 S Atlantic Ave)
--   fid 16553107 Ocean City Beach         (7300 Atlantic Ave)
--   fid 16766773 Nick's Beach Stand       (8500 Coastal Hwy)
--   fid 19712160 9th Street Beach         (7 Eighth St)
--
-- No per-street region carve-outs in the code text — §6-34(24)/(25) apply
-- uniformly to "the public beaches of the municipality." Boardwalk-specific
-- rules differ (no off-season exemption: boardwalk = leashed year-round per
-- §6-34(19) + same May–Sep prohibition per §6-34(24)) but boardwalk is a
-- separate surface from the sand and not represented as a separate beach
-- entity here.

begin;

-- ─── 1. Ensure Ocean City agency row (canonical bare name) ────────────

INSERT INTO public.agency (name, type, short_name, web_url)
VALUES (
  'Ocean City',
  'city',
  'OC MD',
  'https://oceancitymd.gov/'
)
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. Insert codified policy_source for OC Code §6-34 ───────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT
  'municipal_code',
  'Ocean City, MD Code of Ordinances §6-34 (Unlawful acts) — Dogs on public beaches and boardwalk',
  (SELECT id FROM public.agency
    WHERE name='Ocean City' AND type='city'),
  ARRAY['dog_policy']::text[],
  'https://library.municode.com/md/ocean_city/codes/code_of_ordinances?nodeId=PTIICOOR_CH6AN_ARTIIDO_S6-34UNAC',
  $$Ocean City, MD Code of Ordinances, Part II Code of Ordinances, Chapter 6 ANIMALS, Article II DOGS, §6-34 Unlawful acts.

It shall be unlawful for any owner or person having possession of a dog, or for any other person, to: ...
(19) Allow an unleashed dog to be on the public streets, ways or other public property, except under subsection (25) of this section, or on property of others; and, in no event, shall a leash exceed eight feet in length.
(23) Permit the person's dog to be in any public recreational area unless such dog is controlled by leash, except in an official off-leash area established by the Mayor and City Council of Ocean City.
(24) Allow a dog to be on the public beaches or boardwalk of the municipality during the period from May 1 through September 30 of each year.
(25) Allow a dog to run at large on the public beaches of the municipality from October 1 through April 30 of each year.

Cross-reference §6-36(b): Any dog on the public beaches or boardwalks of Ocean City, whether leashed, under control or running at large during the period from May 1 through September 30 of each year, may also be seized [by OCPD / OC Beach Patrol / animal control officer].

[Code 1972, § 23-5.2; Code 1999, § 6-34; Ord. No. 1999-13 (4-15-1999); Ord. No. 2007-15 (7-2-2007). Penalty provisions at §6-4 most recently amended by Ord. No. 2025-27 (11-3-2025). Charter authority: § C-414(20). Hosted on Municode at library.municode.com/md/ocean_city. Fetched verbatim via Playwright 2026-05-22.]$$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'Ocean City, MD Code of Ordinances §6-34%'
);

-- ─── 3. Per-beach BPS rows ────────────────────────────────────────────
--
-- Base rule = off_leash (the §6-34(25) Oct 1 – Apr 30 "run at large" state,
-- which is the codified default when the seasonal prohibition does not
-- apply). The May 1 – Sep 30 NOT_ALLOWED window will be lifted into
-- beach_policy_source_temporal by the post-apply temporal extractor
-- (§9.5 of the playbook).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'off_leash', 'operative'::operative_status,
       'Ocean City, MD Code §6-34(25): "Allow a dog to run at large on the public beaches of the municipality from October 1 through April 30 of each year." Read together with §6-34(24) which prohibits dogs on the public beaches OR boardwalk May 1 through September 30; §6-36(b) authorizes seizure of any dog on the beach or boardwalk during the prohibited window regardless of leash status.',
       ps.source_url,
       'Seasonal: dogs NOT ALLOWED on beach or boardwalk May 1 – Sep 30 (§6-34(24); peak beach season). Dogs may run at large off-leash on the sand Oct 1 – Apr 30 (§6-34(25)). Boardwalk remains leashed year-round outside the seasonal prohibition (§6-34(19), max 8-ft leash). Enforcement: OC Police Dept, OC Beach Patrol, and animal control may seize any dog present during the prohibited window. Temporal carve-outs to be lifted to beach_policy_source_temporal via post-apply extractor.',
       NULL
FROM (values
        (3277290::bigint),
        (3835020::bigint),
        (6397096::bigint),
        (6798134::bigint),
        (9162300::bigint),
        (11965328::bigint),
        (12306921::bigint),
        (14910035::bigint),
        (16500596::bigint),
        (16553107::bigint),
        (16766773::bigint),
        (19712160::bigint)
     ) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Ocean City, MD Code of Ordinances §6-34%'
ON CONFLICT (beach_fid, policy_source_id, section, COALESCE(region_name, '__default__'), rule)
DO NOTHING;

commit;

-- ─── §9.5 follow-up (run AFTER apply, NOT in this migration) ──────────
--
-- After applying this migration:
--
--   python scripts/extract_temporal_from_policy_source.py \
--     --ps-ids <new_ps_id_for_oc_6-34>
--
-- Expected temporal extraction:
--   * window_kind='seasonal', exception_rule='not_allowed',
--     effective_from_md='05-01', effective_to_md='09-30'
--     (the §6-34(24) Memorial-Day-ish-through-Sep prohibition)
--
-- This will populate beach_policy_source_temporal for all 12 beaches via
-- the trigger cascade; bdp.dogs_prohibited_start/end will be set; and
-- zone_rules.sections.sand will gain a time_windows[] entry inverting
-- the base off_leash rule to not_allowed during the seasonal window.
--
-- ─── Follow-ups / notes ───────────────────────────────────────────────
--
-- 1. Boardwalk: §6-34(19) requires a leash (max 8 ft) on all public ways
--    year-round; §6-34(24) adds full prohibition May 1 – Sep 30. Boardwalk
--    is a distinct surface from the sand and not modeled as a separate
--    beach entity here. If a future "boardwalk" section is introduced to
--    beach_policy_source, write a parallel bps row with section='boardwalk'
--    + rule='on_leash' (base) + same seasonal temporal carve-out.
--
-- 2. The Quay Unit 502 (fid 12306921) is a condo-tower address at 10700
--    Coastal Hwy. The bordering Atlantic beachfront is public beach per
--    the OC code; the unit-number address suggests this row may need
--    beaches_gold re-classification. Worth a separate audit pass.
--
-- 3. State baseline (COMAR 08.07.06.17 — MD DNR park-service pet rules)
--    is captured by 20260522_md_state_baseline_codify.sql; that rule
--    governs MD state parks (Assateague SP etc.), NOT OC municipal beaches.
--    OC has no state-park overlay.
--
-- 4. Federal overlay: Assateague Island National Seashore (NPS) lies
--    SOUTH of Ocean City; not represented in this batch. Tracked
--    separately under federal NPS codify (36 CFR §2.15 + Assateague
--    Compendium).
