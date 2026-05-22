-- MD St. Mary's County codify — Chapter 212 Animal Control Regulations
--
-- Source: St. Mary's County Code of Ordinances, Part II General Legislation,
--   Chapter 212 ANIMAL CONTROL REGULATIONS (re-enacted 7-16-2024 by Ord.
--   No. 2024-18; supersedes prior 3-7-2017 Ord. No. 2017-03).
--
-- URL (chapter deep-link via Municode): https://library.municode.com/md/st._mary's_county/codes/code_of_ordinances?nodeId=PTIIGELE_CH212ANCORE
-- Cross-verified eLaws mirror: http://stmaryscounty-md.elaws.us/code/coor_ptii_ch212
--
-- ─── Operative rules (Sec. 212-8 Animal at large — verbatim) ────────
--
-- A. An owner or custodian shall keep domestic animals confined in a
--    building or secure enclosure, or under physical restraint and direct
--    supervision.
--
-- B. Exceptions. An owner or custodian may allow a domestic animal to be
--    outside of a building or secured enclosure, or released from physical
--    restraint and not directly supervised if:
--    (1) the animal is on property owned or occupied by the animal's owner;
--    (2) a dog is undergoing supervised obedience training by a certified
--        professional;
--    (3) a dog is being trained or used for hunting; or
--    (4) an animal is at large with a person in immediate pursuit with the
--        purpose of reacquiring control of the animal.
--
-- C. An owner or custodian shall not permit any domestic animal to be on
--    public or private school grounds when school is in session, or in a
--    public recreation area, excluding a public dog park, unless the
--    animal is controlled by a leash.
--
-- D. Penalty. Violations of subsection A or B of this section are a civil
--    offense punishable by fine not to exceed one thousand dollars
--    ($1,000.00).
--
-- (Ord. No. 2024-18, § I, 7-16-2024)
--
-- Rule classification: on_leash. §212-8(A) establishes the countywide
-- physical-restraint baseline (no off-leash off-owner-property). §212-8(C)
-- explicitly requires a leash in any "public recreation area" (which
-- covers public beaches and waterfront parks). Per
-- [[citywide-leash-inference]] (territorial leash rule → beaches by
-- default), all St. Mary's County public-access beaches inherit on_leash.
--
-- ─── Per-beach mapping ──────────────────────────────────────────────
--
-- 13 scoreable beaches in St. Mary's County. Per-beach disposition:
--
--   Unincorporated St. Mary's County (county code §212-8 applies directly,
--   no incorporated-municipality overlay — Leonardtown is the county's only
--   incorporated town and none of these beaches sit inside its corporate
--   limits):
--     fid 3672326  Cedar Cove Community Beach (Lexington Park CDP)
--     fid 5262464  Chesapeake Beach (Lexington Park CDP)
--     fid 12962    Lane Beach
--     fid 1479467  Myrtle Point Wet Sox Trail (California CDP)
--     fid 12963    Port Sunlight Beach
--     fid 12957    Saint George Island Beach (St. George Island CDP)
--     fid 12958    Saint Jerome Beach
--     fid 12964    Sandyland Beach
--     fid 18174945 Sandyland Beach (duplicate row — Leonardtown address;
--                  not inside Leonardtown corporate limits per geom)
--     fid 14308733 Small Beach (Mechanicsville CDP)
--     fid 4055277  Wicomico (Luckton Point) (Mechanicsville CDP)
--
--   Private/operator-overlay context (county code still applies; bps row
--   added with status_note flagging operator dimension):
--     fid 19004779 Sanner's Lake (Patuxent River Sportsmen's Club —
--                  membership lake; if publicly accessible the county
--                  at-large + leash rule applies; otherwise the operator
--                  controls access. Conservative: include with note.)
--
--   EXCLUDED from this migration (handled by separate authority):
--     fid 18144392 Greenwell State - Pavilion — Greenwell State Park is
--                  MD DNR (Maryland Park Service) and governed by
--                  COMAR 08.07.06.17 (Pets), already shipped via
--                  20260522_md_state_baseline_codify.sql. Layered county
--                  code application deferred — state-park beaches inherit
--                  the COMAR baseline as primary authority.
--
-- ─── Judgment calls ────────────────────────────────────────────────
--
-- 1. No incorporated-municipality overlay. Leonardtown is the only town
--    in St. Mary's County, and none of the 13 beaches sit inside its
--    corporate limits (address_city='Leonardtown' on fid 18174945 is a
--    postal designation, not a corporate-limits assertion). Uniform
--    county-code application is correct.
-- 2. §212-8(B)(3) hunting exception is preserved in the verbatim but does
--    not override the per-beach on_leash classification — recreation-area
--    leash requirement in §212-8(C) trumps for public beach use.
-- 3. State-park beach (Greenwell) deliberately excluded; COMAR §08.07.06.17
--    is the primary authority and already wired.
-- 4. Sanner's Lake included with conservative on_leash + status_note
--    flagging the operator overlay; county code is the legal floor.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "St. Mary's County" type=county agency row exists yet (verified
-- 2026-05-22). Create canonical bare-name row before inserting the
-- policy_source row.

-- ─── 1. Provision St. Mary's County agency (canonical bare-name) ────

INSERT INTO public.agency (name, type, web_url)
SELECT 'St. Mary''s County', 'county'::agency_type, 'https://www.stmaryscountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='St. Mary''s County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'St. Mary''s County Code of Ordinances, Part II General Legislation, Chapter 212 Animal Control Regulations, § 212-8 (Animal at large — physical-restraint baseline and public-recreation-area leash requirement)',
       (SELECT id FROM public.agency WHERE name='St. Mary''s County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/md/st._mary''s_county/codes/code_of_ordinances?nodeId=PTIIGELE_CH212ANCORE',
       E'St. Mary''s County Code of Ordinances, Part II General Legislation, Chapter 212 ANIMAL CONTROL REGULATIONS. [Animal Control Regulations previously adopted 3-7-2017 by Ord. No. 2017-03 have been repealed and reenacted 7-16-2024 by Ord. No. 2024-18.] '
       'Sec. 212-1 Applicability. This Chapter applies to all animals kept in St. Mary''s County except as specifically provided. '
       'Sec. 212-8 Animal at large. A. An owner or custodian shall keep domestic animals confined in a building or secure enclosure, or under physical restraint and direct supervision. '
       'B. Exceptions. An owner or custodian may allow a domestic animal to be outside of a building or secured enclosure, or released from physical restraint and not directly supervised if: (1) the animal is on property owned or occupied by the animal''s owner; (2) a dog is undergoing supervised obedience training by a certified professional; (3) a dog is being trained or used for hunting; or (4) an animal is at large with a person in immediate pursuit with the purpose of reacquiring control of the animal. '
       'C. An owner or custodian shall not permit any domestic animal to be on public or private school grounds when school is in session, or in a public recreation area, excluding a public dog park, unless the animal is controlled by a leash. '
       'D. Penalty. Violations of subsection A or B of this section are a civil offense punishable by fine not to exceed one thousand dollars ($1,000.00). '
       'E. An Animal Control Officer who observes an animal at large that is posing a direct threat to public safety may pursue that animal on public or private property. '
       'F. A dog shall be deemed a "stray dog" if the dog''s owner violates this section three (3) or more times and the dog is subsequently found at large. '
       'G. Stray dogs may be seized by Animal Control. '
       'I. If a violation of this section is also a violation of Title 10 Subtitle 6 of Criminal Law Article the Annotated Code of Maryland, nothing in this section shall be construed to lessen any criminal penalties available under that Subtitle. '
       '(Ord. No. 2024-18, § I, 7-16-2024) '
       '[Hosted on Municode at library.municode.com/md/st._mary''s_county/codes/code_of_ordinances (chapter nodeId PTIIGELE_CH212ANCORE). Cross-verified at stmaryscounty-md.elaws.us/code/coor_ptii_ch212. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'St. Mary''s County Code of Ordinances%§ 212-8%'
);

-- ─── 3. Per-beach BPS rows — 12 in-scope beaches (on_leash) ─────────
--
-- Greenwell State - Pavilion (fid 18144392) deliberately EXCLUDED — MD
-- DNR / COMAR 08.07.06.17 baseline applies (see header).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'St. Mary''s County Code § 212-8(A): "An owner or custodian shall keep domestic animals confined in a building or secure enclosure, or under physical restraint and direct supervision." § 212-8(C): "An owner or custodian shall not permit any domestic animal to be ... in a public recreation area, excluding a public dog park, unless the animal is controlled by a leash." (Ord. No. 2024-18, § I, 7-16-2024)',
       ps.source_url, NULL, NULL
FROM (values (3672326),(5262464),(12962),(1479467),(12963),(12957),(12958),(12964),(18174945),(14308733),(4055277)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'St. Mary''s County Code of Ordinances%§ 212-8%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. Sanner's Lake — county code applies; flag operator overlay ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 19004779, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'St. Mary''s County Code § 212-8(A) physical-restraint baseline + § 212-8(C) public-recreation-area leash requirement apply countywide. (Ord. No. 2024-18, § I, 7-16-2024)',
       ps.source_url,
       'Sanner''s Lake is operated by the Patuxent River Sportsmen''s Club (membership lake). County animal-control code is the legal floor; operator-posted rules may add restrictions. Verify public access + on-site signage during walkthrough; if fully private, reclassify as operator_posted_policy.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'St. Mary''s County Code of Ordinances%§ 212-8%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
