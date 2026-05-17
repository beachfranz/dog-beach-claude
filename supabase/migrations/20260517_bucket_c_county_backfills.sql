-- Bucket C: county-code backfills for 3 unlinked CA tier-1+2 beaches.
--
-- Per the 2026-05-17 morning handoff (session_handoff_2026_05_17_morning),
-- 4 county-only-governance CA tier-1+2 beaches remained unlinked after
-- Wave 2/3. Glenn (1 beach) is an explicit defer; this migration handles
-- the other three:
--
--   fid 9136  San Quentin Beach       Marin County        (tier 1_off-leash)
--   fid 6804  Chambers Landing Beach  Placer County       (tier 2_on-leash)
--   fid 7209  Cocklebur Beach         Stanislaus County   (tier 2_on-leash)
--
-- Each beach is in unincorporated county territory (no city PIP) and has
-- the county agency at precedence_rank=1 for dog_policy. None has any
-- existing beach_policy_source link.
--
-- All three URLs are PAGE-LEVEL deep links (see [[page-level-over-agency-level]]
-- HARD RULE) — chapter-level for Marin (the Municode SPA does not render
-- section-anchor URLs as standalone pages), section-level for Placer and
-- Stanislaus on ecode360.
--
-- Sources fetched verbatim via Playwright 2026-05-17.

-- ─── 1. Marin County — Title 8 Chapter 8.04 (Animal Control) ──────
--
-- §8.04.176 "Leash required in San Quentin Village" (Ord. No. 3820,
-- 2024) explicitly governs the residential development on Point San
-- Quentin in the vicinity of San Quentin State Prison. San Quentin
-- Beach (OSM way 856533985) is the small bay-shore beach at Point San
-- Quentin, directly adjacent to and accessed via that residential
-- development.
--
-- Note: beach is tier 1_off-leash in our taxonomy (some off-leash
-- signal exists in extracted data), but the 2024 ordinance requires
-- a leash no longer than six feet in any public area within San
-- Quentin Village. The tier-1 signal likely reflects pre-2024
-- informal practice; the codified rule supersedes. Walkthrough
-- recommended to confirm any 2024+ posted signage on the beach.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Marin County Code §8.04.176 (Leash required in San Quentin Village)',
       (SELECT id FROM public.agency WHERE name='Marin County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/marin_county/codes/municipal_code?nodeId=TIT8AN_CH8.04ANCO',
       'Marin County Code Title 8 Chapter 8.04 §8.04.176 — Leash required in San Quentin Village. (a) It is unlawful for the owner or person having control of any dog to allow the dog to roam or be at large in any public area or public right-of-way within San Quentin Village without being restrained by a leash, which shall be no longer than six feet. (b) The prohibition against off-leash dogs in this section does not apply to any peace officer or to any public employee or volunteer participating in search and rescue operations. (c) Any duly authorized peace officer or animal services officer shall be entitled to immediately seize and impound any dog found running at large within San Quentin Village and may take such other action as may be reasonably necessary for the protection of public health and safety. (d) Violation of the section shall be punishable as an infraction as specified in California Government Code section 25132. (e) For purposes of this section, San Quentin Village means the residential development on Point San Quentin in the vicinity of San Quentin State Prison, including Main Street, McKenzie Street, San Quentin Terrace and Penny Terrace, as depicted in Marin County Assessors Map Book 18, page 16. (Ord. No. 3820, § II, 2024). Companion rule §8.04.160 prohibits dogs at large in any public park. Fetched verbatim via Playwright 2026-05-17 from Municode.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Marin County Code §8.04.176%'
);

-- ─── 2. Placer County — Chapter 6 Article 6.08 (Animal Control) ───
--
-- §6.08.010 "Violations" makes it unlawful for the owner of any
-- animal to "allow any animal to run at large." §6.04.020 defines
-- "at large" as "an animal off the premises of its owner and not
-- under restraint by lead, leash or adequate enclosure" — i.e.,
-- implicit leash requirement off-premises across unincorporated
-- Placer County. Chambers Landing Beach (Homewood, unincorporated
-- west Lake Tahoe) is therefore subject to the leash rule.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Placer County Code §6.08.010 (Violations — animal at large) + §6.04.020 (Definition of "at large")',
       (SELECT id FROM public.agency WHERE name='Placer County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/43573391',
       'Placer County Code Chapter 6 (Animals), Article 6.08 §6.08.010 — Violations. It is unlawful for the owner of any animal to violate any of the provisions of this chapter hereinbefore or hereinafter set out, or to commit any of the following acts: A. To allow any animal to run at large; B. To allow any animal to trespass upon public property or upon any private property without the consent of the owner of the property; [...] L. To allow any dog known to be potentially dangerous or vicious to run at large upon any street or other public place within the county; [...] (Ord. 5653-B § 12, 2011). §6.04.020 Definitions — "At large" means an animal off the premises of its owner and not under restraint by lead, leash or adequate enclosure. Exceptions: dog assisting in herding/guarding livestock; dog assisting peace officer; dog actually participating in training/obedience class or exhibition with property owner permission; dog accompanying person engaged in legal hunting. Fetched verbatim via Playwright 2026-05-17 from ecode360 (PL4987).'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Placer County Code §6.08.010%'
);

-- ─── 3. Stanislaus County — Title 18 Chapter 18.06 §18.06.040 ─────
--
-- §18.06.040 "Animals" (Ord. CS 1392, 4/29/2025) is the most
-- directly-applicable rule for any county park or recreation area:
-- "Dogs must be kept on a leash at all times, except in designated
-- areas." (E) Walking leash ≤6 ft (G); stake leash ≤10 ft (F).
-- Cocklebur Beach (7394 Eastman Rd, Oakdale, unincorporated
-- Stanislaus County) is on the Stanislaus River and falls under
-- county park/recreation jurisdiction.
--
-- Title 18 Ch 18.18 "Stanislaus River Special Use Areas" was also
-- inspected but governs motorboat/horsepower only — no dog rule.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Stanislaus County Code §18.06.040 (Animals — Parks and Recreation)',
       (SELECT id FROM public.agency WHERE name='Stanislaus County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/46578782',
       'Stanislaus County Code Title 18 (Parks and Recreation), Chapter 18.06 (General Recreation Area Regulations), §18.06.040 — Animals. A. Every individual with a disability has the right to be accompanied by a service animal especially trained for the purpose, in places generally open to the public without being required to pay an extra charge for the service animal. However, the individual shall be liable for any damage done to the premises or facilities by their animal. B. Any animal, including a service animal, may be excluded from the park if that animal''s behavior poses a direct threat to the health or safety of others. C. Owners of dogs over four months of age must provide proof of licensure or proof of current rabies vaccination upon request by park staff or other responsible county staff. D. Authorized agents have the right to refuse entry or mandate the removal of any animal if necessary. This decision may be based on considerations of safety, health regulations, or compliance with specific policies. E. Dogs must be kept on a leash at all times, except in designated areas. F. A stake leash must not exceed ten feet in length. G. A walking leash must not exceed six feet in length. H. The preceding leash requirement shall not apply to dog training permittees who are operating in areas designated by the director or authorized agent for dog training. I. Any animal must be kept under full control at all times ensuring that it does not pose a risk or nuisance to others. J. Horses are permitted at posted designated areas at regional parks with an additional fee as defined in the current adopted fee schedule. K. All owners or handlers shall be responsible for retrieving and properly disposing of fecal material in a sanitary manner. L. It is unlawful to wound, kill or catch any bird or other wild animal in any park, except where hunting is permitted by the director or authorized agent, and the hunter possesses a valid permit. M. It is unlawful to dump, dispose or abandon any animal. (Ord. CS 1392, 4/29/2025). Fetched verbatim via Playwright 2026-05-17 from ecode360 (Stanislaus County).'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Stanislaus County Code §18.06.040%'
);

-- ─── 4. Link San Quentin Beach (fid 9136) → on_leash ──────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9136, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Marin County Code §8.04.176(a): "It is unlawful for the owner or person having control of any dog to allow the dog to roam or be at large in any public area or public right-of-way within San Quentin Village without being restrained by a leash, which shall be no longer than six feet." San Quentin Beach is the bay-shore beach at Point San Quentin, directly adjacent to the residential streets enumerated in §8.04.176(e) (Main Street, McKenzie Street, San Quentin Terrace, Penny Terrace).',
       ps.source_url,
       'Beach is tier 1_off-leash in our taxonomy (extracted off-leash signal). 2024 ordinance (Ord. 3820) postdates much of that signal — codified rule now requires 6-ft leash within San Quentin Village. Walkthrough recommended to confirm 2024+ posted signage on the beach itself and whether the bay-shore beach is considered inside Village boundaries per Marin County Assessors Map Book 18 page 16.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Marin County Code §8.04.176%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 5. Link Chambers Landing Beach (fid 6804) → on_leash ─────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6804, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Placer County Code §6.08.010(A) prohibits allowing any animal "to run at large." §6.04.020 defines "at large" as "an animal off the premises of its owner and not under restraint by lead, leash or adequate enclosure." Chambers Landing Beach (Homewood, unincorporated west Lake Tahoe) is on county-jurisdiction land — leash required by the implicit-leash rule across all unincorporated Placer County off-premises locations.',
       ps.source_url,
       'Chambers Landing has a historic private resort/marina; on-site posted signage may impose stricter or looser rules in concessioned areas — walkthrough recommended.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Placer County Code §6.08.010%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 6. Link Cocklebur Beach (fid 7209) → on_leash ────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 7209, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Stanislaus County Code §18.06.040(E): "Dogs must be kept on a leash at all times, except in designated areas." Walking leash must not exceed six feet (G); stake leash must not exceed ten feet (F). Cocklebur Beach (7394 Eastman Rd, Oakdale, unincorporated Stanislaus County) is a Stanislaus River beach falling under county Parks and Recreation jurisdiction.',
       ps.source_url,
       'Confirm via walkthrough whether the specific beach is a county-owned/operated recreation area vs. USACE federal land — if USACE, 36 CFR §327.11 applies as primary instead.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Stanislaus County Code §18.06.040%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
