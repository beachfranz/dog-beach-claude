-- MD Worcester County codify — Title PS2 (Animals), Subtitle I (Animal Control)
--
-- Source: Worcester County Code, Public Safety Article, Title PS2 Animals,
--   Subtitle I Animal Control, § PS 2-101 (General provisions).
-- URL: https://ecode360.com/14018279 (Subtitle ZS1:I General Provisions of the
--   Animal Control subtitle; section anchor below).
-- Section deep-link: https://ecode360.com/14018282 (§ PS 2-101)
--   (verified via ecode360 chapter at ecode360.com/14011076, fetched
--   verbatim via Playwright 2026-05-22.)
--
-- Adopted: 8-11-1992 by Bill No. 92-13. Amendments: 4-19-2005 (Bill 05-3);
--   10-23-2018 (Bill 18-4); 6-15-2021 (Bill 21-3).
--
-- ─── Applicability (§ PS 2-101(d) — critical scoping) ────────────────
--
-- "The provisions of this Subtitle shall apply throughout the County
--  but shall not apply within the corporate limits of the Town of Ocean
--  City, it being the intention of this subsection that the prior
--  adoption of Ordinance No. 13 by the municipalities of Berlin, Snow
--  Hill and Pocomoke shall hereby be deemed to hold over as an adoption
--  hereof; provided, however, that any municipality in which this
--  Subtitle is applicable may exempt itself from the provisions hereof
--  by appropriate corporate act of such municipality."
--
-- → Ocean City is EXCLUDED. Handled by separate city codify (Ocean City
--   Municipal Code Ch 6 Animals at library.municode.com/md/ocean_city).
-- → This migration covers UNINCORPORATED Worcester only.
--
-- ─── Operative rules ────────────────────────────────────────────────
--
-- § PS 2-101(i) Animals not to run at large (verbatim):
--   "It shall be unlawful for any person to permit a dog, cat or other
--   animal owned or harbored by him to run at large. Any such animal
--   running at large may be apprehended by the Animal Control Warden..."
--
-- § PS 2-101(b) Definitions (verbatim, AT LARGE):
--   "AT LARGE — Any animal will be deemed to be 'at large' when it is
--   off the property of its owner and not under the control of a
--   responsible person."
--
-- § PS 2-101(s) Dogs required to be on leash (verbatim):
--   "In any area described as a leash area in Subsection (t) hereof, it
--   shall be unlawful for any person, or guest of such person, to
--   knowingly, unknowingly, inadvertently, or intentionally permit a dog
--   owned or harbored by him to walk, crawl, or run in any location
--   other than the property of such person unless such dog is attached
--   to a leash."
--
-- § PS 2-101(t) Leash areas (verbatim):
--   "Leash areas may be designated by law or resolution. The following
--   are leash areas: (1) Ocean Pines Area... (2) County-owned property.
--   Any property owned by the County Commissioners of Worcester County,
--   Maryland and whereon the Commissioners have posted a sign indicating
--   that the property is a leash area and which has designated it a
--   leash area by resolution."
--
-- § PS 2-101(u) Clean up (verbatim):
--   "In any leash area, the person in control of a dog on a leash shall
--   remove all excrement deposited by the dog on property other than
--   property of the owner or property of the person in control of the
--   dog and dispose of such excrement in a legal and proper way."
--
-- ─── Per-beach mapping ──────────────────────────────────────────────
--
-- 13 scoreable Worcester County beaches; 12 are Ocean City (EXCLUDED
-- per § PS 2-101(d)). Only 1 in scope here:
--
--   fid 17006526 — Public Landing Beach (Snow Hill 21863)
--     • Unincorporated Worcester County (Public Landing is an
--       unincorporated community ~6.5 mi east of Snow Hill town).
--     • Owned/operated by Worcester County Recreation & Parks
--       (worcesterrecandparks.org/parks/public-landing).
--     • Subject to § PS 2-101(i) at-large prohibition baseline;
--       leash requirement applies under § PS 2-101(t)(2) if Commissioners
--       have posted signage + resolution designating it a leash area.
--     • At a county-owned developed beach/pier/marina park, the
--       at-large prohibition + posted-leash provision yield a baseline
--       on_leash rule.
--
-- Ocean City beaches EXCLUDED from this migration (handled by separate
-- Ocean City city codify migration):
--   3277290, 3835020, 6397096, 6798134, 9162300, 11965328, 12306921,
--   14910035, 16500596, 16553107, 16766773, 19712160.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Worcester County" type=county agency row exists yet (verified
-- 2026-05-22). Create canonical bare-name row before inserting the
-- policy_source row.

-- ─── 1. Provision Worcester County agency (canonical bare-name) ─────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Worcester County', 'county'::agency_type, 'https://www.co.worcester.md.us/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Worcester County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Worcester County Code, Title PS2 Animals, Subtitle I, § PS 2-101 (Animal Control — General provisions; at-large prohibition and posted-leash-area authorization for county-owned property)',
       (SELECT id FROM public.agency WHERE name='Worcester County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/14018282',
       E'Worcester County Code, Public Safety Article, Title PS2 Animals, Subtitle I Animal Control, § PS 2-101 General provisions. [Adopted 8-11-1992 by Bill No. 92-13; amended 4-19-2005 by Bill No. 05-3; 10-23-2018 by Bill No. 18-4; 6-15-2021 by Bill No. 21-3.] '
       '(b) Definitions. AT LARGE — Any animal will be deemed to be "at large" when it is off the property of its owner and not under the control of a responsible person. LEASH — Any lead, leash, cord, remote controlled electronic collar, or other restraint preventing an animal from moving more than fifteen feet from the person holding the electronic device or restraint opposite the end attached to the animal. '
       '(d) Applicability. The provisions of this Subtitle shall apply throughout the County but shall not apply within the corporate limits of the Town of Ocean City, it being the intention of this subsection that the prior adoption of Ordinance No. 13 by the municipalities of Berlin, Snow Hill and Pocomoke shall hereby be deemed to hold over as an adoption hereof; provided, however, that any municipality in which this Subtitle is applicable may exempt itself from the provisions hereof by appropriate corporate act of such municipality. '
       '(i) Animals not to run at large; impoundment. It shall be unlawful for any person to permit a dog, cat or other animal owned or harbored by him to run at large. Any such animal running at large may be apprehended by the Animal Control Warden or other designated officer and may be impounded in the animal pound. '
       '(s) Dogs required to be on leash. In any area described as a leash area in Subsection (t) hereof, it shall be unlawful for any person, or guest of such person, to knowingly, unknowingly, inadvertently, or intentionally permit a dog owned or harbored by him to walk, crawl, or run in any location other than the property of such person unless such dog is attached to a leash. '
       '(t) Leash areas. Leash areas may be designated by law or resolution. The following are leash areas: (1) Ocean Pines Area. All that property located in the Third Tax District of Worcester County, Maryland bound on the northwest by Beauchamp Road, the north and northeast by the St. Martin''s River, on the southeast by the southeasterly line of the Ocean Pines Subdivision and on the southwest by MD Route 589. (2) County-owned property. Any property owned by the County Commissioners of Worcester County, Maryland and whereon the Commissioners have posted a sign indicating that the property is a leash area and which has designated it a leash area by resolution. '
       '(u) Clean up. In any leash area, the person in control of a dog on a leash shall remove all excrement deposited by the dog on property other than property of the owner or property of the person in control of the dog and dispose of such excrement in a legal and proper way. '
       '[Hosted on ecode360 (General Code) at ecode360.com/14018282; chapter root ecode360.com/14011076. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Worcester County Code, Title PS2%§ PS 2-101%'
);

-- ─── 3. Per-beach BPS row — Public Landing Beach (Snow Hill) ────────
--
-- Only non-Ocean-City Worcester County scoreable beach. Unincorporated
-- (Public Landing community); Worcester County owns + operates.
-- Citywide leash inference applies via § PS 2-101(t)(2) county-owned
-- property authorization plus § PS 2-101(i) at-large prohibition.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 17006526, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Worcester County Code § PS 2-101(i): "It shall be unlawful for any person to permit a dog, cat or other animal owned or harbored by him to run at large." § PS 2-101(t)(2) designates County-owned posted property as a leash area; Public Landing Beach is owned and operated by Worcester County Recreation & Parks (unincorporated Public Landing community, ~6.5 mi east of Snow Hill town).',
       ps.source_url,
       'Public Landing Beach is unincorporated Worcester County (NOT inside Snow Hill or Ocean City corporate limits). County code Subtitle I applies directly. Walkthrough recommended to confirm on-site posted leash signage per § PS 2-101(t)(2); even absent posting, § PS 2-101(i) at-large prohibition requires owner control off-property.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Worcester County Code, Title PS2%§ PS 2-101%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
