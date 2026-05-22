-- MD Baltimore County codify — Code of Ordinances, Article 12 Animals
-- (§ 12-3-110 ANIMAL AT LARGE PROHIBITED + § 12-1-101(c) definition).
--
-- NOTE: This is Baltimore COUNTY, MD — NOT the City of Baltimore. Baltimore
-- City is independent of the county; its code is separate and is not
-- addressed here. All beaches in this migration sit in unincorporated /
-- county-jurisdiction territory.
--
-- ─── Sources ─────────────────────────────────────────────────────────
--
-- (1) Baltimore County, MD Code of Ordinances, Article 12 Animals,
--     Title 3 Animal Welfare, Subtitle 1 Owner Responsibilities,
--     § 12-3-110 Animal at Large Prohibited (with § 12-1-101 definitions
--     + § 12-1-110 civil penalties cross-references).
--     Section deep-link:
--       https://library.municode.com/md/baltimore_county/codes/code_of_ordinances?nodeId=ART12AN_TIT3ANWE_SUBTITLE_1OWRE_S12-3-110ANLAPR
--     Article root:
--       https://library.municode.com/md/baltimore_county/codes/code_of_ordinances?nodeId=ART12AN
--     (Hosted on Municode. doc_slug=code_of_ordinances. Fetched verbatim
--     via Playwright 2026-05-22.)
--
-- ─── Operative rules — verbatim ──────────────────────────────────────
--
-- § 12-3-110 Animal at large prohibited.
--   "An owner of an animal may not allow the animal to be an animal at
--   large."
--   (1988 Code, § 6-13) (Bill No. 3-00, § 4, 7-1-2004; Bill No. 63-01,
--   § 1, 8-10-2001)
--
-- § 12-1-101(c) Definitions — Animal at large.
--   (1)(i) "Animal at large" means any animal off the premises of its
--          owner and not under the control, charge, or possession of
--          the owner or other responsible person.
--   (1)(ii) "Animal at large" includes any dog off the premises of its
--          owner and not under the control of the owner or other
--          responsible person by a leash, cord, or chain.
--   (2)  "Animal at large" does not include: (i) a dog on the premises
--        of another property owner with the permission of the property
--        owner or lessee; (ii) a dog being used for hunting or being
--        trained for hunting except within a county owned or Board of
--        Education property; (iii) a feral cat with an eartip; or
--        (iv) a dog authorized to be within the fenced area of a
--        designated Baltimore County dog park...
--
-- § 12-1-110(f) Civil penalties.
--   First violation: $30. Repeated violations: $150.
--
-- ─── Rule classification ─────────────────────────────────────────────
--
-- § 12-3-110 + § 12-1-101(c)(1)(ii) is a countywide territorial restraint
-- requirement: any dog off the owner's premises must be under control by
-- a "leash, cord, or chain". The definition explicitly names physical
-- tether ("leash, cord, or chain") — NOT voice control — as the form of
-- control required for dogs. Voice control is NOT an enumerated
-- alternative. Sibling MD codify migrations (Cecil, Kent) classified
-- similar "restraint OR control" formulations as on_leash; this one is
-- even more clearly on_leash because the leash requirement for dogs is
-- explicit in the operative definition.
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash/at-large
-- rules apply to beaches by default; explicit beach mention is not
-- required.
--
-- Designated county dog-park exemption (§ 12-1-101(c)(2)(iv)) does not
-- attach to any in-scope beach in this migration (no Baltimore County
-- dog park co-located with Miami Beach).
--
-- ─── Per-beach mapping (1 in-scope; 3 excluded) ─────────────────────
--
-- IN SCOPE (this migration writes 1 bps row):
--   - 13756  Miami Beach — small Middle River sand beach near Essex,
--            unincorporated Baltimore County. Not within any state park,
--            federal land, or municipal corporation. § 12-3-110
--            on_leash applies as the codified baseline.
--
-- EXCLUDED FROM BPS (per task scope; covered by state-park baseline):
--   - 19499672 Gunpowder Falls State Park – Hammerman Area
--   - 12995222 Hart-Miller Island State Park Beach
--   - 20146661 Hill Billy Beach
--   Already attached to COMAR 08.07.06.17 (ps id 310; Maryland Park
--   Service Use of State Parks) via 20260522_md_state_park_baseline_v2.
--   The county Animals article still applies as a layered baseline to
--   these state-park beaches, but the playbook directs that per-beach
--   bps be written ONLY for beaches NOT already covered by a higher /
--   site-specific source. The county ps row IS inserted (it's its own
--   codified record), but no county bps is written for the 3 SP beaches
--   to avoid layering noise on the COMAR-controlled record.
--
-- ─── Judgment calls ─────────────────────────────────────────────────
--
-- (a) Single in-scope beach. Per playbook tenet "include county code as
--     its own ps row but write BPS only for genuinely-uncovered beaches"
--     this migration emits one ps row + one bps row (Miami Beach).
--
-- (b) Hill Billy Beach (fid 20146661) sits inside Gunpowder Falls SP
--     boundary per state-park baseline v2 attribution; treating as
--     COMAR-controlled per task brief. If later evidence shows it is
--     OUTSIDE the SP polygon, a follow-up migration can add a county
--     bps row by referencing this same ps row (insert is idempotent
--     under ON CONFLICT).
--
-- (c) No "Baltimore" or generic "Baltimore" agency row exists. Inserting
--     a canonical bare-name "Baltimore County" / type=county row to
--     match sibling MD county agencies (Cecil, Calvert, Anne Arundel,
--     Charles, Queen Anne's, Dorchester, Kent, etc.). Web URL is
--     baltimorecountymd.gov (the official county portal).
--
-- (d) Miami Beach geographic context: ST_Centroid = POINT(-76.3703
--     39.3060), placing it on the Middle River near Essex/Bowleys
--     Quarters in eastern Baltimore County. Not in Baltimore City
--     (which sits to the west, independent of the county). Not within
--     any incorporated municipality (Baltimore County has no
--     incorporated cities/towns — it is the only entirely-
--     unincorporated county in Maryland).

-- ─── 1. Provision Baltimore County agency ────────────────────────────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Baltimore County', 'county'::agency_type, 'https://www.baltimorecountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Baltimore County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source row ──────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Baltimore County Code of Ordinances, Article 12 Animals, Title 3 Animal Welfare, Subtitle 1 Owner Responsibilities, § 12-3-110 (Animal at Large Prohibited) + § 12-1-101(c) Definitions',
       (SELECT id FROM public.agency WHERE name='Baltimore County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/md/baltimore_county/codes/code_of_ordinances?nodeId=ART12AN_TIT3ANWE_SUBTITLE_1OWRE_S12-3-110ANLAPR',
       $body$Baltimore County, Maryland Code of Ordinances, Article 12 Animals [Bill No. 3-00, § 4, 7-1-2004 et seq.]. § 12-3-110 Animal at Large Prohibited: "An owner of an animal may not allow the animal to be an animal at large." (1988 Code, § 6-13) (Bill No. 3-00, § 4, 7-1-2004; Bill No. 63-01, § 1, 8-10-2001). --- § 12-1-101(c) DEFINITIONS — Animal at large. (1)(i) "Animal at large" means any animal off the premises of its owner and not under the control, charge, or possession of the owner or other responsible person. (1)(ii) "Animal at large" includes any dog off the premises of its owner and not under the control of the owner or other responsible person by a leash, cord, or chain. (2) "Animal at large" does not include: (i) A dog on the premises of another property owner with the permission of the property owner or lessee; (ii) A dog being used for hunting or being trained for hunting except within a county owned or Board of Education property; (iii) A feral cat with an eartip; or (iv) A dog authorized to be within the fenced area of a designated Baltimore County dog park operated by the Animal Services Division, the Department of Recreation and Parks or any other person authorized by the county. --- § 12-1-110(f) Civil Penalties: (1)(i)1. A first violation of this article is $30; 2. Repeated violations of this article is $150. --- Enforcement: Health Officer / Animal Services Division of the Health Department (§ 12-1-103, § 12-1-107, § 12-1-108). Appeal to the Animal Hearing Board and then to the Board of Appeals (§ 12-1-114). --- Applicability: Baltimore County is the only entirely-unincorporated county in Maryland; Article 12 applies uniformly across the unincorporated jurisdiction. The County is jurisdictionally distinct from Baltimore City (an independent city not part of the county). Article 12 does NOT apply within Maryland State Park boundaries (COMAR 08.07.06.17 controls inside state parks) but DOES apply to all other unincorporated county territory including waterfront on the Middle River, Back River, Bird River, Gunpowder River, Patapsco River, and Chesapeake Bay frontage. [Hosted on Municode. doc_slug=code_of_ordinances; section nodeId=ART12AN_TIT3ANWE_SUBTITLE_1OWRE_S12-3-110ANLAPR. Article root nodeId=ART12AN. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Baltimore County Code of Ordinances, Article 12 Animals%§ 12-3-110%'
);

-- ─── 3. Per-beach BPS — Miami Beach (fid 13756) ──────────────────────
--
-- ONE in-scope beach. The 3 state-park beaches (19499672, 12995222,
-- 20146661) are EXCLUDED per task brief — already attached to COMAR
-- 08.07.06.17 via ps id 310 (Maryland Park Service state-park
-- baseline).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 13756, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $body$Baltimore County Code § 12-3-110: "An owner of an animal may not allow the animal to be an animal at large." § 12-1-101(c)(1)(ii) defines an "animal at large" to include "any dog off the premises of its owner and not under the control of the owner or other responsible person by a leash, cord, or chain." Countywide territorial leash/at-large requirement applies to beaches by default; the definition names physical tether (leash/cord/chain) as the required form of control for dogs. Miami Beach is unincorporated Baltimore County waterfront on the Middle River near Essex — outside any state park, federal land, or municipal corporation — so § 12-3-110 is the operative codified rule.$body$,
       ps.source_url,
       'Small public-access sand beach on the Middle River in eastern Baltimore County (unincorporated; near Essex/Bowleys Quarters). No higher-tier source (state park, federal refuge, operator-posted rule) identified. § 12-3-110 on_leash baseline applies.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Baltimore County Code of Ordinances, Article 12 Animals%§ 12-3-110%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
