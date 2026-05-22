-- MD Frederick County codify — Ch 1-5 (Animals and Fowl), Art II Div 1
-- § 1-5-24 (Running at Large)
--
-- Inland Catoctin / Piedmont county — only 1 scoreable beach in scope, and
-- it's HOA-private (DEFERRED per playbook). The county-code policy_source
-- row is still provisioned so future public-access beaches in Frederick
-- County (if/when ingested) can hang bps rows off it without redoing
-- platform discovery.
--
-- Cunningham Falls SP (fid 2631955) is in Frederick County but is OUT OF
-- SCOPE for this migration — it's already attached to COMAR 08.07.05.04
-- via the state-park baseline migration (20260522_md_state_park_baseline_v2).
--
-- ─── Beach in scope ─────────────────────────────────────────────────
--
--   fid 9231329  Coldstream Beach  (Lake Linganore, New Market) — DEFER
--
-- ─── Sources ────────────────────────────────────────────────────────
--
-- (a) Frederick County, Maryland Code of Ordinances, Part I: Frederick
--     County Code, Chapter 1-5 ANIMALS AND FOWL, Article II Dogs, Cats and
--     Other Animals, Division 1 Generally — § 1-5-24 RUNNING AT LARGE.
--     Adopted by Ord. 94-24-119 (11-15-1994); amended by Ord. 14-23-678
--     (11-13-2014); most recently amended by Bill No. 26-02 (3-3-2026).
--     URL (section deep-link): https://codelibrary.amlegal.com/codes/frederickcounty/latest/frederickco_md/0-0-0-1130
--     Hosted on amlegal (Frederick County canonical platform; slug
--     `frederickcounty`). Validated 2026-05-22.
--
-- ─── Operative rule (verbatim) ──────────────────────────────────────
--
-- § 1-5-24 RUNNING AT LARGE.
--   It shall be unlawful for an owner to allow any animal to run at
--   large. An animal that is participating in a qualified activity is
--   not at large unless the animal leaves the activity.
--   (Ord. 94-24-119, 11-15-1994; Ord. 14-23-678, 11-13-2014;
--    Bill No. 26-02, 3-3-2026.)
--
-- "At large" defined elsewhere in Ch 1-5 (incorporated via § 1-5-1
-- definitions): an animal not on the owner's real property and not under
-- the immediate physical control of a responsible person capable of
-- physically restraining the animal.
--
-- Rule classification: territorial leash/at-large rule applies countywide
-- to all property not owned by the dog's owner — per [[citywide-leash-
-- inference]] this is the public-floor `on_leash` rule for any public
-- beach in unincorporated Frederick County.
--
-- ─── Per-beach decision ─────────────────────────────────────────────
--
-- fid 9231329  Coldstream Beach — DEFER (HOA-private, no public bps).
--
--   Coldstream Beach sits on Lake Linganore, a 210-acre privately-owned
--   reservoir (Maryland's largest privately-owned lake) impounding
--   Linganore Creek east of New Market, MD. Lake Linganore and its three
--   sand beaches (Coldstream, Nightingale, Pinehurst) are managed by the
--   Lake Linganore Association (LLA), an HOA. The third-party Sandee
--   listing names it "Coldstream Private Beach" and explicitly states
--   "this Beach is not dog-friendly" — the operative restriction here
--   comes from the LLA's posted policy, not the county code. Frederick
--   County Code § 1-5-24 does not extend to private HOA waterbodies as
--   the controlling restraint authority on association-owned property
--   (the county rule applies to animals at large in the public realm —
--   it doesn't supersede a private property's own access/animal rules).
--
--   Per playbook defer rubric (HOA-private = bounded operator; no public
--   bps row written). Pattern matches Lake Lariat Beach (fid 1519331,
--   Chesapeake Ranch Estates HOA, Calvert County). When LLA's posted
--   dog/pet rules are obtained, attach an `operator_posted_policy` row
--   under a Lake Linganore Association operator/agency record.
--
-- ─── Judgment calls ────────────────────────────────────────────────
--
-- 1. ps row IS provisioned (not skipped). Even though Coldstream defers,
--    the Frederick County § 1-5-24 codified rule is the canonical public-
--    floor for the county. Future Frederick County public beaches (if a
--    public-access waterbody surfaces — currently none beyond Coldstream
--    and Cunningham Falls SP) can hang bps rows off this ps without
--    repeating platform discovery.
-- 2. Cunningham Falls SP (fid 2631955) is EXPLICITLY EXCLUDED — already
--    COMAR-attached via state-park baseline. No bps row written here.
-- 3. The City of Frederick has its own Municode-hosted code (Ch 3 Animals
--    at library.municode.com/md/frederick/codes/code_of_ordinances?nodeId=
--    PTIITHCO_CH3AN) which would apply to any incorporated-city public
--    waterbody. No in-scope beaches sit within the City of Frederick
--    corporate limits; city code is documented here as a footnote for
--    future expansion but no city agency/ps row is provisioned.
-- 4. No `beach_policy_source` INSERT statement appears in this migration.
--    That's the deferral signal — agency + ps are recorded; per-beach
--    attachment awaits public-access surface area in the county.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- Create canonical bare-name agency row if missing.

-- ─── 1. Provision Frederick County agency ───────────────────────────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Frederick County', 'county'::agency_type, 'https://www.frederickcountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Frederick County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source row ─────────────────

-- (a) Frederick County Ch 1-5 § 1-5-24 (countywide running-at-large / leash)

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Frederick County Code, Chapter 1-5 Animals and Fowl, Article II Dogs, Cats and Other Animals, Division 1 Generally, § 1-5-24 (Running at Large)',
       (SELECT id FROM public.agency WHERE name='Frederick County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/frederickcounty/latest/frederickco_md/0-0-0-1130',
       $body$Frederick County, Maryland Code of Ordinances, Part I: Frederick County Code, Chapter 1-5 ANIMALS AND FOWL, Article II Dogs, Cats and Other Animals, Division 1 Generally.

§ 1-5-24 RUNNING AT LARGE.
   It shall be unlawful for an owner to allow any animal to run at large. An animal that is participating in a qualified activity is not at large unless the animal leaves the activity.
(Ord. 94-24-119, 11-15-1994; Ord. 14-23-678, 11-13-2014; Bill No. 26-02, 3-3-2026.)

[Companion §§ for context:]

§ 1-5-22 DANGEROUS AND POTENTIALLY DANGEROUS DOGS. (F)(2): Permit a potentially dangerous dog off the owner's real property unless the potentially dangerous dog is under the immediate physical control of a responsible adult and restrained by a chain or leash. (F)(3): Fail to keep a dangerous dog within the owner's real property except for medical treatment or examination. When removed from the owner's property for medical treatment or examination, the dangerous dog shall be caged or under the control of a responsible adult capable of physically restraining the animal, and muzzled and restrained with a chain or leash, not exceeding four feet in length.

§ 1-5-23 FEMALES IN HEAT. The owner of a female dog or cat which is in oestrus or in a condition commonly known as "in heat" or "in season" shall keep such dog or cat confined in a secure enclosure or under the immediate physical control of the owner.

"At large" per § 1-5-1 definitions: an animal not on the owner's real property and not under the immediate physical control of a responsible person capable of physically restraining the animal.

[Hosted on amlegal (codelibrary.amlegal.com) at /codes/frederickcounty/latest/frederickco_md/0-0-0-1130. Frederick County slug `frederickcounty`. Most recent amendment: Bill No. 26-02, effective 3-3-2026. Fetched verbatim via Playwright 2026-05-22.]
$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Frederick County Code, Chapter 1-5%§ 1-5-24%'
);

-- ─── 3. Per-beach BPS rows ──────────────────────────────────────────
--
-- NONE in this migration.
--
-- The single in-scope Frederick County beach (Coldstream Beach,
-- fid 9231329) is HOA-private (Lake Linganore Association). Per playbook
-- defer rubric, HOA-private beaches receive NO public-code bps row;
-- attach `operator_posted_policy` under an LLA operator/agency when those
-- rules are documented.
--
-- Cunningham Falls SP (fid 2631955) intentionally excluded — already
-- COMAR-attached via the MD state-park baseline migration.
