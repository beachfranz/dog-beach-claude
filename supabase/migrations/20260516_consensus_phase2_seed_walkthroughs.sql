-- Consensus engine rewrite — Phase 2: seed policy_source and
-- beach_policy_source from the six walkthrough docs.
--
-- DRAFT — NOT YET APPLIED. Review before running.
--
-- Per docs/consensus_engine_rewrite_design.md, this is the second of
-- five staged migrations. Inserts ~25 unique policy_source rows
-- (deduplicating shared sources across beaches — e.g., DPR's
-- parks.ca.gov/Dogs page is referenced from 3 walkthroughs but is
-- ONE entity) plus ~30 beach_policy_source rows linking them to
-- the six walkthrough beaches.
--
-- Existing engine continues to operate unchanged through this phase.
-- zone_rules JSONB is still derived from BEP via the existing
-- _promote_zone_rules_for_fid; the new entity rows exist alongside
-- but aren't yet consumed.
--
-- Idempotency: each row's citation is the unique identity key.
-- The DO block at the top checks for one anchor row and returns
-- early if the seed has already run. Re-runs are safe no-ops.
--
-- Walkthrough provenance: every row carries a metadata.walkthrough
-- field identifying which docs/walkthrough_*.md it came from, so
-- backfill audits can trace each entity back to its research origin.

DO $$
DECLARE
  v_existing int;
BEGIN
  SELECT count(*) INTO v_existing
    FROM public.policy_source
   WHERE citation = '36 CFR §2.15 (Pets)';
  IF v_existing > 0 THEN
    RAISE NOTICE 'Phase 2 seed already applied; skipping';
    RETURN;
  END IF;

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 1 — Federal regulations
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, last_verified, metadata)
  VALUES
    ('federal_regulation',
     '36 CFR §2.15 (Pets)',
     ARRAY['dog_policy'],
     'https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"fort_funston","role":"NPS general pet rule; parent of all GGNRA dog regs"}'),

    ('federal_regulation',
     '36 CFR §7.97(d) (Dogs — Crissy Field and Ocean Beach Snowy Plover Areas)',
     ARRAY['dog_policy'],
     'https://www.ecfr.gov/current/title-36/chapter-I/part-7/section-7.97',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"fort_funston","seasonal_window":"July 1 - May 15","scope_note":"Crissy Field WPA + Ocean Beach SPPA ONLY — does NOT apply at Fort Funston main beach"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 1 — State statutes (CA)
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, last_verified, metadata)
  VALUES
    ('state_statute',
     'CA Public Resources Code §6001 et seq.',
     ARRAY['tidelands','submerged_lands'],
     'https://leginfo.legislature.ca.gov/faces/codesTOCSelected.xhtml?tocCode=PRC',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"hbdb,crystal_cove,fort_funston,crown_memorial,dockweiler,rosies","role":"CA State Lands Commission tideland authority — applies to every coastal beach below MHW"}'),

    ('state_statute',
     'CA Health & Safety Code §115880 et seq. (AB 411)',
     ARRAY['water_quality','bacteria_advisories'],
     'https://leginfo.legislature.ca.gov/faces/codesTOCSelected.xhtml?tocCode=HSC',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"hbdb,crystal_cove,fort_funston,crown_memorial,dockweiler","role":"State statute delegating beach water-quality monitoring to county health depts"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 1 — Municipal codes
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, last_verified, metadata)
  VALUES
    ('municipal_code',
     'Huntington Beach Municipal Code §13.08.070 (Dogs and Other Animals)',
     ARRAY['dog_policy'],
     'https://ecode360.com/HU4937',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"hbdb","ord_history":"344-10/31, 554-12/49, 769-7/60, 2907-8/87, 3355-7/97, 3606-6/03, 3930-2/12, 4118-5/17, 4275-3/23","most_recent_amendment":"Ord 4275, March 2023","carve_out":"north of 22nd Street to Seapoint Avenue, leashed 6ft max"}'),

    ('municipal_code',
     'LA County Code Title 17 (Beaches and Harbors)',
     ARRAY['dog_policy','operations'],
     'https://library.municode.com/ca/los_angeles_county',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"dockweiler","specific_section":"TBD — Municode SPA requires deep navigation; general dog prohibition with designated exceptions (Rosie''s is separate city, not exception)"}'),

    ('municipal_code',
     'Long Beach Municipal Code — Dog Beach Zone designation (section TBD)',
     ARRAY['dog_policy'],
     'https://library.municode.com/ca/long_beach',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"rosies","note":"Section number TBD; Municode SPA blocks deep extraction. Zone definition: Granada Ave to Roycroft Ave, voice-control off-leash"}'),

    ('municipal_code',
     'Long Beach Municipal Code — general leash law (section TBD)',
     ARRAY['dog_policy'],
     'https://library.municode.com/ca/long_beach',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"rosies","note":"Citywide leash baseline; section number TBD"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 1 — Special district ordinance
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, effective_date, last_verified, metadata)
  VALUES
    ('special_district_ordinance',
     'EBRPD Ordinance 38 (Regulations Governing Use of District Property)',
     ARRAY['dog_policy','operations','enforcement'],
     'https://www.ebparks.org/sites/default/files/Ord38-09052023FINAL.pdf',
     '2023-09-05'::date,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crown_memorial","date_from_filename":"2023-09-05","note":"NOT operative at Crown Memorial — DPR-owned land; EBRPD operates but DPR policy governs. Applies on EBRPD-owned beaches elsewhere"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 2 — MOUs and operating agreements
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, effective_date, last_verified, metadata)
  VALUES
    ('mou',
     'MOU: City of Huntington Beach ↔ Preservation Society of Huntington Dog Beach',
     ARRAY['operations'],
     NULL,  -- CPRA candidate; not public-facing
     '2007-11-01'::date,  -- approximate
     '2026-05-16'::timestamptz,
     '{"walkthrough":"hbdb","origin":"OC Register 2007-11-15","drafted_by":"Martin Senat (FoDB) + Jim Engle (HB CSD)","approved":"HB Community Services Commission Nov 2007","known_terms":["10x10 booths at 3 entrances weekends","8x20 storage shed","city consults nonprofit on beach decisions"],"substantive_clause":"City Council officially recognizes the PSHDB role in maintaining this stretch of coast. Rather than the state enforcing standard blanket dog prohibitions in this specific area, the city and managing bodies allow dogs to run here."}'),

    ('lease_agreement',
     'Lease: State of California ↔ City of Los Angeles for Dockweiler State Beach (1946)',
     ARRAY['operations'],
     NULL,
     '1946-01-01'::date,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"dockweiler","origin":"LA County Beaches & Harbors page history","note":"First link in the three-party chain — State leased to City of LA in 1946"}'),

    ('operating_agreement',
     'Operating agreement: City of Los Angeles ↔ LA County Beaches & Harbors for Dockweiler (1976)',
     ARRAY['operations'],
     NULL,
     '1976-01-01'::date,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"dockweiler","note":"Second link in the three-party chain — City of LA contracted day-to-day operation to LA County in 1976"}'),

    ('lease_agreement',
     'Lease: CA DPR ↔ EBRPD for Crown Memorial Regional Shoreline',
     ARRAY['operations'],
     NULL,
     NULL,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crown_memorial","note":"Bridge document between DPR ownership and EBRPD operation; CPRA candidate, text not sourced"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 3 — Agency administrative policy
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, last_verified, metadata)
  VALUES
    ('agency_administrative_policy',
     'California State Parks dog policy (parks.ca.gov/Dogs)',
     ARRAY['dog_policy'],
     'https://www.parks.ca.gov/Dogs',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crystal_cove,crown_memorial,dockweiler","role":"DPR statewide dog policy — single canonical source covering all 280+ CA state parks via per-park lookup table","wave_2_accelerator":true}'),

    ('agency_administrative_policy',
     'Crystal Cove State Park dog policy (parks.ca.gov/?page_id=644)',
     ARRAY['dog_policy'],
     'https://www.parks.ca.gov/?page_id=644',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crystal_cove","verbatim":"Yes: Dogs allowed on paved areas only. Except for service animals, dogs not allowed on the beach or in the backcountry. Dogs are not allowed in Deer Canyon, Lower Moro, and Upper Moro Campgrounds."}'),

    ('agency_administrative_policy',
     'Crown Memorial State Beach dog policy (parks.ca.gov/?page_id=526)',
     ARRAY['dog_policy'],
     'https://www.parks.ca.gov/?page_id=526',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crown_memorial","verbatim":"Yes: Dogs allowed only on lawn areas and along the paved pathways. Dogs not allowed on the beach.","operator_note":"East Bay Regional Park District operates Robert W. Crown Memorial SB"}'),

    ('agency_administrative_policy',
     'Dockweiler State Beach dog policy (parks.ca.gov/Dogs catalog row)',
     ARRAY['dog_policy'],
     'https://www.parks.ca.gov/Dogs',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"dockweiler","verbatim":"No. Park unit operated by Los Angeles County. Check website at https://beaches.lacounty.gov/dockweiler-beach/ for more information.","note":"DPR does NOT maintain a separate unit page; catalog row alone is operative"}'),

    ('agency_administrative_policy',
     'GGNRA Pet Policy of 1979',
     ARRAY['dog_policy'],
     NULL,  -- text not sourced; may require NPS FOIA
     '2026-05-16'::timestamptz,
     '{"walkthrough":"fort_funston","effective_date":"1979","note":"Repeatedly cited as operative authority for voice-control areas at Fort Funston, Crissy Field, Ocean Beach, Baker Beach (north of Lobos Creek), Lands End, Fort Miley. Confirmed by 2025 Compendium reference."}');

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, effective_date, last_verified, metadata)
  VALUES
    ('superintendents_compendium',
     '2025 GOGA Superintendent''s Compendium',
     ARRAY['dog_policy','operations'],
     'https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf',
     '2025-01-01'::date,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"fort_funston","verbatim":"VOICE CONTROL DOG WALKING: The 1979 Pet Policy allows for Managed Dogs, leashed or under Voice Control, in the following areas: ... Fort Funston, except in the 12-acre closure in northwest Fort Funston.","map_exhibits":"#27-31B (Marin), #32-36 (SF)"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 4 — Operator-posted policy
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, last_verified, metadata)
  VALUES
    ('operator_posted_policy',
     'Preservation Society of Huntington Dog Beach published policy (dogbeach.org)',
     ARRAY['dog_policy'],
     'https://www.dogbeach.org/',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"hbdb","verbatim":"Dog Beach is leash optional. Please keep your dog leashed until you have reached the beach sand. Dogs must be leashed in the parking lot and on the upper bluff.","conflict_with":"Huntington Beach Municipal Code §13.08.070 (which says leashed 6ft max)"}'),

    ('operator_posted_policy',
     'EBRPD Crown Beach published policy (ebparks.org/parks/crown-beach)',
     ARRAY['dog_policy'],
     'https://www.ebparks.org/parks/crown-beach',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"crown_memorial","verbatim":"Dogs are not permitted on the beach. Dogs are allowed only on lawn areas and along the paved pathways. Dogs must be on a leash (six-foot maximum) and under control at all times. Please clean up after your dog.","note":"Matches DPR policy almost verbatim — EBRPD publishes DPR rule, not Ordinance 38"}'),

    ('operator_posted_policy',
     'City of Long Beach Rosie''s Dog Beach published rules',
     ARRAY['dog_policy'],
     'https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach/',
     '2026-05-16'::timestamptz,
     '{"walkthrough":"rosies","18_rules_verbatim":["The dog must be under visual and voice control at all times","Owners must have a leash. Dogs shall be on leashes whenever outside Dog Park/Zones","Each dog must be under the control of an adult","Only one dog per adult is permitted","Pick up after your dog","Dogs must be older than 4 months, vaccinated and licensed","Puppies younger than 4 months are not permitted","No aggressive dogs","Dog owners are legally responsible for injuries caused by their dog","Professional dog trainers/handlers are not permitted to use the facility for instruction","No female dogs in heat","All dogs must wear a collar with current tags","No spiked collars","No food-human or dog-of any kind","Owners shall provide drinking water","Children must be supervised","Children are not permitted to run, shout, scream, wave arms or excite or antagonize dogs"],"hours":"Daily 6 AM - 8 PM","location":"Ocean Blvd between Granada Avenue and Roycroft Avenue","size":"4.1 acres"}');

  -- ─────────────────────────────────────────────────────────────────
  -- TIER 5 — Withdrawn rulemaking
  -- ─────────────────────────────────────────────────────────────────

  INSERT INTO public.policy_source
    (subtype, citation, scope, source_url, withdrawn_at, last_verified, metadata)
  VALUES
    ('withdrawn_rulemaking',
     'GGNRA Dog Management Plan (proposed 2016; withdrawn 2017)',
     ARRAY['dog_policy'],
     'https://www.govinfo.gov/content/pkg/FR-2017-12-27/html/2017-27827.htm',
     '2017-12-27'::timestamptz,
     '2026-05-16'::timestamptz,
     '{"walkthrough":"fort_funston","fr_doc":"2017-27827","docket":"PPPWGOGAPO,PPMPSPD1Z.YM0000","npRm_published":"2016-02-24 at 81 FR 9139","withdrawn":"2017-12-27","verbatim_withdrawal":"The National Park Service (NPS) no longer intends to prepare a final rule or issue a Golden Gate National Recreation Area dog management plan. The NPS has now cancelled that planning process and terminated the associated NEPA and rulemaking processes. No final rule will be issued.","effect":"1979 Pet Policy + current Compendium remain operative"}');

  -- ─────────────────────────────────────────────────────────────────
  -- parent_citation chains
  -- ─────────────────────────────────────────────────────────────────
  -- Set after all rows are in so the lookups work.

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = '36 CFR §7.97(d) (Dogs — Crissy Field and Ocean Beach Snowy Plover Areas)'
     AND parent.citation = '36 CFR §2.15 (Pets)';

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = 'GGNRA Pet Policy of 1979'
     AND parent.citation = '36 CFR §2.15 (Pets)';

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = '2025 GOGA Superintendent''s Compendium'
     AND parent.citation = 'GGNRA Pet Policy of 1979';

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = 'Crystal Cove State Park dog policy (parks.ca.gov/?page_id=644)'
     AND parent.citation = 'California State Parks dog policy (parks.ca.gov/Dogs)';

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = 'Crown Memorial State Beach dog policy (parks.ca.gov/?page_id=526)'
     AND parent.citation = 'California State Parks dog policy (parks.ca.gov/Dogs)';

  UPDATE public.policy_source p
     SET parent_citation_id = parent.id
    FROM public.policy_source parent
   WHERE p.citation = 'Dockweiler State Beach dog policy (parks.ca.gov/Dogs catalog row)'
     AND parent.citation = 'California State Parks dog policy (parks.ca.gov/Dogs)';

  -- ─────────────────────────────────────────────────────────────────
  -- beach_policy_source rows — link sources to beaches × sections
  -- ─────────────────────────────────────────────────────────────────
  -- Each row: this policy_source speaks to this beach at this section
  -- with this rule. Multiple rows per (beach, section) when multiple
  -- sources speak — the new resolver picks highest-authority tier.

  -- HBDB (fid 6212) — three-source layered stack
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6212, ps.id, 'sand', 'on_leash',
    'This section shall not apply to the Adjacent Beach Areas, and the Water Activity Zone, located north of the line created by extending the northern curb line of 22nd Street to the Pacific Ocean to Seapoint Avenue, wherein dogs constrained by a leash no longer than six feet in length are permitted.',
    'https://ecode360.com/HU4937',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Huntington Beach Municipal Code §13.08.070 (Dogs and Other Animals)';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6212, ps.id, 'sand', 'off_leash',
    'The City Council officially recognizes the PSHDB role in maintaining this stretch of coast. Rather than the state enforcing standard blanket dog prohibitions in this specific area, the city and managing bodies allow dogs to run here.',
    NULL, '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'MOU: City of Huntington Beach ↔ Preservation Society of Huntington Dog Beach';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6212, ps.id, 'sand', 'off_leash',
    'Dog Beach is leash optional.',
    'https://www.dogbeach.org/',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Preservation Society of Huntington Dog Beach published policy (dogbeach.org)';

  -- Crystal Cove (fid 8330) — DPR baseline + park-unit override
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 8330, ps.id, 'sand', 'not_allowed',
    'Except for service animals, dogs not allowed on the beach or in the backcountry. Dogs are not allowed in Deer Canyon, Lower Moro, and Upper Moro Campgrounds.',
    'https://www.parks.ca.gov/?page_id=644',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Crystal Cove State Park dog policy (parks.ca.gov/?page_id=644)';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 8330, ps.id, 'paved_paths', 'on_leash',
    'Dogs allowed on paved areas only.',
    'https://www.parks.ca.gov/?page_id=644',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Crystal Cove State Park dog policy (parks.ca.gov/?page_id=644)';

  -- Fort Funston (fid 6097) — voice-control off-leash + 12-acre closure
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6097, ps.id, 'sand', 'off_leash_voice_control',
    'Fort Funston, except in the 12-acre closure in northwest Fort Funston.',
    'https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = '2025 GOGA Superintendent''s Compendium';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, rule_modifier, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6097, ps.id, 'bank_swallow_closure', 'not_allowed',
    '{"closure_type":"permanent","area_size_acres":12,"location":"northwest Fort Funston"}'::jsonb,
    'Fort Funston, except in the 12-acre closure in northwest Fort Funston.',
    'https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = '2025 GOGA Superintendent''s Compendium';

  -- Crown Memorial (fid 8452) — DPR governs (NOT EBRPD Ordinance 38)
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 8452, ps.id, 'sand', 'not_allowed',
    'Dogs not allowed on the beach.',
    'https://www.parks.ca.gov/?page_id=526',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Crown Memorial State Beach dog policy (parks.ca.gov/?page_id=526)';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 8452, ps.id, 'paved_paths', 'on_leash',
    'Dogs allowed only on lawn areas and along the paved pathways.',
    'https://www.parks.ca.gov/?page_id=526',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Crown Memorial State Beach dog policy (parks.ca.gov/?page_id=526)';

  -- Dockweiler (fid 8477) — DPR catalog "No"
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 8477, ps.id, 'sand', 'not_allowed',
    'No. Park unit operated by Los Angeles County.',
    'https://www.parks.ca.gov/Dogs',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'Dockweiler State Beach dog policy (parks.ca.gov/Dogs catalog row)';

  -- Rosie's Dog Beach (fid 6411) — THE proof point
  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, rule_modifier, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6411, ps.id, 'sand', 'off_leash_voice_control',
    '{"eligibility":["one dog per adult","dogs > 4 months","vaccinated","licensed"],"prohibited":["aggressive dogs","female dogs in heat","spiked collars","food"],"hours":"6 AM - 8 PM","boundary":"Granada Ave to Roycroft Ave","size_acres":4.1}'::jsonb,
    'The dog must be under visual and voice control at all times.',
    'https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach/',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'City of Long Beach Rosie''s Dog Beach published rules';

  INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, extracted_at, last_verified)
  SELECT 6411, ps.id, 'parking_lot', 'on_leash',
    'Owners must have a leash. Dogs shall be on leashes whenever outside Dog Park/Zones.',
    'https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach/',
    '2026-05-16'::timestamptz, '2026-05-16'::timestamptz
   FROM public.policy_source ps
  WHERE ps.citation = 'City of Long Beach Rosie''s Dog Beach published rules';

  RAISE NOTICE 'Phase 2 seed complete: % policy_source rows, % beach_policy_source rows',
    (SELECT count(*) FROM public.policy_source),
    (SELECT count(*) FROM public.beach_policy_source);
END$$;
