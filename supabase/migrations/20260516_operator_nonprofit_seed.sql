-- Seed the `operator` and `beach_operator` tables with the two clear
-- non-government operators that have surfaced through the Wave 2
-- walkthroughs.
--
-- Design context — see [[operator-not-pseudo-agency]] pin:
--   "Non-gov operators (Preservation Society HDB, Crystal Cove
--   Conservancy, concessionaires) stay as operators with
--   `delegated_authority_via` FK to tier-2 MOU/lease. NOT in agency
--   table. Agencies issue laws; operators don't."
--
-- This migration handles ONLY the unambiguous non-gov cases. Gov-as-
-- operator cases (EBRPD operating Crown Memorial; LA County Beaches &
-- Harbors operating Dockweiler + SM State Beach; TIDA operating
-- Treasure Island) require dual-role design clarification — they are
-- already in the agency table, and creating parallel operator rows
-- risks double-counting. Pinned for a focused future pass.
--
-- Before this migration: 0 rows in public.operator and 0 rows in
-- public.beach_operator. After: 2 operators, 3 beach links.

-- ─── 1. Preservation Society of Huntington Dog Beach ────────────

INSERT INTO public.operator (name, short_name, type, agency_id, delegated_authority_via, web_url, rules_url)
SELECT 'Preservation Society of Huntington Dog Beach',
       'PSHDB',
       'nonprofit'::operator_type,
       (SELECT id FROM public.agency WHERE name='Huntington Beach' AND type='city'),  -- parent jurisdiction
       (SELECT id FROM public.policy_source WHERE id=10),  -- MOU: City of HB ↔ PSHDB
       'https://dogbeach.org/',
       'https://dogbeach.org/rules/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.operator WHERE name='Preservation Society of Huntington Dog Beach'
);

-- Link HBDB beach to PSHDB
INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT 6212,
       (SELECT id FROM public.operator WHERE name='Preservation Society of Huntington Dog Beach'),
       'sand',
       1,
       'PSHDB operates HBDB day-to-day under a 1990s+ MOU with the City of Huntington Beach. The City retains jurisdictional authority via HBMC §13.08.070; PSHDB''s authority is delegated via the MOU (policy_source id 10).'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_operator
   WHERE beach_fid=6212 AND operator_id=(SELECT id FROM public.operator WHERE name='Preservation Society of Huntington Dog Beach')
);

-- ─── 2. Crystal Cove Conservancy ────────────────────────────────

INSERT INTO public.operator (name, short_name, type, agency_id, delegated_authority_via, web_url, rules_url)
SELECT 'Crystal Cove Conservancy',
       'CCC',
       'nonprofit'::operator_type,
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),  -- parent jurisdiction
       NULL,  -- conservancy concession agreement with CA DPR not yet captured as policy_source
       'https://www.crystalcove.org/',
       'https://www.crystalcove.org/visit/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.operator WHERE name='Crystal Cove Conservancy'
);

-- Link both Crystal Cove State Beach fids (there are two — likely a
-- pending dedup; both get the same operator)
INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT v.fid,
       (SELECT id FROM public.operator WHERE name='Crystal Cove Conservancy'),
       'sand',
       1,
       'Crystal Cove Conservancy operates Crystal Cove State Park concessions (Historic District cottages, etc.) under agreement with CA DPR. CA DPR retains jurisdictional authority via state-park regulations; Conservancy''s authority is delegated via the concession agreement (not yet captured as policy_source — flagged for future capture).'
FROM (values (6082), (8330)) as v(fid)
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_operator
   WHERE beach_fid=v.fid AND operator_id=(SELECT id FROM public.operator WHERE name='Crystal Cove Conservancy')
);
