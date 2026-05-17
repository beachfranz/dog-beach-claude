-- Wave 3 expansion: City of Coronado — link 2 more beaches to
-- existing Coronado policy_source rows.
--
-- Existing rows (from prior walkthrough, commit 2f5ad83):
--   ps id 50: Coronado Municipal Code Chapter 32.08 (Animal Regulations)
--     source_url: https://coronado.municipal.codes/CMC/32.08
--   ps id 51: City of Coronado — Dogs page (agency_administrative_policy)
--     source_url: https://www.coronado.ca.us/757/Dogs
--
-- Coronado-polygon beaches in beaches_gold (4 total):
--   8715 Coronado Beach                — Coronado as precedence-1; already linked
--   6202 Coronado Dog Beach            — no precedence-1 authority; famous off-leash
--                                        beach at far-north end of Coronado (the
--                                        designated dog beach) → link as off_leash
--                                        citing both §32.08 (codified) and Dogs page
--                                        (administrative)
--   8471 Gator Beach                   — at Naval Air Station North Island (federal
--                                        property); Coronado §32.08 does not apply;
--                                        SKIPPED
--   8942 Imperial Beach Muni Beach Other — Coronado as precedence-1; default
--                                        on_leash per §32.08 (sub-beach fragment
--                                        near IB/Coronado border)

-- ─── 1. Coronado Dog Beach (fid 6202) → off_leash ───────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (6202, 50, 'sand', 'off_leash', 'operative'::operative_status,
   'Coronado Dog Beach is the City of Coronado''s designated off-leash dog area at the far-north end of Coronado (between Naval Air Station and the Hotel del Coronado). Citywide §32.08 leash law applies elsewhere; this beach is the named carve-out.',
   'https://coronado.municipal.codes/CMC/32.08',
   'Designated city dog beach; off-leash hours/seasonal restrictions may apply per signage — walkthrough recommended to capture exact times.'),
  (6202, 51, 'sand', 'off_leash', 'operative'::operative_status,
   'City of Coronado Dogs page confirms Coronado Dog Beach as the designated off-leash beach.',
   'https://www.coronado.ca.us/757/Dogs',
   NULL)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. Imperial Beach Muni Beach, Other (fid 8942) → on_leash ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (8942, 50, 'sand', 'on_leash', 'operative'::operative_status,
   'Coronado Municipal Code Chapter 32.08 (Animal Regulations) default leash law applies. fid 8942 is a sub-beach fragment near the Imperial Beach/Coronado border with Coronado as precedence-1 dog_policy authority.',
   'https://coronado.municipal.codes/CMC/32.08',
   'Sub-beach fragment near IB/Coronado border; walkthrough recommended to confirm precise location and any signage-overlay rules.')
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
