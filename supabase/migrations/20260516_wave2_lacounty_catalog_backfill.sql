-- Wave 2 follow-up: LA County Beaches & Harbors catalog backfill.
--
-- Per [[comprehensive-source-accelerator]]: LA County publishes ONE rule
-- ("NO Animals allowed on the beach") covering all of its operated
-- beaches at beaches.lacounty.gov/la-county-beach-rules/. That rule is
-- already captured as policy_source row id 45 (inserted by walkthrough
-- seeds round 2). This migration links all LA County operated beaches
-- to that rule.
--
-- The 16-beach list comes from beaches.lacounty.gov's left-nav (their
-- "our beaches" listing). Matched to beaches_gold by name + county:
--
--   fid  | beach
--   -----+----------------------------------------
--   107  | Dan Blocker Beach
--   8264 | Hermosa City Beach
--   8265 | Manhattan Beach (already linked via walkthrough)
--   8247 | Venice Beach
--   8254 | Zuma Beach
--   8268 | Torrance County Beach
--   8472 | Will Rogers State Beach (DPR-owned, LA County operated)
--   8473 | Las Tunas Beach
--   8477 | Dockweiler State Beach (DPR-owned, LA County operated)
--   8488 | Surfrider Beach (= Malibu Surfrider)
--   8896 | Royal Palms County Beach
--   9334 | Topanga County Beach
--   9383 | Nicholas Canyon Beach
--   9720 | Point Dume State Beach (DPR-owned)
--   5960 | White Point Beach
--
-- Skipped (ambiguous / not in beaches_gold):
--   "Marina Beach" — ambiguous (could be marina del rey adjacent strip)
--   "Leo Carrillo State Beach" — DPR, already linked by Wave 2 DPR
--   "Redondo Beach Pier" (7404) — pier facility, not beach proper

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT fid,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://beaches.lacounty.gov/la-county-beach-rules/'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'NO Animals allowed on the beach (no cats, dogs, horses, etc.) — applies to all LA County operated beaches via beaches.lacounty.gov/la-county-beach-rules/.',
       'https://beaches.lacounty.gov/la-county-beach-rules/'
  FROM (VALUES
    (107), (8264), (8265), (8247), (8254), (8268),
    (8472), (8473), (8477), (8488), (8896), (9334),
    (9383), (9720), (5960)
  ) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
