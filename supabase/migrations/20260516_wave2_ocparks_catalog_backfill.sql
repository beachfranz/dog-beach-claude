-- Wave 2 follow-up: OC Parks catalog backfill.
--
-- Per [[comprehensive-source-accelerator]]: OC Parks publishes regional
-- park rules covering all OC Parks operated facilities at
-- ocparks.com/about-us/park-rules/regional-park-rules (already in DB
-- as policy_source id 47). Per-beach pages (capistrano, dana-point-harbor,
-- salt-creek, sunset-harbour) layer specific overrides.
--
-- OC Parks beach inventory (per ocparks.com/beaches landing page,
-- fetched 2026-05-16):
--   - Capistrano Beach Park       — not in beaches_gold (closest match
--                                    is Doheny State Beach 9718 which is DPR)
--   - Dana Point Harbor            — beaches_gold has Dana Point Headlands
--                                    Beach (fid 5968) as the closest match
--   - Salt Creek Beach (fid 8316)  — already linked via walkthroughs round 2
--   - Sunset Harbour               — beaches_gold has Sunset Beach (fid 8271)
--
-- Strands Beach (fid 8317) is operationally paired with Salt Creek per
-- the Salt Creek rules page ("Beach hours: 5 am - 12 a.m. daily
-- (includes Strands Beach)"). Link Strands to the same Salt Creek rules.
--
-- Authority chain at each:
--   tier 1: OC County Code §2-5-39 (Controlling domestic animals — Beaches)
--           Already in DB as policy_source id 53.
--   tier 3: OC Parks regional rules — baseline "all pets must be on a
--           leash that does not exceed 6 feet"
--   tier 3: per-beach overrides where they exist (Salt Creek + Strands)

-- ─── Link Dana Point Headlands + Sunset to baseline rules ────────
-- No per-beach override observed on their OC Parks pages → leashed
-- allowed per OC Parks regional default.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT fid, ps_id, 'sand', 'on_leash', 'operative'::operative_status, ev, eurl
  FROM (VALUES (5968), (8271)) f(fid)
  CROSS JOIN (VALUES
    (
      (SELECT id FROM public.policy_source
        WHERE source_url='https://www.ocparks.com/about-us/park-rules/regional-park-rules'),
      'All pets must be on a leash that does not exceed 6 feet. [OC Parks regional rules, default for all OC Parks operated facilities including this beach.]',
      'https://www.ocparks.com/about-us/park-rules/regional-park-rules'
    ),
    (
      (SELECT id FROM public.policy_source
        WHERE citation='OC County Code §2-5-39 (Controlling domestic animals — Beaches)'),
      'OC County Code §2-5-39(a) — leashed domestic animals required as the OC County tier-1 baseline.',
      'https://library.municode.com/ca/orange_county/codes/code_of_ordinances?nodeId=TIT2PUFA_DIV5PABEREAR_ART2REARGE'
    )
  ) p(ps_id, ev, eurl)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── Link Strands Beach to Salt Creek-specific rule (sand=not_allowed) ─
-- Per the Salt Creek rules page, Strands is paired with Salt Creek for
-- operational rules. Same regime: dogs not permitted on sand, leashed
-- allowed on paved walkways.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8317,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Dogs are not permitted on the beach. Strands Beach is paired with Salt Creek for OC Parks operational rules per the rules page ("Beach hours: 5 am - 12 a.m. daily (includes Strands Beach)").',
       'https://www.ocparks.com/beaches/salt-creek-beach/park-rules'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8317,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'paved_walkways', 'on_leash', 'operative'::operative_status,
       'Dogs are permitted (on a 6-foot leash) on paved walkways. [Strands inherits Salt Creek''s operational rules.]',
       'https://www.ocparks.com/beaches/salt-creek-beach/park-rules'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Link Strands to the OC County Code tier-1 baseline (matches Salt Creek pattern)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   status_basis_id, evidence_verbatim, evidence_url, status_note)
SELECT 8317,
       (SELECT id FROM public.policy_source
         WHERE citation='OC County Code §2-5-39 (Controlling domestic animals — Beaches)'),
       'sand', 'on_leash', 'superseded_by_lower_tier'::operative_status,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'OC County Code §2-5-39(a) requires leashed domestic animals as the baseline; §2-5-39(d) authorizes the Director of OC Parks to designate restricted areas.',
       'https://library.municode.com/ca/orange_county/codes/code_of_ordinances?nodeId=TIT2PUFA_DIV5PABEREAR_ART2REARGE',
       'Tier-1 baseline (leashed) operatively superseded at Strands sand by tier-3 OC Parks Director designation paired with Salt Creek (no dogs).'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
