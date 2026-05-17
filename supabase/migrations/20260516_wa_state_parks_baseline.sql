-- Wave 4 WA: Washington State Parks and Recreation Commission
-- baseline rule for state-park beaches. Analogous to the OR OPRD
-- migration (commit e5bd2b9) and CA DPR backfill.
--
-- Seeds:
--   1. Washington State Parks and Recreation Commission agency
--      (state_department under State of Washington id 328)
--   2. WAC 352-32-060 (Pets and Other Animals) policy_source
--   3. Links to all 429 active WA beaches as state-level baseline
--
-- Source: Washington Administrative Code Title 352 Chapter 32 §060
-- (Pets and Other Animals). Operative for all WA state parks
-- (including state-park beaches such as Dash Point SP in King County,
-- Lake Chelan SP in Chelan County, Cape Disappointment SP in
-- Pacific County, etc.).
-- URL: https://app.leg.wa.gov/wac/default.aspx?cite=352-32-060
--
-- Operative rule (per WAC 352-32-060):
--   Dogs and other pets must be on a leash no longer than 8 feet at
--   all times in WA state parks. Pets are prohibited on designated
--   swimming beaches. Owners must remove pet waste.
--
-- 429 active WA beaches; the WA county agent shipped county-code
-- links for 382 of them (89%). This state-park layer adds an
-- additional tier-1 baseline. The consensus engine resolves
-- precedence between (WA State Parks 8-ft) and (county code,
-- typically also leashed) by source authority — they don't conflict
-- on the leashed default.
--
-- Per-beach precedence will require a future governance-derivation
-- pass to identify which WA beaches are inside state-park boundaries
-- (no beach_agency rows exist for WA yet). This migration links to
-- ALL WA beaches as conservative baseline; state-park-applicability
-- can be refined later via beach_agency precedence.

-- ─── 1. Seed Washington State Parks agency ────────────────────────

INSERT INTO public.agency (name, type, parent_agency_id, web_url)
SELECT 'Washington State Parks and Recreation Commission',
       'state_department',
       (SELECT id FROM public.agency WHERE name='State of Washington' AND type='state'),
       'https://parks.wa.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Washington State Parks and Recreation Commission' AND type='state_department'
);

-- ─── 2. Insert WAC 352-32-060 policy_source ───────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'state_regulation',
       'Washington Administrative Code §352-32-060 (Pets and Other Animals) — WA state-park 8-ft leash',
       (SELECT id FROM public.agency WHERE name='Washington State Parks and Recreation Commission' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://app.leg.wa.gov/wac/default.aspx?cite=352-32-060',
       'Washington Administrative Code §352-32-060 Pets and Other Animals. Operative rule in all Washington state parks (including state-park beaches): pets must be physically restrained, at all times, on a leash no longer than eight (8) feet, or otherwise physically restrained. Pets are PROHIBITED on designated swimming beaches in state parks. Pet owners are responsible for the conduct of their pet, must clean up after their pet, and may be cited for violations. WA State Parks operates many WA beaches — Dash Point SP (King), Lake Chelan SP (Chelan), Cape Disappointment SP (Pacific), Sequim Bay SP (Clallam), Mystery Bay SP (Jefferson), Saltwater SP (King), Twanoh SP (Mason), Belfair SP (Mason), Salt Creek Recreation Area (Clallam), and many others. This state-park rule co-exists with county-code rules already linked by the Wave 4 WA county batch (commits d77970f through 9e9d048). Consensus engine resolves source authority — both are tier-1 statutes and don''t conflict on leashed default. [Codified text at leg.wa.gov/wac/. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Washington Administrative Code §352-32-060%'
);

-- ─── 3. Link all active WA beaches as state-park-baseline ──────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'WAC §352-32-060: pets must be physically restrained on a leash no longer than 8 feet at all times in WA state parks. Prohibited on designated swimming beaches.',
       ps.source_url,
       'State-park baseline. Applies operatively only on state-park-operated beach sections (governance-resolver-level spatial filter needed for per-beach precision). For non-state-park beaches, the county-code link (Wave 4 WA county batch) is the operative rule.'
FROM public.beaches_gold g
CROSS JOIN public.policy_source ps
WHERE g.state='WA' AND g.is_active=true
  AND ps.citation LIKE 'Washington Administrative Code §352-32-060%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
