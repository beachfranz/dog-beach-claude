-- Wave 4 OR foundation: Oregon Parks and Recreation Department
-- (OPRD) state-park pet rule.
--
-- Source: OAR 736-021-0070 (Pets and Other Animals) — operative for
-- ALL Oregon State Parks including the state-park beaches that line
-- much of the Oregon coast. Many OR coastal beaches are technically
-- state-park-operated even when within county polygons (the OR coast
-- below the high-water mark is state ocean shore per ORS 390.605, and
-- many landward sections are OPRD state-park units).
-- URL: https://oregon.public.law/rules/oar_736-021-0070
--
-- Operative rule (per OPRD pets in state parks summary):
--   Dogs must be on a leash not longer than six (6) feet at all times
--   in Oregon state parks (including state-park beaches), under the
--   physical control of a person capable of restraining the animal.
--
-- Companion rule (OAR 736-030-0010 Prohibition of Dogs Off Leash):
--   Specific designated areas may prohibit off-leash dogs even if
--   exception zones are designated; the operative rule for the
--   general state-park beach experience is on-leash 6 ft.
--
-- Snowy plover seasonal closures (March 15 – September 15) affect
-- specific nesting beaches but are typically posted via OPRD-specific
-- rules and ODFW coordination; flagged for follow-up walkthrough.
--
-- 152 active OR beaches in beaches_gold; 0 currently have any
-- policy_source link. This migration links all 152 to the OPRD rule
-- as a state-level tier-3 fallback. Per-county code migrations will
-- layer more-specific rules on top via additional beach_policy_source
-- rows. The consensus engine resolves precedence via source authority.

-- ─── 1. Insert OPRD policy_source ───────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'state_regulation',
       'Oregon Administrative Rules §736-021-0070 (Pets and Other Animals) — OPRD state-park 6-ft leash',
       (SELECT id FROM public.agency WHERE name='Oregon Parks and Recreation Department' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://oregon.public.law/rules/oar_736-021-0070',
       'Oregon Administrative Rules §736-021-0070 Pets and Other Animals. Operative rule in all Oregon State Parks (including state-park beaches): dogs must be on a leash not longer than six (6) feet at all times, under the physical control of a person capable of restraining the animal. Companion rule §736-030-0010 (Prohibition of Dogs Off Leash) reinforces the on-leash mandate. Snowy plover nesting closures (March 15 – September 15) apply at specific designated beaches via OPRD posting + ODFW coordination — not enumerated here; flagged for per-beach walkthrough refinement. Much of the Oregon coast below the high-water mark is state ocean shore per ORS 390.605, and many landward beach sections are OPRD-operated. This state-level rule is the baseline; county and city ordinances may layer per jurisdiction. [Fetched via web search 2026-05-16; codified text at oregon.public.law/rules/oar_736-021-0070.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Oregon Administrative Rules §736-021-0070%'
);

-- ─── 2. Link all active OR beaches → on_leash (state-level baseline) ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'OAR §736-021-0070: dogs must be on a 6-ft leash at all times in Oregon state parks (including state-park beaches). Default baseline for OR coast.',
       ps.source_url,
       'State-level tier-3 baseline; county/city rules may layer. Snowy plover seasonal closures (Mar 15-Sep 15) may apply at specific beaches per OPRD posting.'
FROM public.beaches_gold g
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true
  AND ps.citation LIKE 'Oregon Administrative Rules §736-021-0070%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
