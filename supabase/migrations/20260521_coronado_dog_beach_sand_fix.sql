-- ─────────────────────────────────────────────────────────────────────
-- Coronado Dog Beach (fid 6202) sand rule fix.
-- Franz 2026-05-21.
--
-- The LLM extraction for SD County Code §62.669 (ps=116) attributed
-- the general countywide leash law to bps.section = 'sand' on
-- Coronado Dog Beach. But §62.669 is a citywide/countywide BASELINE
-- (applies to "any public place"), not a sand-specific rule, and
-- Coronado Dog Beach is an explicit CARVE-OUT from that baseline.
--
-- Effect: the cascade's most-restrictive-wins picked the bogus
-- sand=on_leash over the actual sand=off_leash from City of Coronado
-- sources (ps=50, ps=51). Render flipped the dog beach to on-leash.
--
-- Fix: move bps id=10691 from section=sand → section=global. The
-- baseline now lives at the global level (correct), and sand off_leash
-- from the dog-beach-specific sources wins uncontested.
--
-- Broader pattern: ~25 other beaches have similar §62.669 sand
-- attribution but they're all NORMAL beaches where sand=on_leash is
-- correct, so they don't need the same fix. The pattern should be
-- caught at LLM extraction time (v2 prompt: "baseline leash laws
-- attach to the GLOBAL section, not specific sub-sections").
-- ─────────────────────────────────────────────────────────────────────

UPDATE public.beach_policy_source
   SET section = 'global'
 WHERE id = 10691
   AND beach_fid = 6202
   AND policy_source_id = 116
   AND section = 'sand';

SELECT public._promote_zone_rules_for_fid(6202);
