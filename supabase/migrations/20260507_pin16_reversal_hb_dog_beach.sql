-- 20260507_pin16_reversal_hb_dog_beach.sql
--
-- Reverses the HB Dog Beach side of the pin #16 dedup decision.
--
-- WHAT HAPPENED:
-- Earlier 2026-05-07 (migration 20260507_pin16_dedup_carveouts.sql) I
-- deactivated fid 6212 (POI source, name "Dog Beach") in favor of fid
-- 9717 (manual seed I created the same night, name "Huntington Beach
-- Dog Beach"). The reasoning was "manual > poi per dedupe_canonical_rule".
--
-- WHY THAT WAS WRONG:
-- The dedupe_canonical_rule says "the row with most credible-source
-- METADATA wins" — not the row with the most credible source CODE.
-- Inspecting both:
--
--   fid 6212 (POI): 5 BEP votes, consensus_confidence=0.51,
--     has_on_leash=true, has_off_leash=true (carve-out captured),
--     25 forecast days, continuously enriched by the consensus chain.
--   fid 9717 (manual_curator): 4 BEP votes, consensus_confidence=null
--     (auto-promote blocks `where source <> 'manual_curator'`),
--     has_on_leash=false (Phase 1A backfill of leash_policy='off_leash'
--     collapsed the carve-out), 10 forecast days, FROZEN at incomplete
--     state because manual_curator status blocked enrichment.
--
-- The manual_curator status — which I added by seeding the row through
-- seed_arena_beach.py — is what made 9717 the worse record. It
-- protected the row from being auto-enriched by consensus, so the
-- binaries never got the carve-out signal that 6212 had been quietly
-- accumulating.
--
-- ARCHITECTURAL LESSON:
-- manual_curator-as-blocker is the wrong model when curator hasn't
-- explicitly populated every field. The promoter should be allowed to
-- ENRICH null fields without OVERRIDING explicit fields. That's a
-- Phase 3 follow-up — until then, prefer the row that's been
-- accumulating BEP enrichment over a freshly-seeded manual record.
--
-- THIS MIGRATION:
-- 1. Reactivates fid 6212 (the POI record with rich data)
-- 2. Adds display_name_override='Huntington Beach Dog Beach' to 6212
--    so the consumer-surface name reads correctly (the source `name`
--    field is just "Dog Beach")
-- 3. Deactivates fid 9717 with an inactive_reason explaining the reversal
-- 4. Idempotent via WHERE on source_id

begin;

update public.beaches_gold
   set is_active        = true,
       is_scoreable     = true,
       inactive_reason  = null,
       display_name_override = coalesce(display_name_override, 'Huntington Beach Dog Beach')
 where source_id = 'poi/12000191';

update public.beaches_gold
   set is_active        = false,
       is_scoreable     = false,
       inactive_reason  = 'Pin #16 reversal 2026-05-07: manual seed had worse policy data than the POI dupe (fid 6212) because manual_curator status blocked consensus auto-promote. POI 6212 reactivated as canonical with display_name_override.'
 where source_id = 'manual/huntington-beach-dog-beach-orange';

commit;
