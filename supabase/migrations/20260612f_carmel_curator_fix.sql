-- 20260612f_carmel_curator_fix.sql
--
-- Workstream 3 Path A — minimum-touch Carmel curator fix.
--
-- Context (full forensic in conversation log):
--   fid 8287 carries name "Carmel River State Beach" but its
--   beaches_gold.geom is at 36.5512/-70.9301 — the DOWNTOWN Carmel
--   City Beach coords (the famous off-leash beach). The geom got
--   swapped by run_late_stage_dedup's LIMIT 1 OSM nav pull during
--   cluster 8286's collapse on 2026-05-08.
--
--   fid 15569 carries name "Carmel Beach City Park" with geom at
--   36.5374/-70.9279 — the RIVER beach coords, where the on-leash
--   Carmel River State Beach actually is.
--
-- This migration corrects the consumer-surface presentation only:
--   1. fid 8287:  display_name_override = 'Carmel City Beach'.
--                 beach_dog_policy → off-leash (Carmel Municipal Code
--                 4.04.030 permits dogs off-leash under voice
--                 control on Carmel City Beach).
--                 zone_rules NULL'd — its content is river-beach
--                 evidence and is wrong for the city-beach location.
--                 Section-tile rendering on beach.html falls back to
--                 the binary flag; can be re-extracted later.
--   2. fid 15569: display_name_override = 'Carmel River State Beach'.
--                 Policy unchanged (it's already on_leash, which is
--                 correct for the river state beach).
--
-- Doesn't touch the inactive duplicates (fid 4911, 6417, 8286). They
-- carry the right names + coords for their actual locations but
-- restoring them would require BEP / photo migration reversal — out
-- of scope for Path A. Punt to Path B if/when Franz wants identity
-- realignment.
--
-- Source marked 'manual_curator' so audit-must-respect-prior-curation-
-- work guards skip these rows.

BEGIN;

-- ── fid 8287: rename to Carmel City Beach + flip to off-leash ────
UPDATE public.beaches_gold
   SET display_name_override = 'Carmel City Beach'
 WHERE fid = 8287;

UPDATE public.beach_dog_policy
   SET leash_policy     = 'off_leash',
       has_off_leash    = true,
       has_on_leash     = false,
       off_leash_flag   = true,
       dogs_allowed     = 'yes',
       zone_rules       = NULL,
       notes            = 'Curator correction 2026-06-12: arena dedup mislabeled this row. Coords ARE Carmel City Beach (downtown, famously off-leash per Carmel Municipal Code). Original "Carmel River State Beach" identity belongs to inactive fid 6417 / active fid 15569 (river coords). Binary flags drive consumer surface; zone_rules NULL pending re-extraction.',
       source           = 'manual_curator',
       curated_at       = now(),
       zone_rules_updated_at = now()
 WHERE arena_group_id = 8287;

-- ── fid 15569: rename to the correct Carmel River State Beach ────
-- Policy unchanged: already on_leash, which is correct for the river SB.
UPDATE public.beaches_gold
   SET display_name_override = 'Carmel River State Beach'
 WHERE fid = 15569;

COMMIT;

NOTIFY pgrst, 'reload schema';
