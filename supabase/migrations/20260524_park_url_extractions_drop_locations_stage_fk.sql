-- 20260524_park_url_extractions_drop_locations_stage_fk.sql
--
-- Drop the stale FK on park_url_extractions.fid → locations_stage.fid.
-- locations_stage is a CA-only legacy table; post-path-3 (2026-05-02)
-- beaches_gold is the cross-state spine and the description generator
-- already keys on park_url_extractions.arena_group_id (which is the
-- beaches_gold.fid).
--
-- Required to land OR/WA descriptive-text harvests via
-- scripts/harvest_park_text.py — those beaches have no row in
-- locations_stage so the FK rejects every insert.
--
-- Safe: drops only the constraint, no column or data changes. Existing
-- 655 CA rows are unaffected. Future rows write `fid = arena_group_id`
-- (the beaches_gold.fid) for non-CA states; CA rows continue to write
-- the legacy locations_stage.fid (preserves backward compatibility).
--
-- park_url_extractions_fid_source_url_key UNIQUE on (fid, source_url)
-- is preserved.

BEGIN;

ALTER TABLE public.park_url_extractions
  DROP CONSTRAINT IF EXISTS park_url_extractions_fid_fkey;

COMMIT;
