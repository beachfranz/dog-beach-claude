-- 20260523_archive_orphan_tables_tier3.sql
--
-- Archives 2 confirmed-dead tables from Tier 3 review (Franz approved
-- 2026-05-23 EVENING). Same archive-schema pattern as the Tier 1+2
-- migration — no DROP, fully reversible via ALTER TABLE archive.X
-- SET SCHEMA public.
--
--   extraction_calibration   (1,470 rows, 312 kB)
--     Output of the paused-extraction loop (extract_beach_policies.py
--     per [[project_policy_extraction_pause]] 2026-04-24). Zero
--     consumers in active pipeline. Current calibration tier uses
--     field_source_calibration (different table).
--
--   dogs_verdict_override    (24 rows, 48 kB)
--     Verdict-cascade-era override table. Zero readers (no fns, no
--     views, no edge fns, no HTML). The cascade itself was demoted to
--     reference-only per CLAUDE.md ("currently broken — pending repoint
--     or retire decision"). 24 hand-derived rows preserved verbatim in
--     archive schema.
--
-- The remaining 5 Tier 3 candidates were determined false-positive
-- orphans (active in populators or admin curator UI) and KEPT in
-- public. Audit seed expanded separately to reclassify them.

BEGIN;

DO $$
DECLARE
  tbl text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY[
    'extraction_calibration',
    'dogs_verdict_override'
  ]
  LOOP
    IF EXISTS (
      SELECT 1 FROM information_schema.tables
       WHERE table_schema = 'public' AND table_name = tbl
    ) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA archive', tbl);
      RAISE NOTICE 'archived: public.% → archive.%', tbl, tbl;
    ELSE
      RAISE NOTICE 'skip (not in public): %', tbl;
    END IF;
  END LOOP;
END $$;

COMMIT;
