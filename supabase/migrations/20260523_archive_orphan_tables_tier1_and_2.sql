-- 20260523_archive_orphan_tables_tier1_and_2.sql
--
-- Moves 15 confirmed-orphan tables from public to a new `archive`
-- schema. Per Franz directive 2026-05-23 EVENING: "we don't DROP
-- anything, mister" — these are namespaced-aside, not removed. Data
-- preserved verbatim.
--
-- Surfaced by scripts/audit_artifacts.py 2026-05-23 (commit 5bb33e9):
-- after expanding the audit seed list, 52 orphan tables remained. This
-- migration archives the safest subset.
--
-- TIER 1 — date-stamped one-off snapshots (12 tables, ~6 MB total):
--   _snap_2026_05_18_ca_bdp                     (1,211 rows)
--   _snap_2026_05_18_ca_bps                     (  904 rows)
--   _snap_2026_05_18_ca_bps_temporal            (  141 rows)
--   _snap_2026_05_18_ca_policy_source           (  171 rows)
--   beaches_staging_new_v1_snapshot             (  668 rows)
--   truth_snapshot_before_50                    (    ? rows)
--   verdict_snapshot_cdpr_scrape                (3,835 rows)
--   verdict_snapshot_pass6                      (1,588 rows)
--   verdict_snapshot_pass7                      (1,588 rows)
--   verdict_snapshot_pass9                      (3,835 rows)
--   verdict_snapshot_pass10                     (3,835 rows)
--   verdict_snapshot_pass11                     (3,835 rows)
--
-- TIER 2 — pre-harmony era, superseded by beaches_gold / arena (3
-- tables, ~3.8 MB):
--   beaches_staging                              (  955 rows)
--   beaches_staging_new                          (1,316 rows)
--   beach_enrichment_provenance_legacy_fid_archive (5,532 rows)
--
-- Reversible: `ALTER TABLE archive.X SET SCHEMA public` brings any
-- table back. Audit script will classify these as no-longer-orphan
-- because they're no longer in the public schema scan.

BEGIN;

-- ── Create archive schema (idempotent) ──────────────────────────────
CREATE SCHEMA IF NOT EXISTS archive;

COMMENT ON SCHEMA archive IS
  'Cold storage for orphan tables that have no reachable references '
  'from any seed surface (consumer edge fns, HTML pages, scheduled '
  'crons). Data preserved; reversible via ALTER TABLE archive.X SET '
  'SCHEMA public. Per Franz: "we don''t DROP anything".';

-- Revoke anon/authenticated access to the entire schema (PostgREST
-- shouldn't expose archived tables on the public REST endpoint).
REVOKE ALL ON SCHEMA archive FROM anon, authenticated;
GRANT USAGE ON SCHEMA archive TO service_role;

-- ── TIER 1 — date-stamped snapshots ─────────────────────────────────
DO $$
DECLARE
  tbl text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY[
    '_snap_2026_05_18_ca_bdp',
    '_snap_2026_05_18_ca_bps',
    '_snap_2026_05_18_ca_bps_temporal',
    '_snap_2026_05_18_ca_policy_source',
    'beaches_staging_new_v1_snapshot',
    'truth_snapshot_before_50',
    'verdict_snapshot_cdpr_scrape',
    'verdict_snapshot_pass6',
    'verdict_snapshot_pass7',
    'verdict_snapshot_pass9',
    'verdict_snapshot_pass10',
    'verdict_snapshot_pass11',
    -- TIER 2 — pre-harmony staging:
    'beaches_staging',
    'beaches_staging_new',
    'beach_enrichment_provenance_legacy_fid_archive'
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

-- Sanity:
--   SELECT table_name FROM information_schema.tables
--    WHERE table_schema = 'archive' ORDER BY table_name;
--   Expected: 15 tables listed.
