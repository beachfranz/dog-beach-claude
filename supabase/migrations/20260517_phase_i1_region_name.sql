-- Phase I1 — sub-area / spatial extension: region_name column on bps.
--
-- Per docs/consensus_phase_i_subarea_spec.md §I1. Adds ONE nullable text
-- column to beach_policy_source so a single beach can carry multiple
-- bps rows whose section='sand' but apply to distinct named sub-areas
-- ("Sand Pebble Off-Leash Area", "Crab Cove", "North of Sloat Blvd",
-- etc.). The injector (Phase I4, out of scope here) groups bps rows by
-- region_name and emits one zone_rules.regions[] entry per distinct
-- value; rows with NULL region_name fold into the beach default region.
--
-- The PK (beach_fid, policy_source_id, section) stays — adding
-- region_name does not change the M:M structure. To split a beach into
-- two regions today, encode the second region via either:
--   (a) a different policy_source_id (typical when authority differs),
--   (b) a different section value (today's overlay pattern), or
--   (c) (post-I3+I4) duplicate ps+section but distinct region_name —
--       not blockable by the current PK because each pair already has a
--       distinct ps_id or section.
--
-- I3 (promoter) and I4 (injector) will read this column. This migration
-- is data-shape only — no functions or triggers are altered here.
--
-- Backfill of the column for the known sub-area beaches happens in the
-- sibling migration 20260517_phase_i2_subarea_backfill.sql.

ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS region_name text NULL;

COMMENT ON COLUMN public.beach_policy_source.region_name IS
  'When non-null, places this bps row into a named sub-area / zone within '
  'the beach. Injector (Phase I4) emits one regions[] entry per distinct '
  'region_name; rows with NULL region_name fold into the beach default region.';
