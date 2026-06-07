-- 20260607d_accessibility_features_compute_promote.sql
--
-- Phase 2 of the ADA cleanup (see [[dwell-ada-crowd-autocurate]]).
--
-- The 2026-05-12 migrations already shipped:
--   - beach_amenities.accessibility_features JSONB column
--   - promote_canonical_accessibility_to_beach_amenities(p_fid)
--   - refire_bep_cascade calls the promoter once per fid
--
-- But two real problems remain:
--
--   (1) The existing promoter loses data on multi-source fids. It writes
--       claimed_values from ONE canonical row at a time via UPSERT — when
--       2+ canonical rows exist (one per source, complementary fields,
--       0 conflicts per the 2026-06-07 audit), the last-processed source
--       overwrites the first one's features. ~12 fids today carry only
--       one source's data despite having evidence from both.
--
--   (2) Auto-promotion is broken. tg_promote_other_fields_chain (the BEP
--       trigger) calls promote_canonical_to_consumer_tables, which today
--       only dispatches dogs + practical. Accessibility BEP changes don't
--       propagate to beach_amenities.accessibility_features until a full
--       cascade refire fires.
--
-- This migration:
--   - Adds _compute_accessibility_features(p_fid) for union-of-non-nulls
--     aggregation
--   - Rewrites promote_canonical_accessibility_to_beach_amenities to use
--     it, preserving the manual_curator guard from the existing fn
--   - Adds accessibility dispatch to promote_canonical_to_consumer_tables
--     so the existing BEP trigger now auto-propagates
--
-- Signature is preserved (p_fid bigint DEFAULT NULL → table(ins, upd)) so
-- the existing refire_bep_cascade call site keeps working.
--
-- Schema lock-in for accessibility_features JSONB (versioned implicitly
-- via the BEP claimed_values shape):
--   _sources                  text[]   array of BEP source identifiers
--   _updated_at               timestamptz  max(bep.updated_at)
--   accessible_parking        boolean
--   accessible_restrooms      boolean
--   path_to_sand              text ('boardwalk'|'ramp'|'mat'|'stairs_only'|'none')
--   beach_mat                 boolean
--   accessible_viewing        boolean
--   boardwalk                 boolean   -- CCC distinguishes from path_to_sand
--   wheelchair_rental         jsonb {available, provider, url}
--   beach_wheelchair_program  jsonb {available, provider, reservation_url, phone}
--   notes                     text  freeform paraphrase

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- _compute_accessibility_features(p_fid) — union-of-non-nulls across all
-- BEP rows for field_group='accessibility' + is_canonical=true. First
-- non-null wins per scalar field; nested objects taken whole. Returns
-- NULL when no evidence.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._compute_accessibility_features(p_fid bigint)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  WITH src AS (
    SELECT source, claimed_values, updated_at
      FROM public.beach_enrichment_provenance
     WHERE gold_fid = p_fid
       AND field_group = 'accessibility'
       AND is_canonical = true
  ),
  agg AS (
    SELECT
      (SELECT array_agg(DISTINCT source) FROM src)              AS sources,
      (SELECT max(updated_at) FROM src)                         AS updated_at,
      (SELECT (claimed_values->>'accessible_parking')::boolean
         FROM src WHERE claimed_values ? 'accessible_parking' LIMIT 1)     AS accessible_parking,
      (SELECT (claimed_values->>'accessible_restrooms')::boolean
         FROM src WHERE claimed_values ? 'accessible_restrooms' LIMIT 1)   AS accessible_restrooms,
      (SELECT claimed_values->>'path_to_sand'
         FROM src WHERE claimed_values ? 'path_to_sand' LIMIT 1)           AS path_to_sand,
      (SELECT (claimed_values->>'beach_mat')::boolean
         FROM src WHERE claimed_values ? 'beach_mat' LIMIT 1)              AS beach_mat,
      (SELECT (claimed_values->>'accessible_viewing')::boolean
         FROM src WHERE claimed_values ? 'accessible_viewing' LIMIT 1)     AS accessible_viewing,
      (SELECT (claimed_values->>'boardwalk')::boolean
         FROM src WHERE claimed_values ? 'boardwalk' LIMIT 1)              AS boardwalk,
      (SELECT claimed_values->'wheelchair_rental'
         FROM src WHERE claimed_values ? 'wheelchair_rental' LIMIT 1)      AS wheelchair_rental,
      (SELECT claimed_values->'beach_wheelchair_program'
         FROM src WHERE claimed_values ? 'beach_wheelchair_program' LIMIT 1) AS beach_wheelchair_program,
      (SELECT claimed_values->>'notes'
         FROM src WHERE claimed_values ? 'notes' LIMIT 1)                  AS notes
  )
  SELECT CASE
    WHEN sources IS NULL THEN NULL
    ELSE jsonb_strip_nulls(jsonb_build_object(
      '_sources',                  to_jsonb(sources),
      '_updated_at',               to_jsonb(updated_at),
      'accessible_parking',        to_jsonb(accessible_parking),
      'accessible_restrooms',      to_jsonb(accessible_restrooms),
      'path_to_sand',              to_jsonb(path_to_sand),
      'beach_mat',                 to_jsonb(beach_mat),
      'accessible_viewing',        to_jsonb(accessible_viewing),
      'boardwalk',                 to_jsonb(boardwalk),
      'wheelchair_rental',         wheelchair_rental,
      'beach_wheelchair_program',  beach_wheelchair_program,
      'notes',                     to_jsonb(notes)
    ))
  END
  FROM agg;
$$;

COMMENT ON FUNCTION public._compute_accessibility_features IS
  'Union-of-non-nulls across canonical BEP rows for field_group=accessibility. '
  'First non-null wins per scalar; nested objects taken whole. Returns NULL when no evidence. '
  'Per [[dwell-ada-crowd-autocurate]] Phase 2, 2026-06-07.';

-- ──────────────────────────────────────────────────────────────────────────
-- promote_canonical_accessibility_to_beach_amenities(p_fid)
--
-- Same signature as the existing fn (so refire_bep_cascade keeps working).
-- Body rewritten to:
--   1. Call _compute_accessibility_features (union, not last-write-wins)
--   2. Preserve the manual_curator guard from the original
--   3. xmax-distinguish inserts vs updates honestly
--
-- p_fid=NULL means "every fid with accessibility evidence" — used by
-- the backfill DO block at the end of this migration.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.promote_canonical_accessibility_to_beach_amenities(
  p_fid bigint DEFAULT NULL
)
RETURNS TABLE(rows_inserted bigint, rows_updated bigint)
LANGUAGE plpgsql
AS $$
DECLARE
  v_ins bigint := 0;
  v_upd bigint := 0;
BEGIN
  WITH targets AS (
    SELECT DISTINCT gold_fid
      FROM public.beach_enrichment_provenance
     WHERE field_group = 'accessibility'
       AND is_canonical = true
       AND gold_fid IS NOT NULL
       AND (p_fid IS NULL OR gold_fid = p_fid)
  ),
  computed AS (
    SELECT t.gold_fid,
           public._compute_accessibility_features(t.gold_fid) AS features
      FROM targets t
  ),
  upserted AS (
    INSERT INTO public.beach_amenities
      (arena_group_id, accessibility_features, accessibility_features_updated_at,
       source, curated_at)
    SELECT c.gold_fid, c.features, now(),
           COALESCE((SELECT source FROM public.beach_amenities ba
                      WHERE ba.arena_group_id = c.gold_fid),
                    'auto_promoted_from_consensus'),
           now()
      FROM computed c
     WHERE c.features IS NOT NULL
    ON CONFLICT (arena_group_id) DO UPDATE
       SET accessibility_features            = EXCLUDED.accessibility_features,
           accessibility_features_updated_at = now()
     WHERE beach_amenities.source IS DISTINCT FROM 'manual_curator'
       AND beach_amenities.accessibility_features IS DISTINCT FROM EXCLUDED.accessibility_features
    RETURNING (xmax = 0) AS inserted
  )
  SELECT count(*) FILTER (WHERE inserted),
         count(*) FILTER (WHERE NOT inserted)
    INTO v_ins, v_upd
    FROM upserted;

  RETURN QUERY SELECT v_ins, v_upd;
END;
$$;

COMMENT ON FUNCTION public.promote_canonical_accessibility_to_beach_amenities IS
  'Calls _compute_accessibility_features per fid; UPSERTs into beach_amenities.accessibility_features. '
  'Respects manual_curator guard. p_fid=NULL fires on every fid with evidence. Per [[dwell-ada-crowd-autocurate]] Phase 2, 2026-06-07.';

-- ──────────────────────────────────────────────────────────────────────────
-- Add accessibility dispatch to promote_canonical_to_consumer_tables.
-- This is what makes the existing BEP trigger auto-fire accessibility:
-- tg_promote_other_fields_chain -> promote_canonical_to_consumer_tables
-- -> (now also) promote_canonical_accessibility_to_beach_amenities.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.promote_canonical_to_consumer_tables(
  p_fid bigint DEFAULT NULL,
  p_min_confidence numeric DEFAULT 0.5
)
RETURNS TABLE(field_group text, rows_inserted bigint, rows_updated bigint, side_effects bigint)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT 'dogs'::text, r.rows_inserted, r.rows_updated, 0::bigint
      FROM public.promote_canonical_dogs_to_beach_dog_policy(p_fid, p_min_confidence) r;
  RETURN QUERY
    SELECT 'practical'::text, r.rows_inserted, r.rows_updated, r.gold_open_close_set
      FROM public.promote_canonical_practical_to_beach_amenities(p_fid, p_min_confidence) r;
  -- 2026-06-07: accessibility dispatch. Existing signature is (p_fid)
  -- only — no confidence threshold today; reserved for CCC ingest later.
  RETURN QUERY
    SELECT 'accessibility'::text, r.rows_inserted, r.rows_updated, 0::bigint
      FROM public.promote_canonical_accessibility_to_beach_amenities(p_fid) r;
END;
$$;

COMMIT;

-- ──────────────────────────────────────────────────────────────────────────
-- POST-APPLY backfill — refire the accessibility promoter across every fid
-- with evidence. This fixes the multi-source-data-loss bug on the ~12 fids
-- that had it. Idempotent: re-running produces the same JSONB.
-- ──────────────────────────────────────────────────────────────────────────

DO $$
DECLARE
  r record;
BEGIN
  SELECT * INTO r FROM public.promote_canonical_accessibility_to_beach_amenities(NULL);
  RAISE NOTICE 'accessibility_features backfill: % inserted, % updated', r.rows_inserted, r.rows_updated;
END $$;
