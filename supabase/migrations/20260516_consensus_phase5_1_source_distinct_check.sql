-- Consensus engine rewrite — Phase 5.1: entity promoter source-distinct fix.
--
-- Phase 5's entity promoter's ON CONFLICT WHERE clause only checked
-- if VALUE columns changed. If flag values happened to already match
-- (Crystal Cove + Crown Memorial — the legacy consensus had reached
-- the same answer), the source column stayed at
-- 'auto_promoted_from_consensus'. Functionally harmless (both
-- promoters are gated so neither will touch these beaches now), but
-- the source column lies about provenance.
--
-- Fix: add `OR beach_dog_policy.source IS DISTINCT FROM EXCLUDED.source`
-- to the upsert WHERE so source updates even when values match.
-- Then re-fire the entity promoter to backfill source on the
-- already-correct beaches.
--
-- Idempotent: re-runs harmlessly re-update the same rows.

CREATE OR REPLACE FUNCTION public.promote_entity_dogs_to_beach_dog_policy(p_fid bigint DEFAULT NULL)
RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
LANGUAGE plpgsql
AS $$
DECLARE
  v_inserted bigint := 0;
  v_updated  bigint := 0;
  v_skipped  bigint := 0;
BEGIN
  WITH targets AS (
    SELECT DISTINCT beach_fid
      FROM public.beach_policy_source
     WHERE (p_fid IS NULL OR beach_fid = p_fid)
  ),
  derived AS (
    SELECT
      t.beach_fid AS fid,
      c.dogs_allowed,
      c.leash_policy,
      c.off_leash_flag,
      c.has_on_leash,
      c.has_off_leash,
      c.section_count,
      c.canonical_tier_min
    FROM targets t
    CROSS JOIN LATERAL public._canonical_dogs_from_policy_sources(t.beach_fid) c
    WHERE c.has_canonical
  ),
  protected AS (
    SELECT arena_group_id FROM public.beach_dog_policy WHERE source = 'manual_curator'
  ),
  upsert AS (
    INSERT INTO public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    SELECT
      d.fid, d.dogs_allowed, d.leash_policy, d.off_leash_flag,
      d.has_on_leash, d.has_off_leash,
      'entity_promoted', NOW(),
      format(
        'Phase 5 entity-promoted: dogs=%s, has_on=%s, has_off=%s. '
        '%s section(s); canonical tier %s.',
        d.dogs_allowed,
        coalesce(d.has_on_leash::text, '?'),
        coalesce(d.has_off_leash::text, '?'),
        d.section_count,
        d.canonical_tier_min
      ),
      1.0,
      FALSE
    FROM derived d
    WHERE d.fid NOT IN (SELECT arena_group_id FROM protected)
    ON CONFLICT (arena_group_id) DO UPDATE
      SET dogs_allowed         = EXCLUDED.dogs_allowed,
          leash_policy         = EXCLUDED.leash_policy,
          off_leash_flag       = EXCLUDED.off_leash_flag,
          has_on_leash         = EXCLUDED.has_on_leash,
          has_off_leash        = EXCLUDED.has_off_leash,
          source               = EXCLUDED.source,
          curated_at           = NOW(),
          notes                = EXCLUDED.notes,
          consensus_confidence = EXCLUDED.consensus_confidence,
          disagreement_flag    = EXCLUDED.disagreement_flag
      WHERE beach_dog_policy.source <> 'manual_curator'
        AND (
          beach_dog_policy.dogs_allowed   IS DISTINCT FROM EXCLUDED.dogs_allowed
          OR beach_dog_policy.leash_policy   IS DISTINCT FROM EXCLUDED.leash_policy
          OR beach_dog_policy.off_leash_flag IS DISTINCT FROM EXCLUDED.off_leash_flag
          OR beach_dog_policy.has_on_leash   IS DISTINCT FROM EXCLUDED.has_on_leash
          OR beach_dog_policy.has_off_leash  IS DISTINCT FROM EXCLUDED.has_off_leash
          -- Phase 5.1: also force update when source label is stale.
          -- Otherwise beaches whose flags happen to match a previous
          -- consensus output keep their old source label and lie about
          -- which promoter owns them.
          OR beach_dog_policy.source IS DISTINCT FROM EXCLUDED.source
        )
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    count(*) FILTER (WHERE inserted),
    count(*) FILTER (WHERE NOT inserted),
    0
    INTO v_inserted, v_updated, v_skipped
    FROM upsert;

  RETURN QUERY SELECT v_inserted, v_updated, v_skipped;
END $$;

-- Re-fire entity promoter to backfill source on the already-correct beaches.
SELECT * FROM public.promote_entity_dogs_to_beach_dog_policy();
