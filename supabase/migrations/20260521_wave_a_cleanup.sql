-- ─────────────────────────────────────────────────────────────────────
-- Wave A cleanup tasks — Franz 2026-05-21 batch.
--   #3 HBDB stranded bps cleanup (v2 inserts on manual_curator beach)
--   #5 Olde Port amenities backfill (copy Avila's shared facility row)
--   #6 Class B residue (4 jurisdictional baseline rows that escaped P5)
-- All applied via scripts/_tmp_wave_a.py 2026-05-21; this file is the
-- formal record for replayability.
-- ─────────────────────────────────────────────────────────────────────

BEGIN;

-- ── #3 HBDB (fid 6212) stranded bps cleanup ──────────────────────────
-- v2 re-extracted ps=5 + ps=20 for HBDB and inserted 7 bps rows. But
-- fid 6212's beach_dog_policy.source = 'manual_curator', so the cascade
-- preserves zone_rules. Those bps rows have no effect — delete them
-- so the beach is back to its pre-v2 bps state.
DELETE FROM public.beach_policy_source_temporal
 WHERE bps_id IN (SELECT id FROM public.beach_policy_source
                   WHERE beach_fid = 6212 AND policy_source_id IN (5, 20));
DELETE FROM public.beach_policy_source
 WHERE beach_fid = 6212 AND policy_source_id IN (5, 20);

-- ── #5 Olde Port amenities backfill ──────────────────────────────────
-- fid 8768 (Olde Port) had NO beach_amenities row. Port San Luis
-- Harbor District manages both Olde Port and Avila Beach (8751) with
-- the same shared facility (parking, restrooms, picnic, showers).
-- Copy Avila's amenities row to Olde Port.
INSERT INTO public.beach_amenities
  (arena_group_id, has_restrooms, has_showers, has_lifeguards,
   has_drinking_water, has_disabled_access, has_food, has_fire_pits,
   has_picnic_area, parking_type, parking_notes, hours_text, source,
   curated_at, notes, consensus_confidence, disagreement_flag, has_parking,
   accessibility_features, accessibility_features_updated_at,
   parking_fee, parking_subtype, parking_options)
SELECT
   8768, has_restrooms, has_showers, has_lifeguards,
   has_drinking_water, has_disabled_access, has_food, has_fire_pits,
   has_picnic_area, parking_type, parking_notes, hours_text,
   'shared_facility_with_avila', now(),
   'Stopgap: Olde Port co-located with Avila under Port San Luis Harbor District. Franz 2026-05-21.',
   consensus_confidence, disagreement_flag, has_parking,
   accessibility_features, accessibility_features_updated_at,
   parking_fee, parking_subtype, parking_options
FROM public.beach_amenities WHERE arena_group_id = 8751
ON CONFLICT (arena_group_id) DO NOTHING;

-- ── #6 Class B residue ───────────────────────────────────────────────
-- Phase 5's regex missed: "Dana Point — highway, street, alley, or
-- other public properties" (4 rows on 4 different fids, all from a
-- generic Dana Point municipal code clause that applies city-wide).
WITH targets AS (
  SELECT id, beach_fid FROM public.beach_policy_source
   WHERE region_name IS NOT NULL
     AND LOWER(region_name) LIKE '%highway, street, alley%'
)
DELETE FROM public.beach_policy_source_temporal
 WHERE bps_id IN (SELECT id FROM targets);

DELETE FROM public.beach_policy_source
 WHERE region_name IS NOT NULL
   AND LOWER(region_name) LIKE '%highway, street, alley%';

-- Re-cascade affected fids (4 Dana Point area beaches: 8681, 8682, 8838, 9718)
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT unnest(ARRAY[8681, 8682, 8838, 9718, 6212, 8768]::int[]) AS fid LOOP
    BEGIN PERFORM public._promote_zone_rules_for_fid(r.fid);
    EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'fid % promote fail: %', r.fid, SQLERRM;
    END;
  END LOOP;
END $$;

COMMIT;
