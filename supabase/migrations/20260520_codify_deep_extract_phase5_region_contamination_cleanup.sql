-- ─────────────────────────────────────────────────────────────────────
-- Codify deep-extract — Phase 5: region contamination cleanup
-- Franz 2026-05-20.
--
-- Three classes of noise accumulated in beach_policy_source.region_name:
--
--   A. Cross-beach contamination
--      A bps row tagged with region_name = ANOTHER beach's name in
--      beaches_gold. Created when a single policy_source covers
--      multiple sibling beaches (e.g. Port San Luis covers Avila +
--      Fisherman's + Olde Port; all three got rows for the others).
--      Audited 2026-05-20: 227 rows / 42 fids.
--
--   B. Jurisdictional baseline noise
--      region_name describes a citywide / statewide / "all parks"
--      scope rather than an actual area at THIS beach. Examples:
--      "All Washington State Parks", "City of Newport Beach — all…",
--      "Designated off-leash recreational areas", "…(general)",
--      "Citywide…". These rules are technically true at the
--      jurisdiction level but add no value at the per-beach
--      rendering and crowd out real region detail.
--      Audited 2026-05-20: ~1,943 rows / 620 fids.
--
--   C. Multi-phrasing of the same area (Rosie's-style — DEFERRED)
--      A single beach has multiple region_name variants all
--      describing one physical area. Not addressed here; needs
--      cross-source semantic merge in the consensus engine.
--      See project_phase3_region_merge_tomorrow memory pin.
--
-- This migration deletes A + B from beach_policy_source. After the
-- DELETEs, _promote_zone_rules_for_fid is re-run for every affected
-- fid so beach_dog_policy.zone_rules reflects the cleaned data.
-- ─────────────────────────────────────────────────────────────────────

BEGIN;

-- ─── A. cross-beach contamination ────────────────────────────────────

CREATE TEMP TABLE _classA_targets AS
SELECT bps.id, bps.beach_fid
  FROM public.beach_policy_source bps
  JOIN public.beaches_gold bg_self  ON bg_self.fid  = bps.beach_fid
  JOIN public.beaches_gold bg_other ON LOWER(TRIM(bg_other.name)) = LOWER(TRIM(bps.region_name))
                                   AND bg_other.fid <> bps.beach_fid
 WHERE bps.region_name IS NOT NULL
   AND LOWER(TRIM(bps.region_name)) <> LOWER(TRIM(bg_self.name));

DO $$
DECLARE c_rows int; c_fids int;
BEGIN
  SELECT COUNT(*), COUNT(DISTINCT beach_fid) INTO c_rows, c_fids FROM _classA_targets;
  RAISE NOTICE 'Class A targets: % rows across % fids', c_rows, c_fids;
END $$;

DELETE FROM public.beach_policy_source
 WHERE id IN (SELECT id FROM _classA_targets);

-- ─── B. jurisdictional baseline noise ────────────────────────────────
--
-- Patterns derived from the 2026-05-20 audit. SQL LIKE expressions
-- (case-insensitive via lower()). New patterns can be added as more
-- noise is discovered.

CREATE TEMP TABLE _classB_targets AS
SELECT bps.id, bps.beach_fid
  FROM public.beach_policy_source bps
 WHERE bps.region_name IS NOT NULL
   AND (
        LOWER(bps.region_name) LIKE 'all % beaches%'
     OR LOWER(bps.region_name) LIKE 'all % public places%'
     OR LOWER(bps.region_name) LIKE 'all public places%'
     OR LOWER(bps.region_name) LIKE 'all % parks%'
     OR LOWER(bps.region_name) LIKE 'all % park %'
     OR LOWER(bps.region_name) LIKE 'all % county %'
     OR LOWER(bps.region_name) LIKE 'all % state parks%'
     OR LOWER(bps.region_name) LIKE 'all city beaches%'
     OR LOWER(bps.region_name) LIKE 'all county beaches%'
     OR LOWER(bps.region_name) LIKE 'all port of %'
     OR LOWER(bps.region_name) LIKE 'all ocean beaches%'
     OR LOWER(bps.region_name) LIKE 'all public beaches%'
     OR LOWER(bps.region_name) LIKE 'all public streets%'
     OR LOWER(bps.region_name) LIKE 'all other %'
     OR LOWER(bps.region_name) LIKE 'all % city beaches%'
     OR LOWER(bps.region_name) LIKE 'all santa barbara %'
     OR LOWER(bps.region_name) LIKE 'all pacifica %'
     OR LOWER(bps.region_name) LIKE 'all malibu %'
     OR LOWER(bps.region_name) LIKE 'all la county %'
     OR LOWER(bps.region_name) LIKE 'all long beach %'
     OR LOWER(bps.region_name) LIKE 'los angeles city %'
     OR LOWER(bps.region_name) LIKE 'long beach city %'
     OR LOWER(bps.region_name) LIKE 'city of % all%'
     OR LOWER(bps.region_name) LIKE 'city of % — all%'
     OR LOWER(bps.region_name) LIKE 'city of % - all%'
     OR LOWER(bps.region_name) LIKE 'citywide%'
     OR LOWER(bps.region_name) LIKE '% citywide%'
     OR LOWER(bps.region_name) LIKE '%(citywide)%'
     OR LOWER(bps.region_name) LIKE 'designated off-leash%'
     OR LOWER(bps.region_name) LIKE 'designated off leash%'
     OR LOWER(bps.region_name) LIKE 'designated dog %areas%'
     OR LOWER(bps.region_name) LIKE 'designated dog play %'
     OR LOWER(bps.region_name) LIKE 'designated nesting %'
     OR LOWER(bps.region_name) LIKE 'designated nature %'
     OR LOWER(bps.region_name) LIKE 'designated picnic %'
     OR LOWER(bps.region_name) LIKE 'designated swimming %'
     OR LOWER(bps.region_name) LIKE 'designated % park %'
     OR LOWER(bps.region_name) LIKE 'designated % county %'
     OR LOWER(bps.region_name) LIKE 'designated % areas %'
     OR LOWER(bps.region_name) LIKE 'designated areas%'
     OR LOWER(bps.region_name) LIKE 'formally designated %'
     OR LOWER(bps.region_name) LIKE 'off-leash/no-leash %'
     OR LOWER(bps.region_name) LIKE '% county designated %'
     OR LOWER(bps.region_name) LIKE '%(general)%'
     OR LOWER(bps.region_name) LIKE '% (default)%'
     OR LOWER(bps.region_name) LIKE 'backcountry%(general)%'
     OR LOWER(bps.region_name) LIKE 'jurisdiction%'
     OR LOWER(bps.region_name) LIKE '% jurisdiction%'
     OR LOWER(bps.region_name) LIKE '%seattle city parks%'
     OR LOWER(bps.region_name) LIKE '%monterey city parks%'
     OR LOWER(bps.region_name) LIKE '%laguna beach public beaches%'
     OR LOWER(bps.region_name) LIKE '%state-park-wide%'
   );

DO $$
DECLARE c_rows int; c_fids int;
BEGIN
  SELECT COUNT(*), COUNT(DISTINCT beach_fid) INTO c_rows, c_fids FROM _classB_targets;
  RAISE NOTICE 'Class B targets: % rows across % fids', c_rows, c_fids;
END $$;

DELETE FROM public.beach_policy_source
 WHERE id IN (SELECT id FROM _classB_targets);

-- ─── Affected fids — collect for re-cascade ──────────────────────────

CREATE TEMP TABLE _affected_fids AS
SELECT DISTINCT beach_fid FROM _classA_targets
UNION
SELECT DISTINCT beach_fid FROM _classB_targets;

DO $$
DECLARE n int;
BEGIN
  SELECT COUNT(*) INTO n FROM _affected_fids;
  RAISE NOTICE 'Re-cascade needed for % distinct fids', n;
END $$;

-- ─── Re-cascade ──────────────────────────────────────────────────────
--
-- Calling _promote_zone_rules_for_fid for each affected beach.
-- Function is idempotent and respects manual_curator preservation.
-- Wrapped in PL/pgSQL with exception swallowing per fid so one bad
-- promotion can't roll back the whole batch.

DO $$
DECLARE
  r record;
  n_ok int := 0;
  n_skipped int := 0;
  n_err int := 0;
BEGIN
  FOR r IN SELECT beach_fid FROM _affected_fids ORDER BY beach_fid LOOP
    BEGIN
      PERFORM public._promote_zone_rules_for_fid(r.beach_fid);
      n_ok := n_ok + 1;
    EXCEPTION WHEN OTHERS THEN
      n_err := n_err + 1;
      RAISE NOTICE 'promote failed for fid=%: %', r.beach_fid, SQLERRM;
    END;
  END LOOP;
  RAISE NOTICE 'Re-cascade complete: ok=% err=%', n_ok, n_err;
END $$;

-- ─── Final audit ─────────────────────────────────────────────────────

DO $$
DECLARE n_a int; n_b int;
BEGIN
  SELECT COUNT(*) INTO n_a
    FROM public.beach_policy_source bps
    JOIN public.beaches_gold bg_self  ON bg_self.fid  = bps.beach_fid
    JOIN public.beaches_gold bg_other ON LOWER(TRIM(bg_other.name)) = LOWER(TRIM(bps.region_name))
                                     AND bg_other.fid <> bps.beach_fid
   WHERE bps.region_name IS NOT NULL
     AND LOWER(TRIM(bps.region_name)) <> LOWER(TRIM(bg_self.name));
  RAISE NOTICE 'POST-AUDIT  Class A remaining: %', n_a;

  -- Sample Class B remaining (most-common patterns)
  SELECT COUNT(*) INTO n_b
    FROM public.beach_policy_source
   WHERE region_name IS NOT NULL
     AND (LOWER(region_name) LIKE 'all % beaches%'
       OR LOWER(region_name) LIKE 'all % state parks%'
       OR LOWER(region_name) LIKE 'designated off-leash%'
       OR LOWER(region_name) LIKE '%citywide%'
       OR LOWER(region_name) LIKE '%(general)%');
  RAISE NOTICE 'POST-AUDIT  Class B common-patterns remaining: % (should be 0)', n_b;
END $$;

COMMIT;
