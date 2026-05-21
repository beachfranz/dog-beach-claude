-- ─────────────────────────────────────────────────────────────────────
-- Codify deep-extract — Phase 3: cross-source region canonicalization
-- Franz 2026-05-21.
--
-- Class C of the cascade contamination cleanup. Class A (cross-beach,
-- 227 rows) and Class B (jurisdictional baselines, 1,990 rows) were
-- deleted yesterday (2026-05-20). This phase canonicalizes the
-- remaining region_name noise via TWO passes:
--
--   Pass A — Amenity-class sections shouldn't have region_name at all.
--   parking_lot / walkway / restrooms / showers / restrooms_showers /
--   picnic_area / fire_pits / playground / campground are AMENITIES,
--   not zone regions. The rule (parking is on-leash, etc.) still
--   applies — it just attaches to the amenity tile in the default
--   region. Audited 2026-05-21: 150 rows / 108 fids.
--
--   Pass B — Multiple region_name variants for the SAME canonical
--   (beach_fid, section, rule, time_windows-signature) collapse to
--   NULL (default region). This handles Rosie's-style multi-phrasing
--   ("Rosie's Dog Beach Dog Zone" + "Off-leash zone (Granada to
--   Roycroft)" + "Dog exercise area between Granada Ave..." all
--   describe the same sand → NULL). Audited 2026-05-21: 419 groups /
--   1,205 rows / 331 fids.
--
-- Mechanism: UPDATE bps.region_name in place. Re-cascade affected
-- fids. Raw region_name strings lost (acceptable — they were noise).
-- ─────────────────────────────────────────────────────────────────────

BEGIN;

-- ─── Pass A: amenity-class sections → NULL region_name ───────────────

CREATE TEMP TABLE _passA_targets AS
SELECT id, beach_fid
  FROM public.beach_policy_source
 WHERE section IN ('parking_lot','walkway','restrooms','showers',
                   'restrooms_showers','picnic_area','fire_pits',
                   'playground','campground')
   AND region_name IS NOT NULL;

DO $$
DECLARE n_rows int; n_fids int;
BEGIN
  SELECT COUNT(*), COUNT(DISTINCT beach_fid) INTO n_rows, n_fids FROM _passA_targets;
  RAISE NOTICE 'Pass A targets: % rows across % fids', n_rows, n_fids;
END $$;

-- Conflict-aware UPDATE: nulling region_name can collide with an
-- existing NULL-region row at the same (beach, source, section, rule)
-- unique key. For each affected canonical key: pick one keeper (prefer
-- already-NULL, else longest evidence, else min id), DELETE the rest,
-- then UPDATE the keeper.
WITH ck AS (  -- canonical keys touched by Pass A
  SELECT DISTINCT bps.beach_fid, bps.policy_source_id, bps.section, bps.rule
    FROM public.beach_policy_source bps
    JOIN _passA_targets t ON t.id = bps.id
),
keepers AS (
  SELECT DISTINCT ON (bps.beach_fid, bps.policy_source_id, bps.section, bps.rule)
         bps.id
    FROM public.beach_policy_source bps
    JOIN ck USING (beach_fid, policy_source_id, section, rule)
   ORDER BY bps.beach_fid, bps.policy_source_id, bps.section, bps.rule,
            (bps.region_name IS NULL) DESC,
            LENGTH(COALESCE(bps.evidence_verbatim,'')) DESC,
            bps.id ASC
),
losers AS (
  SELECT bps.id
    FROM public.beach_policy_source bps
    JOIN ck USING (beach_fid, policy_source_id, section, rule)
   WHERE bps.id NOT IN (SELECT id FROM keepers)
)
DELETE FROM public.beach_policy_source WHERE id IN (SELECT id FROM losers);

UPDATE public.beach_policy_source
   SET region_name = NULL
 WHERE id IN (SELECT id FROM _passA_targets)
   AND region_name IS NOT NULL;  -- keepers that survived the DELETE

-- ─── Pass B: collapse multi-phrasing of same canonical group ─────────
--
-- For each (beach_fid, section, rule, time_windows-signature) where
-- multiple region_name variants exist, set region_name = NULL on all
-- rows in the group. The "default region" then carries the rule, and
-- the renderer's most-restrictive-wins + per-minute merge (shipped
-- 2026-05-20) handles overlap correctly.

CREATE TEMP TABLE _passB_targets AS
WITH sigs AS (
  SELECT bps.id, bps.beach_fid, bps.section, bps.rule, bps.region_name,
         COALESCE(string_agg(
           concat_ws('|', t.window_kind, t.daily_start, t.daily_end,
                     t.exception_rule, t.anchor_start, t.anchor_end),
           ',' ORDER BY t.daily_start NULLS FIRST, t.daily_end NULLS FIRST,
                       t.exception_rule),
           '__nw__') AS tw_sig
    FROM public.beach_policy_source bps
    LEFT JOIN public.beach_policy_source_temporal t ON t.bps_id = bps.id
   WHERE bps.section NOT IN ('parking_lot','walkway','restrooms','showers',
                             'restrooms_showers','picnic_area','fire_pits',
                             'playground','campground')
     AND bps.region_name IS NOT NULL
   GROUP BY bps.id, bps.beach_fid, bps.section, bps.rule, bps.region_name
),
groups AS (
  SELECT beach_fid, section, rule, tw_sig
    FROM sigs
   GROUP BY beach_fid, section, rule, tw_sig
  HAVING COUNT(DISTINCT region_name) > 1
)
SELECT sigs.id, sigs.beach_fid
  FROM sigs
  JOIN groups USING (beach_fid, section, rule, tw_sig);

DO $$
DECLARE n_rows int; n_fids int;
BEGIN
  SELECT COUNT(*), COUNT(DISTINCT beach_fid) INTO n_rows, n_fids FROM _passB_targets;
  RAISE NOTICE 'Pass B targets: % rows across % fids', n_rows, n_fids;
END $$;

-- Same conflict-aware DELETE-losers + UPDATE-keepers pattern as Pass A.
WITH ck AS (
  SELECT DISTINCT bps.beach_fid, bps.policy_source_id, bps.section, bps.rule
    FROM public.beach_policy_source bps
    JOIN _passB_targets t ON t.id = bps.id
),
keepers AS (
  SELECT DISTINCT ON (bps.beach_fid, bps.policy_source_id, bps.section, bps.rule)
         bps.id
    FROM public.beach_policy_source bps
    JOIN ck USING (beach_fid, policy_source_id, section, rule)
   ORDER BY bps.beach_fid, bps.policy_source_id, bps.section, bps.rule,
            (bps.region_name IS NULL) DESC,
            LENGTH(COALESCE(bps.evidence_verbatim,'')) DESC,
            bps.id ASC
),
losers AS (
  SELECT bps.id
    FROM public.beach_policy_source bps
    JOIN ck USING (beach_fid, policy_source_id, section, rule)
   WHERE bps.id NOT IN (SELECT id FROM keepers)
)
DELETE FROM public.beach_policy_source WHERE id IN (SELECT id FROM losers);

UPDATE public.beach_policy_source
   SET region_name = NULL
 WHERE id IN (SELECT id FROM _passB_targets)
   AND region_name IS NOT NULL;

-- ─── Re-cascade ──────────────────────────────────────────────────────

CREATE TEMP TABLE _affected_fids AS
SELECT DISTINCT beach_fid FROM _passA_targets
UNION
SELECT DISTINCT beach_fid FROM _passB_targets;

DO $$
DECLARE n int;
BEGIN
  SELECT COUNT(*) INTO n FROM _affected_fids;
  RAISE NOTICE 'Re-cascade needed for % distinct fids', n;
END $$;

DO $$
DECLARE
  r record;
  n_ok int := 0;
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

-- ─── Post-audit ──────────────────────────────────────────────────────

DO $$
DECLARE n_amen int; n_groups int;
BEGIN
  SELECT COUNT(*) INTO n_amen FROM public.beach_policy_source
   WHERE section IN ('parking_lot','walkway','restrooms','showers',
                     'restrooms_showers','picnic_area','fire_pits',
                     'playground','campground')
     AND region_name IS NOT NULL;
  RAISE NOTICE 'POST-AUDIT  Pass A residue (amenity with region_name): % (should be 0)', n_amen;

  WITH sigs AS (
    SELECT bps.beach_fid, bps.section, bps.rule, bps.region_name,
           COALESCE(string_agg(concat_ws('|', t.window_kind, t.daily_start,
                                         t.daily_end, t.exception_rule,
                                         t.anchor_start, t.anchor_end),
                               ',' ORDER BY t.daily_start NULLS FIRST), '__nw__') AS sig
      FROM public.beach_policy_source bps
      LEFT JOIN public.beach_policy_source_temporal t ON t.bps_id = bps.id
     WHERE bps.region_name IS NOT NULL
     GROUP BY bps.id, bps.beach_fid, bps.section, bps.rule, bps.region_name
  )
  SELECT COUNT(*) INTO n_groups FROM (
    SELECT beach_fid, section, rule, sig
      FROM sigs
     GROUP BY beach_fid, section, rule, sig
    HAVING COUNT(DISTINCT region_name) > 1
  ) g;
  RAISE NOTICE 'POST-AUDIT  Pass B residue (collapsible groups remaining): % (should be 0)', n_groups;
END $$;

COMMIT;
