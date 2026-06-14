-- 20260613j_county_policy_cap_confidence_093.sql
--
-- Cap county_policy confidence at 0.93 so it sits BELOW codified
-- statutes (0.95) and per-beach extractors (~0.86) in canonical
-- resolution. Before this:
--
--   codified_v1:*           0.95
--   county_policy           up to 0.97 (LA County blanket-ban tier)
--   per_beach_offleash_v2   0.86
--
-- LA County's `beaches.lacounty.gov/la-county-beach-rules/` set 34
-- per-beach prohibition rows at 0.97, beating codified statutes for
-- those beaches. Architecturally that's debatable — codified is
-- statute-grade with cite-required quotes; the county page is
-- official but aggregated/summary text. Capping at 0.93:
--
--   codified_v1:*           0.95   ← wins where it exists
--   county_policy           ≤ 0.93 ← wins fallback when codified absent
--   per_beach_offleash_v2   0.86
--
-- Three parts:
--   (1) Self-patch populate_from_county_dog_policy_gold to write
--       LEAST(confidence, 0.93) for future runs.
--   (2) Backfill: cap existing BEP rows.
--   (3) Re-resolve canonical for affected fids + fire consensus +
--       promote so the consumer surface reflects the new ranking.

BEGIN;

-- ─── 1. Self-patch the populator function ────────────────────────────
DO $patch$
DECLARE
  v_src text;
  v_old text := 'coalesce(op_conf, 0.85),';
  v_new text := 'LEAST(coalesce(op_conf, 0.85), 0.93),  -- 20260613j: cap below codified';
BEGIN
  v_src := pg_get_functiondef('public.populate_from_county_dog_policy_gold'::regproc);

  -- The function has TWO matches for that anchor (one each for city +
  -- county SELECT clauses, where city is left alone). Be specific.
  IF position(v_old IN v_src) = 0 THEN
    RAISE EXCEPTION 'populate_from_county_dog_policy_gold: confidence anchor missing — function body drifted';
  END IF;

  v_src := replace(v_src, v_old, v_new);
  EXECUTE v_src;
END
$patch$;

-- ─── 2. Backfill existing BEP rows ───────────────────────────────────
CREATE TEMP TABLE _capped_fids ON COMMIT DROP AS
SELECT DISTINCT gold_fid AS fid
  FROM public.beach_enrichment_provenance
 WHERE field_group = 'dogs'
   AND source = 'county_policy'
   AND confidence > 0.93;

UPDATE public.beach_enrichment_provenance
   SET confidence = 0.93,
       updated_at = now()
 WHERE field_group = 'dogs'
   AND source = 'county_policy'
   AND confidence > 0.93;

-- ─── 3. Re-resolve + propagate for affected fids ─────────────────────
-- The tg_normalize_dogs_bep_before_write trigger doesn't re-fire on a
-- confidence-only UPDATE (it gates on claimed_values changes per
-- 20260613f). Explicitly walk the chain for each affected fid.
DO $propagate$
DECLARE
  v_fid bigint;
BEGIN
  FOR v_fid IN SELECT fid FROM _capped_fids LOOP
    PERFORM public._resolve_dogs_gold(v_fid);
    PERFORM public.compute_beach_field_consensus(v_fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(v_fid, 0.5);
  END LOOP;
END
$propagate$;

COMMIT;

NOTIFY pgrst, 'reload schema';
