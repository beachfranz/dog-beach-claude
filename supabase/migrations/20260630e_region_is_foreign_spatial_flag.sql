-- 20260630e_region_is_foreign_spatial_flag.sql
--
-- Spatial-pass persistence for the cross-beach zone_rules residual (2026-06-30).
-- The name-based guard (20260630d) catches shared-source contamination
-- (region carried on >=2 beaches) but MISSES single-beach contamination — a
-- multi-place ordinance attached to ONE beach (Otter Cove -> Berwick Park;
-- Rosemont -> Bellevue Botanical Garden; Capitola -> Jade Street Park). Those
-- regions are unique to one beach AND not self-named, so the name guard keeps
-- them. Distinguishing a genuine unique sub-feature from a foreign named place
-- needs geometry: scripts/spatial_filter_foreign_regions.py geocodes each
-- candidate (biased to the beach) and, when a name-matching place resolves
-- clearly far from the beach, sets region_is_foreign=true here.
--
-- This adds the per-row flag and folds it into _region_is_foreign_to_beach so
-- the injector (which already calls that guard) drops flagged regions on the
-- next promote. Descriptive/regulatory sub-zones ("roped-off nesting areas",
-- "Ocean Shore State Recreation Area - prohibition zone") never name-match a
-- geocoded place, so they are never flagged.

ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS region_is_foreign boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.beach_policy_source.region_is_foreign IS
  'TRUE when the spatial pass (spatial_filter_foreign_regions.py) confirmed this region_name geocodes to a name-matching place clearly outside THIS beach -> a foreign place, not a sub-zone. Folded into _region_is_foreign_to_beach so the zone_rules injector drops it.';

CREATE OR REPLACE FUNCTION public._region_is_foreign_to_beach(p_region_name text, p_fid bigint)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $fn$
DECLARE
  v_beach_name  text;
  v_shared_count int;
BEGIN
  -- NULL region_name = the canonical whole-beach default → keep (not foreign).
  IF p_region_name IS NULL THEN RETURN false; END IF;

  -- Spatial verdict (20260630e): geocoded to a name-matching place far from the
  -- beach. Authoritative over the name heuristics below.
  IF EXISTS (
    SELECT 1 FROM public.beach_policy_source b
     WHERE b.beach_fid = p_fid
       AND lower(b.region_name) = lower(p_region_name)
       AND b.region_is_foreign
  ) THEN
    RETURN true;
  END IF;

  SELECT coalesce(g.display_name_override, g.name) INTO v_beach_name
    FROM public.beaches_gold g WHERE g.fid = p_fid;

  -- Self-named: region_name mentions THIS beach → a physical sub-zone → keep.
  IF v_beach_name IS NOT NULL AND length(v_beach_name) >= 4
     AND lower(p_region_name) LIKE '%' || lower(v_beach_name) || '%' THEN
    RETURN false;
  END IF;

  -- Jurisdictional scope (county / city / agency) → foreign.
  IF public._is_jurisdictional_region_name(p_region_name) THEN
    RETURN true;
  END IF;

  -- Shared multi-place-source fingerprint: a genuine sub-zone of THIS beach
  -- appears ONLY on this beach. A region_name carried on >=2 beaches comes from
  -- a shared ordinance that lists many places → foreign.
  SELECT count(DISTINCT beach_fid) INTO v_shared_count
    FROM public.beach_policy_source
   WHERE lower(region_name) = lower(p_region_name);

  RETURN (v_shared_count >= 2);
END
$fn$;
