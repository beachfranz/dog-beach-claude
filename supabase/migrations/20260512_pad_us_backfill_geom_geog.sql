-- Backfill pad_us_units.geom_geog from geom for any row where geom_geog is null.
--
-- Found during PAD-US method A/B comparison (2026-05-12 evening): OR (9,598)
-- and WA (10,391) rows have geom populated but geom_geog null. Both
-- populate_pad_us_containment_gold (method A) and v2 (method B) spatial-
-- join on geom_geog, so OR/WA returned zero candidates and the entire
-- PAD-US layer failed silently for those states (142+353 both_missing in
-- the comparison view).
--
-- Idempotent: only touches rows where geom_geog is null and geom is not.

update public.pad_us_units
   set geom_geog = geom::geography
 where geom_geog is null
   and geom is not null;
