-- 20260611i_canonicalize_water_sections.sql
--
-- Canonicalize zone_rules section keys: collapse `swimming_beach` and
-- `water_swim` (rare variants, 177 + 77 beaches) → `water` (the
-- dominant key, 1,073 beaches and matches beach.html's SECTION_NOUN).
--
-- Cause: zone_rules JSONB was emitted by multiple extractors and
-- consensus paths over time; section naming drifted between `water`,
-- `swimming_beach`, and `water_swim` even though all three mean "the
-- swim-side of the beach". beach.html's _SECTION_TILE only recognized
-- `water_swim` and `water`, so beaches whose policy used `swimming_beach`
-- didn't get the 🌊 Water tile rendered.
--
-- This migration:
--   1. Adds a helper public._zone_rules_canonicalize_water(jsonb) → jsonb
--      that walks the structure (seasonal or regions-bare) and renames
--      `swimming_beach` and `water_swim` keys to `water` everywhere.
--   2. UPDATEs every beach_dog_policy row containing one of the legacy
--      section keys, rewriting zone_rules in-place.
--
-- Future writes: not gated by a trigger (lower complexity, lower risk).
-- The frontend keeps belt-and-suspenders aliases for legacy keys per
-- the matching find.html / beach.html edits in this commit.

begin;

-- ── Helper: recursively walk JSONB and rename keys in sections ──────
-- Section objects live at zone_rules.[seasons[].]regions[].sections.{key}.
-- Two shapes: {seasons: [{regions: [...]}]} or {regions: [...]} bare.

CREATE OR REPLACE FUNCTION public._zone_rules_canonicalize_water(p_rules jsonb)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $function$
DECLARE
  v_seasons jsonb;
  v_season  jsonb;
  v_regions jsonb;
  v_region  jsonb;
  v_sections jsonb;
  v_canonical_sections jsonb;
  v_canonical_regions jsonb;
  v_canonical_seasons jsonb;
  v_canonical_result jsonb;
  v_key text;
  v_val jsonb;
  v_is_seasonal boolean;
BEGIN
  IF p_rules IS NULL OR jsonb_typeof(p_rules) <> 'object' THEN
    RETURN p_rules;
  END IF;

  -- Normalize to a seasons-array: either pull existing seasons, or wrap
  -- the bare {regions:[...]} object in a single-element array.
  IF p_rules ? 'seasons' AND jsonb_typeof(p_rules->'seasons') = 'array' THEN
    v_seasons := p_rules->'seasons';
    v_is_seasonal := true;
  ELSIF p_rules ? 'regions' AND jsonb_typeof(p_rules->'regions') = 'array' THEN
    v_seasons := jsonb_build_array(p_rules);
    v_is_seasonal := false;
  ELSE
    RETURN p_rules;
  END IF;

  v_canonical_seasons := '[]'::jsonb;

  FOR v_season IN SELECT * FROM jsonb_array_elements(v_seasons) LOOP
    v_regions := coalesce(v_season->'regions', '[]'::jsonb);
    v_canonical_regions := '[]'::jsonb;

    FOR v_region IN SELECT * FROM jsonb_array_elements(v_regions) LOOP
      v_sections := coalesce(v_region->'sections', '{}'::jsonb);

      IF jsonb_typeof(v_sections) <> 'object' THEN
        v_canonical_regions := v_canonical_regions || jsonb_build_array(v_region);
        CONTINUE;
      END IF;

      v_canonical_sections := '{}'::jsonb;
      FOR v_key, v_val IN SELECT * FROM jsonb_each(v_sections) LOOP
        IF v_key IN ('water_swim', 'swimming_beach') THEN
          -- Rename to 'water'. If 'water' already present, prefer the
          -- existing 'water' entry over the legacy one (most beaches
          -- only have one or the other anyway).
          IF v_canonical_sections ? 'water' THEN
            -- skip — keep existing water
            NULL;
          ELSE
            v_canonical_sections := v_canonical_sections || jsonb_build_object('water', v_val);
          END IF;
        ELSE
          IF v_key = 'water' AND v_canonical_sections ? 'water' THEN
            -- already populated by an earlier legacy alias — overwrite
            -- with the real 'water' entry
            v_canonical_sections := v_canonical_sections - 'water' || jsonb_build_object('water', v_val);
          ELSE
            v_canonical_sections := v_canonical_sections || jsonb_build_object(v_key, v_val);
          END IF;
        END IF;
      END LOOP;

      v_canonical_regions := v_canonical_regions
        || jsonb_build_array(v_region || jsonb_build_object('sections', v_canonical_sections));
    END LOOP;

    v_canonical_seasons := v_canonical_seasons
      || jsonb_build_array(v_season || jsonb_build_object('regions', v_canonical_regions));
  END LOOP;

  -- Reassemble in the original shape (seasonal vs bare).
  IF v_is_seasonal THEN
    v_canonical_result := p_rules || jsonb_build_object('seasons', v_canonical_seasons);
  ELSE
    -- Bare shape: unwrap the single season's regions back to top-level.
    v_canonical_result := p_rules
      || jsonb_build_object('regions', v_canonical_seasons->0->'regions');
  END IF;

  RETURN v_canonical_result;
END;
$function$;


-- ── Sweep existing beach_dog_policy rows ─────────────────────────────
-- Filter to rows that actually contain a legacy key, so we don't
-- gratuitously rewrite the 99% that already use `water`.
UPDATE public.beach_dog_policy dp
   SET zone_rules = public._zone_rules_canonicalize_water(zone_rules),
       zone_rules_updated_at = now()
 WHERE zone_rules::text ~* '\m(swimming_beach|water_swim)\M';

commit;
notify pgrst, 'reload schema';
