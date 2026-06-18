-- v3 dog-access redesign + water access helpers (zone_rules-derived).
--
-- Dog access was a binary off_leash_flag → +5. New spec (Franz 2026-06-18):
--   sand:  off_leash 10 / on_leash 5
--   water: off_leash +3 / on_leash +2
--   trails:off_leash +2 / on_leash +1
-- Water access also feeds a separate water_score (kept in both, intentional).
--
-- Rule → leash class: anything 'off_leash%' → off; 'not_allowed'/absent → 0;
-- everything else (on_leash, on_leash_or_voice, mixed, *_required, nuisance_
-- restriction, …) → on (dogs allowed, leashed by default).
-- Most-permissive across regions: off beats on beats none.

BEGIN;

-- per-section leash class across all regions: 'off' | 'on' | 'no' | NULL(absent)
CREATE OR REPLACE FUNCTION public._zone_leash_class(p_zone_rules jsonb, p_section text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  WITH rules AS (
    SELECT (r->'sections'->p_section->>'rule') AS rule
    FROM jsonb_array_elements(
           CASE WHEN jsonb_typeof(p_zone_rules->'regions')='array'
                THEN p_zone_rules->'regions' ELSE '[]'::jsonb END) r
    WHERE r->'sections' ? p_section
  )
  SELECT CASE
           WHEN count(*) = 0 THEN NULL
           WHEN bool_or(rule LIKE 'off_leash%')      THEN 'off'
           WHEN bool_or(rule IS DISTINCT FROM 'not_allowed') THEN 'on'
           ELSE 'no'
         END
  FROM rules;
$$;

-- water/swimming access class (most permissive of water + swimming_beach)
CREATE OR REPLACE FUNCTION public._water_access_class_v3(p_zone_rules jsonb)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN 'off' IN (public._zone_leash_class(p_zone_rules,'water'),
                   public._zone_leash_class(p_zone_rules,'swimming_beach')) THEN 'off'
    WHEN 'on'  IN (public._zone_leash_class(p_zone_rules,'water'),
                   public._zone_leash_class(p_zone_rules,'swimming_beach')) THEN 'on'
    WHEN 'no'  IN (public._zone_leash_class(p_zone_rules,'water'),
                   public._zone_leash_class(p_zone_rules,'swimming_beach')) THEN 'no'
    ELSE NULL
  END;
$$;

-- dog-access score: sand(10/5) + water(3/2) + trails(2/1). Sand falls back to
-- the whole-beach 'global' rule, then the policy booleans, when no sand section.
CREATE OR REPLACE FUNCTION public._dog_access_score_v3(
  p_zone_rules jsonb, p_off_leash_flag boolean, p_has_off boolean, p_has_on boolean)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
  WITH c AS (
    SELECT
      COALESCE(
        public._zone_leash_class(p_zone_rules,'sand'),
        public._zone_leash_class(p_zone_rules,'global'),
        CASE WHEN p_has_off THEN 'off' WHEN p_has_on THEN 'on' ELSE NULL END
      ) AS sand,
      public._water_access_class_v3(p_zone_rules)        AS water,
      public._zone_leash_class(p_zone_rules,'trails')    AS trails
  )
  SELECT (CASE c.sand   WHEN 'off' THEN 10 WHEN 'on' THEN 5 ELSE 0 END)
       + (CASE c.water  WHEN 'off' THEN 3  WHEN 'on' THEN 2 ELSE 0 END)
       + (CASE c.trails WHEN 'off' THEN 2  WHEN 'on' THEN 1 ELSE 0 END)
  FROM c;
$$;

-- water_score access base (zone): off 6 / on 3 / else 0
CREATE OR REPLACE FUNCTION public._water_access_base_v3(p_zone_rules jsonb)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE public._water_access_class_v3(p_zone_rules)
           WHEN 'off' THEN 6 WHEN 'on' THEN 3 ELSE 0 END;
$$;

COMMIT;
