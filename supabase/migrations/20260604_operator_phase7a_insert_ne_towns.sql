-- Operator unification — Phase 7a: insert canonical operator rows for the
-- U1/U2 town candidates that don't exist in the bulk pool.
--
-- The bulk extraction loader (tiger_places) only loaded C1/C2/C5 place_types,
-- so most New England towns (U1) are completely absent. That's why Phase 6b
-- only flipped 9 flags — the rows we wanted to promote didn't exist.
--
-- Insert ~282 rows across states, following the existing pool's conventions:
--   canonical_name: "City of X" (yes, MA towns use "City of" prefix in pool —
--                                 see "City of Barnstable Town" precedent)
--   level: 'city'
--   origin_source: 'tiger_u1u2' (new marker for these added during NE fix)
--   is_canonical: true (intentionally canonical from creation)
--
-- After insert, re-run the resolver to attribute beaches to the new rows.

BEGIN;

WITH active AS (
  SELECT fid, state, geom FROM public.beaches_gold
   WHERE is_active AND scoring_tier IN ('hourly','daily')
),
ne_candidates AS (
  SELECT DISTINCT b.state, j.name AS town_name
    FROM active b
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
    JOIN public.jurisdictions j ON j.id=jb.id
   WHERE j.place_type IN ('U1','U2')
     AND j.funcstat IN ('A','S')
),
missing AS (
  SELECT nc.state, nc.town_name
  FROM ne_candidates nc
  WHERE NOT EXISTS (
    SELECT 1 FROM public.operators op
    WHERE op.state_code = nc.state
      AND (lower(op.canonical_name) = 'town of ' || lower(nc.town_name)
           OR lower(op.canonical_name) = 'city of ' || lower(nc.town_name)
           OR lower(op.canonical_name) = lower(nc.town_name))
  )
)
INSERT INTO public.operators
  (slug, canonical_name, level, state_code, origin_source, is_canonical)
SELECT
  public.slugify_agency('City of ' || m.town_name) ||
    '-' || lower(m.state),  -- state suffix prevents slug collisions across states
  'City of ' || m.town_name,
  'city',
  m.state,
  'tiger_county_subdivisions',
  true
FROM missing m
-- One row per (state, town_name); slug uniqueness depends on slugify+state suffix
ON CONFLICT DO NOTHING;

-- Re-resolve any beach whose Phase-9 backfill row still points at the wrong operator
UPDATE public.entity_operator eo
   SET operator_id = public._resolve_beach_operator(eo.entity_id),
       notes = 'Re-resolved by Phase 7a (NE town inserts, 2026-06-04)'
 WHERE eo.entity_type = 'beach'
   AND (eo.notes LIKE 'PIP-attribution backfill (operator unification phase 9%'
        OR eo.notes LIKE 'Re-resolved by Phase 9b%')
   AND public._resolve_beach_operator(eo.entity_id) IS NOT NULL
   AND public._resolve_beach_operator(eo.entity_id) <> eo.operator_id;

COMMIT;
