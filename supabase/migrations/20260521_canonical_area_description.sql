-- ─────────────────────────────────────────────────────────────────────
-- beaches_gold.canonical_area_description — Class D support column.
-- Franz 2026-05-21.
--
-- One sentence describing the beach's geographic identity (boundaries
-- + landmarks) in plain English. Consumed by scripts/region_scope_filter.py
-- when judging whether an extracted region_name from a multi-beach
-- policy_source applies to THIS specific beach.
--
-- Example for Del Mar Dog Beach (fid 8560):
--   "The North Beach Area of Del Mar: the beach and waters located
--    NORTH of an east-west line extending the centerline of 29th
--    Street, continuing north to the Solana Beach city border."
--
-- For most beaches the column is NULL and Class D defers to LLM
-- judgment from beach name + lat/lng alone.
-- ─────────────────────────────────────────────────────────────────────

ALTER TABLE public.beaches_gold
  ADD COLUMN IF NOT EXISTS canonical_area_description text;

COMMENT ON COLUMN public.beaches_gold.canonical_area_description IS
  'Optional curator-supplied sentence describing this beach''s geographic '
  'identity (boundaries + landmarks). Used by Class D scope filter to '
  'route multi-beach policy_source clauses to the correct beach. NULL = '
  'Class D falls back to name + lat/lng judgment.';

-- Seed Del Mar Dog Beach (8560) and Del Mar Beach (8992) as the type
-- cases that motivated this column.
UPDATE public.beaches_gold
   SET canonical_area_description =
       'The North Beach Area of Del Mar: the beach and waters located '
       'NORTH of an east-west line extending the centerline of 29th Street, '
       'continuing north to the Solana Beach city border. Often called '
       '"Rivermouth" or simply "Del Mar Dog Beach". This is where Del Mar '
       'Municipal Code §4.08.020(B)(1) applies (off-leash carve-out).'
 WHERE fid = 8560;

UPDATE public.beaches_gold
   SET canonical_area_description =
       'The main Del Mar City Beach: the beach and waters located '
       'SOUTH of an east-west line extending the centerline of 29th Street, '
       'INCLUDING the area between Powerhouse Park and 29th Street, and '
       'continuing south to the southern Del Mar city limit (around 6th '
       'Street). Excludes the North Beach Area (Del Mar Dog Beach) north '
       'of 29th. This is where Del Mar Municipal Code §4.08.020(B)(2), '
       '§4.08.020(C) (summer no-dogs zone), and §4.08.020(D) (Powerhouse '
       'Park Tot Lot / southern turf) apply.'
 WHERE fid = 8992;
