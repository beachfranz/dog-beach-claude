-- Backfill the 4 existing singular-only operators (orphans w.r.t. the
-- 2026-05-22 LATE canonical_operator_id FK) into public.operators (plural)
-- with origin_source='manual_canonical_bridge_backfill' so the FK bridges
-- both ways for every singular operator.
--
-- Per Franz 2026-05-22 LATE: when a new-state nonprofit gets added, the
-- discovery flow MUST populate BOTH tables (singular + plural with FK set)
-- — not the 4-orphan pattern that CA accidentally accumulated.
--
-- This migration brings the existing 4 CA orphans in line with that rule.
-- After this: every public.operator row has at least one public.operators
-- row pointing back via canonical_operator_id.
--
-- Orphans to backfill:
--   id=7   Preservation Society of Huntington Dog Beach (PSHDB) — CA nonprofit
--   id=8   Crystal Cove Conservancy (CCC)                       — CA nonprofit
--   id=166 DOGPAW                                                — WA nonprofit
--   id=169 Long Beach Parks, Recreation and Marine               — CA city_department
--
-- Level/subtype mapping:
--   operator.type='nonprofit'       → operators.level='private', subtype='ngo'
--   operator.type='city_department' → operators.level='city',    subtype='special-district'
--
-- Reversibility: DELETE FROM public.operators
--                 WHERE notes LIKE 'canonical-bridge backfill 2026-05-22%';

BEGIN;

WITH inserts AS (
  INSERT INTO public.operators
    (slug, canonical_name, short_name, level, subtype, state_code,
     origin_source, is_active, canonical_operator_id, notes)
  VALUES
    ('preservation-society-huntington-dog-beach',
     'Preservation Society of Huntington Dog Beach', 'PSHDB',
     'private', 'ngo', 'CA',
     'manual', true, 7,
     'canonical-bridge backfill 2026-05-22: paired with operator(id=7)'),
    ('crystal-cove-conservancy',
     'Crystal Cove Conservancy', 'CCC',
     'private', 'ngo', 'CA',
     'manual', true, 8,
     'canonical-bridge backfill 2026-05-22: paired with operator(id=8)'),
    ('dogpaw',
     'DOGPAW', 'DOGPAW',
     'private', 'ngo', 'WA',
     'manual', true, 166,
     'canonical-bridge backfill 2026-05-22: paired with operator(id=166)'),
    ('long-beach-parks-recreation-and-marine',
     'Long Beach Parks, Recreation and Marine', 'LB Parks Rec Marine',
     'city', 'special-district', 'CA',
     'manual', true, 169,
     'canonical-bridge backfill 2026-05-22: paired with operator(id=169)')
  ON CONFLICT (slug) DO NOTHING
  RETURNING id, canonical_operator_id, canonical_name
)
SELECT * FROM inserts;

-- Assertion: every operator (singular) row now has a backref via the FK
DO $$
DECLARE
  n_orphans INT;
BEGIN
  SELECT count(*) INTO n_orphans
    FROM public.operator op
   WHERE NOT EXISTS (SELECT 1 FROM public.operators o
                      WHERE o.canonical_operator_id = op.id);
  RAISE NOTICE 'singular operators without plural backref: %', n_orphans;
  IF n_orphans > 0 THEN
    RAISE EXCEPTION 'backfill incomplete: % singular orphans remain', n_orphans;
  END IF;
END $$;

COMMIT;
