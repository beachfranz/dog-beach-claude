-- Formalize the bridge between public.operators (plural, 9,275 bulk-extracted
-- TIGER/CPAD/OSM entities) and public.operator (singular, 159 hand-curated
-- consumer-surface operators).
--
-- Before: code joined the two tables by `LOWER(name)` match — brittle on
-- punctuation/case/spacing divergence. See bridge_operator_to_cascade.py
-- line ~135 for an example. Per HARD memory rule operate-two-table-finding:
-- "NEVER join by raw operator_id across tables — coincidental ID collisions
-- produce bogus findings. Bridge plural→singular by NAME match."
--
-- After: explicit FK column. Code joins on `operators.canonical_operator_id
-- = operator.id` — fast, correct, no false-negatives.
--
-- Backfill: 155 of 159 singular operators have an exact (lower-cased) name
-- match in operators(plural). The other 4 (PSHDB, Crystal Cove Conservancy,
-- DOGPAW, Long Beach Parks Rec Marine) are nonprofits / sub-agencies not
-- present in TIGER. They stay singular-only — they're already cascade-wired
-- via entity_operator + policy_source.issuing_operator_id and don't need
-- extraction-side rows in operators(plural).
--
-- Reversibility: drop the column to undo. No destructive changes.

BEGIN;

ALTER TABLE public.operators
  ADD COLUMN canonical_operator_id BIGINT
  REFERENCES public.operator(id) ON DELETE SET NULL;

COMMENT ON COLUMN public.operators.canonical_operator_id IS
  'FK to public.operator(id) — the canonical consumer-surface operator '
  'row. Replaces the brittle LOWER(name) join. NULL when this plural row '
  'has no curated singular counterpart (most TIGER cities — they exist as '
  'extraction targets but never get a consumer-surface operator entry).';

-- Partial index: most rows will be NULL (only ~155 of 9275 have a bridge).
CREATE INDEX operators_canonical_operator_id_idx
  ON public.operators(canonical_operator_id)
  WHERE canonical_operator_id IS NOT NULL;

-- One-shot backfill via case-insensitive name match.
WITH backfill AS (
  SELECT o.id AS plural_id, op.id AS singular_id
    FROM public.operators o
    JOIN public.operator op ON LOWER(op.name) = LOWER(o.canonical_name)
)
UPDATE public.operators o
   SET canonical_operator_id = b.singular_id
  FROM backfill b
 WHERE o.id = b.plural_id;

-- Assertion: at least 100 backfilled (sanity floor; expected ~155).
DO $$
DECLARE
  n_linked INT;
  n_singular INT;
BEGIN
  SELECT count(*) INTO n_linked FROM public.operators WHERE canonical_operator_id IS NOT NULL;
  SELECT count(*) INTO n_singular FROM public.operator;
  RAISE NOTICE 'canonical_operator_id linked: % rows in operators(plural) → % distinct singular operators',
    n_linked, (SELECT count(DISTINCT canonical_operator_id) FROM public.operators WHERE canonical_operator_id IS NOT NULL);
  IF n_linked < 100 THEN
    RAISE EXCEPTION 'expected ≥100 backfilled rows; got %', n_linked;
  END IF;
END $$;

COMMIT;
