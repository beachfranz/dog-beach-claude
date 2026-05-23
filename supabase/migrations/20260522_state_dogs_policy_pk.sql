-- Add scope discriminator + PRIMARY KEY to state_dogs_policy.
--
-- Per task #150 + the [[state-dogs-policy-honest-framing]] memory pin.
-- The table has no PK and no UNIQUE — but the data correctly has 2 rows
-- for OR (one statewide default + one coastal Beach Bill carve-out).
-- Those aren't dupes; they're scope-segmented policy. The schema needs
-- to express this so:
--   (a) future scoped states (MA tidal vs inland? CA Coastal Act?) get
--       the same shape
--   (b) writers can use ON CONFLICT (state_code, scope_label) DO UPDATE
--       instead of unsafe DELETE-by-state-code-then-INSERT (which would
--       nuke scoped rows on re-seed)
--
-- Scope labels seeded:
--   'default'  — statewide fallback (the one most states have today)
--   'coastal'  — OR Beach Bill carve-out for Pacific coastal counties
--                (signal: county_fips_filter IS NOT NULL)
--
-- Reversibility: drop the column + PK to undo.

BEGIN;

-- Step 1: add nullable scope_label
ALTER TABLE public.state_dogs_policy
  ADD COLUMN scope_label TEXT;

-- Step 2: default all rows to 'default'
UPDATE public.state_dogs_policy SET scope_label = 'default';

-- Step 3: OR coastal row (with county_fips_filter) → 'coastal'
UPDATE public.state_dogs_policy
   SET scope_label = 'coastal'
 WHERE state_code = 'OR'
   AND county_fips_filter IS NOT NULL;

-- Step 4: make scope_label required
ALTER TABLE public.state_dogs_policy
  ALTER COLUMN scope_label SET NOT NULL;

-- Step 5: add the composite PK
ALTER TABLE public.state_dogs_policy
  ADD CONSTRAINT state_dogs_policy_pk PRIMARY KEY (state_code, scope_label);

-- Step 6: index for the common state-only lookup (criterion function uses it)
CREATE INDEX state_dogs_policy_state_idx
  ON public.state_dogs_policy (state_code);

COMMENT ON COLUMN public.state_dogs_policy.scope_label IS
  'Scope discriminator for per-state policy multiplicity. Most states '
  'have a single ''default'' row. States with carve-outs (e.g., OR''s '
  'Beach Bill coastal counties) add scoped rows like ''coastal''. '
  'PK is (state_code, scope_label) — writers should use ON CONFLICT '
  'instead of DELETE-by-state-code (which would nuke scoped carve-outs).';

-- Verify
DO $$
DECLARE
  n_total INT;
  n_states INT;
BEGIN
  SELECT count(*), count(DISTINCT state_code) INTO n_total, n_states
    FROM public.state_dogs_policy;
  RAISE NOTICE 'state_dogs_policy post-migration: % rows across % distinct states', n_total, n_states;
  IF n_total < 9 THEN
    RAISE EXCEPTION 'expected ≥9 rows; got %', n_total;
  END IF;
END $$;

COMMIT;
