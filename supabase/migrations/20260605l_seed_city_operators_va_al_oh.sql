-- Seed city operators for VA/AL/OH dog-park walker.
--
-- Why: dog-park-state-launch walker requires
--   dog_parks_gold.inferred_operator_id IS NOT NULL for ≥3 parks per
--   is_canonical operator. Today's first sweep across new MVP+ states
--   yielded only +6 promotions because walker was a no-op everywhere
--   (preflight reported `low_operator_coverage:0_ops_with_3plus_parks`
--   for VA — same shape for AL/OH/DE/MA).
--
-- Preflight's city suggestions (cities with ≥3 active scoreable parks):
--   VA: Arlington 9, Alexandria 8, Richmond 5, Norfolk 3 (4 cities, 25 parks)
--   AL: Huntsville 7, Birmingham 6                       (2 cities, 13 parks)
--   OH: Columbus 3                                       (1 city, 3 parks)
--   DE: none (state too small)
--   MA: none (only 9 total parks, no city ≥3)
--
-- Two-part fix:
--   1. Make the operators canonical (flip existing or INSERT new)
--   2. PIP-backfill inferred_operator_id on parks via address_city match
--
-- scripts/dog_park_pipeline/seed_operators.py is the codified path, but
-- it depends on the deprecated scripts/common/operator.py (Phase 5a
-- unified the two-table model — common/operator.py raises
-- NotImplementedError). Doing this directly via SQL; seed_operators.py
-- needs a separate rewrite to use the unified operators table.

-- ── Part 1a: flip existing operators to canonical ─────────────────────
UPDATE public.operators
   SET is_canonical = true
 WHERE id IN (
   71828,  -- VA: Arlington County
   71917,  -- VA: Alexandria city
   71946,  -- VA: Richmond city
   63124   -- OH: City of Columbus
 );
-- VA Norfolk (id 71741, "City of Norfolk") already canonical=true.

-- ── Part 1b: INSERT new canonical operators for AL ────────────────────
-- AL has no existing city-level operators for Huntsville/Birmingham.
-- Insert as level='city', is_canonical=true, state_code='AL'.
INSERT INTO public.operators
  (slug, canonical_name, level, state_code, is_canonical, origin_source)
VALUES
  ('al-city-of-huntsville', 'City of Huntsville', 'city', 'AL', true, 'manual'),
  ('al-city-of-birmingham', 'City of Birmingham', 'city', 'AL', true, 'manual')
ON CONFLICT (slug) DO UPDATE SET is_canonical = true;

-- ── Part 2: PIP-backfill inferred_operator_id on dog_parks_gold ───────
-- Match dog_parks_gold (state, address_city) → operators (id) for the
-- 7 cities. Existing parks already have address_city set by
-- pip_address_city_backfill (no rows updated in today's pipeline run
-- = already populated). attribution_method records the linkage source.

UPDATE public.dog_parks_gold dpg
   SET inferred_operator_id = sub.op_id,
       attribution_method = 'seed_city_match_2026_06_05',
       attribution_confidence = 'medium'
  FROM (
    SELECT 'VA' AS state, 'Arlington'  AS city, 71828::bigint AS op_id UNION ALL
    SELECT 'VA',          'Alexandria',          71917 UNION ALL
    SELECT 'VA',          'Richmond',            71946 UNION ALL
    SELECT 'VA',          'Norfolk',             71741 UNION ALL
    SELECT 'OH',          'Columbus',            63124 UNION ALL
    SELECT 'AL',          'Huntsville',          (SELECT id FROM public.operators WHERE slug='al-city-of-huntsville') UNION ALL
    SELECT 'AL',          'Birmingham',          (SELECT id FROM public.operators WHERE slug='al-city-of-birmingham')
  ) sub
 WHERE dpg.state = sub.state
   AND dpg.address_city = sub.city
   AND dpg.is_active
   AND dpg.is_scoreable;
