-- Operator unification — Phase 6: flip is_canonical=true on PIP-reachable
-- city / county / CPAD agency operators that already exist as plural rows.
--
-- Sizing pre-Phase-6:
--   city    265 distinct candidates · 59 already canonical · 186 flippable · 20 need_new_insert
--   county  261                     · 40                  · 189            · 32
--   cpad    54                      · 21                  · 10             · 23
--   ────────────────────────────────────────────────────────────────────────
--   total   580                     · 120                 · 385            · 75
--
-- Phase 6 (this file) does the 385 flips. Phase 7 handles the 75 inserts
-- separately because new-insert decisions need per-candidate review.
--
-- Behavior change: cascade Tier 0 doesn't fire on a beach until entity_operator
-- ALSO links that beach to the operator. Promotion alone doesn't create
-- entity_operator linkages — that happens in Phase 9 (beach_operator backfill).
-- So Phase 6 is purely additive to the canonical pool with no immediate
-- consumer-surface impact. Reversible by UPDATE ... SET is_canonical = false
-- WHERE id IN (...).

BEGIN;

-- ── 1. Pre-flight: total canonical baseline before promotion ─────────
DO $$
DECLARE v_n int;
BEGIN
  SELECT count(*) INTO v_n FROM public.operators WHERE is_canonical = true;
  IF v_n NOT BETWEEN 200 AND 220 THEN
    RAISE EXCEPTION 'Unexpected canonical baseline before Phase 6: %', v_n;
  END IF;
END $$;

-- ── 2. Promote cities — match by state + (canonical_name = 'City of X' OR = 'X') ──
-- Candidates: TIGER place names of incorporated cities that contain ≥1 active
-- scored beach via 200m buffer.
WITH active AS (SELECT fid, state, geom FROM public.beaches_gold WHERE is_active AND scoring_tier IN ('hourly','daily')),
city_cands AS (
  SELECT DISTINCT j.name AS city_name, b.state
  FROM active b
  JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
  JOIN public.jurisdictions j ON j.id=jb.id AND j.funcstat='A' AND j.place_type LIKE 'C%'
)
UPDATE public.operators p
   SET is_canonical = true
  FROM city_cands cc
 WHERE p.state_code = cc.state
   AND NOT p.is_canonical
   AND (lower(p.canonical_name) = 'city of ' || lower(cc.city_name)
        OR lower(p.canonical_name) = lower(cc.city_name));

-- ── 3. Promote counties — match by state + canonical_name = 'X County' ──
WITH active AS (SELECT fid, state, geom FROM public.beaches_gold WHERE is_active AND scoring_tier IN ('hourly','daily')),
county_cands AS (
  SELECT DISTINCT co.name || ' County' AS county_name, b.state
  FROM active b JOIN public.counties co ON ST_Contains(co.geom, b.geom)
)
UPDATE public.operators p
   SET is_canonical = true
  FROM county_cands cc
 WHERE p.state_code = cc.state
   AND NOT p.is_canonical
   AND lower(p.canonical_name) = lower(cc.county_name);

-- ── 4. Promote CPAD agencies — match by canonical_name = mng_agncy ───
WITH active AS (SELECT fid, state, geom FROM public.beaches_gold WHERE is_active AND scoring_tier IN ('hourly','daily')),
cpad_cands AS (
  SELECT DISTINCT c.mng_agncy AS agncy_name
  FROM (SELECT DISTINCT ON (b.fid) b.fid, cu.mng_agncy
        FROM active b JOIN public.cpad_units cu ON ST_Contains(cu.geom, b.geom) AND cu.mng_agncy IS NOT NULL
        ORDER BY b.fid, ST_Area(cu.geom) ASC) c
)
UPDATE public.operators p
   SET is_canonical = true
  FROM cpad_cands cc
 WHERE NOT p.is_canonical
   AND lower(p.canonical_name) = lower(cc.agncy_name);

-- ── 5. Post-condition: canonical count grew by ~385 ──────────────────
DO $$
DECLARE v_n int;
BEGIN
  SELECT count(*) INTO v_n FROM public.operators WHERE is_canonical = true;
  IF v_n NOT BETWEEN 500 AND 700 THEN
    RAISE EXCEPTION 'Canonical count after Phase 6 out of expected range: %', v_n;
  END IF;
  RAISE NOTICE 'Phase 6 complete: % rows now is_canonical=true', v_n;
END $$;

COMMIT;
