-- 20260523_state_government_strength.sql
--
-- Per-state classification of which municipal tier holds dog-policy
-- authority. Drives:
--   - _op_pass1_cities: whether to promote U-class CDPs in addition to
--     C-class formal cities (town-strong states need both)
--   - Whether to load TIGER County Subdivisions (MCDs) layer + promote
--     "Town of X" operators (town-strong only)
--   - Future: picker tier-priority ordering, codify-cascade enumeration
--     priority, hand-curated-URL backstop scope
--
-- Surfaced by NH virgin-state test 2026-05-23 LATE. NH has 13 cities +
-- 234 incorporated towns. The cities-only operator promoter captured
-- only 13 NH entities; the 234 towns (where actual dog-policy authority
-- sits) were invisible. MVP+ states (CA/MD/MA/MI/etc.) reached high
-- coverage because county_policy + city_policy extraction was enough.
-- For "strong-town" states (the 6 New England states + 2 mid-Atlantic
-- with similar gov structure), this is structurally insufficient.
--
-- Classification rules:
--   - 'town':   New England's 6 states + NJ/PA (boroughs/townships hold
--               primary authority over dog policy on public land).
--   - 'city':   DC only (the whole place is one city).
--   - 'county': default — most states. Counties + cities are the
--               primary dog-policy operators; townships are weak or
--               absent.
--
-- Seed values are starting estimates. Easy to refine per-state as we
-- launch more states; the registry is consumed at planning time, not
-- as a hard gate.

BEGIN;

CREATE TABLE IF NOT EXISTS public.state_government_strength (
  state_code text PRIMARY KEY,
  dominant_municipal_tier text NOT NULL
    CHECK (dominant_municipal_tier IN ('city', 'town', 'county')),
  notes text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.state_government_strength IS
  'Per-state classification of which municipal tier holds primary '
  'dog-policy authority. Drives operator-promotion, MCD-load, and '
  'picker decisions. Seeded 2026-05-23 LATE per NH virgin-state test '
  'finding.';

COMMENT ON COLUMN public.state_government_strength.dominant_municipal_tier IS
  '''town'' = New England + similar (towns/townships/boroughs hold primary '
  'authority); ''city'' = DC; ''county'' = default for most states '
  '(counties + cities are the primary operators).';

-- ── Town-strong states (New England + boroughs/townships east) ──────
INSERT INTO public.state_government_strength
  (state_code, dominant_municipal_tier, notes) VALUES
  ('CT', 'town', 'Connecticut: 169 towns; no county government since 1960.'),
  ('MA', 'town', 'Massachusetts: 351 cities + towns; counties largely abolished or ceremonial.'),
  ('ME', 'town', 'Maine: ~480 towns + plantations + unorganized territory; counties are weak.'),
  ('NH', 'town', 'New Hampshire: 234 towns + 13 cities; counties are weak (no exec branch). Canary 2026-05-23.'),
  ('NJ', 'town', 'New Jersey: 564 boroughs/townships/cities; counties exist but municipal tier dominates.'),
  ('PA', 'town', 'Pennsylvania: 2,500+ boroughs/townships; counties are intermediate.'),
  ('RI', 'town', 'Rhode Island: 39 cities + towns; no county government.'),
  ('VT', 'town', 'Vermont: 237 towns + 9 cities; counties are weak.')
ON CONFLICT (state_code) DO UPDATE SET
  dominant_municipal_tier = excluded.dominant_municipal_tier,
  notes = excluded.notes,
  updated_at = now();

-- ── DC: city-state ──────────────────────────────────────────────────
INSERT INTO public.state_government_strength
  (state_code, dominant_municipal_tier, notes) VALUES
  ('DC', 'city', 'Washington DC is a single city/federal district.')
ON CONFLICT (state_code) DO UPDATE SET
  dominant_municipal_tier = excluded.dominant_municipal_tier,
  notes = excluded.notes,
  updated_at = now();

-- ── County-strong default for the remaining 42 jurisdictions ────────
-- AK is weird (boroughs + unorganized) but classified county-strong for
-- pipeline purposes (the borough = county equivalent).
INSERT INTO public.state_government_strength
  (state_code, dominant_municipal_tier, notes)
SELECT s, 'county', 'Default classification: counties + cities are the primary dog-policy operators.'
  FROM unnest(ARRAY[
    'AL','AK','AZ','AR','CA','CO','DE','FL','GA','HI','ID','IL','IN','IA',
    'KS','KY','LA','MD','MI','MN','MS','MO','MT','NE','NV','NM','NY','NC',
    'ND','OH','OK','OR','SC','SD','TN','TX','UT','VA','WA','WV','WI','WY'
  ]) AS s
ON CONFLICT (state_code) DO UPDATE SET
  dominant_municipal_tier = excluded.dominant_municipal_tier,
  notes = excluded.notes,
  updated_at = now();

-- Indexes / grants
GRANT SELECT ON public.state_government_strength
  TO anon, authenticated, service_role;

COMMIT;

-- Sanity:
--   SELECT dominant_municipal_tier, COUNT(*) AS n
--     FROM public.state_government_strength
--    GROUP BY 1 ORDER BY 2 DESC;
--   Expected: county=42, town=8, city=1. Total 51 (50 states + DC).
