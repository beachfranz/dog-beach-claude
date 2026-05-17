-- Wave 4: Seed WA state + WA county agencies for dog-policy backfill.
--
-- WA is a greenfield state for `public.agency`: 0 rows pre-existed
-- (verified 2026-05-16). 29 WA counties have active beaches in
-- beaches_gold totaling ~440 beaches. This migration seeds the state
-- and all 29 county agencies so subsequent per-county backfills can
-- FK on (name, type) without needing to worry about ordering.
--
-- Parent linkage: counties parent under "State of Washington" so the
-- existing CA pattern (Sonoma/Humboldt parent_agency_id = 1 = State
-- of California) is matched.
--
-- Per-county scope used for seeding:
--   King 53, Island 49, Kitsap 45, San Juan 44, Jefferson 35,
--   Pierce 33, Clallam 33, Skagit 24, Snohomish 20, Grays Harbor 17,
--   Mason 13, Whatcom 13, Pacific 10, Chelan 9, Clark 6, Stevens 5,
--   Thurston 5, Grant 2, Lewis 2, Skamania 2, Kittitas 1,
--   Pend Oreille 1, Franklin 1, Whitman 1, Lincoln 1, Douglas 1,
--   Benton 1, Ferry 1, Spokane 1.

-- ─── 1. State of Washington ──────────────────────────────────────

INSERT INTO public.agency (name, short_name, type, default_authority_domains)
VALUES ('State of Washington', 'WA', 'state',
        ARRAY['statute', 'state_park_policy']::text[])
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. WA counties ──────────────────────────────────────────────

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
SELECT n, 'county'::agency_type,
       (SELECT id FROM public.agency WHERE name='State of Washington' AND type='state'),
       ARRAY['water_quality','public_health','unincorporated_dog_policy']::text[]
FROM (VALUES
  ('King County'),
  ('Island County'),
  ('Kitsap County'),
  ('San Juan County'),
  ('Jefferson County'),
  ('Pierce County'),
  ('Clallam County'),
  ('Skagit County'),
  ('Snohomish County'),
  ('Grays Harbor County'),
  ('Mason County'),
  ('Whatcom County'),
  ('Pacific County'),
  ('Chelan County'),
  ('Clark County'),
  ('Stevens County'),
  ('Thurston County'),
  ('Grant County'),
  ('Lewis County'),
  ('Skamania County'),
  ('Kittitas County'),
  ('Pend Oreille County'),
  ('Franklin County'),
  ('Whitman County'),
  ('Lincoln County'),
  ('Douglas County'),
  ('Benton County'),
  ('Ferry County'),
  ('Spokane County')
) AS v(n)
ON CONFLICT (name, type) DO NOTHING;
