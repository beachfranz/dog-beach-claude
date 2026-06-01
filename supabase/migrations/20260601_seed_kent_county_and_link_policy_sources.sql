-- 20260601_seed_kent_county_and_link_policy_sources.sql
--
-- Captures today's orphan-policy_source sweep:
--   - Seed Kent County Parks & Recreation (MD) as a new operator,
--     applied to BOTH public.operator + public.operators per pin
--     [[new-operator-both-tables]].
--   - Link policy_source 21  -> operator 93   (EBRPD Crown Beach)
--   - Link policy_source 331 -> operator 241  (Kent County Parks)
--   - Link policy_source 338 -> agency  101   (City of San Diego, the
--                                               SD Muni Code §63.0102 driver)
--
-- Idempotent: all inserts gated on NOT EXISTS so a replay is a no-op.
-- The new operator (id 241) was first created at runtime via
-- scripts/common/operator.add_canonical_operator(); this migration is
-- the durable record so dev/staging environments can reproduce.

BEGIN;

-- ─── 1. Kent County Parks & Recreation — singular table ──────────────

INSERT INTO public.operator (
  id, name, short_name, type, web_url, metadata, created_at, updated_at
)
SELECT 241, 'Kent County Parks & Recreation', 'Kent County Parks',
       'county_department'::operator_type,
       'https://kentparksandrec.org/',
       '{"created_for": "MD-launch", "agent_verified": false}'::jsonb,
       now(), now()
 WHERE NOT EXISTS (
   SELECT 1 FROM public.operator WHERE LOWER(name) = LOWER('Kent County Parks & Recreation')
 );

-- ─── 2. Kent County Parks & Recreation — plural table ────────────────

INSERT INTO public.operators (
  id, slug, canonical_name, short_name, level, subtype, state_code,
  origin_source, canonical_operator_id, created_at, updated_at
)
SELECT 83752, 'kent-county-parks-recreation', 'Kent County Parks & Recreation',
       'Kent County Parks',
       'county',
       'county-department',
       'MD', 'manual',
       (SELECT id FROM public.operator
         WHERE LOWER(name) = LOWER('Kent County Parks & Recreation')),
       now(), now()
 WHERE NOT EXISTS (
   SELECT 1 FROM public.operators WHERE slug = 'kent-county-parks-recreation'
 );

-- ─── 3. Re-point policy_source rows ──────────────────────────────────

-- EBRPD Crown Beach -> East Bay Regional Park District
UPDATE public.policy_source
   SET issuing_agency_id = NULL,
       issuing_operator_id = 93,
       updated_at = now()
 WHERE id = 21
   AND issuing_operator_id IS DISTINCT FROM 93;

-- Kent County Parks Betterton Beach -> the operator we just seeded
UPDATE public.policy_source
   SET issuing_agency_id = NULL,
       issuing_operator_id = (
         SELECT id FROM public.operator
          WHERE LOWER(name) = LOWER('Kent County Parks & Recreation')
       ),
       updated_at = now()
 WHERE id = 331
   AND issuing_operator_id IS DISTINCT FROM (
     SELECT id FROM public.operator
      WHERE LOWER(name) = LOWER('Kent County Parks & Recreation')
   );

-- SD Muni Code §63.0102 -> City of San Diego agency
UPDATE public.policy_source
   SET issuing_agency_id = 101,
       issuing_operator_id = NULL,
       updated_at = now()
 WHERE id = 338
   AND issuing_agency_id IS DISTINCT FROM 101;

COMMIT;
