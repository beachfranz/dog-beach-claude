-- Consensus engine rewrite — Phase 7.1: backfill beach_agency m:m
-- by linking tier-1+2 CA beaches to their agency rows.
--
-- Scope: tier-1+2 CA active beaches (~245 beaches per Phase 7 survey).
-- Tier-3/4 beaches don't get backfilled here — they're out of the
-- MVP+ scope per [[mvp-plus-ca-or-wa]].
--
-- Authority-domain assignment logic:
--
-- 1. PRIMARY DOG_POLICY JURISDICTION — exactly one agency gets
--    rank=1 on `dog_policy` per beach, determined by land ownership:
--      (a) CPAD owner is State or Federal → that agency
--      (b) CPAD owner is gov Special District park → that agency
--      (c) Else c1_jurisdiction is incorporated city → city
--      (d) Else → county
--
-- 2. COUNTY ALWAYS LINKED — counties have state-mandated authority
--    over beach water quality + public health regardless of who
--    owns the land. Linked with rank=1 on those domains.
--
-- 3. CITY ALWAYS LINKED IF PRESENT — even when state/federal land,
--    the city still controls parking, fire, water_safety (lifeguards
--    when contracted), and adjacent operations. rank=1 on those
--    secondary domains.
--
-- 4. MILITARY OVERLAY — beaches with military_lands hit get a
--    federal_military agency link on access_restriction. Adjacent
--    beaches get rank=2 (advisory; main authority stays civilian).
--
-- 5. OPERATIONAL OWNER — CPAD county_department / special_district
--    rows get rank=1 on operations + land_use even when not the
--    dog_policy primary.
--
-- Precedence ranks used here:
--   1 = primary authority (canonical resolver picks this)
--   2 = secondary authority (concurrent jurisdiction)
--   3 = background / advisory (rare; informational only)
--
-- Idempotent: PRIMARY KEY on (beach_fid, agency_id, authority_domain)
-- means ON CONFLICT DO NOTHING is the right pattern.

-- ─── Scope CTE: tier-1+2 CA beaches with derived ownership ────────

WITH scope AS (
  SELECT g.fid, g.county_name, g.c1_jurisdiction_id, g.cpad_unit_id,
         j.name AS city_name, j.place_type AS city_place_type,
         c.agncy_lev AS cpad_lev, c.agncy_name AS cpad_agency_name,
         c.agncy_typ AS cpad_agency_typ,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
    LEFT JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
    LEFT JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
   WHERE g.state='CA' AND g.is_active=true
),
tiered AS (
  SELECT * FROM scope WHERE tier IN ('1_off-leash','2_on-leash')
)

-- ─── 1. COUNTY → water_quality + public_health (rank=1) ───────────

INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT t.fid, a.id, d.dom, 1
  FROM tiered t
  JOIN public.agency a
    ON a.type='county' AND a.name = t.county_name || ' County'
  CROSS JOIN (VALUES ('water_quality'), ('public_health')) d(dom)
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 2. CITY → operations + parking + fire (rank=1) ────────────────

WITH tiered AS (
  SELECT g.fid, j.name AS city_name,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
    JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
   WHERE g.state='CA' AND g.is_active=true
     AND j.place_type IN ('C1','C5','C9','M2')
)
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT t.fid, a.id, d.dom, 1
  FROM tiered t
  JOIN public.agency a ON a.type='city' AND a.name = t.city_name
  CROSS JOIN (VALUES ('operations'), ('parking'), ('fire')) d(dom)
 WHERE t.tier IN ('1_off-leash','2_on-leash')
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 3. PRIMARY DOG_POLICY JURISDICTION (rank=1, exactly one) ─────
-- Land-ownership cascade per top-of-file logic.

WITH tiered AS (
  SELECT g.fid, g.county_name, j.name AS city_name, j.place_type AS city_place_type,
         c.agncy_lev AS cpad_lev, c.agncy_name AS cpad_agency_name,
         c.agncy_typ AS cpad_agency_typ,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
    LEFT JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
    LEFT JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
   WHERE g.state='CA' AND g.is_active=true
),
filt AS (SELECT * FROM tiered WHERE tier IN ('1_off-leash','2_on-leash')),
resolved AS (
  SELECT f.fid,
    CASE
      -- (a) State/Federal CPAD owner → that agency
      WHEN f.cpad_lev IN ('State','Federal')
        THEN (SELECT id FROM public.agency
               WHERE name = f.cpad_agency_name
                 AND type IN ('state_department','federal_department','federal_military')
               LIMIT 1)
      -- (b) Special District park-type owner → that agency
      WHEN f.cpad_lev = 'Special District'
       AND f.cpad_agency_typ ILIKE '%Park%'
        THEN (SELECT id FROM public.agency
               WHERE name = f.cpad_agency_name AND type='special_district'
               LIMIT 1)
      -- (c) Incorporated city → city
      WHEN f.city_name IS NOT NULL
       AND f.city_place_type IN ('C1','C5','C9','M2')
        THEN (SELECT id FROM public.agency
               WHERE name = f.city_name AND type='city'
               LIMIT 1)
      -- (d) Fallback: county (unincorporated coast)
      ELSE (SELECT id FROM public.agency
             WHERE name = f.county_name || ' County' AND type='county'
             LIMIT 1)
    END AS primary_agency_id
  FROM filt f
)
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT fid, primary_agency_id, 'dog_policy', 1
  FROM resolved
 WHERE primary_agency_id IS NOT NULL
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 4. STATE/FEDERAL LAND_USE (rank=1, when CPAD owner is gov) ───

WITH tiered AS (
  SELECT g.fid, c.agncy_name AS cpad_agency_name, c.agncy_lev AS cpad_lev,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
    JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
   WHERE g.state='CA' AND g.is_active=true
     AND c.agncy_lev IN ('State','Federal','Special District')
)
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT t.fid, a.id, 'land_use', 1
  FROM tiered t
  JOIN public.agency a
    ON a.name = t.cpad_agency_name
   AND a.type IN ('state_department','federal_department','federal_military','special_district')
 WHERE t.tier IN ('1_off-leash','2_on-leash')
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 5. COUNTY_DEPARTMENT OPERATIONAL OVERLAY (when CPAD owner) ───

WITH tiered AS (
  SELECT g.fid, c.agncy_name AS cpad_agency_name,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
    JOIN public.cpad_units c ON c.unit_id = g.cpad_unit_id
   WHERE g.state='CA' AND g.is_active=true
     AND c.agncy_lev = 'County'
)
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT t.fid, a.id, d.dom, 1
  FROM tiered t
  JOIN public.agency a
    ON a.name = t.cpad_agency_name
   AND a.type IN ('county','county_department')
  CROSS JOIN (VALUES ('operations'), ('land_use')) d(dom)
 WHERE t.tier IN ('1_off-leash','2_on-leash')
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;
