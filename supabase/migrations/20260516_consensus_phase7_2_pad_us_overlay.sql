-- Consensus engine rewrite — Phase 7.2: pad_us_units spatial overlay
-- to catch federal-land beaches that CPAD doesn't cover.
--
-- Background: Phase 7.1 used CPAD (California Protected Areas Database)
-- to identify state/federal/special-district landowners. CPAD has
-- partial federal coverage — notably misses GGNRA-style NPS lands
-- (Fort Funston was the surfacing case). PAD-US (the federal
-- equivalent) covers these.
--
-- Survey: 46 tier-1+2 CA beaches have a pad_us hit but no cpad_unit_id.
-- These currently default to city/county dog_policy primary in
-- beach_agency, which is wrong for federal land.
--
-- Strategy:
-- 1. Seed missing federal/state agencies from pad_us own_name codes
-- 2. For beaches with federal/state pad_us ownership AND no existing
--    state/federal dog_policy primary, insert federal/state at rank=1
-- 3. Demote any conflicting city/county dog_policy rank=1 → rank=2
--    so the new federal/state row wins canonical resolution
--
-- Mapping (PAD-US own_name → agency.name + agency.type):
--   FED/NPS    → National Park Service (federal_department)
--   FED/USFWS  → United States Fish and Wildlife Service
--   FED/BLM    → Bureau of Land Management
--   FED/USFS   → United States Forest Service
--   FED/USACE  → United States Army Corps of Engineers
--   FED/DOD    → United States Department of Defense (federal_military)
--   FED/USBR   → United States Bureau of Reclamation
--   STAT/SPR   → California Department of Parks and Recreation
--   STAT/SFW   → California Department of Fish and Wildlife
--   STAT/SLB   → California State Lands Commission
--   STAT/OTHS  → (generic state — skip, ambiguous)
--   DESG       → SKIP (designation overlay, not ownership)
--   LOC/CITY   → SKIP (cities already linked via jurisdictions)
--   LOC/CNTY   → SKIP (counties already linked)
--   DIST/REG   → handled case-by-case if surfaces; skip default

-- ─── 1. Seed missing agencies from PAD-US ─────────────────────────

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
VALUES
  ('National Park Service', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['dog_policy','land_use','enforcement']),
  ('United States Fish and Wildlife Service', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['wildlife_protection','land_use']),
  ('Bureau of Land Management', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['land_use','dog_policy']),
  ('United States Forest Service', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['land_use','dog_policy']),
  ('United States Army Corps of Engineers', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['water_management','land_use']),
  ('United States Department of Defense', 'federal_military'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['access_restriction','enforcement']),
  ('United States Bureau of Reclamation', 'federal_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='United States' AND type='federal'),
   ARRAY['water_management','land_use'])
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. Seed missing state agencies (CDFW + SLC) ──────────────────
-- DPR was seeded by Phase 7. CDFW + SLC are usually CPAD-covered too,
-- but defensive-insert in case some beaches only show up via PAD-US.

INSERT INTO public.agency (name, type, parent_agency_id, default_authority_domains)
VALUES
  ('California Department of Fish and Wildlife', 'state_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='State of California' AND type='state'),
   ARRAY['wildlife_protection','mpa']),
  ('California State Lands Commission', 'state_department'::agency_type,
   (SELECT id FROM public.agency WHERE name='State of California' AND type='state'),
   ARRAY['tidelands','land_use'])
ON CONFLICT (name, type) DO NOTHING;

-- ─── 3. Resolve per-beach federal/state primary from PAD-US ──────
-- For tier-1+2 CA beaches, find the best PAD-US owner (excluding
-- DESG / LOC). Where multiple FED/STAT rows hit, prefer FED.

CREATE TEMP TABLE _pad_us_primary AS
WITH tiered AS (
  SELECT g.fid, g.lon, g.lat,
         public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) AS tier
    FROM public.beaches_gold g
    LEFT JOIN public.beach_dog_policy d ON d.arena_group_id = g.fid
   WHERE g.state='CA' AND g.is_active=true
),
filt AS (SELECT * FROM tiered WHERE tier IN ('1_off-leash','2_on-leash')),
hits AS (
  SELECT f.fid, p.own_type, p.own_name, p.unit_name,
         CASE p.own_type WHEN 'FED' THEN 1 WHEN 'STAT' THEN 2 ELSE 3 END AS priority
    FROM filt f
    JOIN public.pad_us_units p
      ON ST_Intersects(p.geom, ST_SetSRID(ST_MakePoint(f.lon, f.lat), 4326))
   WHERE p.own_type IN ('FED','STAT')
     AND p.own_name NOT IN ('DESG','OTHS')
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY fid ORDER BY priority, unit_name) AS rn
    FROM hits
)
SELECT fid, own_type, own_name, unit_name,
  CASE own_name
    WHEN 'NPS'   THEN 'National Park Service'
    WHEN 'USFWS' THEN 'United States Fish and Wildlife Service'
    WHEN 'BLM'   THEN 'Bureau of Land Management'
    WHEN 'USFS'  THEN 'United States Forest Service'
    WHEN 'USACE' THEN 'United States Army Corps of Engineers'
    WHEN 'DOD'   THEN 'United States Department of Defense'
    WHEN 'USBR'  THEN 'United States Bureau of Reclamation'
    WHEN 'SPR'   THEN 'California Department of Parks and Recreation'
    WHEN 'SFW'   THEN 'California Department of Fish and Wildlife'
    WHEN 'SLB'   THEN 'California State Lands Commission'
    ELSE NULL
  END AS agency_name,
  CASE own_name
    WHEN 'DOD' THEN 'federal_military'
    WHEN 'SPR' THEN 'state_department'
    WHEN 'SFW' THEN 'state_department'
    WHEN 'SLB' THEN 'state_department'
    ELSE 'federal_department'
  END::agency_type AS agency_type
FROM ranked
WHERE rn = 1;

-- ─── 4. Insert pad_us-derived agencies as dog_policy + land_use ──
-- These are the federal/state owners that override the city/county
-- fallback assigned by Phase 7.1.

INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT p.fid, a.id, d.dom, 1
  FROM _pad_us_primary p
  JOIN public.agency a ON a.name = p.agency_name AND a.type = p.agency_type
  CROSS JOIN (VALUES ('dog_policy'), ('land_use')) d(dom)
 WHERE p.agency_name IS NOT NULL
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- ─── 5. Demote conflicting city/county dog_policy rank=1 → rank=2 ─
-- A beach can have ONLY ONE rank=1 dog_policy primary per the
-- resolver semantics. For beaches where pad_us just assigned a
-- federal/state rank=1, any pre-existing city/county rank=1
-- dog_policy row needs to become rank=2 (fallback / advisory).

UPDATE public.beach_agency ba
   SET precedence_rank = 2
  FROM _pad_us_primary p, public.agency new_a, public.agency existing_a
 WHERE new_a.name = p.agency_name
   AND new_a.type = p.agency_type
   AND existing_a.id = ba.agency_id
   AND ba.beach_fid = p.fid
   AND ba.authority_domain = 'dog_policy'
   AND ba.precedence_rank = 1
   AND ba.agency_id <> new_a.id
   AND existing_a.type IN ('city','county','county_department');

DROP TABLE _pad_us_primary;
