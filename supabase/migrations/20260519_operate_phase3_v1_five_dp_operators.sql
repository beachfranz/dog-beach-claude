-- Operate Phase 3 v1 — first 5 dog-park operators (16 verified parks).
--
-- Per Franz 2026-05-18 verification dispatch + 2026-05-19 ship.
-- Builds on Phase (b) cascade (20260519_dog_park_cascade_v1.sql):
-- INSERT into dog_park_policy_source auto-fires the refresh trigger
-- which flips the affected dog_park_dog_policy rows from osm_default
-- (leash_policy=off_leash, conf=0.5) to operator_posted (conf=1.0)
-- with verified hours + additional rules.
--
-- 5 verified operators (agent dispatches 2026-05-18):
--   - Tualatin Hills Park & Recreation District (THPRD)  OR  5 parks
--   - DOGPAW                                              WA  3 parks
--   - PenMet Parks                                        WA  1 park (Tubby's Trail; Rotary Bark not in OSM)
--   - Conejo Recreation and Parks District (CRPD)         CA  2 parks (Conejo Creek + Kimber; Estella + Walnut Grove not in OSM)
--   - Long Beach Parks, Recreation and Marine             CA  5 parks (matched via PIP; 6 of 11 verified parks not in OSM)
--
-- Total: 16 parks ship today; 9 of the agent's verified 25 are
-- missing from osm_dog_parks. OSM coverage gap pinned for Phase 3 v2
-- (alternate source ingest or manual curator entry).

BEGIN;

-- ─── 1. Agencies (3 new special districts) ────────────────────────────

INSERT INTO public.agency (name, short_name, type, web_url, metadata)
SELECT 'Tualatin Hills Park & Recreation District', 'THPRD', 'special_district',
       'https://www.tualatinhillsparks.org/',
       jsonb_build_object('created_for', 'operate_phase3_v1_2026_05_19',
                          'oversees', 'dog parks in Beaverton/Aloha area, OR')
WHERE NOT EXISTS (SELECT 1 FROM public.agency WHERE name = 'Tualatin Hills Park & Recreation District');

INSERT INTO public.agency (name, short_name, type, web_url, metadata)
SELECT 'Peninsula Metropolitan Park District', 'PenMet', 'special_district',
       'https://www.penmetparks.org/',
       jsonb_build_object('created_for', 'operate_phase3_v1_2026_05_19',
                          'oversees', 'parks in Gig Harbor peninsula, WA')
WHERE NOT EXISTS (SELECT 1 FROM public.agency WHERE name = 'Peninsula Metropolitan Park District');

INSERT INTO public.agency (name, short_name, type, web_url, metadata)
SELECT 'Conejo Recreation and Park District', 'CRPD', 'special_district',
       'https://www.crpd.org/',
       jsonb_build_object('created_for', 'operate_phase3_v1_2026_05_19',
                          'oversees', 'parks in Conejo Valley / Thousand Oaks, CA')
WHERE NOT EXISTS (SELECT 1 FROM public.agency WHERE name = 'Conejo Recreation and Park District');

-- ─── 2. Operators (5 new) ─────────────────────────────────────────────

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT 'Tualatin Hills Park & Recreation District', 'THPRD', 'special_district'::operator_type,
       (SELECT id FROM public.agency WHERE name = 'Tualatin Hills Park & Recreation District'),
       'https://www.tualatinhillsparks.org/',
       'https://www.tualatinhillsparks.org/285/Dog-Parks',
       jsonb_build_object('created_for', 'operate_phase3_v1', 'agent_verified', true)
WHERE NOT EXISTS (SELECT 1 FROM public.operator WHERE name = 'Tualatin Hills Park & Recreation District');

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT 'DOGPAW', 'DOGPAW', 'nonprofit'::operator_type,
       343,  -- Clark County WA agency (links nonprofit to authorizing jurisdiction)
       'https://www.dogpawoffleashparks.org/',
       'https://www.dogpawoffleashparks.org/our-rules',
       jsonb_build_object('created_for', 'operate_phase3_v1', 'agent_verified', true,
                          'authority_basis', 'Clark County permits DOGPAW to operate designated off-leash parks; no formal MOU document indexed')
WHERE NOT EXISTS (SELECT 1 FROM public.operator WHERE name = 'DOGPAW');

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT 'Peninsula Metropolitan Park District', 'PenMet', 'special_district'::operator_type,
       (SELECT id FROM public.agency WHERE name = 'Peninsula Metropolitan Park District'),
       'https://www.penmetparks.org/',
       'https://www.penmetparks.org/parks/rotary-bark-park/',
       jsonb_build_object('created_for', 'operate_phase3_v1', 'agent_verified', true)
WHERE NOT EXISTS (SELECT 1 FROM public.operator WHERE name = 'Peninsula Metropolitan Park District');

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT 'Conejo Recreation and Park District', 'CRPD', 'special_district'::operator_type,
       (SELECT id FROM public.agency WHERE name = 'Conejo Recreation and Park District'),
       'https://www.crpd.org/',
       'https://www.crpd.org/dog-park-off-leash-areas',
       jsonb_build_object('created_for', 'operate_phase3_v1', 'agent_verified', true)
WHERE NOT EXISTS (SELECT 1 FROM public.operator WHERE name = 'Conejo Recreation and Park District');

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT 'Long Beach Parks, Recreation and Marine', 'LB Parks Rec Marine', 'city_department'::operator_type,
       76,  -- City of Long Beach agency (city department parent)
       'https://www.longbeach.gov/park/',
       'https://www.longbeach.gov/acs/laws-enforcement/animal-laws/dog-park-regulations/',
       jsonb_build_object('created_for', 'operate_phase3_v1', 'agent_verified', true,
                          'note', 'distinct from citywide leash code — operator-posted off_leash_voice_control inside designated parks')
WHERE NOT EXISTS (SELECT 1 FROM public.operator WHERE name = 'Long Beach Parks, Recreation and Marine');

-- ─── 3. Policy_sources (5 operator_posted_policy rows) ────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata)
SELECT 'operator_posted_policy',
       'Tualatin Hills Park & Recreation District — Dog Parks page',
       (SELECT id FROM public.operator WHERE name = 'Tualatin Hills Park & Recreation District'),
       ARRAY['dog_policy']::text[],
       'https://www.tualatinhillsparks.org/285/Dog-Parks',
       'THPRD operates 5 designated off-leash dog parks (Hazeldale, Winkelman, PCC Rock Creek, Schiffler, Jackie Husen). Dogs must be on-leash outside dog-park areas. Parks open dawn to dusk.',
       NOW(),
       jsonb_build_object('agent_verified_2026_05_18', true, 'operate_phase', '3_v1')
WHERE NOT EXISTS (SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.tualatinhillsparks.org/285/Dog-Parks');

INSERT INTO public.policy_source (subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata)
SELECT 'operator_posted_policy',
       'DOGPAW — Park Rules',
       (SELECT id FROM public.operator WHERE name = 'DOGPAW'),
       ARRAY['dog_policy']::text[],
       'https://www.dogpawoffleashparks.org/our-rules',
       'DOGPAW operates 3 off-leash dog parks in Clark County WA (Dakota, Ike, Kane). Dogs must be on-leash entering/exiting parking lot; off-leash inside park. Hours 7am-dusk.',
       NOW(),
       jsonb_build_object('agent_verified_2026_05_18', true, 'operate_phase', '3_v1')
WHERE NOT EXISTS (SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.dogpawoffleashparks.org/our-rules');

INSERT INTO public.policy_source (subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata)
SELECT 'operator_posted_policy',
       'PenMet Parks — Rotary Bark Park',
       (SELECT id FROM public.operator WHERE name = 'Peninsula Metropolitan Park District'),
       ARRAY['dog_policy']::text[],
       'https://www.penmetparks.org/parks/rotary-bark-park/',
       'PenMet Parks operates designated off-leash dog parks on the Gig Harbor peninsula (Rotary Bark Park, Tubby''s Trail). Off-leash inside designated zones; on-leash elsewhere. Hours 7am-dusk.',
       NOW(),
       jsonb_build_object('agent_verified_2026_05_18', true, 'operate_phase', '3_v1')
WHERE NOT EXISTS (SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.penmetparks.org/parks/rotary-bark-park/');

INSERT INTO public.policy_source (subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata)
SELECT 'operator_posted_policy',
       'CRPD — Dog Park / Off-Leash Areas',
       (SELECT id FROM public.operator WHERE name = 'Conejo Recreation and Park District'),
       ARRAY['dog_policy']::text[],
       'https://www.crpd.org/dog-park-off-leash-areas',
       'CRPD operates 4 off-leash facilities (Conejo Creek, Estella, Kimber, Walnut Grove). Off-leash inside fenced areas, on-leash outside. Hours 7:00 AM to 10:00 PM with weekly maintenance closures.',
       NOW(),
       jsonb_build_object('agent_verified_2026_05_18', true, 'operate_phase', '3_v1')
WHERE NOT EXISTS (SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.crpd.org/dog-park-off-leash-areas');

INSERT INTO public.policy_source (subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata)
SELECT 'operator_posted_policy',
       'Long Beach ACS — Dog Park Regulations',
       (SELECT id FROM public.operator WHERE name = 'Long Beach Parks, Recreation and Marine'),
       ARRAY['dog_policy']::text[],
       'https://www.longbeach.gov/acs/laws-enforcement/animal-laws/dog-park-regulations/',
       'Long Beach Parks, Recreation and Marine operates 11 designated dog parks. Inside fenced areas: off-leash voice-control permitted. Distinct from citywide leash code (which applies outside designated areas). Hours 6am-10pm.',
       NOW(),
       jsonb_build_object('agent_verified_2026_05_18', true, 'operate_phase', '3_v1')
WHERE NOT EXISTS (SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.longbeach.gov/acs/laws-enforcement/animal-laws/dog-park-regulations/');

-- ─── 4. dog_park_policy_source linkages (16 matched parks) ────────────
-- Cascade fires per-row → flips dog_park_dog_policy from osm_default → operator_posted.

-- THPRD: 5 parks
INSERT INTO public.dog_park_policy_source (
  dog_park_fid, policy_source_id, rule, hours_open_time, hours_close_time, hours_text,
  evidence_url, last_verified
)
SELECT v.fid, ps.id, 'off_leash', NULL, NULL, 'dawn-dusk',
       'https://www.tualatinhillsparks.org/285/Dog-Parks', NOW()
FROM (VALUES
  (183::bigint),  -- Hazeldale Dog Park
  (227::bigint),  -- Jackie Husen Dog Run
  (87::bigint),   -- PCC Rock Creek Dog Park
  (182::bigint),  -- Schiffler Park Dog Run
  (88::bigint)    -- Winkelman Dog Park
) AS v(fid)
CROSS JOIN (SELECT id FROM public.policy_source WHERE source_url = 'https://www.tualatinhillsparks.org/285/Dog-Parks') ps
ON CONFLICT (dog_park_fid, policy_source_id) DO NOTHING;

-- DOGPAW: 3 parks
INSERT INTO public.dog_park_policy_source (
  dog_park_fid, policy_source_id, rule, hours_open_time, hours_close_time, hours_text,
  additional_rules, evidence_url, last_verified
)
SELECT v.fid, ps.id, 'off_leash', '07:00', NULL, '7am-dusk',
       'Dogs must be on-leash entering/exiting parking lot.',
       'https://www.dogpawoffleashparks.org/our-rules', NOW()
FROM (VALUES
  (66::bigint),   -- Dakota Dog Park
  (75::bigint),   -- Ike Memorial Dog Park
  (208::bigint)   -- Kane Memorial Dog Park
) AS v(fid)
CROSS JOIN (SELECT id FROM public.policy_source WHERE source_url = 'https://www.dogpawoffleashparks.org/our-rules') ps
ON CONFLICT (dog_park_fid, policy_source_id) DO NOTHING;

-- PenMet: 1 park (Tubby's Trail; Rotary Bark Park not in OSM)
INSERT INTO public.dog_park_policy_source (
  dog_park_fid, policy_source_id, rule, hours_open_time, hours_close_time, hours_text,
  evidence_url, last_verified
)
SELECT v.fid, ps.id, 'off_leash', '07:00', NULL, '7am-dusk',
       'https://www.penmetparks.org/parks/rotary-bark-park/', NOW()
FROM (VALUES
  (117::bigint)   -- Tubby's Trail Dog Park
) AS v(fid)
CROSS JOIN (SELECT id FROM public.policy_source WHERE source_url = 'https://www.penmetparks.org/parks/rotary-bark-park/') ps
ON CONFLICT (dog_park_fid, policy_source_id) DO NOTHING;

-- CRPD: 2 parks (Conejo Creek + Kimber; Estella + Walnut Grove not in OSM)
INSERT INTO public.dog_park_policy_source (
  dog_park_fid, policy_source_id, rule, hours_open_time, hours_close_time, hours_text,
  additional_rules, evidence_url, last_verified
)
SELECT v.fid, ps.id, 'off_leash', '07:00', '22:00', '7am-10pm',
       'Weekly maintenance closures (check operator site for schedule).',
       'https://www.crpd.org/dog-park-off-leash-areas', NOW()
FROM (VALUES
  (528::bigint),  -- Conejo Creek Dog Park
  (616::bigint)   -- Kimber Dog Park
) AS v(fid)
CROSS JOIN (SELECT id FROM public.policy_source WHERE source_url = 'https://www.crpd.org/dog-park-off-leash-areas') ps
ON CONFLICT (dog_park_fid, policy_source_id) DO NOTHING;

-- LB: 5 parks via spatial PIP (6 of 11 verified not in OSM)
INSERT INTO public.dog_park_policy_source (
  dog_park_fid, policy_source_id, rule, hours_open_time, hours_close_time, hours_text,
  additional_rules, evidence_url, last_verified
)
SELECT v.fid, ps.id, 'off_leash_voice_control', '06:00', '22:00', '6am-10pm',
       'Inside designated fenced areas only. Citywide leash code applies outside.',
       'https://www.longbeach.gov/acs/laws-enforcement/animal-laws/dog-park-regulations/', NOW()
FROM (VALUES
  (632::bigint),  -- Bixby Dog Park
  (630::bigint),  -- El Dorado Dog Park
  (671::bigint),  -- Pike Dog Park
  (577::bigint),  -- Recreation Dog Park
  (611::bigint)   -- Wrigley Heights Dog Park
) AS v(fid)
CROSS JOIN (SELECT id FROM public.policy_source WHERE source_url = 'https://www.longbeach.gov/acs/laws-enforcement/animal-laws/dog-park-regulations/') ps
ON CONFLICT (dog_park_fid, policy_source_id) DO NOTHING;

-- ─── 5. Verification ──────────────────────────────────────────────────

SELECT
  'agencies_added'             AS metric,
  count(*)::text               AS val
  FROM public.agency
  WHERE metadata->>'created_for' = 'operate_phase3_v1_2026_05_19'
UNION ALL SELECT
  'operators_added',
  count(*)::text
  FROM public.operator
  WHERE metadata->>'created_for' = 'operate_phase3_v1'
UNION ALL SELECT
  'policy_sources_added',
  count(*)::text
  FROM public.policy_source
  WHERE metadata->>'operate_phase' = '3_v1'
UNION ALL SELECT
  'dog_park_policy_source_rows',
  count(*)::text
  FROM public.dog_park_policy_source
UNION ALL SELECT
  'dog_park_dog_policy_operator_posted',
  count(*)::text
  FROM public.dog_park_dog_policy
  WHERE source = 'operator_posted'
UNION ALL SELECT
  'by_leash_after_cascade',
  string_agg(leash_policy || ':' || n::text, ', ' ORDER BY leash_policy)
  FROM (SELECT leash_policy, count(*) AS n FROM public.dog_park_dog_policy
        WHERE source = 'operator_posted' GROUP BY leash_policy) t
UNION ALL SELECT
  'distinct_operators_attributed',
  count(DISTINCT operator_id)::text
  FROM public.dog_park_dog_policy
  WHERE source = 'operator_posted';

COMMIT;
