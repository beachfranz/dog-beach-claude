-- Phase 2 — Seed the new config tables with California's current hardcoded
-- values. After this, all CA-specific URLs, field mappings, operator
-- overrides, private-land zones, BLM SMA codes, and Claude prompt context
-- live in data rather than edge-function source.
--
-- No edge functions are refactored yet — that's Phase 3+. This migration is
-- pure data, safe to apply.

begin;

-- ── pipeline_sources ──────────────────────────────────────────────────────

insert into public.pipeline_sources (source_key, state_code, kind, url, query_defaults, field_map, priority, notes) values

-- National sources (state_code = NULL)

('federal_polygon', null, 'polygon',
 'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Federal_Lands/FeatureServer/0/query',
 '{"outFields":"Agency,unit_name"}'::jsonb,
 '{"agency":"Agency","unit":"unit_name"}'::jsonb,
 100,
 'Esri Living Atlas — NPS, USFS, BLM, DOD, FWS, USBR'),

('city_polygon', null, 'polygon',
 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer/4/query',
 '{"outFields":"BASENAME,NAME,LSADC,GEOID"}'::jsonb,
 '{"name":"BASENAME","full_name":"NAME","geoid":"GEOID","lsadc":"LSADC"}'::jsonb,
 100,
 'Census TIGER/Line Places — filter LSADC=25 for incorporated places only'),

('blm_sma_national', null, 'polygon',
 'https://gis.blm.gov/arcgis/rest/services/lands/BLM_Natl_SMA_Cached_with_PriUnk/MapServer/1/query',
 '{"outFields":"ADMIN_UNIT_NAME,ADMIN_AGENCY_CODE,ADMIN_ST,SMA_ID"}'::jsonb,
 '{"unit":"ADMIN_UNIT_NAME","agency_code":"ADMIN_AGENCY_CODE","state":"ADMIN_ST","sma_id":"SMA_ID"}'::jsonb,
 200,
 'BLM national SMA layer — fallback when state-specific layer unavailable'),

('cpad_polygon', null, 'polygon',
 'https://arcgis.netl.doe.gov/server/rest/services/Hosted/Protected_Areas_Database_for_the_United_States_PADUS/FeatureServer/32/query',
 '{"outFields":"own_name,mang_name,own_type,mang_type,category"}'::jsonb,
 '{"owner_name":"own_name","manager_name":"mang_name","owner_type":"own_type","manager_type":"mang_type"}'::jsonb,
 200,
 'USGS PAD-US national protected areas — fallback when state source unavailable'),

('noaa_tide_stations', null, 'rest_json',
 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions',
 '{}'::jsonb,
 '{"id":"id","name":"name","lat":"lat","lon":"lng","reference_id":"reference_id","state":"state"}'::jsonb,
 100,
 'NOAA CO-OPS — filter reference_id empty for prediction-capable stations'),

('geocoder_google', null, 'rest_json',
 'https://maps.googleapis.com/maps/api/geocode/json',
 '{}'::jsonb,
 '{"city":"locality","county":"administrative_area_level_2","state":"administrative_area_level_1","zip":"postal_code","street_number":"street_number","route":"route"}'::jsonb,
 100,
 'Google reverse geocoder'),

('geocoder_census_incorporated', null, 'rest_json',
 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates',
 '{"benchmark":"Public_AR_Current","vintage":"Current_Current","layers":"Incorporated Places","format":"json"}'::jsonb,
 '{"name":"NAME"}'::jsonb,
 100,
 'US Census incorporated-place point lookup'),

-- California-specific sources

('state_park_polygon', 'CA', 'polygon',
 'https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis/rest/services/ParkBoundaries/FeatureServer/0/query',
 '{"outFields":"UNITNAME,SUBTYPE"}'::jsonb,
 '{"unit":"UNITNAME","subtype":"SUBTYPE"}'::jsonb,
 10,
 'CA State Parks ParkBoundaries — authoritative state-park polygon source for CA'),

('cpad_polygon', 'CA', 'polygon',
 'https://gis.cnra.ca.gov/arcgis/rest/services/Boundaries/CPAD_AgencyLevel/MapServer/0/query',
 '{"outFields":"UNIT_NAME,AGNCY_NAME,AGNCY_LEV,COUNTY"}'::jsonb,
 '{"unit":"UNIT_NAME","agency":"AGNCY_NAME","agency_level":"AGNCY_LEV","county":"COUNTY"}'::jsonb,
 10,
 'CPAD — California Protected Areas Database; finer-grained than PAD-US for CA'),

('blm_sma', 'CA', 'polygon',
 'https://gis.blm.gov/caarcgis/rest/services/lands/BLM_CA_LandStatus_SurfaceManagementAgency/FeatureServer/0/query',
 '{"outFields":"SMA_ID,Tmp_Text_ca"}'::jsonb,
 '{"sma_id":"SMA_ID","unit":"Tmp_Text_ca"}'::jsonb,
 10,
 'BLM California SMA — BIA tribal + SMA_ID coded-value domain'),

('coastal_access_points', 'CA', 'point',
 'https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/services/AccessPoints/FeatureServer/0/query',
 '{"outFields":"Name,LATITUDE,LONGITUDE,DOG_FRIEND,PARKING,RESTROOMS,SHOWERS,LIFEGUARD,FOOD,DRINKWTR,FIREPITS,PCNC_AREA,DSABLDACSS,COUNTY"}'::jsonb,
 '{"name":"Name","lat":"LATITUDE","lon":"LONGITUDE","dog":"DOG_FRIEND","parking":"PARKING","restrooms":"RESTROOMS","showers":"SHOWERS","lifeguard":"LIFEGUARD","food":"FOOD","drinking_water":"DRINKWTR","fire_pits":"FIREPITS","picnic":"PCNC_AREA","disabled":"DSABLDACSS","county":"COUNTY"}'::jsonb,
 10,
 'California Coastal Commission Public Access Points — 1,631 statewide');

-- ── state_config ──────────────────────────────────────────────────────────

insert into public.state_config (state_code, state_name, enabled, coastal_default_tier, coastal_default_body, has_coastal_access_source, research_context_notes) values
('CA', 'California', true, 'county', null, true,
 'California state parks typically prohibit dogs on the beach proper (leashed-only in developed areas). Many state beaches are operationally managed by an adjacent city or county under lease — that operator sets the actual dog policy. California Coastal Commission maintains a curated public-access point inventory with dog_friend hints.');

-- ── park_operators ────────────────────────────────────────────────────────

insert into public.park_operators (state_code, park_name, operator_jurisdiction, operator_body, notes) values
('CA', 'Santa Monica SB',     'governing city',   'City of Santa Monica',
     'State-owned, operated by City of Santa Monica under long-standing lease.'),
('CA', 'Corona del Mar SB',   'governing city',   'City of Newport Beach',
     'State-owned, operated by City of Newport Beach.'),
('CA', 'Leucadia SB',         'governing city',   'City of Encinitas',
     'State-owned, operated by City of Encinitas.'),
('CA', 'Moonlight SB',        'governing city',   'City of Encinitas',
     'State-owned, operated by City of Encinitas.'),
('CA', 'Seabright SB',        'governing city',   'City of Santa Cruz',
     'State-owned, operated by City of Santa Cruz.'),
('CA', 'Pacifica SB',         'governing city',   'City of Pacifica',
     'State-owned, operated by City of Pacifica.'),
('CA', 'San Buenaventura SB', 'governing city',   'City of Ventura',
     'State-owned, operated by City of San Buenaventura (Ventura).'),
('CA', 'Monterey SB',         'governing city',   'City of Monterey',
     'State-owned, operated by City of Monterey.'),
('CA', 'Will Rogers SB',      'governing county', 'County of Los Angeles',
     'State-owned, operated by LA County Beaches & Harbors.'),
('CA', 'Dockweiler SB',       'governing county', 'County of Los Angeles',
     'State-owned, operated by LA County Beaches & Harbors.');

-- ── private_land_zones ────────────────────────────────────────────────────

insert into public.private_land_zones (state_code, name, min_lat, max_lat, min_lon, max_lon, reason) values
('CA', 'Del Monte Forest / 17-Mile Drive', 36.555, 36.615, -121.985, -121.935,
 'Private Pebble Beach Company land. Beaches along 17-Mile Drive (Fanshell, Point Joe, Bird Rock, Moss Beach, Stillwater Cove, Seal Rock, Granite Beach) are tourist scenic stops on private property, not public beaches.');

-- ── sma_code_mappings ─────────────────────────────────────────────────────
-- Full coded-value domain from the BLM SMA service metadata.
-- is_public = whether a beach in a unit of this agency is generally
-- accessible to the public (false for active military installations).

insert into public.sma_code_mappings (sma_id, agency_name, agency_type, is_public) values
(1,    'Undetermined',     'undetermined', true),
(2,    'BLM',              'federal',      true),
(3,    'BIA',              'tribal',       true),
(305,  'USAF',             'federal',      false),
(488,  'ARMY',             'federal',      false),
(914,  'USDA',             'federal',      true),
(915,  'USFS',             'federal',      true),
(1535, 'FWS',              'federal',      true),
(2012, 'NPS',              'federal',      true),
(2365, 'DOD',              'federal',      false),
(2366, 'USBR',             'federal',      true),
(2367, 'USACE',            'federal',      true),
(2370, 'USMC',             'federal',      false),
(2371, 'NAVY',             'federal',      false),
(2378, 'Other Federal',    'federal',      true),
(2386, 'State',            'state',        true),
(2387, 'Local Gov',        'local',        true),
(2388, 'Private',          'private',      false),
(4896, 'USCG',             'federal',      false);

-- ── research_prompts ──────────────────────────────────────────────────────
-- Tier-specific Claude prompt context, migrated from the tierContext()
-- function in v2-enrich-operational. Per-state so Oregon and future states
-- can override with their own context (Beach Bill, statewide leash law, etc.)

insert into public.research_prompts (state_code, tier, system_context) values
('CA', 'state',
 'California state parks — typically leashed-only in developed areas, varies per beach. Some state beaches (e.g. Corona del Mar SB, Santa Monica SB, Leucadia SB) are operationally run by the adjacent city/county. State Coastal Conservancy, UC reserves, and CDFW lands also use this tier.'),
('CA', 'city',
 'Municipal California coastal city — typically has a designated "dog beach" section plus prohibition at other city beaches. City parks departments publish specific rules.'),
('CA', 'county',
 'California county parks & beaches department — varies widely. LA County Beaches & Harbors operates ~20 beaches with formal rules; inland counties may have lake/reservoir beach rules.'),
('CA', 'federal',
 'Federal land unit — NPS (leashed in developed areas, per-beach rules), USFS (generally more permissive, leash required), BLM (varies), military base (restricted public access → unknown), National Wildlife Refuge (typically prohibited).');

commit;
