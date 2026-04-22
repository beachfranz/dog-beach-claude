-- Phase 6 — Seed Oregon config rows.
--
-- Oregon activation via data only: no code changes needed. The pipeline
-- already branches on state_config.coastal_default_tier in v2-default-county,
-- so setting it to 'state' here automatically enables the Beach Bill /
-- Ocean Shore default for unclassified coastal beaches.
--
-- No park_operators or private_land_zones seeded — OR doesn't have the
-- state-lease patterns that CA does (Beach Bill simplified operations).
-- Add entries if we discover specific cases during validation.
--
-- has_coastal_access_source = false: ODF Beach Access (273 points) exists
-- but lacks the amenity + dog-friendly fields that CCC has. Running
-- v2-ccc-crossref against ODF would match names but add no amenity value.
-- Left off. Can enable later if ODF integration proves worthwhile.

begin;

-- ── pipeline_sources: Oregon-specific ────────────────────────────────────
insert into public.pipeline_sources (source_key, state_code, kind, url, query_defaults, field_map, priority, notes) values
('state_park_polygon', 'OR', 'polygon',
 'https://maps.prd.state.or.us/arcgis/rest/services/Land_ownership/Oregon_State_Parks/FeatureServer/0/query',
 '{"outFields":"NAME,FULL_NAME,DESIGNATION,USE_TYPE,GIS_ACRES"}'::jsonb,
 '{"unit":"NAME","subtype":"DESIGNATION"}'::jsonb,
 10,
 'Oregon Parks and Recreation Department (OPRD) state park boundaries. 422 units statewide.');

-- ── state_config: Oregon ─────────────────────────────────────────────────
insert into public.state_config (
  state_code, state_name, enabled,
  coastal_default_tier, coastal_default_body,
  has_coastal_access_source,
  research_context_notes,
  excluded_federal_units
) values (
  'OR', 'Oregon', true,
  'state', 'Oregon Parks and Recreation Department (Ocean Shore)',
  false,
  'Oregon''s 1967 Beach Bill established the entire Ocean Shore (average high water to 16 vertical feet above low water) as state-managed by OPRD. Dogs must be on leash (max 6 ft) per OPRD rules. Oregon is broadly more dog-friendly than California. Cannon Beach and Tolovana are famous off-leash destinations. Some coastal beaches restrict dogs seasonally for snowy plover nesting.',
  '[]'::jsonb
);

-- ── research_prompts: 4 tiers for OR ─────────────────────────────────────
insert into public.research_prompts (state_code, tier, system_context) values

('OR', 'state',
 'Oregon state parks and state-managed land. Oregon Parks and Recreation Department (OPRD) operates 250+ parks and the entire Ocean Shore under the 1967 Beach Bill. Dogs must be on leash (6 ft max) per OPRD rules. Oregon is broadly dog-friendly compared to California. Snowy plover nesting restrictions (Mar-Sep) apply to some coastal beaches. UC-equivalent reserves, ODFW wildlife areas, and the Oregon State Marine Board also use this tier.'),

('OR', 'city',
 'Municipal Oregon coastal city. Most Oregon coast cities are small tourism-oriented towns: Astoria, Seaside, Cannon Beach, Manzanita, Rockaway Beach, Tillamook (inland), Pacific City, Lincoln City, Depoe Bay, Newport, Waldport, Yachats, Florence, Reedsport, North Bend, Coos Bay, Bandon, Port Orford, Gold Beach, Brookings. Cannon Beach (including Tolovana) is famous for dog access; Seaside promenade allows leashed dogs. Most cities defer to OPRD rules for the Ocean Shore; leash laws typically apply off-beach.'),

('OR', 'county',
 'Oregon county parks and beaches. Coastal counties: Clatsop, Tillamook, Lincoln, Lane, Douglas, Coos, Curry. County park systems are typically small compared to CA counties. The Ocean Shore itself is state-managed, not county. Some counties operate inland/river beaches and boat launches.'),

('OR', 'federal',
 'Federal land unit in Oregon. Primary: Siuslaw National Forest (includes Oregon Dunes National Recreation Area — USFS, leash required), Lewis and Clark National Historical Park, Nestucca Bay NWR and other wildlife refuges (typically no dogs). Minor: Coos Bay USACE, Fort Stevens is actually state not federal. BIA tribal lands: Confederated Tribes of the Siletz, Coquille Indian Tribe, Confederated Tribes of Grand Ronde, Confederated Tribes of the Coos / Lower Umpqua / Siuslaw. No significant military beaches on the Oregon coast.');

commit;
