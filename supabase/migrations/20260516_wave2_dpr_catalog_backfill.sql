-- Wave 2 DPR catalog backfill — auto-generated.
-- Parses parks.ca.gov/Dogs and creates per-park policy_source rows
-- + beach_policy_source links for all DPR-primary tier-1+2 CA beaches
-- whose park_name has an exact match in the catalog.
--
-- Idempotent via WHERE NOT EXISTS / ON CONFLICT DO NOTHING.

-- Asilomar State Beach: Yes — Dogs allowed on beach, trails, and conference grounds. Dogs not allowed in conference ground buildings. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Asilomar State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed on beach, trails, and conference grounds. Dogs not allowed in conference ground buildings.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Asilomar State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9302,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Asilomar State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on beach, trails, and conference grounds. Dogs not allowed in conference ground buildings.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Año Nuevo State Park: No -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Año Nuevo State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Año Nuevo State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9143,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Año Nuevo State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Bean Hollow State Beach: Yes — Dogs are allowed on the beach on leash. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Bean Hollow State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs are allowed on the beach on leash.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Bean Hollow State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8605,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Bean Hollow State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs are allowed on the beach on leash.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Candlestick Point State Recreation Area: Yes — Dogs must be on a maximum 6-foot leash at all times. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Candlestick Point State Recreation Area',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs must be on a maximum 6-foot leash at all times.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Candlestick Point State Recreation Area'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6186,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Candlestick Point State Recreation Area'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs must be on a maximum 6-foot leash at all times.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Carmel River State Beach: Yes -> 2 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Carmel River State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Carmel River State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8287,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Carmel River State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8569,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Carmel River State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Caspar Headlands State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Caspar Headlands State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Caspar Headlands State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8660,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Caspar Headlands State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Cayucos State Beach: Yes — Unless posted signs say otherwise. Dogs are allowed only on leash in permitted areas. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Cayucos State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Unless posted signs say otherwise. Dogs are allowed only on leash in permitted areas.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Cayucos State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8599,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Cayucos State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Unless posted signs say otherwise. Dogs are allowed only on leash in permitted areas.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Del Norte Coast Redwoods State Park: Yes — Except for service animals, dogs not allowed on trails. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Del Norte Coast Redwoods State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Del Norte Coast Redwoods State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 5945,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Del Norte Coast Redwoods State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Doheny State Beach: Yes — Except for service animals, dogs not allowed on beaches. Dogs must be leashed and attended to by an adult at all times. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Doheny State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on beaches. Dogs must be leashed and attended to by an adult at all times.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Doheny State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8839,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Doheny State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on beaches. Dogs must be leashed and attended to by an adult at all times.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- El Capitán State Beach: Yes — Dogs allowed in campground and day-use area. Dogs not allowed on beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — El Capitán State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed in campground and day-use area. Dogs not allowed on beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — El Capitán State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8524,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — El Capitán State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed in campground and day-use area. Dogs not allowed on beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Fort Ord Dunes State Park: Yes — Dogs allowed on recreation trail that runs north/south through Fort Ord. Dogs not allowed on spur trails from there to beach or on the beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Fort Ord Dunes State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed on recreation trail that runs north/south through Fort Ord. Dogs not allowed on spur trails from there to beach or on the beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Fort Ord Dunes State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8567,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Fort Ord Dunes State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on recreation trail that runs north/south through Fort Ord. Dogs not allowed on spur trails from there to beach or on the beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Fort Ross State Historic Park: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Fort Ross State Historic Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Fort Ross State Historic Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8742,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Fort Ross State Historic Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Greenwood State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Greenwood State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Greenwood State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6003,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Greenwood State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Half Moon Bay State Beach: Yes — Dogs not allowed on beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Half Moon Bay State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs not allowed on beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Half Moon Bay State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8570,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Half Moon Bay State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs not allowed on beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Humboldt Redwoods State Park: Yes — Except for service animals, dogs not allowed on trails. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Humboldt Redwoods State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Humboldt Redwoods State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9438,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Humboldt Redwoods State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Lake Del Valle State Recreation Area: Yes — $2 entry fee per dog. -> 2 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Lake Del Valle State Recreation Area',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       '$2 entry fee per dog.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Lake Del Valle State Recreation Area'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9431,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Lake Del Valle State Recreation Area'),
       'sand', 'on_leash', 'operative'::operative_status,
       '$2 entry fee per dog.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9432,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Lake Del Valle State Recreation Area'),
       'sand', 'on_leash', 'operative'::operative_status,
       '$2 entry fee per dog.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Leo Carrillo State Park: Yes — Dogs allowed in day-use areas, campground and North Beach (north of Lifeguard Tower 3). Dogs not allowed on backcountry trails or South Beach (south of Lifeguard Tower 3). -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Leo Carrillo State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed in day-use areas, campground and North Beach (north of Lifeguard Tower 3). Dogs not allowed on backcountry trails or South Beach (south of Lifeguard Tower 3).',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Leo Carrillo State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8595,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Leo Carrillo State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed in day-use areas, campground and North Beach (north of Lifeguard Tower 3). Dogs not allowed on backcountry trails or South Beach (south of Lifeguard Tower 3).',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Little River State Beach: Yes — Yes on waveslope only, on leash. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Little River State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes on waveslope only, on leash.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Little River State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8214,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Little River State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes on waveslope only, on leash.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- McLaughlin Eastshore State Park: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — McLaughlin Eastshore State Park State Seashore',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — McLaughlin Eastshore State Park State Seashore'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8659,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — McLaughlin Eastshore State Park State Seashore'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Mendocino Headlands State Park: Yes -> 2 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Mendocino Headlands State Park (incl. Ford House)',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Mendocino Headlands State Park (incl. Ford House)'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8952,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Mendocino Headlands State Park (incl. Ford House)'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9695,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Mendocino Headlands State Park (incl. Ford House)'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Montara State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Montara State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Montara State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 3310,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Montara State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Montaña de Oro State Park: Yes — Dogs are allowed only in the campground, roadways, and Spooner’s Cove Beach. Dogs are allowed only on leash in these permitted areas and are not allowed on trails. A word of caution: one of the predominant undergrowth plants in the area is poison oak and ticks are present which may carry Lyme disease. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Montaña de Oro State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs are allowed only in the campground, roadways, and Spooner’s Cove Beach. Dogs are allowed only on leash in these permitted areas and are not allowed on trails. A word of caution: one of the predominant undergrowth plants in the area is poison oak and ticks are present which may carry Lyme disease.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Montaña de Oro State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8774,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Montaña de Oro State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs are allowed only in the campground, roadways, and Spooner’s Cove Beach. Dogs are allowed only on leash in these permitted areas and are not allowed on trails. A word of caution: one of the predominant undergrowth plants in the area is poison oak and ticks are present which may carry Lyme disease.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Morro Strand State Beach: No — Dogs allowed only in Morro Strand Campground. Dogs not allowed on the beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Morro Strand State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed only in Morro Strand Campground. Dogs not allowed on the beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Morro Strand State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8231,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Morro Strand State Beach'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Dogs allowed only in Morro Strand Campground. Dogs not allowed on the beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Pacifica State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Pacifica State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Pacifica State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8607,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Pacifica State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Pescadero State Beach: No -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Pescadero State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Pescadero State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8457,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Pescadero State Beach'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Point Mugu State Park: Yes — Dogs allowed in campground and day-use areas. Dogs are not allowed on trails. -> 3 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Point Mugu State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed in campground and day-use areas. Dogs are not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Point Mugu State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 5996,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Point Mugu State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed in campground and day-use areas. Dogs are not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6171,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Point Mugu State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed in campground and day-use areas. Dogs are not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6271,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Point Mugu State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed in campground and day-use areas. Dogs are not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Point Sal State Beach: Yes — Dogs allowed on trail. Dogs not allowed on beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Point Sal State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed on trail. Dogs not allowed on beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Point Sal State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9041,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Point Sal State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on trail. Dogs not allowed on beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- San Elijo State Beach: Yes — Dogs allowed only in campground. Dogs not allowed on beach. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — San Elijo State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed only in campground. Dogs not allowed on beach.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — San Elijo State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8341,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — San Elijo State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed only in campground. Dogs not allowed on beach.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Schooner Gulch State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Schooner Gulch State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Schooner Gulch State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6365,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Schooner Gulch State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Sinkyone Wilderness State Park: Yes — Except for service animals, dogs not allowed on trails. -> 2 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Sinkyone Wilderness State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Sinkyone Wilderness State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8654,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sinkyone Wilderness State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8655,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sinkyone Wilderness State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Sonoma Coast State Park: Yes — Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan's Cover, Wright's Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds. -> 7 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Sonoma Coast State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Sonoma Coast State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8274,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8276,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8745,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8746,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8748,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8854,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8893,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Sonoma Coast State Park'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Dogs allowed on specified beaches: Marshall Gulch, Carmet Beach, Schoolhouse Beach, Portuguese Beach, Duncan''s Cover, Wright''s Beach, Furlong Gulch, Shell Beach, Blind Beach, Russian Gulch. Dogs not allowed in Pomo Canyon and Willow Creek environmental campgrounds.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Tolowa Dunes State Park: No — Except for service animals, dogs not allowed on trails. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Tolowa Dunes State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Tolowa Dunes State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 9244,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Tolowa Dunes State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Trinidad State Beach: Yes — Except for service animals, dogs not allowed on trails. -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Trinidad State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Except for service animals, dogs not allowed on trails.',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Trinidad State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8691,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Trinidad State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Except for service animals, dogs not allowed on trails.',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Westport-Union Landing State Beach: Yes -> 1 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Westport-Union Landing State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Westport-Union Landing State Beach'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 6425,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Westport-Union Landing State Beach'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Yes. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Wilder Ranch State Park: No -> 3 beach(es)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Wilder Ranch State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/Dogs',
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Wilder Ranch State Park'
);
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8443,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Wilder Ranch State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8585,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Wilder Ranch State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)
SELECT 8698,
       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy — Wilder Ranch State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]',
       'https://www.parks.ca.gov/Dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── Summary ─────────────────────────────────────────
-- Matched parks: 35
-- Matched beaches: 49
-- Unmatched parks (need fuzzy review): 1
-- Unmatched park_name values:
--   'Conservation Easement'
